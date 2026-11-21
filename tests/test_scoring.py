"""Scoring path with stand-in bundles that have the same structure as the real ones.

(The real XGBoost bundles are checked inside Docker by ops/verify_models.py.)
"""
import joblib
import numpy as np
import pandas as pd
import pytest
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression

from common.schemas import decode_event, to_json
from producer.replay import load_timeline, to_message
from scorer.logic import process
from scorer.models import StageModel
from tests.conftest import DATA_EVENTS, DATA_ORDERS

M1_FEATURES = ["promise_days", "purchase_hour", "price", "freight", "category", "customer_state", "dist_km",
               "seller_overrun_rate_prior", "seller_handling_days_prior", "route_recent_days",
               "promise_buffer_route", "promise_buffer_state"]
M3_FEATURES = ["promise_days", "weight_g", "customer_state", "dist_km", "route_recent_days", "promise_buffer_route",
               "promise_buffer_state", "handling_days", "seller_overrun_days", "post_approval_days"]


class FillNa(LogisticRegression):
    """LogisticRegression that tolerates NaN (XGBoost does this natively)."""

    def fit(self, X, y):
        return super().fit(np.nan_to_num(np.asarray(X, float)), y)

    def predict_proba(self, X):
        return super().predict_proba(np.nan_to_num(np.asarray(X, float)))


def make_bundle(tmp_path, name, features, rows, label, top_categories=None):
    df = pd.DataFrame(rows)
    codes = {"customer_state": {v: i for i, v in enumerate(sorted(df.customer_state.astype(str).unique()))}}
    cat = ["customer_state"]
    if "category" in features:
        cat = ["category", "customer_state"]
        codes["category"] = {v: i for i, v in enumerate(sorted(set(top_categories) | {"other"}))}
    b = {"features": features, "categorical": cat, "codes": codes, "top_categories": top_categories,
         "threshold": 0.6, "versions": {}}
    sm = StageModel.__new__(StageModel)
    sm.features, sm.categorical, sm.codes = features, cat, codes
    sm.top_categories = set(top_categories or [])
    X = sm.prepare(rows)
    m = FillNa(max_iter=500).fit(X, label)
    raw = m.predict_proba(X)[:, 1]
    b.update(model=m, calibrator=IsotonicRegression(out_of_bounds="clip").fit(raw, label))
    path = tmp_path / f"{name}.joblib"
    joblib.dump(b, path)
    return StageModel(str(path), name)


@pytest.fixture(scope="module")
def setup(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("models")
    tl, _ = load_timeline(DATA_ORDERS, DATA_EVENTS, "2018-06-01", "2018-06-04")
    msgs = [decode_event(to_json(to_message(r, "t"))) for r in tl.to_dict("records")]
    placed = [m for m in msgs if m["event_type"] == "order_placed"]
    hand = [m for m in msgs if m["event_type"] == "handed_to_carrier"]
    rng = np.random.default_rng(0)
    top = pd.Series([m["category"] for m in placed]).value_counts().head(20).index.tolist()
    m1 = make_bundle(tmp, "model1", M1_FEATURES, placed, rng.integers(0, 2, len(placed)), top)
    m3 = make_bundle(tmp, "model3", M3_FEATURES, hand, rng.integers(0, 2, len(hand)))
    return msgs, m1, m3


def test_every_event_gets_one_prediction(setup):
    msgs, m1, m3 = setup
    out = process(msgs, m1, m3, "test")
    assert len(out) == len(msgs)
    stages = pd.Series([p["stage"] for p, _ in out]).value_counts().to_dict()
    assert set(stages) == {"placed", "approved", "handoff", "delivered"}
    for (p, a), m in zip(out, msgs):
        assert p["order_id"] == m["order_id"] and p["event_time"] == m["event_time"]
        if p["stage"] == "placed":
            assert p["model"] == "model1" and 0 <= p["late_probability"] <= 1
        if p["stage"] == "handoff":
            assert p["model"] == "model3" and 0 <= p["late_probability"] <= 1
        if p["stage"] == "delivered":
            assert p["is_late_actual"] in (0, 1)
        if a is not None:
            assert p["alert"] and a["reasons"] and a["alert_id"] == f"order_risk:{p['order_id']}:{p['stage']}"


def test_r2_alone_raises_alert(setup):
    msgs, m1, m3 = setup
    for p, a in process(msgs, m1, m3, "test"):
        if p["stage"] == "handoff" and "R2_missed_ship_deadline" in p["reason_tags"]:
            assert a is not None
    # unseen state and missing numbers must not crash
    m = next(x for x in msgs if x["event_type"] == "handed_to_carrier").copy()
    m.update(customer_state="ZZ", route_recent_days=None, promise_buffer_route=None)
    (p, _), = process([m], m1, m3, "test")
    assert 0 <= p["late_probability"] <= 1
