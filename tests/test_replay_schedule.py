import pandas as pd
import pytest

from common.schemas import decode_event, to_json
from producer.replay import due_seconds, load_timeline, to_message
from tests.conftest import DATA_EVENTS, DATA_ORDERS


@pytest.fixture(scope="module")
def timeline():
    return load_timeline(DATA_ORDERS, DATA_EVENTS)


def test_timeline_sorted_and_complete(timeline):
    tl, bad = timeline
    assert tl.event_time.is_monotonic_increasing
    assert len(bad) == 154 and (bad.event_type == "handed_to_carrier").all()
    counts = tl.event_type.value_counts().to_dict()
    assert counts["order_placed"] == 25314
    assert counts["order_approved"] == 25314
    assert counts["handed_to_carrier"] == 25138 - 154
    assert counts["delivered"] == 24928


def test_each_order_placed_first_delivered_last(timeline):
    tl, _ = timeline
    p = tl.pivot_table(index="order_id", columns="event_type", values="event_time", aggfunc="first")
    assert (p.order_approved >= p.order_placed).all()
    h = p.dropna(subset=["handed_to_carrier"])
    assert (h.handed_to_carrier >= h.order_placed).all()
    d = p.dropna(subset=["delivered", "handed_to_carrier"])   # 154 orders lost their bad-date handoff event
    assert (d.delivered >= d.handed_to_carrier).all()
    # Olist quirk: ~2.7% of orders are approved AFTER the carrier handoff. The scorer is stateless per
    # event and order_status keeps the furthest stage, so this is harmless; keep it visible here.
    assert (p.order_approved > p.handed_to_carrier).sum() == 675


def test_no_valid_order_is_rejected(timeline):
    tl, _ = timeline
    for r in tl.to_dict("records"):
        decode_event(to_json(to_message(r, "t")))      # raises if a real event would be rejected


def test_one_day_per_minute_timing():
    assert due_seconds("2018-05-02 00:00:00", "2018-05-01", 1440) == pytest.approx(60.0)
    assert due_seconds("2018-05-03 12:00:00", "2018-05-01", 1440) == pytest.approx(150.0)


def test_start_end_filter():
    tl, _ = load_timeline(DATA_ORDERS, DATA_EVENTS, "2018-06-01", "2018-06-03")
    assert tl.event_time.min() >= pd.Timestamp("2018-06-01")
    assert tl.event_time.max() < pd.Timestamp("2018-06-03")


def test_messages_carry_only_stage_information(timeline):
    tl, _ = timeline
    rows = {t: tl[tl.event_type == t].iloc[0].to_dict() for t in tl.event_type.unique()}
    placed = to_message(rows["order_placed"], "r1")
    approved = to_message(rows["order_approved"], "r1")
    handoff = to_message(rows["handed_to_carrier"], "r1")
    delivered = to_message(rows["delivered"], "r1")
    assert "handling_days" not in placed and "is_late_actual" not in placed
    assert "handling_days" not in approved and approved["approval_lag_h"] is not None
    assert handoff["handling_days"] is not None and handoff["customer_state"]
    assert delivered["is_late_actual"] in (0, 1, 0.0, 1.0)
    for m in (placed, approved, handoff, delivered):          # round trip through Kafka bytes
        assert decode_event(to_json(m))["order_id"] == m["order_id"]
