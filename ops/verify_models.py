"""Check that the scorer's feature preparation reproduces the Colab test scores exactly.

  docker compose run --rm --entrypoint python scorer-1 ops/verify_models.py

Model 1 is checked on order_placed rows of orders_stream.csv, Model 3 on handed_to_carrier
rows of shipment_events_enriched.csv, against the test-prediction files saved in Colab.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scorer.models import StageModel  # noqa: E402


def check(model, rows, ref_csv, label):
    ref = pd.read_csv(ref_csv)[["order_id", "raw_score"]]
    rows = rows.merge(ref, on="order_id")
    recs = rows.drop(columns=["raw_score"]).to_dict("records")
    raw, prob, alert = model.score(recs)
    diff = np.abs(raw - rows.raw_score.values)
    ok = diff.max() < 1e-5
    print(f"{label}: {len(rows):,} orders compared | max |difference| {diff.max():.2e} | "
          f"alerts {alert.mean():.1%} | mean probability {prob.mean():.3f} -> {'OK' if ok else 'MISMATCH'}")
    for p in model.check_versions():
        print(f"  version warning: {p}")
    return ok


if __name__ == "__main__":
    m1 = StageModel(os.environ["MODEL1_PATH"], "model1")
    m3 = StageModel(os.environ["MODEL3_PATH"], "model3")
    orders = pd.read_csv(os.environ["DATA_ORDERS"])
    events = pd.read_csv(os.environ["DATA_EVENTS"])
    handoffs = events[(events.event_type == "handed_to_carrier") & (events.bad_timestamp == 0)]
    a = check(m1, orders, "/app/models/model1_production_test_predictions.csv", "Model 1")
    b = check(m3, handoffs, "/app/models/model3_production_test_predictions.csv", "Model 3")
    sys.exit(0 if a and b else 1)
