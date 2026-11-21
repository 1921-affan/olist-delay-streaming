"""
Checks that the three pipeline files are consistent and leak-free.
Run after build_datasets.py:   python check_datasets.py --dir .
Exit code 0 = all checks passed.
"""
import argparse, json, os, sys
import numpy as np
import pandas as pd

ap = argparse.ArgumentParser(); ap.add_argument("--dir", default="."); ap.add_argument("--master", default=None, help="optional: olist_master_orders.csv, enables the order_load_ratio re-computation"); a = ap.parse_args()
P = lambda f: os.path.join(a.dir, f)
reg = json.load(open(P("feature_registry.json")))
clean = pd.read_csv(P("orders_clean.csv"), parse_dates=["purchase_ts", "approved_ts", "carrier_ts", "delivered_ts", "estimated_delivery_date", "shipping_limit_ts"])
stream = pd.read_csv(P("orders_stream.csv"), parse_dates=["event_time", "estimated_delivery_date", "shipping_limit_ts"])
ev = pd.read_csv(P("shipment_events.csv"), parse_dates=["event_time", "estimated_delivery_date"])

failed = []


def check(name, cond, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + (f"   {detail}" if detail else ""))
    if not cond:
        failed.append(name)


S1, S2, S3 = reg["stage1_features"], reg["stage2_extra_features"], reg["stage3_extra_features"]
M1, M3 = reg["models"]["model_1"]["features"], reg["models"]["model_3"]["features"]
FORBIDDEN = {"is_late", "delivered_ts", "carrier_ts", "approved_ts", "estimated_delivery_date", "shipping_limit_ts",
             "order_id", "seller_id", "customer_zip_prefix", "purchase_ts", "split", "approved_before_handoff"}

print("--- feature registry")
check("every registry column exists in orders_clean.csv", set(M3 + ["is_late"]) <= set(clean.columns))
check("Model 1 uses only checkout-time columns (23)", M1 == S1 and len(M1) == 23, f"n={len(M1)}")
check("Model 3 = Model 1 + approval + handoff columns", M3 == S1 + S2 + S3, f"n={len(M3)}")
check("no model uses an id, timestamp, split or the label", not (set(M1) | set(M3)) & FORBIDDEN, str((set(M1) | set(M3)) & FORBIDDEN))

print("--- orders_clean.csv")
check("order_id is unique", clean.order_id.is_unique)
check("label has no missing values and is 0/1", clean.is_late.isin([0, 1]).all())
recomputed = (clean.delivered_ts.dt.normalize() > clean.estimated_delivery_date.dt.normalize()).astype(int)
check("is_late equals 'delivered DATE after promised DATE'", (recomputed == clean.is_late).all())
check("late rate overall is realistic (4-9%)", 0.04 < clean.is_late.mean() < 0.09, f"{clean.is_late.mean() * 100:.2f}%")
sp = clean.groupby("split").purchase_ts.agg(["min", "max"])
check("splits are chronological and do not overlap",
      sp.loc["train", "max"] < sp.loc["gap", "min"] <= sp.loc["gap", "max"] < sp.loc["valid", "min"] and sp.loc["valid", "max"] < sp.loc["test", "min"],
      str({k: (str(v["min"].date()), str(v["max"].date())) for k, v in sp.iterrows()}))
check("no multi-seller or undelivered rows", clean.delivered_ts.notna().all() and clean.carrier_ts.notna().all())
check("stage-1 columns have no future information (no value requires a later timestamp)",
      (clean.promise_days > 0).all() and (clean.purchase_hour.between(0, 23)).all())

print("--- seller history is point-in-time (brute force re-computation on 150 random orders)")
pool = clean[(clean.is_late.notna())]
rng = np.random.default_rng(1)
bad = 0
for _, r in clean.sample(150, random_state=1).iterrows():
    prior = clean[(clean.seller_id == r.seller_id) & (clean.delivered_ts < r.purchase_ts)]
    # pool in the builder is a superset of clean (it also contains 2016 and gap-period orders), so counts can only be >=
    if len(prior) > r.seller_prior_orders:
        bad += 1
check("seller_prior_orders never counts an order delivered after this order was placed", bad == 0, f"violations={bad}")
check("seller history of an order never includes itself", (clean.seller_prior_orders >= 0).all())


print("--- route / load columns (the 4 new columns)")
NEW = ["route_recent_days", "promise_buffer_route", "promise_buffer_state", "order_load_ratio"]
check("the 4 new columns exist in training file and stream file", set(NEW) <= set(clean.columns) and set(NEW) <= set(stream.columns))
check("route columns are mostly filled (>=85% route, >=90% state)", clean.route_recent_days.notna().mean() >= 0.85 and clean.promise_buffer_state.notna().mean() >= 0.90,
      f"route {clean.route_recent_days.notna().mean() * 100:.1f}%, state {clean.promise_buffer_state.notna().mean() * 100:.1f}%")
ok_ = clean.route_recent_days.notna()
check("promise_buffer_route = promise_days - route_recent_days exactly", np.allclose(clean.promise_buffer_route[ok_], (clean.promise_days - clean.route_recent_days)[ok_]))
# brute-force re-computation: use ONLY orders delivered before the order was placed, in the 60 days before
sub = clean[clean.purchase_ts >= "2017-06-01"].sample(150, random_state=2)
bad_r = 0
for _, r in sub.iterrows():
    w = clean[(clean.seller_state + ">" + clean.customer_state == r.seller_state + ">" + r.customer_state)
              & (clean.delivered_ts < r.purchase_ts) & (clean.delivered_ts >= r.purchase_ts - pd.Timedelta(days=60))]
    exp = ((w.delivered_ts - w.purchase_ts).dt.total_seconds() / 86400).mean() if len(w) >= 15 else np.nan
    if not (np.isnan(exp) and np.isnan(r.route_recent_days)) and not np.isclose(exp, r.route_recent_days, atol=1e-6):
        bad_r += 1
check("route_recent_days uses only orders delivered before the order was placed (150 orders re-computed)", bad_r == 0, f"mismatches={bad_r}")
if a.master:
    allp = pd.read_csv(a.master, usecols=["order_purchase_timestamp"], parse_dates=["order_purchase_timestamp"]).order_purchase_timestamp
    bad_l = 0
    for _, r in clean.sample(150, random_state=3).iterrows():
        l7 = ((allp < r.purchase_ts) & (allp >= r.purchase_ts - pd.Timedelta(days=7))).sum()
        p28 = ((allp < r.purchase_ts - pd.Timedelta(days=7)) & (allp >= r.purchase_ts - pd.Timedelta(days=35))).sum()
        if not np.isclose(l7 / max(p28 / 4.0, 1.0), r.order_load_ratio, atol=1e-6):
            bad_l += 1
    check("order_load_ratio counts only orders placed before this one (150 orders re-computed from the master)", bad_l == 0, f"mismatches={bad_l}")

print("--- orders_stream.csv")
leak_cols = set(stream.columns) & {"is_late", "delivered_ts", "carrier_ts", "approved_ts", "approval_lag_h", "handling_days", "seller_overrun_days", "post_approval_days", "order_status"}
check("stream has no label and no later-stage columns", not leak_cols, str(leak_cols))
check("stream stage-1 columns are exactly the registry's stage-1 list", [c for c in stream.columns if c in S1] == S1)
check("stream is sorted by event_time", stream.event_time.is_monotonic_increasing)
check("stream order_id is unique", stream.order_id.is_unique)
test = clean[clean.split == "test"]
check("every labelled test order is also in the stream", set(test.order_id) <= set(stream.order_id), f"{len(test)} of {len(stream)}")
both = test.merge(stream, on="order_id", suffixes=("", "_s"))
same = all(np.allclose(both[c].astype(float), both[c + "_s"].astype(float), equal_nan=True) for c in ["promise_days", "price", "freight", "dist_km", "seller_late_rate_prior", "seller_prior_orders"] + NEW)
check("same feature values in training file and stream file for shared orders", same)
check("stream orders are all in the test period", stream.event_time.min() >= pd.Timestamp("2018-05-01") and stream.event_time.max() < pd.Timestamp("2018-09-01"))

print("--- shipment_events.csv")
check("events are sorted by time", ev.event_time.is_monotonic_increasing)
check("event types are the three expected", set(ev.event_type) == {"order_approved", "handed_to_carrier", "delivered"})
check("every event belongs to a streamed order", ev.order_id.isin(stream.order_id).all())
dl = ev[ev.event_type == "delivered"].merge(test[["order_id", "is_late"]], on="order_id")
check("delivered events carry the same truth as the training label", (dl.is_late_actual == dl.is_late).all(), f"{len(dl)} orders compared")
check("only delivered events carry is_late_actual", ev[ev.event_type != "delivered"].is_late_actual.isna().all())
oo = ev.merge(stream[["order_id", "event_time"]].rename(columns={"event_time": "placed"}), on="order_id")
check("no event happens before its order was placed (out-of-order events are only among later events)", (oo.event_time >= oo.placed).mean() > 0.99, f"{(oo.event_time < oo.placed).sum()} events earlier than purchase")
hb = (ev.groupby(["order_id", "event_type"]).event_time.first().unstack())
n_ooo = int((hb.handed_to_carrier < hb.order_approved).sum())
print(f"INFO  {n_ooo} orders have handoff recorded before approval (real data quirk; the scorer must tolerate it)")

print()
if failed:
    print(f"{len(failed)} CHECK(S) FAILED: {failed}"); sys.exit(1)
print("ALL CHECKS PASSED")
