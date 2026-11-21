"""
Build the three pipeline files from olist_master_orders.csv

  orders_clean.csv     real, labelled orders for TRAINING / VALIDATION / TESTING (never goes into Kafka)
                       Model 1 uses 23 checkout-time columns: 15 order basics, 4 seller-history, 4 route/load.
  orders_stream.csv    new orders for the Kafka replay: checkout information only, NO label
  shipment_events.csv  later events (approved, handed to carrier, delivered), sorted by time
  feature_registry.json  which columns each model may use (guards against leakage)

Every row is a real Olist order. Nothing is edited or synthesised. Balancing (if any) is done inside the
training code, not in these files.

Usage:  python build_datasets.py --master olist_master_orders.csv --out .
"""
import argparse, json, os
import numpy as np
import pandas as pd

# ------------------------------------------------------------------ settings
TRAIN = ("2017-01-01", "2018-01-01")
GAP = ("2018-01-01", "2018-02-01")           # one-month gap so train labels cannot leak into validation
VALID = ("2018-02-01", "2018-05-01")
TEST = ("2018-05-01", "2018-09-01")           # also the period replayed through Kafka
SMOOTH, BASE_LATE, BASE_OVER = 10, 0.07, 0.08  # smoothing for seller-history features

RENAME = {
    "order_purchase_timestamp": "purchase_ts", "order_approved_at": "approved_ts",
    "order_delivered_carrier_date": "carrier_ts", "order_delivered_customer_date": "delivered_ts",
    "order_estimated_delivery_date": "estimated_delivery_date", "ship_limit": "shipping_limit_ts",
    "customer_zip_code_prefix": "customer_zip_prefix", "promise_d": "promise_days", "p_hour": "purchase_hour",
    "p_dow": "purchase_weekday", "vol_cm3": "volume_cm3", "appr_h": "approval_lag_h", "handling_d": "handling_days",
    "seller_overrun_d": "seller_overrun_days", "post_appr_d": "post_approval_days", "late": "is_late",
}
STAGE1_BASE = ["promise_days", "purchase_hour", "purchase_weekday", "price", "freight", "n_items", "weight_g", "volume_cm3",
               "category", "customer_state", "seller_state", "same_state", "dist_km", "pay_type", "installments"]
SELLER_HIST = ["seller_prior_orders", "seller_late_rate_prior", "seller_overrun_rate_prior", "seller_handling_days_prior"]
NEW_FEATS = ["route_recent_days", "promise_buffer_route", "promise_buffer_state", "order_load_ratio"]
STAGE1 = STAGE1_BASE + SELLER_HIST + NEW_FEATS                       # 23 columns, known when the order is placed
STAGE2 = ["approval_lag_h"]                                          # known at approval (used by rule R1, no model)
STAGE3 = ["handling_days", "seller_overrun_days", "post_approval_days"]   # known at carrier handoff
META = ["order_id", "seller_id", "customer_zip_prefix", "purchase_ts", "approved_ts", "carrier_ts", "delivered_ts",
        "estimated_delivery_date", "shipping_limit_ts", "approved_before_handoff", "split"]


def to_ns(s):
    return s.values.astype("datetime64[ns]").astype("int64")


def seller_history(targets, pool):
    """Point-in-time seller record: for each target order use ONLY the seller's pool orders that were
    delivered BEFORE the target order was placed. Returns the 4 SELLER_HIST columns."""
    out = pd.DataFrame(index=targets.index, columns=SELLER_HIST, dtype=float)
    out[:] = np.nan
    pools = {sid: g.sort_values("delivered_ts") for sid, g in pool.groupby("seller_id")}
    for sid, tg in targets.groupby("seller_id"):
        g = pools.get(sid)
        if g is None:
            k = np.zeros(len(tg), dtype=int)
            cl = co = ch = np.zeros(1)
        else:
            dts = to_ns(g.delivered_ts)
            k = np.searchsorted(dts, to_ns(tg.purchase_ts), side="left")      # prior orders already delivered
            cl = np.concatenate([[0], np.cumsum(g.is_late.values)])
            co = np.concatenate([[0], np.cumsum((g.seller_overrun_days > 0).astype(float).values)])
            ch = np.concatenate([[0], np.cumsum(g.handling_days.values)])
        out.loc[tg.index, "seller_prior_orders"] = k
        out.loc[tg.index, "seller_late_rate_prior"] = (cl[k] + SMOOTH * BASE_LATE) / (k + SMOOTH)
        out.loc[tg.index, "seller_overrun_rate_prior"] = (co[k] + SMOOTH * BASE_OVER) / (k + SMOOTH)
        out.loc[tg.index, "seller_handling_days_prior"] = np.where(k > 0, ch[k] / np.maximum(k, 1), np.nan)
    return out


def recent_delivery_days(targets, pool, key, window_days, min_n):
    """Mean REAL delivery time (days) of pool orders in the same group (`key` column) that were DELIVERED in the
    `window_days` before the target order was placed. Point-in-time: later deliveries are never used.
    Returns NaN when fewer than min_n such orders exist."""
    win = int(window_days * 86400e9)
    out = pd.Series(np.nan, index=targets.index)
    pools = {k: g.sort_values("delivered_ts") for k, g in pool.groupby(key)}
    for k, tg in targets.groupby(key):
        g = pools.get(k)
        if g is None:
            continue
        d = to_ns(g.delivered_ts)
        cs = np.concatenate([[0], np.cumsum(g.actual_days.values)])
        p = to_ns(tg.purchase_ts)
        hi = np.searchsorted(d, p, side="left")                    # delivered before this order was placed
        lo = np.searchsorted(d, p - win, side="left")               # ...and not longer ago than the window
        n = hi - lo
        out.loc[tg.index] = np.where(n >= min_n, (cs[hi] - cs[lo]) / np.maximum(n, 1), np.nan)
    return out


def order_load_ratio(targets, all_purchase_ts):
    """Orders placed in the 7 days before this order, divided by the weekly average of the 4 weeks before that.
    Uses every order of the platform (any status), counting only orders placed strictly earlier."""
    p = np.sort(to_ns(all_purchase_ts))
    t = to_ns(targets.purchase_ts)
    d7, d35 = int(7 * 86400e9), int(35 * 86400e9)
    last7 = np.searchsorted(p, t, side="left") - np.searchsorted(p, t - d7, side="left")
    prev28 = np.searchsorted(p, t - d7, side="left") - np.searchsorted(p, t - d35, side="left")
    return pd.Series(last7 / np.maximum(prev28 / 4.0, 1.0), index=targets.index)


def route_features(frame, pool, all_purchase_ts):
    """The 4 route / load columns. 'Route' = seller state > customer state."""
    f = frame.copy()
    f["route"] = f.seller_state.astype(str) + ">" + f.customer_state.astype(str)
    f["route_recent_days"] = recent_delivery_days(f, pool, "route", 60, 15)
    state_days = recent_delivery_days(f, pool, "customer_state", 60, 30)
    f["promise_buffer_route"] = f.promise_days - f.route_recent_days
    f["promise_buffer_state"] = f.promise_days - state_days
    f["order_load_ratio"] = order_load_ratio(f, all_purchase_ts)
    return f[NEW_FEATS]


def main(master, out_dir):
    date_cols = ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                 "order_delivered_customer_date", "order_estimated_delivery_date", "ship_limit"]
    m = pd.read_csv(master, parse_dates=date_cols).rename(columns=RENAME)
    m["category"] = m.category_en.fillna(m.category).fillna("unknown")
    m["pay_type"] = m.pay_type.fillna("unknown")
    m.loc[m.weight_g == 0, "weight_g"] = np.nan                                  # weight 0 means missing
    m["approved_before_handoff"] = (m.approved_ts <= m.carrier_ts).astype(float)
    m.loc[m.approved_ts.isna() | m.carrier_ts.isna(), "approved_before_handoff"] = np.nan

    # ---- history pool: delivered, single-seller orders with a handoff date (point-in-time source for seller features)
    pool = m[m.delivered_ts.notna() & (m.order_status == "delivered") & (m.n_sellers == 1) & m.carrier_ts.notna() & m.is_late.notna()]

    pool = pool.copy()
    pool["actual_days"] = (pool.delivered_ts - pool.purchase_ts).dt.total_seconds() / 86400
    pool["route"] = pool.seller_state.astype(str) + ">" + pool.customer_state.astype(str)
    all_ts = m.purchase_ts                                                       # every order of the platform (for the load ratio)

    in_range = lambda s, r: (s >= r[0]) & (s < r[1])
    # ================================================= orders_clean.csv  (labelled, real)
    c = m[(m.order_status == "delivered") & m.delivered_ts.notna() & (m.n_sellers == 1) & m.carrier_ts.notna()
          & m.n_items.notna() & m.is_late.notna() & (m.purchase_ts >= TRAIN[0]) & (m.purchase_ts < TEST[1])].copy()
    c["split"] = np.select([in_range(c.purchase_ts, TRAIN), in_range(c.purchase_ts, GAP), in_range(c.purchase_ts, VALID), in_range(c.purchase_ts, TEST)],
                           ["train", "gap", "valid", "test"], default="other")
    c = pd.concat([c, seller_history(c, pool), route_features(c, pool, all_ts)], axis=1).sort_values("purchase_ts")
    c["is_late"] = c.is_late.astype(int)
    clean_cols = META + STAGE1 + STAGE2 + STAGE3 + ["is_late"]
    c[clean_cols].to_csv(os.path.join(out_dir, "orders_clean.csv"), index=False, date_format="%Y-%m-%d %H:%M:%S")

    # ================================================= orders_stream.csv  (replay period, no label, checkout info only)
    s = m[(m.n_sellers == 1) & m.n_items.notna() & in_range(m.purchase_ts, TEST)].copy()
    s = pd.concat([s, seller_history(s, pool), route_features(s, pool, all_ts)], axis=1).sort_values(["purchase_ts", "order_id"])
    s["event_time"] = s.purchase_ts
    stream_cols = ["event_time", "order_id", "seller_id", "customer_zip_prefix", "estimated_delivery_date", "shipping_limit_ts"] + STAGE1
    s[stream_cols].to_csv(os.path.join(out_dir, "orders_stream.csv"), index=False, date_format="%Y-%m-%d %H:%M:%S")

    # ================================================= shipment_events.csv  (later events of the same orders)
    ev = []
    for col, name in [("approved_ts", "order_approved"), ("carrier_ts", "handed_to_carrier"), ("delivered_ts", "delivered")]:
        keep = ["order_id", "seller_id", "customer_state", "estimated_delivery_date", "delivered_ts"] + ([col] if col != "delivered_ts" else [])
        e = s[s[col].notna()][keep].copy()
        e["event_time"] = e[col]; e["event_type"] = name
        e["is_late_actual"] = np.where(name == "delivered", (e.delivered_ts.dt.normalize() > e.estimated_delivery_date.dt.normalize()).astype(float), np.nan)
        ev.append(e[["event_time", "order_id", "seller_id", "event_type", "customer_state", "estimated_delivery_date", "is_late_actual"]])
    ev = pd.concat(ev)
    ev["_o"] = ev.event_type.map({"order_approved": 0, "handed_to_carrier": 1, "delivered": 2})
    ev = ev.sort_values(["event_time", "_o", "order_id"]).drop(columns="_o")
    ev.to_csv(os.path.join(out_dir, "shipment_events.csv"), index=False, date_format="%Y-%m-%d %H:%M:%S")

    # ================================================= feature registry
    registry = {
        "label": "is_late",
        "label_definition": "1 if the delivered calendar date is after estimated_delivery_date, else 0",
        "prediction_unit": "one row per order (single-seller, delivered) for training; replay uses all test-period orders",
        "stage1_features": STAGE1, "stage2_extra_features": STAGE2, "stage3_extra_features": STAGE3,
        "categorical_features": ["category", "customer_state", "seller_state", "pay_type"],
        "models": {
            "model_1": {"scored_at": "order placed", "features": STAGE1},
            "model_3": {"scored_at": "handed to carrier", "features": STAGE1 + STAGE2 + STAGE3},
        },
        "feature_groups": {"order_basics": STAGE1_BASE, "seller_history": SELLER_HIST, "route_and_load": NEW_FEATS},
        "approval_stage": "no model: the scorer keeps the Model 1 score and attaches rule R1 (approval_lag_h > 24) as a reason tag",
        "meta_columns_never_features": META + ["is_late"],
        "splits": {"train": TRAIN, "gap": GAP, "valid": VALID, "test": TEST},
        "balancing": "done inside the training code (class weights or undersampling of train rows only); files contain only real rows",
    }
    json.dump(registry, open(os.path.join(out_dir, "feature_registry.json"), "w"), indent=2)

    print("orders_clean.csv    :", len(c), "rows;", c.split.value_counts().to_dict())
    print("   late rate by split:", (c.groupby("split").is_late.mean() * 100).round(1).to_dict())
    print("orders_stream.csv   :", len(s), "rows (replay period, no label)")
    print("new columns filled  :", (c[NEW_FEATS].notna().mean() * 100).round(1).to_dict(), "(% of orders_clean rows)")
    print("shipment_events.csv :", len(ev), "events;", ev.event_type.value_counts().to_dict())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--master", default="olist_master_orders.csv")
    ap.add_argument("--out", default=".")
    a = ap.parse_args()
    main(a.master, a.out)
