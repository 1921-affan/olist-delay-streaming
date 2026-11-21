"""Replay timeline logic (pure pandas — no Kafka), so it can be unit-tested.

One merged timeline of all events sorted by their 2018 time. Each row becomes one
Kafka message that carries only what was known at that moment.
"""
import hashlib

import numpy as np
import pandas as pd

from common.schemas import HANDOFF_FEATURES, STAGE1_FEATURES

TYPE_ORDER = {"order_placed": 0, "order_approved": 1, "handed_to_carrier": 2, "delivered": 3}

ORDER_FIELDS = ["seller_id", "customer_zip_prefix", "estimated_delivery_date", "shipping_limit_ts"] + STAGE1_FEATURES
COMMON_EVENT_FIELDS = ["seller_id", "customer_state", "estimated_delivery_date"]
FIELDS_BY_TYPE = {
    "order_placed": ORDER_FIELDS,
    "order_approved": COMMON_EVENT_FIELDS + ["approval_lag_h"],
    "handed_to_carrier": COMMON_EVENT_FIELDS + HANDOFF_FEATURES + ["shipping_limit_ts"]
                         + [c for c in STAGE1_FEATURES if c != "customer_state"],
    "delivered": COMMON_EVENT_FIELDS + ["is_late_actual"],
}


def load_timeline(orders_csv, events_csv, start=None, end=None):
    """Returns (timeline DataFrame, rejected DataFrame of bad-date events)."""
    orders = pd.read_csv(orders_csv)
    orders["event_type"] = "order_placed"
    events = pd.read_csv(events_csv)

    bad = events[events.get("bad_timestamp", 0) == 1].copy()
    events = events[events.get("bad_timestamp", 0) != 1].drop(columns=["bad_timestamp"], errors="ignore")

    tl = pd.concat([orders, events], ignore_index=True, sort=False)
    tl["event_time"] = pd.to_datetime(tl["event_time"])
    tl["_order"] = tl["event_type"].map(TYPE_ORDER)
    tl = tl.sort_values(["event_time", "_order", "order_id"], kind="mergesort").reset_index(drop=True)
    if start is not None:
        tl = tl[tl.event_time >= pd.Timestamp(start)]
    if end is not None:
        tl = tl[tl.event_time < pd.Timestamp(end)]
    return tl.drop(columns="_order").reset_index(drop=True), bad


def event_id(order_id, event_type):
    return hashlib.md5(f"{order_id}|{event_type}".encode()).hexdigest()[:16]


def _fmt_time(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    return str(v).replace(" ", "T")


def to_message(row, run_id):
    """One timeline row (dict) -> message dict for its topic."""
    etype = row["event_type"]
    msg = {
        "event_id": event_id(row["order_id"], etype),
        "event_type": etype,
        "event_time": _fmt_time(row["event_time"]),
        "order_id": row["order_id"],
        "run_id": run_id,
    }
    for f in FIELDS_BY_TYPE[etype]:
        v = row.get(f)
        if f in ("estimated_delivery_date", "shipping_limit_ts"):
            v = _fmt_time(v)
        msg[f] = v
    return msg


def due_seconds(event_time, sim_start, speed):
    """Real seconds after replay start at which this event must be sent."""
    return (pd.Timestamp(event_time) - pd.Timestamp(sim_start)).total_seconds() / float(speed)
