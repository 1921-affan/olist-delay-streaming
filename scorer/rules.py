"""Operational rules R1–R7 evaluated on a single event (pure Python).

R8 (region surge) lives in the window aggregator; R9 (high model risk) is the model's alert flag.
"""
import os


def _f(name, default):
    return float(os.environ.get(name, default))


R1_APPROVAL_HOURS = _f("R1_APPROVAL_HOURS", 24) 
R5_PROMISE_DAYS = _f("R5_PROMISE_DAYS", 10) 
R6_DISTANCE_KM = _f("R6_DISTANCE_KM", 1000)
R7_SELLER_LATE_RATE = _f("R7_SELLER_LATE_RATE", 0.15)
R7_SELLER_MIN_ORDERS = _f("R7_SELLER_MIN_ORDERS", 20)


def _num(msg, key):
    v = msg.get(key)
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def placed_rules(msg):
    tags = []
    pd_ = _num(msg, "promise_days")
    if pd_ is not None and pd_ <= R5_PROMISE_DAYS:
        tags.append("R5_tight_promise")
    km = _num(msg, "dist_km")
    if km is not None and km > R6_DISTANCE_KM:
        tags.append("R6_long_route")
    rate, n = _num(msg, "seller_late_rate_prior"), _num(msg, "seller_prior_orders")
    if rate is not None and n is not None and rate > R7_SELLER_LATE_RATE and n >= R7_SELLER_MIN_ORDERS:
        tags.append("R7_unreliable_seller")
    return tags


def approved_rules(msg):
    h = _num(msg, "approval_lag_h")
    return ["R1_slow_approval"] if h is not None and h > R1_APPROVAL_HOURS else []


def handoff_rules(msg):
    d = _num(msg, "seller_overrun_days")
    return ["R2_missed_ship_deadline"] if d is not None and d > 0 else []
