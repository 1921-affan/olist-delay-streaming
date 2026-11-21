"""Message formats for the 4 topics and validation.

Pure Python (no Kafka imports) so it can be unit-tested anywhere.
"""
import json
import math
from datetime import datetime

STAGE1_FEATURES = [
    "promise_days", "purchase_hour", "purchase_weekday", "price", "freight", "n_items", "weight_g",
    "volume_cm3", "category", "customer_state", "seller_state", "same_state", "dist_km", "pay_type",
    "installments", "seller_prior_orders", "seller_late_rate_prior", "seller_overrun_rate_prior",
    "seller_handling_days_prior", "route_recent_days", "promise_buffer_route", "promise_buffer_state",
    "order_load_ratio",
]
HANDOFF_FEATURES = ["approval_lag_h", "handling_days", "seller_overrun_days", "post_approval_days"]

EVENT_TYPES = ("order_placed", "order_approved", "handed_to_carrier", "delivered")
STAGE_OF_EVENT = {"order_placed": "placed", "order_approved": "approved",
                  "handed_to_carrier": "handoff", "delivered": "delivered"}
STAGE_RANK = {"placed": 1, "approved": 2, "handoff": 3, "delivered": 4}

COMMON_REQUIRED = ["event_id", "event_type", "event_time", "order_id"]
REQUIRED_BY_TYPE = {
    # model features may be missing (NaN) — XGBoost handles that — so only identity fields are required
    "order_placed": ["seller_id", "customer_state", "estimated_delivery_date"],
    "order_approved": ["customer_state", "approval_lag_h"],
    "handed_to_carrier": ["customer_state", "handling_days", "seller_overrun_days", "post_approval_days"],
    "delivered": ["customer_state", "is_late_actual"],
}


class InvalidMessage(ValueError):
    """Raised with a short machine-readable reason."""

    def __init__(self, reason, detail=""):
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


def clean_value(v):
    """Turn pandas/numpy values into plain JSON values (NaN -> None)."""
    if v is None:
        return None
    if hasattr(v, "item"):          # numpy scalar
        v = v.item()
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def to_json(record):
    return json.dumps({k: clean_value(v) for k, v in record.items()}, separators=(",", ":")).encode("utf-8")


def parse_time(value):
    return datetime.fromisoformat(str(value).replace("Z", ""))


def decode_event(raw_bytes):
    """Bytes from Kafka -> validated dict. Raises InvalidMessage."""
    try:
        msg = json.loads(raw_bytes)
    except (ValueError, TypeError) as e:
        raise InvalidMessage("bad_json", str(e)[:200])
    if not isinstance(msg, dict):
        raise InvalidMessage("not_an_object")
    missing = [f for f in COMMON_REQUIRED if msg.get(f) in (None, "")]
    if missing:
        raise InvalidMessage("missing_fields", ",".join(missing))
    etype = msg["event_type"]
    if etype not in EVENT_TYPES:
        raise InvalidMessage("unknown_event_type", str(etype)[:50])
    missing = [f for f in REQUIRED_BY_TYPE[etype] if msg.get(f) is None]
    if missing:
        raise InvalidMessage("missing_fields", ",".join(missing))
    try:
        parse_time(msg["event_time"])
    except ValueError:
        raise InvalidMessage("bad_event_time", str(msg["event_time"])[:50])
    return msg


def decode_json(raw_bytes):
    """Lenient decode for downstream topics (predictions, alerts)."""
    try:
        msg = json.loads(raw_bytes)
    except (ValueError, TypeError) as e:
        raise InvalidMessage("bad_json", str(e)[:200])
    if not isinstance(msg, dict):
        raise InvalidMessage("not_an_object")
    return msg
