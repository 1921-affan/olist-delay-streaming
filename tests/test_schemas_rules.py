import pytest

from common.schemas import InvalidMessage, decode_event
from scorer.rules import approved_rules, handoff_rules, placed_rules


@pytest.mark.parametrize("raw,reason", [
    (b"{not json", "bad_json"),
    (b"[1,2]", "not_an_object"),
    (b'{"event_type":"order_placed","order_id":"x"}', "missing_fields"),
    (b'{"event_id":"1","event_type":"teleported","event_time":"2018-06-01T10:00:00","order_id":"x"}',
     "unknown_event_type"),
    (b'{"event_id":"1","event_type":"delivered","event_time":"nope","order_id":"x","customer_state":"SP",'
     b'"is_late_actual":1}', "bad_event_time"),
])
def test_bad_messages_rejected_with_reason(raw, reason):
    with pytest.raises(InvalidMessage) as e:
        decode_event(raw)
    assert e.value.reason == reason


def test_rules_edges():
    assert placed_rules({"promise_days": 10, "dist_km": 1000.1, "seller_late_rate_prior": 0.16,
                         "seller_prior_orders": 20}) == ["R5_tight_promise", "R6_long_route", "R7_unreliable_seller"]
    assert placed_rules({"promise_days": 10.01, "dist_km": 1000, "seller_late_rate_prior": 0.5,
                         "seller_prior_orders": 19}) == []
    assert approved_rules({"approval_lag_h": 24.5}) == ["R1_slow_approval"]
    assert approved_rules({"approval_lag_h": 24}) == []
    assert handoff_rules({"seller_overrun_days": 0.01}) == ["R2_missed_ship_deadline"]
    assert handoff_rules({"seller_overrun_days": -3}) == []
    assert placed_rules({}) == [] and handoff_rules({"seller_overrun_days": None}) == []
