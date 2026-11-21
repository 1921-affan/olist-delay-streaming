"""Turn validated input events into prediction (and alert) records. Pure logic, testable."""
from datetime import datetime, timezone

from common.schemas import STAGE_OF_EVENT
from scorer.rules import approved_rules, handoff_rules, placed_rules


def _now():
    return datetime.now(timezone.utc).isoformat()


def _base(msg, stage, instance):
    return {
        "prediction_id": f"{msg['order_id']}:{stage}",
        "order_id": msg["order_id"],
        "stage": stage,
        "event_time": msg["event_time"],
        "customer_state": msg.get("customer_state"),
        "seller_id": msg.get("seller_id"),
        "estimated_delivery_date": msg.get("estimated_delivery_date"),
        "model": "none", "raw_score": None, "late_probability": None, "alert": False, "threshold": None,
        "reason_tags": [], "is_late_actual": None,
        "scored_at": _now(), "scorer_instance": instance,
    }


def _alert(pred, reasons, severity):
    return {
        "alert_id": f"order_risk:{pred['order_id']}:{pred['stage']}",
        "alert_type": "order_risk",
        "order_id": pred["order_id"],
        "stage": pred["stage"],
        "severity": severity,
        "score": pred["late_probability"],
        "reasons": reasons,
        "customer_state": pred["customer_state"],
        "seller_id": pred["seller_id"],
        "event_time": pred["event_time"],
        "created_at": _now(),
        "source": pred["scorer_instance"],
    }


def process(msgs, model1, model3, instance):
    """msgs: validated event dicts. Returns list of (prediction, alert_or_None) in input order."""
    out = [None] * len(msgs)

    # score in batches per model (much faster than row by row)
    for etype, model, stage, rules in [("order_placed", model1, "placed", placed_rules),
                                       ("handed_to_carrier", model3, "handoff", handoff_rules)]:
        idx = [i for i, m in enumerate(msgs) if m["event_type"] == etype]
        if not idx:
            continue
        rows = [msgs[i] for i in idx]
        raw, prob, flag = model.score(rows)
        for j, i in enumerate(idx):
            m = msgs[i]
            p = _base(m, stage, instance)
            tags = rules(m)
            p.update(model=model.name, raw_score=float(raw[j]), late_probability=float(prob[j]),
                     alert=bool(flag[j]), threshold=model.threshold, reason_tags=tags)
            alert = None
            r2 = "R2_missed_ship_deadline" in tags
            if p["alert"] or r2:                       # R9 = model flag; R2 alone also alerts (lift 4.2x)
                reasons = (["R9_high_model_risk"] if p["alert"] else []) + tags
                severity = "high" if (p["alert"] and (r2 or stage == "handoff")) else "medium"
                alert = _alert(p, reasons, severity)
                p["alert"] = True
            out[i] = (p, alert)

    for i, m in enumerate(msgs):
        if out[i] is not None:
            continue
        stage = STAGE_OF_EVENT[m["event_type"]]
        p = _base(m, stage, instance)
        if stage == "approved":
            p["reason_tags"] = approved_rules(m)
        elif stage == "delivered":
            v = m.get("is_late_actual")
            p["is_late_actual"] = None if v is None else int(float(v))
        out[i] = (p, None)
    return out
