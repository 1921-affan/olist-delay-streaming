"""SQL statements and record -> parameter mapping for the DB writer (pure Python)."""
from datetime import datetime

INSERT_PREDICTION = """
INSERT INTO predictions (order_id, stage, event_time, model, raw_score, late_probability, alert, threshold,
                         reason_tags, customer_state, seller_id, estimated_delivery_date, is_late_actual,
                         scorer_instance, scored_at)
VALUES (%(order_id)s, %(stage)s, %(event_time)s, %(model)s, %(raw_score)s, %(late_probability)s, %(alert)s,
        %(threshold)s, %(reason_tags)s, %(customer_state)s, %(seller_id)s, %(estimated_delivery_date)s,
        %(is_late_actual)s, %(scorer_instance)s, %(scored_at)s)
ON CONFLICT (order_id, stage) DO NOTHING
"""

UPSERT_STATUS = """
INSERT INTO order_status (order_id, customer_state, seller_id, estimated_delivery_date, placed_time,
                          current_stage, stage_rank, last_event_time, m1_probability, m1_alert,
                          m3_probability, m3_alert, latest_probability, any_alert, reason_tags,
                          is_late_actual, delivered_time)
VALUES (%(order_id)s, %(customer_state)s, %(seller_id)s, %(estimated_delivery_date)s, %(placed_time)s,
        %(stage)s, %(stage_rank)s, %(event_time)s, %(m1_probability)s, %(m1_alert)s,
        %(m3_probability)s, %(m3_alert)s, %(latest_probability)s, %(alert)s, %(reason_tags)s,
        %(is_late_actual)s, %(delivered_time)s)
ON CONFLICT (order_id) DO UPDATE SET
    customer_state          = COALESCE(EXCLUDED.customer_state, order_status.customer_state),
    seller_id               = COALESCE(EXCLUDED.seller_id, order_status.seller_id),
    estimated_delivery_date = COALESCE(order_status.estimated_delivery_date, EXCLUDED.estimated_delivery_date),
    placed_time             = COALESCE(EXCLUDED.placed_time, order_status.placed_time),
    current_stage           = CASE WHEN EXCLUDED.stage_rank > order_status.stage_rank
                                   THEN EXCLUDED.current_stage ELSE order_status.current_stage END,
    stage_rank              = GREATEST(EXCLUDED.stage_rank, order_status.stage_rank),
    last_event_time         = GREATEST(EXCLUDED.last_event_time, order_status.last_event_time),
    m1_probability          = COALESCE(EXCLUDED.m1_probability, order_status.m1_probability),
    m1_alert                = COALESCE(EXCLUDED.m1_alert, order_status.m1_alert),
    m3_probability          = COALESCE(EXCLUDED.m3_probability, order_status.m3_probability),
    m3_alert                = COALESCE(EXCLUDED.m3_alert, order_status.m3_alert),
    latest_probability      = COALESCE(EXCLUDED.m3_probability, order_status.m3_probability,
                                       EXCLUDED.m1_probability, order_status.m1_probability),
    any_alert               = order_status.any_alert OR EXCLUDED.any_alert,
    reason_tags             = ARRAY(SELECT DISTINCT t FROM unnest(order_status.reason_tags || EXCLUDED.reason_tags) AS t
                                    ORDER BY t),
    is_late_actual          = COALESCE(EXCLUDED.is_late_actual, order_status.is_late_actual),
    delivered_time          = COALESCE(EXCLUDED.delivered_time, order_status.delivered_time),
    updated_at              = now()
"""

INSERT_ALERT = """
INSERT INTO alerts (alert_id, alert_type, order_id, stage, severity, score, reasons, customer_state, seller_id,
                    event_time, source, created_at)
VALUES (%(alert_id)s, %(alert_type)s, %(order_id)s, %(stage)s, %(severity)s, %(score)s, %(reasons)s,
        %(customer_state)s, %(seller_id)s, %(event_time)s, %(source)s, %(created_at)s)
ON CONFLICT (alert_id) DO NOTHING
"""

STAGE_RANK = {"placed": 1, "approved": 2, "handoff": 3, "delivered": 4}


def ts(v):
    if v in (None, ""):
        return None
    return datetime.fromisoformat(str(v).replace("Z", "+00:00"))


def prediction_params(p):
    return {
        "order_id": p["order_id"], "stage": p["stage"], "event_time": ts(p["event_time"]),
        "model": p.get("model"), "raw_score": p.get("raw_score"), "late_probability": p.get("late_probability"),
        "alert": bool(p.get("alert")), "threshold": p.get("threshold"),
        "reason_tags": list(p.get("reason_tags") or []), "customer_state": p.get("customer_state"),
        "seller_id": p.get("seller_id"), "estimated_delivery_date": ts(p.get("estimated_delivery_date")),
        "is_late_actual": p.get("is_late_actual"), "scorer_instance": p.get("scorer_instance"),
        "scored_at": ts(p.get("scored_at")),
    }


def status_params(p):
    q = prediction_params(p)
    stage = p["stage"]
    q.update({
        "stage_rank": STAGE_RANK[stage],
        "placed_time": q["event_time"] if stage == "placed" else None,
        "delivered_time": q["event_time"] if stage == "delivered" else None,
        "m1_probability": q["late_probability"] if p.get("model") == "model1" else None,
        "m1_alert": q["alert"] if p.get("model") == "model1" else None,
        "m3_probability": q["late_probability"] if p.get("model") == "model3" else None,
        "m3_alert": q["alert"] if p.get("model") == "model3" else None,
    })
    q["latest_probability"] = q["m3_probability"] if q["m3_probability"] is not None else q["m1_probability"]
    return q


def alert_params(a):
    return {
        "alert_id": a["alert_id"], "alert_type": a["alert_type"], "order_id": a.get("order_id"),
        "stage": a.get("stage"), "severity": a.get("severity"), "score": a.get("score"),
        "reasons": list(a.get("reasons") or []), "customer_state": a.get("customer_state"),
        "seller_id": a.get("seller_id"), "event_time": ts(a["event_time"]), "source": a.get("source"),
        "created_at": ts(a.get("created_at")),
    }
