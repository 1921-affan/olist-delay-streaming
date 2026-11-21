import json
import random

from window_aggregator.windows import WindowAggregator


def pred(t, state, stage, prob=None, alert=False, late=None, seller="s1", tags=()):
    return {"event_time": t, "customer_state": state, "stage": stage, "late_probability": prob, "alert": alert,
            "is_late_actual": late, "seller_id": seller, "reason_tags": list(tags)}


def stream():
    rnd = random.Random(1)
    out = []
    for day in range(1, 21):
        for k in range(40):
            h = f"2018-06-{day:02d}T{rnd.randint(0, 23):02d}:{rnd.randint(0, 59):02d}:00"
            st = rnd.choice(["SP", "RJ", "BA"])
            out.append(pred(h, st, "placed", prob=rnd.random(), alert=rnd.random() < 0.1))
            out.append(pred(h, st, "handoff", prob=rnd.random(), tags=["R2_missed_ship_deadline"] if k % 7 == 0 else []))
            # BA has a surge: half late
            out.append(pred(h, st, "delivered", late=int(rnd.random() < (0.5 if st == "BA" else 0.05))))
    out.sort(key=lambda p: p["event_time"])
    return out


def run(events, crash_at=None):
    agg, alerts, saved = WindowAggregator(r8_min_deliveries=50), [], None
    rows = {}
    for i, p in enumerate(events):
        if crash_at is not None and i == crash_at:
            # simulate a crash: only what was checkpointed survives
            agg = WindowAggregator(r8_min_deliveries=50)
            agg.load(json.loads(saved["state"]))
            i_resume = saved["next"]
            return rows, alerts, agg, i_resume
        agg.add(p)
        if i % 50 == 0:
            alerts += agg.close_ready()
            st, _ = agg.take_dirty()
            rows.update({(r["customer_state"], r["window_start"]): r for r in st})
            saved = {"state": json.dumps(agg.to_dict()), "next": i + 1}
    alerts += agg.close_ready()
    st, _ = agg.take_dirty()
    rows.update({(r["customer_state"], r["window_start"]): r for r in st})
    return rows, alerts, agg, None


def test_tumbling_windows_count_everything():
    ev = stream()
    rows, alerts, agg, _ = run(ev)
    assert sum(r["orders_placed"] for r in rows.values()) == sum(p["stage"] == "placed" for p in ev)
    assert sum(r["deliveries"] for r in rows.values()) == sum(p["stage"] == "delivered" for p in ev)
    assert all(r["window_start"][:10] == r["window_start"][:10] for r in rows.values())


def test_r8_surge_fires_for_ba_only_once_per_day():
    rows, alerts, _, _ = run(stream())
    states = {a["customer_state"] for a in alerts}
    assert states == {"BA"}
    ids = [a["alert_id"] for a in alerts]
    assert len(ids) == len(set(ids))


def test_restart_from_checkpoint_gives_identical_totals():
    ev = stream()
    full_rows, full_alerts, _, _ = run(ev)
    rows, alerts, agg, resume = run(ev, crash_at=1234)
    for p in ev[resume:]:                               # re-read from the saved offset
        agg.add(p)
        alerts += agg.close_ready()
    alerts += agg.close_ready()
    st, _ = agg.take_dirty()
    rows.update({(r["customer_state"], r["window_start"]): r for r in st})
    key = lambda r: (r["orders_placed"], r["handoffs"], r["deliveries"], r["late_deliveries"])
    assert {k: key(v) for k, v in rows.items()} == {k: key(v) for k, v in full_rows.items()}
    # same R8 alerts as the uninterrupted run (a repeat after restore has the same alert_id -> DB ignores it)
    assert {a["alert_id"] for a in alerts} == {a["alert_id"] for a in full_alerts}


def test_redelivered_predictions_counted_once():
    ev = stream()
    for i, p in enumerate(ev):
        p["prediction_id"] = f"o{i}:{p['stage']}"
    once, _, _, _ = run(ev)
    twice, _, agg, _ = run(ev + ev[-500:-100])         # re-deliver recent predictions (scorer re-processing)
    key = lambda r: (r["orders_placed"], r["handoffs"], r["deliveries"], r["late_deliveries"])
    assert {k: key(v) for k, v in once.items()} == {k: key(v) for k, v in twice.items()}
    assert agg.duplicates == 400 and agg.dropped_too_late == 0


def test_events_beyond_allowed_lateness_are_ignored():
    ev = stream()
    for i, p in enumerate(ev):
        p["prediction_id"] = f"o{i}:{p['stage']}"
    once, _, _, _ = run(ev)
    old_chunk = [dict(p, prediction_id=p["prediction_id"] + "-replayed") for p in ev[:300]]   # new ids, very old dates
    twice, _, agg, _ = run(ev + old_chunk)
    key = lambda r: (r["orders_placed"], r["handoffs"], r["deliveries"], r["late_deliveries"])
    assert {k: key(v) for k, v in once.items()} == {k: key(v) for k, v in twice.items()}
    assert agg.dropped_too_late == 300
