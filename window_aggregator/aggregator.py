"""Window aggregator — consumer group "window-aggregator" on `delay-predictions`.

1-day tumbling windows (event time) per customer state and per seller; R8 region-surge
alerts to `alerts`. Every few seconds the changed windows, the open-window state AND the
Kafka offsets are saved in ONE PostgreSQL transaction. On restart it loads that state and
seeks to those offsets, so the totals are exactly the same as an uninterrupted run.
"""
import json
import os
import signal
import sys
import time

from confluent_kafka import KafkaError, TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from psycopg.types.json import Jsonb

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.db import connect, record_rejected  # noqa: E402
from common.kafka_utils import manual_commit_consumer, reliable_producer, wait_for_topics  # noqa: E402
from common.schemas import InvalidMessage, decode_json, to_json  # noqa: E402
from window_aggregator.windows import WindowAggregator  # noqa: E402

CONSUMED = Counter("agg_messages_consumed_total", "Prediction records read")
WINDOWS_CLOSED = Gauge("agg_closed_days", "Number of closed day windows kept in memory")
OPEN_WINDOWS = Gauge("agg_open_state_windows", "State-day windows in memory")
LATE_EVENTS = Gauge("agg_late_events_total", "Events that arrived after their window closed")
DUPLICATES = Gauge("agg_duplicates_skipped_total", "Re-delivered predictions ignored (already counted)")
TOO_LATE = Gauge("agg_dropped_too_late_total", "Events older than the allowed lateness, ignored")
SURGE_ALERTS = Counter("agg_surge_alerts_total", "R8 region-surge alerts raised")
CHECKPOINT_SECONDS = Histogram("agg_checkpoint_seconds", "Time to save windows + state + offsets",
                               buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5))
WATERMARK = Gauge("agg_watermark_timestamp", "Event-time watermark (unix seconds)")

STATE_ID = "window-aggregator"
CHECKPOINT_EVERY = float(os.environ.get("AGG_CHECKPOINT_SECONDS", 5))

UPSERT_STATE_WINDOW = """
INSERT INTO state_daily_windows (customer_state, window_start, window_end, orders_placed, avg_m1_probability,
    m1_alerts, approvals, r1_slow_approvals, handoffs, avg_m3_probability, m3_alerts, deliveries,
    late_deliveries, late_rate, rolling7_deliveries, rolling7_late_rate)
VALUES (%(customer_state)s, %(window_start)s, %(window_end)s, %(orders_placed)s, %(avg_m1_probability)s,
    %(m1_alerts)s, %(approvals)s, %(r1_slow_approvals)s, %(handoffs)s, %(avg_m3_probability)s, %(m3_alerts)s,
    %(deliveries)s, %(late_deliveries)s, %(late_rate)s, %(rolling7_deliveries)s, %(rolling7_late_rate)s)
ON CONFLICT (customer_state, window_start) DO UPDATE SET
    orders_placed = EXCLUDED.orders_placed, avg_m1_probability = EXCLUDED.avg_m1_probability,
    m1_alerts = EXCLUDED.m1_alerts, approvals = EXCLUDED.approvals, r1_slow_approvals = EXCLUDED.r1_slow_approvals,
    handoffs = EXCLUDED.handoffs, avg_m3_probability = EXCLUDED.avg_m3_probability, m3_alerts = EXCLUDED.m3_alerts,
    deliveries = EXCLUDED.deliveries, late_deliveries = EXCLUDED.late_deliveries, late_rate = EXCLUDED.late_rate,
    rolling7_deliveries = COALESCE(EXCLUDED.rolling7_deliveries, state_daily_windows.rolling7_deliveries),
    rolling7_late_rate = COALESCE(EXCLUDED.rolling7_late_rate, state_daily_windows.rolling7_late_rate),
    updated_at = now()
"""

UPSERT_SELLER_WINDOW = """
INSERT INTO seller_daily_windows (seller_id, window_start, handoffs, avg_m3_probability, r2_missed_deadline,
    deliveries, late_deliveries)
VALUES (%(seller_id)s, %(window_start)s, %(handoffs)s, %(avg_m3_probability)s, %(r2_missed_deadline)s,
    %(deliveries)s, %(late_deliveries)s)
ON CONFLICT (seller_id, window_start) DO UPDATE SET
    handoffs = EXCLUDED.handoffs, avg_m3_probability = EXCLUDED.avg_m3_probability,
    r2_missed_deadline = EXCLUDED.r2_missed_deadline, deliveries = EXCLUDED.deliveries,
    late_deliveries = EXCLUDED.late_deliveries, updated_at = now()
"""

SAVE_STATE = """
INSERT INTO aggregator_state (id, state, offsets, saved_at) VALUES (%s, %s, %s, now())
ON CONFLICT (id) DO UPDATE SET state = EXCLUDED.state, offsets = EXCLUDED.offsets, saved_at = now()
"""

stop = False


def handle_stop(*_):
    global stop
    stop = True


def main():
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)
    start_http_server(config.METRICS_PORT)

    agg = WindowAggregator(
        grace_hours=float(os.environ.get("WINDOW_GRACE_HOURS", 6)),
        r8_late_rate=float(os.environ.get("R8_LATE_RATE", 0.15)),
        r8_min_deliveries=int(os.environ.get("R8_MIN_DELIVERIES", 50)),
        r8_days=int(os.environ.get("R8_ROLLING_DAYS", 7)),
        max_lateness_days=float(os.environ.get("WINDOW_MAX_LATENESS_DAYS", 7)))
    conn = connect()
    saved_offsets = {}
    with conn.cursor() as cur:
        cur.execute("SELECT state, offsets, saved_at FROM aggregator_state WHERE id = %s", (STATE_ID,))
        row = cur.fetchone()
    conn.commit()
    if row:
        agg.load(row[0])
        saved_offsets = {int(k): int(v) for k, v in row[1].items()}
        print(f"RESTORED window state saved at {row[2]} | watermark {agg.watermark} | offsets {saved_offsets}",
              flush=True)
    else:
        print("no saved state — starting from the beginning of the topic", flush=True)

    topic = config.TOPIC_PREDICTIONS
    wait_for_topics([topic, config.TOPIC_ALERTS])
    consumer = manual_commit_consumer("window-aggregator", config.INSTANCE)
    producer = reliable_producer(f"{config.INSTANCE}-alerts")
    positions = dict(saved_offsets)          # partition -> next offset to read

    def on_assign(c, parts):
        for p in parts:
            if p.partition in saved_offsets:
                p.offset = saved_offsets[p.partition]          # resume exactly where the saved state ends
        c.assign(parts)
        print(f"ASSIGNED {[(p.partition, p.offset) for p in parts]}", flush=True)

    consumer.subscribe([topic], on_assign=on_assign)
    last_ckpt = time.monotonic()

    def checkpoint():
        t0 = time.perf_counter()
        alerts = agg.close_ready()
        for a in alerts:
            a = {**a, "source": "window-aggregator", "created_at": time.strftime("%Y-%m-%dT%H:%M:%S")}
            a.pop("detail", None)
            producer.produce(config.TOPIC_ALERTS, key=a["customer_state"].encode(), value=to_json(a))
            SURGE_ALERTS.inc()
            print(f"R8 REGION SURGE {a['customer_state']} {a['event_time'][:10]} late rate {a['score']:.1%}",
                  flush=True)
        if producer.flush(30):
            raise RuntimeError("R8 alerts not confirmed by Kafka")
        state_rows, seller_rows = agg.take_dirty()
        with conn.cursor() as cur:
            if state_rows:
                cur.executemany(UPSERT_STATE_WINDOW, state_rows)
            if seller_rows:
                cur.executemany(UPSERT_SELLER_WINDOW, seller_rows)
            cur.execute(SAVE_STATE, (STATE_ID, Jsonb(agg.to_dict()),
                                     Jsonb({str(k): v for k, v in positions.items()})))
        conn.commit()                                       # windows + state + offsets: all or nothing
        if positions:                                       # also tell Kafka (for the lag dashboard)
            consumer.commit(offsets=[TopicPartition(topic, p, o) for p, o in positions.items()],
                            asynchronous=True)
        agg.evict()
        WINDOWS_CLOSED.set(len(agg.closed_days))
        OPEN_WINDOWS.set(len(agg.state))
        LATE_EVENTS.set(agg.late_events)
        DUPLICATES.set(agg.duplicates)
        TOO_LATE.set(agg.dropped_too_late)
        if agg.watermark:
            WATERMARK.set(agg.watermark.timestamp())
        CHECKPOINT_SECONDS.observe(time.perf_counter() - t0)

    while not stop:
        for m in consumer.consume(num_messages=1000, timeout=0.5):
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    print(f"consumer error: {m.error()}", flush=True)
                continue
            positions[m.partition()] = m.offset() + 1
            CONSUMED.inc()
            try:
                agg.add(decode_json(m.value()))
            except (InvalidMessage, KeyError, ValueError) as e:
                reason = getattr(e, "reason", type(e).__name__)
                record_rejected(conn, "window-aggregator", reason, str(e), m.topic(), m.partition(), m.offset(),
                                m.value().decode("utf-8", "replace") if m.value() else None)
                conn.commit()
        if time.monotonic() - last_ckpt >= CHECKPOINT_EVERY:
            checkpoint()
            last_ckpt = time.monotonic()

    checkpoint()
    consumer.close()
    conn.close()
    print("window aggregator stopped cleanly", flush=True)


if __name__ == "__main__":
    main()
