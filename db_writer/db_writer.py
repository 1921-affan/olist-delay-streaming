"""DB writer — consumer group "db-writer" on `delay-predictions` + `alerts`.

Writes in batches; commits Kafka offsets only after the PostgreSQL transaction commits.
All inserts are idempotent (ON CONFLICT), so re-reading after a crash creates no duplicates.
"""
import os
import signal
import sys
import time

from confluent_kafka import KafkaError
from prometheus_client import Counter, Histogram, start_http_server

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.db import connect, record_rejected  # noqa: E402
from common.kafka_utils import manual_commit_consumer, wait_for_topics  # noqa: E402
from common.schemas import InvalidMessage, decode_json  # noqa: E402
from db_writer.sql import (INSERT_ALERT, INSERT_PREDICTION, UPSERT_STATUS, alert_params,  # noqa: E402
                           prediction_params, status_params)

ROWS = Counter("dbwriter_rows_written_total", "Rows inserted", ["table"])
DUPLICATES = Counter("dbwriter_duplicates_skipped_total", "Rows already present (re-delivered messages)", ["table"])
BATCH_SECONDS = Histogram("dbwriter_batch_seconds", "DB transaction time per batch",
                          buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5))
REJECTED = Counter("dbwriter_rejected_total", "Messages that could not be written", ["reason"])

BATCH_SIZE = int(os.environ.get("DB_BATCH_SIZE", 500))
BATCH_SECONDS_MAX = float(os.environ.get("DB_BATCH_SECONDS", 1.0))
stop = False


def handle_stop(*_):
    global stop
    stop = True


def write_batch(conn, preds, alerts):
    with conn.cursor() as cur:
        if preds:
            cur.executemany(INSERT_PREDICTION, [prediction_params(p) for p in preds])
            n = max(cur.rowcount, 0)
            ROWS.labels("predictions").inc(n)
            DUPLICATES.labels("predictions").inc(len(preds) - n)
            cur.executemany(UPSERT_STATUS, [status_params(p) for p in preds])
            ROWS.labels("order_status").inc(len(preds))
        if alerts:
            cur.executemany(INSERT_ALERT, [alert_params(a) for a in alerts])
            n = max(cur.rowcount, 0)
            ROWS.labels("alerts").inc(n)
            DUPLICATES.labels("alerts").inc(len(alerts) - n)
    conn.commit()


def main():
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)
    start_http_server(config.METRICS_PORT)
    conn = connect()
    topics = [config.TOPIC_PREDICTIONS, config.TOPIC_ALERTS]
    wait_for_topics(topics)
    consumer = manual_commit_consumer("db-writer", config.INSTANCE)
    consumer.subscribe(topics, on_assign=lambda c, p: print(f"db-writer ASSIGNED {len(p)} partitions", flush=True))

    preds, alerts, pending = [], [], 0
    last_flush = time.monotonic()
    while not stop:
        for m in consumer.consume(num_messages=BATCH_SIZE, timeout=0.5):
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    print(f"consumer error: {m.error()}", flush=True)
                continue
            pending += 1
            try:
                rec = decode_json(m.value())
                (alerts if m.topic() == config.TOPIC_ALERTS else preds).append(rec)
            except InvalidMessage as e:
                REJECTED.labels(e.reason).inc()
                record_rejected(conn, "db-writer", e.reason, e.detail, m.topic(), m.partition(), m.offset(),
                                m.value().decode("utf-8", "replace") if m.value() else None)
        if pending and (pending >= BATCH_SIZE or time.monotonic() - last_flush >= BATCH_SECONDS_MAX):
            t0 = time.perf_counter()
            try:
                write_batch(conn, preds, alerts)
            except Exception as e:                      # DB problem: do not commit offsets, restart
                print(f"DB write failed, exiting without committing offsets: {e}", flush=True)
                conn.rollback()
                sys.exit(1)
            consumer.commit(asynchronous=False)         # only after the DB transaction succeeded
            BATCH_SECONDS.observe(time.perf_counter() - t0)
            preds, alerts, pending = [], [], 0
            last_flush = time.monotonic()

    if pending:
        write_batch(conn, preds, alerts)
        consumer.commit(asynchronous=False)
    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
