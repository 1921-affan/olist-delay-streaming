"""Scorer — run as 3 copies in consumer group "scorer".

Reads `orders` + `shipment-events`, applies Model 1 / Model 3 / rules, writes to
`delay-predictions` (+ `alerts`). Offsets are committed only AFTER the outputs are
confirmed by Kafka -> at-least-once; downstream upserts make repeats harmless.
"""
import os
import signal
import sys
import time

from confluent_kafka import KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.db import connect, record_rejected  # noqa: E402
from common.kafka_utils import manual_commit_consumer, reliable_producer, wait_for_topics  # noqa: E402
from common.schemas import InvalidMessage, decode_event, to_json  # noqa: E402
from scorer.logic import process  # noqa: E402
from scorer.models import StageModel  # noqa: E402

CONSUMED = Counter("scorer_messages_consumed_total", "Input messages read", ["topic"])
PREDICTIONS = Counter("scorer_predictions_total", "Prediction records produced", ["stage", "model"])
ALERTS = Counter("scorer_alerts_total", "Order alerts produced", ["reason"])
REJECTED = Counter("scorer_rejected_total", "Input messages rejected", ["reason"])
BATCH_SECONDS = Histogram("scorer_batch_seconds", "Time to score + publish one batch",
                          buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5))
EVENT_LATENCY = Histogram("scorer_event_latency_seconds", "Producer send -> prediction published",
                          buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30))
ASSIGNED = Gauge("scorer_assigned_partitions", "Partitions currently owned by this copy", ["topic"])
REBALANCES = Counter("scorer_rebalances_total", "Partition assignment changes", ["kind"])

stop = False


def handle_stop(*_):
    global stop
    stop = True


def main():
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)
    start_http_server(config.METRICS_PORT)
    me = config.INSTANCE

    model1 = StageModel(os.environ["MODEL1_PATH"], "model1")
    model3 = StageModel(os.environ["MODEL3_PATH"], "model3")
    for m in (model1, model3):
        for problem in m.check_versions():
            print(f"WARNING {m.name} version mismatch -> {problem}", flush=True)
    print(f"{me}: Model 1 ({len(model1.features)} cols, thr {model1.threshold:.4f}) | "
          f"Model 3 ({len(model3.features)} cols, thr {model3.threshold:.4f}) loaded", flush=True)

    conn = connect()
    topics = [config.TOPIC_ORDERS, config.TOPIC_SHIPMENT_EVENTS]
    wait_for_topics(topics + [config.TOPIC_PREDICTIONS, config.TOPIC_ALERTS])
    consumer = manual_commit_consumer("scorer", me)
    producer = reliable_producer(f"{me}-out")

    def on_assign(c, parts):
        REBALANCES.labels("assigned").inc()
        for t in topics:
            ASSIGNED.labels(t).set(sum(1 for p in parts if p.topic == t))
        print(f"{me}: ASSIGNED {sorted((p.topic, p.partition) for p in parts)}", flush=True)

    def on_revoke(c, parts):
        REBALANCES.labels("revoked").inc()
        producer.flush(10)
        try:
            c.commit(asynchronous=False)
        except Exception:
            pass
        for t in topics:
            ASSIGNED.labels(t).set(0)
        print(f"{me}: REVOKED {sorted((p.topic, p.partition) for p in parts)}", flush=True)

    consumer.subscribe(topics, on_assign=on_assign, on_revoke=on_revoke)

    while not stop:
        batch = consumer.consume(num_messages=500, timeout=0.5)
        if not batch:
            continue
        t_start = time.perf_counter()
        good, sent_at = [], []
        for m in batch:
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    print(f"consumer error: {m.error()}", flush=True)
                continue
            CONSUMED.labels(m.topic()).inc()
            try:
                good.append(decode_event(m.value()))
                sent_at.append(m.timestamp()[1] / 1000.0)
            except InvalidMessage as e:
                REJECTED.labels(e.reason).inc()
                raw = m.value().decode("utf-8", "replace") if m.value() else None
                record_rejected(conn, me, e.reason, e.detail, m.topic(), m.partition(), m.offset(), raw)
        conn.commit()

        results = process(good, model1, model3, me) if good else []
        for (pred, alert), t_sent in zip(results, sent_at):
            key = pred["order_id"].encode()
            producer.produce(config.TOPIC_PREDICTIONS, key=key, value=to_json(pred))
            PREDICTIONS.labels(pred["stage"], pred["model"]).inc()
            if alert:
                producer.produce(config.TOPIC_ALERTS, key=key, value=to_json(alert))
                for r in alert["reasons"]:
                    ALERTS.labels(r).inc()
            producer.poll(0)
        remaining = producer.flush(30)
        if remaining:
            # Do NOT commit. Exit; Docker restarts this copy and it re-reads from the last commit.
            print(f"{me}: {remaining} outputs not confirmed by Kafka — exiting without commit", flush=True)
            sys.exit(1)
        consumer.commit(asynchronous=False)          # at-least-once: commit after outputs are safe
        now = time.time()
        for t_sent in sent_at:
            EVENT_LATENCY.observe(max(0.0, now - t_sent))
        BATCH_SECONDS.observe(time.perf_counter() - t_start)

    print(f"{me}: shutting down", flush=True)
    producer.flush(10)
    consumer.close()
    conn.close()


if __name__ == "__main__":
    main()
