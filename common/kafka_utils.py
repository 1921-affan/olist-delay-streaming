"""Kafka producer/consumer factories with the reliability settings used everywhere."""
import time

from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.admin import AdminClient

from common import config


def reliable_producer(client_id, extra=None):
    """acks=all + idempotence + retries + batching (CIE programs 2 and 5)."""
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 10,
        "retry.backoff.ms": 200,
        "linger.ms": 20,
        "batch.num.messages": 1000,
        "compression.type": "lz4",
        "delivery.timeout.ms": 120000,
    }
    conf.update(extra or {})
    return Producer(conf)


def manual_commit_consumer(group_id, client_id, extra=None):
    """At-least-once consumer: offsets are committed by our code after the work is saved (CIE program 7)."""
    conf = {
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": group_id,
        "client.id": client_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "range",   # same partition numbers of both input topics -> same copy
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
        "max.poll.interval.ms": 300000,
    }
    conf.update(extra or {})
    return Consumer(conf)


def wait_for_kafka(timeout_s=120):
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    deadline = time.time() + timeout_s
    while True:
        try:
            md = admin.list_topics(timeout=5)
            return md
        except KafkaException as e:
            if time.time() > deadline:
                raise
            print(f"waiting for Kafka at {config.KAFKA_BOOTSTRAP}: {e}", flush=True)
            time.sleep(3)


def wait_for_topics(topics, timeout_s=120):
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    deadline = time.time() + timeout_s
    while True:
        try:
            present = set(admin.list_topics(timeout=5).topics)
            missing = [t for t in topics if t not in present]
            if not missing:
                return
            print(f"waiting for topics {missing}", flush=True)
        except KafkaException as e:
            print(f"waiting for Kafka: {e}", flush=True)
        if time.time() > deadline:
            raise RuntimeError(f"topics not available: {topics}")
        time.sleep(3)
