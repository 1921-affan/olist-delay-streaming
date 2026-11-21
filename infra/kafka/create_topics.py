"""Create the 4 project topics if they do not exist (safe to run many times)."""
import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic

from common import config
from common.kafka_utils import wait_for_kafka

TOPICS = {
    config.TOPIC_ORDERS: config.TOPIC_PARTITIONS,
    config.TOPIC_SHIPMENT_EVENTS: config.TOPIC_PARTITIONS,
    config.TOPIC_PREDICTIONS: config.TOPIC_PARTITIONS,
    config.TOPIC_ALERTS: 1,
}



def main():
    wait_for_kafka()
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(name, num_partitions=p, replication_factor=1,
                    config={"retention.ms": str(7 * 24 * 3600 * 1000)})
           for name, p in TOPICS.items() if name not in existing]
    if new:
        for name, fut in admin.create_topics(new).items():
            try:
                fut.result()
                print(f"created topic {name}", flush=True)
            except Exception as e:
                print(f"topic {name}: {e}", flush=True)
    time.sleep(1)
    md = admin.list_topics(timeout=10)
    ok = True
    for name, p in TOPICS.items():
        t = md.topics.get(name)
        n = len(t.partitions) if t else 0
        print(f"  {name:20s} partitions={n} (expected {p})", flush=True)
        ok &= n == p
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
