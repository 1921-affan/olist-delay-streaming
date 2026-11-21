"""Fault-tolerance demo: send broken messages to the input topics.

  docker compose run --rm --entrypoint python scorer-1 ops/inject_bad_message.py

The scorers must keep running; each broken message appears in the rejected_events table.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.kafka_utils import reliable_producer  # noqa: E402

BAD = [
    (config.TOPIC_ORDERS, b"demo-1", b"{this is not json"),
    (config.TOPIC_ORDERS, b"demo-2", b'{"event_type":"order_placed","order_id":"demo-2"}'),            # missing fields
    (config.TOPIC_SHIPMENT_EVENTS, b"demo-3",
     b'{"event_id":"x","event_type":"teleported","event_time":"2018-06-01T10:00:00","order_id":"demo-3"}'),
    (config.TOPIC_SHIPMENT_EVENTS, b"demo-4",
     b'{"event_id":"y","event_type":"delivered","event_time":"not-a-date","order_id":"demo-4",'
     b'"customer_state":"SP","is_late_actual":1}'),
]

if __name__ == "__main__":
    p = reliable_producer("bad-message-injector")
    for topic, key, value in BAD:
        p.produce(topic, key=key, value=value)
        print(f"sent to {topic}: {value[:60]!r}")
    p.flush(10)
    print("done — check: SELECT reason, detail, topic FROM rejected_events ORDER BY id DESC LIMIT 5;")
