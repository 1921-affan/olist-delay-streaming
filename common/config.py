"""All settings come from environment variables (see .env)."""
import os
import socket


def env(name, default=None, cast=str):
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing setting {name} (add it to .env)")
    return cast(value)


KAFKA_BOOTSTRAP = env("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_ORDERS = env("TOPIC_ORDERS", "orders")
TOPIC_SHIPMENT_EVENTS = env("TOPIC_SHIPMENT_EVENTS", "shipment-events")
TOPIC_PREDICTIONS = env("TOPIC_PREDICTIONS", "delay-predictions")
TOPIC_ALERTS = env("TOPIC_ALERTS", "alerts")
TOPIC_PARTITIONS = env("TOPIC_PARTITIONS", "3", int)

PG_DSN = (f"host={env('POSTGRES_HOST', 'localhost')} port={env('POSTGRES_PORT', '5432')} "
          f"dbname={env('POSTGRES_DB', 'olist')} user={env('POSTGRES_USER', 'olist')} "
          f"password={env('POSTGRES_PASSWORD', 'olist')}")

INSTANCE = os.environ.get("INSTANCE_NAME") or socket.gethostname()
METRICS_PORT = env("METRICS_PORT", "8000", int)
