"""PostgreSQL connection helper (psycopg 3)."""
import time

import psycopg

from common import config


def connect(retries=40):
    for attempt in range(retries):
        try:
            return psycopg.connect(config.PG_DSN, autocommit=False)
        except psycopg.OperationalError as e:
            print(f"waiting for PostgreSQL ({attempt + 1}/{retries}): {str(e).splitlines()[0]}", flush=True)
            time.sleep(3)
    raise RuntimeError("PostgreSQL not reachable")


def record_rejected(conn, source, reason, detail, topic=None, partition=None, offset=None, payload=None):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO rejected_events (source, reason, detail, topic, kafka_partition, kafka_offset, payload)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (topic, kafka_partition, kafka_offset) DO NOTHING""",
            (source, reason, (detail or "")[:500], topic, partition, offset,
             None if payload is None else payload[:4000]))
