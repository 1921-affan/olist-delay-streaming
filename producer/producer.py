"""Replay producer: plays the May–Oct 2018 order events into Kafka on a simulated clock.

  docker compose run --rm --name producer producer [--start 2018-05-20] [--end 2018-10-31]
                                                   [--speed 1440] [--max-events N] [--resume]

speed = simulated seconds per real second (1440 -> 1 simulated day per real minute).
"""
import argparse
import json
import os
import signal
import sys
import time
import uuid

import pandas as pd
from prometheus_client import Counter, Gauge, start_http_server

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common import config  # noqa: E402
from common.db import connect  # noqa: E402
from common.kafka_utils import reliable_producer, wait_for_topics  # noqa: E402
from common.schemas import to_json  # noqa: E402
from producer.replay import due_seconds, load_timeline, to_message  # noqa: E402

EVENTS_SENT = Counter("producer_events_sent_total", "Messages handed to Kafka", ["topic", "event_type"])
EVENTS_CONFIRMED = Counter("producer_events_confirmed_total", "Messages confirmed by the broker", ["topic"])
DELIVERY_FAILED = Counter("producer_delivery_failed_total", "Messages the broker did not confirm", ["topic"])
SIM_CLOCK = Gauge("producer_sim_clock_timestamp", "Simulated 2018 time of the last sent event (unix seconds)")
SCHEDULE_LAG = Gauge("producer_schedule_lag_seconds", "How far the producer is behind its schedule")
PROGRESS = Gauge("producer_progress_ratio", "Share of planned events already sent")

stop = False


def handle_stop(*_):
    global stop
    stop = True


def delivery_report(err, msg):
    """Async delivery callback (CIE program 2)."""
    if err is not None:
        DELIVERY_FAILED.labels(msg.topic()).inc()
        print(f"DELIVERY FAILED topic={msg.topic()} key={msg.key()} err={err}", flush=True)
    else:
        EVENTS_CONFIRMED.labels(msg.topic()).inc()



def save_checkpoint(path, run_id, index, sim_time, args):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"run_id": run_id, "next_index": index, "sim_time": str(sim_time),
                   "start": args.start, "end": args.end, "speed": args.speed,
                   "max_events": args.max_events}, f)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=os.environ.get("REPLAY_START"))
    ap.add_argument("--end", default=os.environ.get("REPLAY_END"))
    ap.add_argument("--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", 1440)))
    ap.add_argument("--max-events", type=int, default=None)
    ap.add_argument("--resume", action="store_true", help="continue from the last checkpoint")
    ap.add_argument("--no-sleep", action="store_true", help="send as fast as possible (testing)")
    args = ap.parse_args()

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)
    start_http_server(config.METRICS_PORT)

    ckpt_path = os.environ.get("PRODUCER_CHECKPOINT", "/app/state/producer_checkpoint.json")
    first_index, run_id = 0, uuid.uuid4().hex[:8]
    if args.resume:
        if not os.path.exists(ckpt_path):
            print("no checkpoint found — nothing to resume", flush=True)
            return
        ck = json.load(open(ckpt_path))
        # the event index only means something on the SAME timeline -> reuse the original run's settings
        args.start, args.end = ck.get("start", args.start), ck.get("end", args.end)
        args.speed, args.max_events = ck.get("speed", args.speed), ck.get("max_events", args.max_events)
        first_index, run_id = int(ck["next_index"]), ck["run_id"]
        print(f"resuming run {run_id} at event {first_index:,} (sim time {ck['sim_time']}) "
              f"with its original settings start={args.start} end={args.end} speed={args.speed:g}", flush=True)

    timeline, bad = load_timeline(os.environ["DATA_ORDERS"], os.environ["DATA_EVENTS"], args.start, args.end)
    if args.max_events:
        timeline = timeline.head(args.max_events)
    if timeline.empty:
        print("nothing to replay for these dates", flush=True)
        return
    if first_index >= len(timeline):
        print("checkpoint is at the end of the run — nothing left to send", flush=True)
        return

    sim_start = pd.Timestamp(args.start) if args.start else timeline.event_time.iloc[0].normalize()
    if first_index > 0:
        sim_start = timeline.event_time.iloc[min(first_index, len(timeline) - 1)]
    n = len(timeline)
    print(f"run {run_id}: {n:,} events {timeline.event_time.iloc[0]} -> {timeline.event_time.iloc[-1]} "
          f"| speed {args.speed:g}x (1 simulated day = {86400 / args.speed:.1f} real s)", flush=True)

    # record the run + events skipped because they are dated before their order
    conn = connect()
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO replay_runs (run_id, speed, sim_start, sim_end, events_planned)
                       VALUES (%s, %s, %s, %s, %s) ON CONFLICT (run_id) DO NOTHING""",
                    (run_id, args.speed, timeline.event_time.iloc[0].to_pydatetime(),
                     timeline.event_time.iloc[-1].to_pydatetime(), n))
        for _, r in bad.iterrows():
            cur.execute("""INSERT INTO rejected_events (source, reason, detail, payload)
                           SELECT 'producer', 'event_before_purchase', %s, %s
                           WHERE NOT EXISTS (SELECT 1 FROM rejected_events
                                             WHERE source='producer' AND detail=%s)""",
                        (f"{r['event_type']} {r['order_id']} at {r['event_time']}", None,
                         f"{r['event_type']} {r['order_id']} at {r['event_time']}"))
    conn.commit()
    print(f"skipped {len(bad)} events dated before their order (stored in rejected_events)", flush=True)

    wait_for_topics([config.TOPIC_ORDERS, config.TOPIC_SHIPMENT_EVENTS])
    producer = reliable_producer(f"replay-producer-{run_id}")
    topic_of = {"order_placed": config.TOPIC_ORDERS}

    records = timeline.to_dict("records")
    t0 = time.monotonic()
    last_report = t0
    sent = 0
    for i in range(first_index, n):
        if stop:
            break
        row = records[i]
        if not args.no_sleep:
            wait = due_seconds(row["event_time"], sim_start, args.speed) - (time.monotonic() - t0)
            if wait > 0:
                # sleep in small steps so Ctrl+C stays responsive and callbacks keep firing
                end_wait = time.monotonic() + wait
                while not stop and time.monotonic() < end_wait:
                    producer.poll(0)
                    time.sleep(min(0.2, max(0.0, end_wait - time.monotonic())))
                if stop:
                    break
            SCHEDULE_LAG.set(max(0.0, -wait))
        topic = topic_of.get(row["event_type"], config.TOPIC_SHIPMENT_EVENTS)
        msg = to_message(row, run_id)
        while True:
            try:
                producer.produce(topic, key=row["order_id"].encode(), value=to_json(msg),
                                 on_delivery=delivery_report)
                break
            except BufferError:          # local queue full -> let it drain
                producer.poll(0.5)
        producer.poll(0)
        sent += 1
        EVENTS_SENT.labels(topic, row["event_type"]).inc()
        SIM_CLOCK.set(row["event_time"].timestamp())
        PROGRESS.set((i + 1) / n)

        now = time.monotonic()
        if now - last_report >= 10 or i == n - 1:
            producer.flush(10)
            save_checkpoint(ckpt_path, run_id, i + 1, row["event_time"], args)
            with conn.cursor() as cur:
                cur.execute("UPDATE replay_runs SET events_sent=%s, sim_clock=%s WHERE run_id=%s",
                            (i + 1, row["event_time"].to_pydatetime(), run_id))
            conn.commit()
            print(f"[{now - t0:7.0f}s] sim {row['event_time']} | sent {i + 1:,}/{n:,}", flush=True)
            last_report = now

    remaining = producer.flush(30)
    with conn.cursor() as cur:
        cur.execute("UPDATE replay_runs SET finished_at=now() WHERE run_id=%s AND %s", (run_id, not stop))
    conn.commit()
    conn.close()
    print(f"done: {sent:,} messages sent this session, {remaining} still unconfirmed"
          + (" (stopped early — use --resume to continue)" if stop else ""), flush=True)


if __name__ == "__main__":
    main()
