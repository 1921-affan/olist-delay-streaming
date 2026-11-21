# Olist Delivery-Delay Streaming Pipeline — Implementation Plan

**Course:** AI372IA Stream Processing and Analytics — Project Based Learning (Semester VII)
**Phase II demo:** 19–26 Nov 2026 · **Technical paper:** due at the Phase II demo · **Lab CIE:** 14–18 Dec 2026
**Last updated:** 3 Nov 2026 01:45 (ALL circular requirements shown live, all 6 fault demos passed; next: paper, diagram, slides)

Legend: `[x]` done · `[ ]` to do · `[~]` in progress · `(opt)` optional, only if time allows

---

## 0. How this file is used

* This is the single source of truth for what is decided, built and checked.
* After each step, the item is ticked here and a dated line is added to the **Progress log** (section 14).
* Keep a copy of this file in the project folder (`olist-delay-streaming/plan.md`). If the chat session is ever reset, upload this file to restore the full context.

---

## 1. Circular requirements → how we meet them

| # | Circular requirement | Our implementation | Status |
|---|---|---|---|
| 1 | Kafka is the central platform | Every stage passes through Kafka; no component skips it | [x] |
| 2 | ≥ 1 producer and ≥ 1 consumer | Producer: replay producer. Consumers: scorer, DB writer, window aggregator (scorer + aggregator also produce) | [x] |
| 3 | ≥ 2 topics | 4 topics: `orders`, `shipment-events`, `delay-predictions`, `alerts` | [x] |
| 4 | Partitioning and consumer groups | 3 partitions, key = `order_id`; 3 scorer copies in group `scorer`; DB writer and aggregator in their own groups reading the same topic | [x] |
| 5 | ≥ 1 window-based streaming operation | Window aggregator: 1-day tumbling windows per customer state on event time (+ 7-day rolling view for R8) | [x] |
| 6 | ≥ 1 aggregation / summarization | Late rate, avg Model 1 / Model 3 risk, alert counts per state per day; top delay-prone sellers per window | [x] |
| 7 | Fault tolerance / recovery | Manual offset commits after work is saved; idempotent upserts; aggregator saves state + offsets in one DB transaction; bad messages to `rejected_events`; producer acks=all + idempotence + retries + resume | [x] all 6 scenarios shown live |
| 8 | Store processed results in a DB / time-series store | PostgreSQL 16 | [x] |
| 9 | Real-time visualization or alert mechanism | Grafana (business + system dashboards), `alerts` topic, Grafana alert rules | [x] |

CIE programs reused inside the project (doubles as CIE practice):
JSON serialization (P3/P4) · async send + delivery callbacks (P2) · acks/retries/batching (P5) · manual offset commits, at-least-once (P7) · threshold alert published to a separate topic (P10) · (opt) transactional publish of prediction + alert together (P6).

---

## 2. Final decisions (locked)

| Topic | Decision |
|---|---|
| Stream processing engine | **No Spark.** Plain Python consumers (confluent-kafka). Window logic written by us in the window aggregator. |
| Topics | 4: `orders`, `shipment-events`, `delay-predictions`, `alerts`. No DLQ topic; bad messages go to the `rejected_events` table. |
| Partitions | 3 per input topic (`orders`, `shipment-events`, `delay-predictions`); `alerts` 1 partition. Replication factor 1 (single broker). |
| Message key | `order_id` on every order-level message (keeps each order's events on the same partition number in both input topics). |
| Database | PostgreSQL 16 (plain; TimescaleDB not needed). |
| Monitoring | Prometheus (metrics from our services + Kafka lag exporter) and Grafana. |
| Deployment | One `docker-compose.yml` for everything. |
| Kafka port | Containerised Kafka exposed on host port **9094** so it does not clash with the locally installed Kafka on 9092. |
| Replay | One producer, one merged timeline sorted by 2018 event time, simulated clock. Default speed **1 simulated day = 1 real minute**; settings `--speed`, `--start`, `--end`. |
| Demo start | Replay from about **20 May 2018** (earlier days look quiet because April orders' later events are not in the files). |
| Time in dashboards | Business panels use `event_time` (2018, simulated). System panels use real time. Every stored row keeps both `event_time` and `processed_at`. |
| Model 1 | XGB_12, `model1_production_XGB_12.joblib`, threshold 0.7493, isotonic calibrator inside the bundle |
| Model 3 | XGBoost, 10 features, `model3_production.joblib`, threshold 0.7364, isotonic calibrator inside the bundle |
| Approval stage | No model; keep Model 1 score, add R1 tag |
| Python | 3.12 in all service images; scikit-learn 1.6.1, xgboost 3.4.1 pinned |

---

## 3. Offline work (done before streaming)

- [x] EDA notebook (data quality, label, stage signals, rules R1–R9)
- [x] `build_datasets.py` → `orders_clean.csv`, `orders_stream.csv`, `shipment_events.csv`, `feature_registry.json` (point-in-time seller/route features)
- [x] Model 1 candidates trained (HGB_15, XGB_12, CAT_15) and compared → **XGB_12 chosen**
- [x] Split problem found (late-only Feb–Apr in training) and fixed: train Jan 2017–Mar 2018, calibrate Apr 2018, test May–Aug 2018
- [x] Production Model 1 retrained (test PR-AUC 0.104, ROC-AUC 0.725)
- [x] `enrich_shipment_events.py` → `shipment_events_enriched.csv` (each event carries only what is known at that moment; 154 bad-date handoffs flagged)
- [x] Model 3 feature selection notebook: 27 → 10 columns
- [x] Model 3 training notebook: LogReg / XGBoost / LightGBM / CatBoost compared → **XGBoost kept** by pre-set rule
- [x] Production Model 3 (test PR-AUC 0.292, ROC-AUC 0.829; 2.8× Model 1)
- [x] Versions recorded: scikit-learn 1.6.1, xgboost 3.4.1 (lightgbm 4.6.0, catboost 1.2.10 training-only)
- [x] Results summary + results zip for the paper / slides

---

## 4. Project folder

```
olist-delay-streaming/
├── plan.md
├── README.md
├── docker-compose.yml
├── .env                         # speed, start/end, thresholds, ports, passwords
├── data/
│   ├── orders_stream.csv
│   └── shipment_events_enriched.csv
├── models/
│   ├── model1_production_XGB_12.joblib
│   └── model3_production.joblib
├── common/                      # shared code, copied into each image
│   ├── config.py                # reads .env
│   ├── kafka_utils.py           # producer/consumer factories, JSON (de)serialization
│   ├── schemas.py               # message field lists + validation
│   └── metrics.py               # Prometheus helpers
├── infra/
│   ├── kafka/create_topics.py   # idempotent topic creation (run by an init container)
│   ├── postgres/init.sql        # all tables, indexes, views
│   ├── prometheus/prometheus.yml
│   └── grafana/
│       ├── provisioning/datasources/datasources.yml
│       ├── provisioning/dashboards/dashboards.yml
│       ├── provisioning/alerting/alert_rules.yml
│       └── dashboards/{business.json, system.json}
├── producer/   {Dockerfile, requirements.txt, producer.py}
├── scorer/     {Dockerfile, requirements.txt, scorer.py, rules.py, models.py}
├── db_writer/  {Dockerfile, requirements.txt, db_writer.py}
├── window_aggregator/ {Dockerfile, requirements.txt, aggregator.py, windows.py}
├── ops/        {stuck_order_check.py (R3/R4), inject_bad_message.py, fault_demo.md}
└── tests/      {test_replay_schedule.py, test_rules.py, test_scoring.py, test_windows.py, test_schemas.py}
```

- [x] Create the folder skeleton
- [x] Copy data files and model files into `data/` and `models/`
- [x] `.env` with all settings documented
- [x] `.gitignore` (data, models, volumes)

---

## 5. Kafka topics and message formats

All values are JSON (UTF-8). Every message has `event_id` (unique), `event_type`, `event_time` (2018, ISO), `order_id`, `sent_at` (real time), `run_id`.

### 5.1 `orders` (3 partitions, key `order_id`) — written by producer
`event_type = order_placed`; fields: `seller_id, customer_zip_prefix, estimated_delivery_date, shipping_limit_ts` + the 23 checkout columns (Model 1 uses 12 of them; rules R5, R6, R7 use promise_days, dist_km, seller_late_rate_prior, seller_prior_orders).

### 5.2 `shipment-events` (3 partitions, key `order_id`) — written by producer
| event_type | extra fields |
|---|---|
| `order_approved` | `approval_lag_h` |
| `handed_to_carrier` | `approval_lag_h, handling_days, seller_overrun_days, post_approval_days, shipping_limit_ts` + 23 checkout columns (Model 3 uses 10) |
| `delivered` | `is_late_actual` |
Common: `seller_id, customer_state, estimated_delivery_date`.

### 5.3 `delay-predictions` (3 partitions, key `order_id`) — written by scorer
`prediction_id = order_id:stage`, `stage` (placed / approved / handoff / delivered), `model` (model1 / model3 / none), `raw_score`, `late_probability`, `alert` (bool), `threshold`, `reason_tags` (list), `customer_state`, `seller_id`, `estimated_delivery_date`, `is_late_actual` (delivered only), `scored_at`, `scorer_instance`.

### 5.4 `alerts` (1 partition, key `order_id` or `state`) — written by scorer and aggregator
`alert_id`, `alert_type` (order_risk / region_surge / stuck_order / slow_transit), `stage`, `severity`, `score`, `reasons`, `customer_state`, `seller_id`, `event_time`, `created_at`, `source`.

- [x] `common/schemas.py` with required fields per message type + validation function
- [x] `infra/kafka/create_topics.py` (creates 4 topics if missing, prints partitions)

---

## 6. Components

### 6.1 Replay producer (`producer/`)
- [x] Load `orders_stream.csv` (→ `order_placed`) and `shipment_events_enriched.csv`; merge into one timeline sorted by `event_time`, tie-break placed → approved → handoff → delivered
- [x] Skip `bad_timestamp = 1` events; write them to `rejected_events` with reason `event_before_purchase` (through a small DB call or a log file — decide in build)
- [x] Simulated clock: `real_due = run_start + (event_time − sim_start) / speed`; sleep until due; settings `--speed` (sim seconds per real second, default 1440 = 1 day/min), `--start`, `--end`, `--max-events`
- [x] Producer settings: `acks=all`, `enable.idempotence=true`, retries, `linger.ms` batching, compression
- [x] Async send with delivery callback; count confirmed / failed
- [x] Resume: save last sent index to a checkpoint file every N events; `--resume` continues from it
- [x] Prometheus metrics: `events_sent_total{topic,event_type}`, `delivery_failures_total`, `sim_clock_timestamp`, `schedule_lag_seconds`
- [x] Graceful shutdown (flush on Ctrl+C / SIGTERM)

### 6.2 Scorer, 3 copies (`scorer/`)
- [x] Consumer group `scorer`, subscribes to `orders` + `shipment-events`; `enable.auto.commit=false`
- [x] Load both model bundles once at start; check stored versions match installed versions (warn if not)
- [x] Validate each message (schema, types, required features); invalid → `rejected_events` + metric, never crash
- [x] Stage logic:
  - `order_placed` → Model 1 (12 columns) → raw score, calibrated probability, alert flag; tags R5, R6, R7
  - `order_approved` → no model; tag R1 if `approval_lag_h > 24`
  - `handed_to_carrier` → Model 3 (10 columns); tag R2 if `seller_overrun_days > 0`
  - `delivered` → record `is_late_actual`
- [x] Alert policy: raise `order_risk` alert when the model flag is true (this is R9, high model risk); always attach the rule tags that fired as reasons; R2 alone also raises an alert (lift 4.2×)
- [x] Produce to `delay-predictions` (+ `alerts` when raised), wait for delivery, **then** commit the input offset (at-least-once)
- [x] Rebalance callbacks log assigned / revoked partitions (for the consumer-group demo)
- [x] Prometheus metrics: `messages_consumed_total{topic}`, `predictions_total{stage,model}`, `alerts_total{reason}`, `scoring_seconds` histogram, `rejected_total{reason}`, `assigned_partitions` gauge
- [-] cut: (opt) Transactional producer: prediction + alert published atomically (CIE program 6)

### 6.3 DB writer (`db_writer/`)
- [x] Consumer group `db-writer`, subscribes to `delay-predictions` + `alerts`
- [x] Batch insert (e.g. every 500 messages or 1 s) with `INSERT … ON CONFLICT DO NOTHING/UPDATE` → duplicates after a crash are harmless
- [x] Commit offsets only after the DB transaction succeeds
- [x] Maintain `order_status` (latest stage, latest score per order) via upsert
- [x] Metrics: `rows_written_total{table}`, `duplicates_skipped_total`, `batch_write_seconds`

### 6.4 Window aggregator (`window_aggregator/`)
- [x] Consumer group `window-aggregator`, subscribes to `delay-predictions`
- [x] 1-day tumbling windows on `event_time`, keyed by `customer_state`: orders placed, avg Model 1 risk, approvals, R1 count, handoffs, avg Model 3 risk, deliveries, late deliveries, late rate, alerts
- [x] Per-window seller summary → top 10 delay-prone sellers (by late deliveries and avg Model 3 risk, min-volume filter)
- [x] Watermark = max event_time seen − grace (default 6 simulated hours); window closes when watermark passes its end; late events after close → counted in `late_events_total`, applied as an update
- [x] Every N seconds, in **one** DB transaction: upsert closed windows, save open-window state, save consumer offsets → on restart, load state + seek to saved offsets (exactly-once results)
- [x] R8 region surge: on each closed day, 7-day rolling late rate per state ≥ 15% with ≥ 50 deliveries → `region_surge` alert to `alerts`
- [x] Metrics: `windows_closed_total`, `open_windows`, `late_events_total`, `surge_alerts_total`, `state_checkpoint_seconds`

### 6.5 Stuck / slow-transit check (`ops/stuck_order_check.py`) (opt)
- [-] cut: Runs every simulated day using the replay clock from the DB
- [-] cut: R3: handed over > 8 simulated days ago, no delivery yet → `slow_transit` alert
- [-] cut: R4: promised date passed, no delivery yet → `stuck_order` alert

---

## 7. Database (`infra/postgres/init.sql`)

| Table | Key | Purpose |
|---|---|---|
| `predictions` | (`order_id`, `stage`) | Every stage result: model, raw score, probability, alert, reason tags, event_time, processed_at, scorer_instance |
| `alerts` | `alert_id` | All alerts with type, reasons, severity, state, seller, event_time |
| `order_status` | `order_id` | Latest stage, latest score/probability, alert flag, promised date, actual outcome |
| `state_daily_windows` | (`customer_state`, `window_start`) | Window aggregates |
| `seller_daily_windows` | (`seller_id`, `window_start`) | Seller aggregates (top-10 view built on it) |
| `aggregator_state` | `id` | Open-window state (JSON) + saved offsets |
| `rejected_events` | `id` | Bad / invalid messages with reason and raw payload |
| `replay_runs` | `run_id` | Speed, start/end, started_at, finished_at |

Views: `v_live_risk` (open orders ranked by latest probability), `v_predicted_vs_actual` (per day: mean predicted vs actual late rate, by model), `v_top_sellers`.

- [x] Write `init.sql` (tables, indexes on event_time / state, views)
- [x] Test the SQL locally in the Postgres container (PostgreSQL 16, all tables/views + every dashboard query)

---

## 8. Monitoring

### 8.1 Prometheus (`infra/prometheus/prometheus.yml`)
- [x] Scrape producer, 3 scorers, DB writer, aggregator (each exposes `/metrics`), Kafka lag exporter, Prometheus itself
- [-] cut: (opt) postgres-exporter

### 8.2 Grafana (provisioned automatically)
- [x] Data sources: PostgreSQL, Prometheus
- [x] **Business dashboard** (event time): live risk list · alert feed with reasons · late rate by state and day (heatmap) · Model 1 vs Model 3 predicted vs actual late rate · top delay-prone sellers · R8 surge alerts · funnel of stages
- [x] **System dashboard** (real time): events/s per topic · consumer lag per group and partition · scoring latency p50/p95 · partitions per scorer copy (shows rebalance) · rejected messages · DB write rate
- [~] Alert rules: (1) consumer lag above limit for 1 min; (2) region surge alert appeared; (3) rejected messages spike — done by hand in Grafana UI (README §8)

---

## 9. Docker Compose

| Service | Image | Notes |
|---|---|---|
| `kafka` | apache/kafka (KRaft, single broker) | host port 9094 |
| `kafka-init` | our image | runs `create_topics.py`, then exits |
| `kafka-ui` | provectuslabs/kafka-ui | browse topics, partitions, groups (port 8080) |
| `postgres` | postgres:16 | runs `init.sql` on first start (port 5432) |
| `producer` | our image | started on demand (`docker compose run producer …`) |
| `scorer` | our image | `deploy.replicas: 3` |
| `db-writer` | our image | |
| `window-aggregator` | our image | |
| `kafka-exporter` | danielqsj/kafka-exporter | consumer lag for Prometheus |
| `prometheus` | prom/prometheus | port 9090 |
| `grafana` | grafana/grafana | port 3000, provisioned dashboards |

- [x] Write Dockerfiles (python:3.12-slim, pinned requirements)
- [x] Health checks and `depends_on` with conditions
- [x] Named volumes: Kafka data, Postgres data, Grafana data, aggregator / producer checkpoints
- [x] Image versions pinned (exact tags chosen during build)
- [x] Docker Desktop runs the full stack

---

## 10. Tests

Offline (run in the build environment, no Kafka needed):
- [x] `test_replay_schedule.py` — merged order, 1 day = 1 min timing, start/end filters, bad events skipped
- [x] `test_schemas_rules.py` (schemas + rules) — every message type validates; broken messages rejected with a reason
- [x] `test_scoring.py` (stand-in bundles; real bundles checked in Docker by `ops/verify_models.py`) — both bundles load; Model 1/Model 3 scores on real rows match the saved test predictions
- [x] rules covered in `test_schemas_rules.py`
- [x] `test_windows.py` — window assignment, watermark close, late events, restart from saved state gives identical totals, R8 trigger

- [x] Offline end-to-end (no Kafka): replay → scorer logic → DB writer SQL (written twice, no duplicates) → windows with crash/restore (totals exact) → all 14 business panel queries OK

Live (on the laptop with Docker):
- [x] `ops/verify_models.py` inside Docker → Model 1 OK, Model 3 OK (max difference 3e-8 on 24,928 / 24,774 orders)
- [x] `docker compose up` → all 13 services up; 4 topics created with partitions 3/3/3/1; scorer group rebalanced to 1 orders + 1 shipment-events partition per copy
- [x] Short replay 1–2 Jun 2018: 1,102 events sent and confirmed = 1,102 predictions stored (319 placed, 304 approved, 194 handoff, 285 delivered)
- [x] Dashboards show data (business view: 20 May–20 Jul filled; R8 surges PR/MG/RJ/BA in June; alert severities checked)

---

## 11. Fault-tolerance and demo scenarios (`ops/fault_demo.md`)

- [x] **Scorer copy dies:** stop scorer-2 → scorer-1 took orders 1 + shipment-events 1 (2→4 partitions, Grafana panel) → restart → scorer-2 got partition 1 of both topics back. Logs + screenshots saved. No loss: 57,752 stored vs 57,736 sent (sent count saved every 10 s).
- [x] **DB writer down:** lag grew to ~3,500 → restart → back to 0 within seconds; 64,599 rows = 64,599 distinct (order, stage). Screenshot saved.
- [x] **Aggregator crash mid-window:** `docker kill` → restart → RESTORED state (watermark 2018-08-10 10:48, offsets 22524/22199/21734) → window total 17,046 = 17,046 placed orders (exact)
- [x] **Bad message:** 4 injected → 4 rows in `rejected_events` (bad_json, missing_fields, unknown_event_type, bad_event_time) caught by scorer-1 and scorer-3; nothing crashed
- [x] **Producer restart:** (after fix) stopped at event 1,492 (sim 22 Jul 10:37) → `--resume` → run 3353fa7f resumed at event 1,490 with original settings (start 2018-07-20), continued 22 Jul 14:41 … — 2 re-sent events skipped as repeats. (First attempt had exposed the resume bug, now fixed.)
- [x] **Broker restart:** `docker compose restart kafka` → partitions 0 → back to 2, scorer lag spike ~60, latency p95 ~2 s → normal; db-writer exited and Docker restarted it automatically; 72,810 rows = 72,810 distinct
- [x] `demo_reset.ps1`: one command = wipe, build, up, wait for scorers, fill history 20 May–20 Jul
- [ ] Rehearse the full demo end to end from a clean start, at least twice

- [x] `ops/fault_demo.md` written (commands + what to show + what to say)

Demo script (≈ 15 min): architecture slide → `docker compose up` → Kafka UI (topics, partitions, groups) → start replay at 1 day = 1 min from ~20 May → follow one order through all 4 stages → business dashboard → system dashboard → kill a scorer (rebalance) → bad message → aggregator restart → results slide.

---

## 12. Documentation and paper

- [x] `README.md`: requirements, setup, run commands, settings, ports, troubleshooting
- [x] Architecture diagram updated: no Spark, 4 topics, window aggregator, Prometheus → `docs/architecture.png` (2480×3360) + `.svg`
- [ ] Methodology diagram updated to match
- [ ] Technical paper: abstract · introduction · related work (tabular ML benchmarks, delay prediction) · dataset · offline method (split, features, Model 1, Model 3) · streaming architecture · fault tolerance · results (model + system: throughput, latency, lag) · limitations (surge under-reaction, calibration, replay warm-up) · conclusion
- [ ] Measure system numbers during a full run: events/s, scoring latency p95, end-to-end delay, lag recovery time after a scorer kill
- [ ] Presentation slides

---

## 13. Timeline

**Compressed to 3 days (decided 2 Nov):** Claude builds and offline-tests all code in one go; the laptop is used only for live runs. Cut: R3/R4 timer check, transactional publish, postgres-exporter, provisioned Grafana alert rules (one alert rule is added by hand in the Grafana UI, 2 minutes, steps in README).

| Day | Work | Who |
|---|---|---|
| Day 0 (2–3 Nov night) | All code, configs, dashboards, tests, README → project zip | Claude |
| Day 1 | `docker compose up`, fix issues, short replay, check counts and dashboards | Team + Claude |
| Day 2 | Full replay, fault-tolerance scenarios, measurements, screenshots | Team + Claude |
| Day 3 | Paper, slides, diagrams, rehearsal | Team + Claude |

---

## 14. Progress log

| Date | What was done |
|---|---|
| 2 Nov 2026 (a) | Models finalised (Model 1 XGB_12, Model 3 XGBoost-10). Events file enriched. Results summary + zip. Circular read; plan agreed: 4 topics, no Spark, PostgreSQL, Prometheus + Grafana, Docker Compose, replay at 1 day = 1 min. plan.md created. |
| 2–3 Nov 2026 (b) | Whole project built in `olist-delay-streaming/`: Docker Compose (13 services), 4 topics, init.sql, producer, scorer ×3, DB writer, window aggregator, Prometheus, 2 Grafana dashboards, README, fault_demo.md. Offline: 17 tests pass; end-to-end without Kafka against real PostgreSQL passes. Fixes found by tests: 113 orders with missing dist_km would have been rejected (fixed); R8 now alerts only when a state first crosses 15%; PostgreSQL host port 5433. Data quirk documented: 675 orders approved after handoff. **Next: Day 1 live run on laptop.** |
| 3 Nov 2026 00:30 (c) | **Live on laptop (A:\olist-delay-streaming).** Fixed compose build race (only kafka-init builds the image, `pull_policy: never`). All services up; topics 3/3/3/1; scorer rebalance seen in logs. verify_models: both OK. Smoke replay 1–2 Jun: 1,102 sent = 1,102 stored. **Next: dashboards check, full replay, fault demos.** |
| 3 Nov 2026 00:45 (d) | Window aggregator now skips re-delivered predictions (prediction_id per day, saved in checkpoint) — 18 tests pass. Clean restart (`down -v`), history fill 20 May–20 Jul at 1 day/s: 11.0k placed, 10.9k handoffs, 11.2k delivered, 1.73k order alerts (561 high, 716 R2-only medium, 450 Model 1 medium), 6 R8 surges, 154 rejected. Model 3 "100%" checked: 45 test orders at 1.0, all actually late. **Next: system dashboard, live replay at 1 day/min, fault demos 1–6, screenshots.** |
| 3 Nov 2026 00:55 (e) | Live replay at 1 day/min from 20 Jul. Fault demo 1 done (rebalance seen in logs and Grafana). System numbers: end-to-end latency p95 ≈ 0.8–1 s (0.5 s batching); lag peaks ≈ 300 in bursts, back to ~0 within seconds. **Next: lost-message check, demos 2–6.** |
| 3 Nov 2026 01:06 (f) | Demo 1 loss check OK (57,752 stored / 57,736 sent). Demo 2 done: db-writer lag ~3.5k → 0 in seconds, no duplicates. **Next: demos 3 (aggregator crash), 4 (bad messages), 5 (producer resume), 6 (broker restart).** |
| 3 Nov 2026 01:11 (g) | Demo 3 done: aggregator hard-killed, restored from DB checkpoint, window total = placed orders (17,046). Demo 4 done: 4 bad messages rejected with correct reasons. Tip: psql `-P pager=off`. **Next: demo 5 (producer resume), demo 6 (broker restart).** |
| 3 Nov 2026 01:16 (h) | Demo 6 done: Kafka broker restart, full recovery without intervention, no duplicates. Demo 5 (producer --resume): result not yet confirmed. **Next: confirm demo 5, then Day 3: diagrams, paper, slides, rehearsal.** |
| 3 Nov 2026 01:35 (i) | Demo 5 found the resume bug (above) — fixed + allowed lateness added + `demo_reset.ps1` + README "Presentation day" section. Presentation-day flow decided: fresh system (`demo_reset.ps1`, ~2–3 min), then live from 20 Jul at 1 day/min, 2–3 fault demos live. |
| 3 Nov 2026 01:45 (j) | Docker build-cache error fixed with `docker builder prune -f` + `build --no-cache`. `demo_reset.ps1` works end to end (fresh stack + 44,123-event history fill). Demo 5 passed after fix. **Build + test phase complete. Next: architecture diagram, technical paper, slides, rehearsal.** |
| 3 Nov 2026 02:20 (k) | Demo guide doc (commands, key ideas, rules R1–R9, 6 fault demos, presentation-day order). Architecture diagram v2 saved to `docs/`. |
