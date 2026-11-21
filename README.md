# Olist delivery-delay prediction — real-time streaming pipeline

Kafka-centred pipeline that replays real Olist orders (May–Oct 2018) and predicts late delivery at each stage:
**Model 1** when the order is placed, a rule tag at approval, **Model 3** at carrier handoff, and the real outcome at delivery.

```
orders_stream.csv ─┐                       ┌─► delay-predictions ─┬─► db-writer ──────────► PostgreSQL ─► Grafana (business)
                   ├─► replay producer ─► orders / shipment-events ─► scorer ×3 ──┤                      └─► window-aggregator ─► PostgreSQL (1-day windows)
shipment_events ───┘     (simulated clock)   (3 partitions, key = order_id)   (group "scorer")   └─► alerts ─► db-writer         └─► alerts (R8 region surge)
                                                                                     Prometheus ◄── metrics from every service + Kafka lag exporter ─► Grafana (system)
```

| Circular requirement | Where |
|---|---|
| Kafka central, producer + consumer | `producer/`, `scorer/`, `db_writer/`, `window_aggregator/` |
| ≥ 2 topics | `orders`, `shipment-events`, `delay-predictions`, `alerts` |
| Partitioning + consumer groups | 3 partitions keyed by `order_id`; `scorer` group with 3 copies; `db-writer`, `window-aggregator` groups |
| Window operation | 1-day tumbling event-time windows per state and seller (`window_aggregator/windows.py`) |
| Aggregation | late rate, average risk, alert counts, top sellers, 7-day rolling late rate |
| Fault tolerance | manual offset commits after work is saved, idempotent upserts, aggregator state + offsets in one DB transaction, bad messages to `rejected_events`, producer acks=all + idempotence + `--resume` |
| Storage | PostgreSQL 16 |
| Visualisation / alerts | Grafana dashboards, `alerts` topic |

---

## 1. Requirements

* **Docker Desktop** running, with **at least 6 GB memory** (Settings → Resources → Memory).
* Free ports on your laptop: **9094** (Kafka), **8080** (Kafka UI), **5433** (PostgreSQL), **9090** (Prometheus), **3000** (Grafana).
  Kafka uses 9094 and PostgreSQL 5433 so they don't clash with a Kafka or PostgreSQL already installed on your laptop. You do not need your local Kafka for this project.
* `data/` must contain `orders_stream.csv` and `shipment_events_enriched.csv`; `models/` must contain both `.joblib` files and both `*_test_predictions.csv` files (already included in the zip).

All commands below are run in a terminal **inside this folder** (VS Code terminal is fine; PowerShell or bash).

## 2. Start everything

```bash
docker compose build                 # first time: ~3–5 min (installs scikit-learn 1.6.1, xgboost 3.4.1, …)
docker compose up -d                 # starts Kafka, creates topics, PostgreSQL, 3 scorers, db-writer, aggregator, monitoring
docker compose ps                    # all "running" / "healthy"; kafka-init shows "exited (0)"
docker compose logs kafka-init       # should list the 4 topics with partitions 3/3/3/1
docker compose logs scorer-1         # "Model 1 (12 cols, thr 0.7493) | Model 3 (10 cols, thr 0.7364) loaded" + ASSIGNED partitions
```

## 3. Check the models inside Docker (once)

```bash
docker compose run --rm --entrypoint python scorer-1 ops/verify_models.py
```
Expected: `Model 1: … max |difference| … -> OK` and `Model 3: … -> OK`. This proves the streaming code gives the same scores as Colab.

## 4. Smoke test — 2 simulated days in ~2 seconds per day

```bash
docker compose run --rm --name producer producer --start 2018-06-01 --end 2018-06-03 --speed 86400
docker compose exec postgres psql -U olist -d olist -c "SELECT stage, COUNT(*) FROM predictions GROUP BY stage ORDER BY 1;"
```
`--speed 86400` = 1 simulated day per real second. You should see counts for placed / approved / handoff / delivered.

## 5. Open the UIs

| UI | URL | Login |
|---|---|---|
| Grafana — business dashboard (home) | http://localhost:3000 | admin / admin |
| Grafana — system dashboard | Dashboards → *Olist delay streaming* → system view | |
| Kafka UI (topics, partitions, consumer groups) | http://localhost:8080 | |
| Prometheus (targets, raw metrics) | http://localhost:9090/targets | |

The business dashboard uses **simulated 2018 time** (default range 15 May – 31 Oct 2018). The system dashboard uses real time (last 15 minutes).

## 6. The demo replay (1 simulated day = 1 real minute)

```bash
docker compose run --rm --name producer producer                      # uses .env: start 2018-05-20, speed 1440
docker compose run --rm --name producer producer --start 2018-07-15   # start later
docker compose run --rm --name producer producer --speed 2880         # 1 day = 30 s
```
Stop with **Ctrl+C**; continue later with `--resume` — it reuses the original run's start, end and speed automatically, so just type `--resume`. Each order's approval, handoff and delivery are sent later, after the same gap they had in 2018, interleaved with every other order's events.

### Presentation day (fresh system every time)

The CSV files are only read, never used up — replay them as often as you like; results are identical every time.

```powershell
powershell -ExecutionPolicy Bypass -File .\demo_reset.ps1          # wipe, start, fill 20 May - 20 Jul (~2-3 min)
docker compose run --rm --name producer producer --start 2018-07-20   # LIVE in front of the examiners, 1 day / min
```
`demo_reset.ps1` runs `docker compose down -v` (deletes all data), `build`, `up -d`, waits for the scorers, then fills the history at 1 simulated day per second.

## 7. Fault-tolerance demos

See **`ops/fault_demo.md`** — scorer copy killed (rebalance), DB writer down (lag + catch-up, no duplicates), window aggregator crash (identical totals), bad messages, producer resume, broker restart.

## 8. Add a Grafana alert rule (2 minutes, by hand)

Grafana → Alerting → Alert rules → **New alert rule**:
1. Query (Prometheus): `sum(kafka_consumergroup_lag{consumergroup="scorer"})`
2. Threshold: **IS ABOVE 500**; evaluate every 10s, pending period 30s.
3. Folder *Olist*, group *pipeline*, name *Scorer lag high* → Save.
Second rule (PostgreSQL, table format): `SELECT COUNT(*) FROM alerts WHERE alert_type='region_surge' AND processed_at > now() - interval '1 minute'` IS ABOVE 0 → *Region surge detected*.

## 9. Useful SQL

```bash
docker compose exec postgres psql -U olist -d olist
```
```sql
SELECT stage, COUNT(*) FROM predictions GROUP BY 1;                               -- one row per order per stage
SELECT * FROM v_live_risk ORDER BY late_prob_pct DESC LIMIT 10;                   -- riskiest open orders
SELECT * FROM alerts ORDER BY event_time DESC LIMIT 10;                           -- alert feed
SELECT * FROM state_daily_windows ORDER BY window_start DESC, late_rate DESC LIMIT 20;
SELECT reason, COUNT(*) FROM rejected_events GROUP BY 1;                          -- bad / skipped messages
SELECT * FROM replay_runs ORDER BY started_at DESC;
```

## 10. Reset

```bash
docker compose down          # stop, keep data
docker compose down -v       # stop and DELETE all data (Kafka topics, database, checkpoints) — fresh start
```

## 11. Settings (`.env`)

| Setting | Default | Meaning |
|---|---|---|
| `REPLAY_SPEED` | 1440 | simulated seconds per real second (1440 = 1 day/min, 86400 = 1 day/s) |
| `REPLAY_START` / `REPLAY_END` | 2018-05-20 / 2018-10-31 | replay window (2018 event time) |
| `R1_APPROVAL_HOURS` … `R7_*` | 24, 10, 1000, 0.15, 20 | rule thresholds |
| `R8_LATE_RATE`, `R8_MIN_DELIVERIES`, `R8_ROLLING_DAYS` | 0.15, 50, 7 | region surge rule |
| `WINDOW_GRACE_HOURS` | 6 | how long (simulated) a day window waits for late events before closing |
| `WINDOW_MAX_LATENESS_DAYS` | 7 | events older than this behind the watermark are ignored (allowed lateness) |
| `AGG_CHECKPOINT_SECONDS` | 5 | aggregator save interval |

## 12. Troubleshooting

| Problem | Fix |
|---|---|
| `port is already allocated` | another program uses that port — stop it or change the left-hand port in `docker-compose.yml` |
| scorer logs `version mismatch` | the image must have scikit-learn 1.6.1 and xgboost 3.4.1 (`requirements.txt`); rebuild with `docker compose build --no-cache` |
| Prometheus target `producer` is DOWN | normal when no replay is running |
| Grafana business panels empty | run a replay; check the time range is in 2018 |
| Kafka unhealthy after a crash | `docker compose down -v` and `up -d` again (deletes data) |
| Need to re-run from scratch | `docker compose down -v` then `up -d` |

## 13. Known data notes

* 154 handoff events are dated before their order was placed (Olist data error) → skipped, stored in `rejected_events` with reason `event_before_purchase`.
* 675 orders (2.7%) were approved *after* the carrier handoff in the raw data. The scorer handles each event on its own and `order_status` keeps the furthest stage, so this is harmless.
* The first days after 1 May look quiet because orders placed in April (whose handoffs/deliveries happen in early May) are not in the replay — hence the default start of 20 May.

## 14. Project layout

```
common/            config, Kafka factories, message schemas, DB helper
producer/          replay.py (timeline logic), producer.py
scorer/            models.py (bundles), rules.py (R1–R7), logic.py (stage logic), scorer.py
db_writer/         sql.py (idempotent upserts), db_writer.py
window_aggregator/ windows.py (tumbling windows, R8, checkpoint), aggregator.py
infra/             kafka/create_topics.py, postgres/init.sql, prometheus/, grafana/
ops/               verify_models.py, inject_bad_message.py, fault_demo.md
tests/             offline tests (pytest), no Kafka needed
```

Offline tests (optional, needs Python with pandas, scikit-learn, pytest): `python -m pytest -q tests`
