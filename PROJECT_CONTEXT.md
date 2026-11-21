# Project context — Olist delivery-delay streaming (upload this file to restore context in a new chat)

**Course:** AI372IA Stream Processing and Analytics (Sem VII). Phase II demo 19–26 Nov 2026, technical paper due at the demo, Lab CIE 14–18 Dec 2026.
**Folder on the laptop:** `A:\olist-delay-streaming` (Windows, Docker Desktop, VS Code, PowerShell).
**Status (3 Nov 2026):** built, tested live, all circular requirements and all 6 fault-tolerance demos passed. Left: technical paper, slides, methodology diagram, rehearsal. Full detail and progress log: `plan.md`.

## What the system does
Predicts whether an Olist order will be delivered late, at each stage of its life, on a replayed stream of real May–Oct 2018 orders.
- Replay producer (simulated clock, 1 day = 1 min) → topics `orders`, `shipment-events` (3 partitions, key = order_id)
- Scorer ×3 (consumer group `scorer`): Model 1 at order placed, rule R1 at approval, Model 3 at carrier handoff, outcome at delivery → topics `delay-predictions` (3 partitions), `alerts` (1 partition)
- DB writer (idempotent upserts) and window aggregator (1-day windows per state/seller, R8 region surge, state + offsets saved in one DB transaction) → PostgreSQL 16
- Prometheus + Kafka lag exporter + Grafana (business view, system view); all in one `docker-compose.yml`
- Architecture diagram: `docs/architecture.png`

## Models (trained in Colab; scikit-learn 1.6.1, xgboost 3.4.1)
| Model | Stage | Columns | Test PR-AUC | Test ROC-AUC | Threshold |
|---|---|---|---|---|---|
| Model 1 (XGB_12) | order placed | 12 | 0.104 | 0.725 | 0.7493 |
| Model 3 (XGBoost) | carrier handoff | 10 | 0.292 | 0.829 | 0.7364 |
Split: train Jan 2017–Mar 2018, calibrate Apr 2018 (threshold at 10% FPR + isotonic probabilities), test May–Aug 2018 (24,928 orders, 1,114 late). Full numbers: `Olist_Delay_Prediction_Results_Summary.md` (results zip).

## Fault-tolerance results (live)
1 scorer killed → rebalance, no loss (57,752 stored / 57,736 sent) · 2 DB writer down → lag 3.5k → 0, 64,599 rows = 64,599 distinct · 3 aggregator `docker kill` → restored, window total 17,046 = 17,046 · 4 bad messages → 4 rejected with reasons · 5 producer `--resume` → same timeline · 6 broker restart → full recovery, 72,810 = 72,810 distinct.

## Day-of commands
`powershell -ExecutionPolicy Bypass -File .\demo_reset.ps1` (fresh system + history fill) → `docker compose run --rm --name producer producer --start 2018-07-20` (live). Demo steps: `ops/fault_demo.md` and the "Demo Commands & Fault-Tolerance Guide" doc.

## Lessons learned (reuse in the next Kafka project)
- **One image, many services:** only ONE service gets `build:`; the others use `image:` + `pull_policy: never` (otherwise parallel builds collide: "image already exists").
- **Ports:** expose Kafka on 9094 and PostgreSQL on 5433 so they never clash with local installs.
- **Kafka in Docker (KRaft):** internal listener `kafka:9092`, external `localhost:9094`; a one-shot `kafka-init` service creates topics; services wait on `service_completed_successfully`.
- **Docker build error "parent snapshot … does not exist":** `docker builder prune -f` then `docker compose build --no-cache`.
- **psql stuck at `(END)`:** press `q`; use `-P pager=off`.
- **Grafana "invalid username or password" after resets:** 5 failed tries = 5-minute lockout; type `admin`/`admin` by hand.
- **Reliability pattern that passed every demo:** producer `acks=all` + idempotence + retries + delivery callbacks; consumers commit offsets only after outputs/DB writes succeed (at-least-once); every write idempotent (primary keys + `ON CONFLICT`); stateful consumer saves state + offsets in one DB transaction; bad messages → a `rejected_events` table, never a crash; `restart: unless-stopped`.
- **Windowed consumer must de-duplicate** re-delivered records (per-day id sets in the checkpoint) and **drop events beyond an allowed lateness** (7 days), or replays/resumes inflate old windows.
- **Producer resume must save the run's settings** (start, end, speed), not just the event index — an index means nothing on a different timeline.
- **Validate only identity fields** as required; model features may be missing (XGBoost handles NaN) — requiring them rejected 113 real orders.
- **Replay design:** one merged timeline sorted by event time + simulated clock; each message carries only what is known at that moment (no future features).
- **Test offline first:** pure-logic modules (replay, rules, scoring, windows) unit-tested without Kafka; schema and every dashboard SQL query run against a real PostgreSQL before the laptop run.
