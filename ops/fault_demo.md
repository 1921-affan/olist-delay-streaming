# Fault-tolerance demo script

Start a replay in one terminal first (so data is flowing), e.g.
`docker compose run --rm --name producer producer --start 2018-07-01 --speed 1440`
and keep **Grafana → system view** and **Kafka UI → Consumers** open.

Shortcut for SQL below: `docker compose exec postgres psql -U olist -d olist -c "<SQL>"`

---

## 1. A scorer copy dies → consumer-group rebalance (partitioning + consumer groups)

```bash
docker compose stop scorer-2
docker compose logs --tail 5 scorer-1 scorer-3        # "REVOKED … ASSIGNED …" — they took over scorer-2's partitions
```
* Grafana: *Partitions owned per scorer copy* — scorer-2 drops to 0, the others go up.
* Kafka UI → Consumers → `scorer`: 2 members, all 6 partitions still assigned.

```bash
docker compose start scorer-2                          # partitions are shared again
```
**Check nothing was lost** (after the replay has passed the moment of the kill):
```sql
SELECT COUNT(*) FROM predictions;                                        -- predictions stored
SELECT events_sent FROM replay_runs ORDER BY started_at DESC LIMIT 1;    -- events sent (same number once caught up)
```
Why: offsets are committed only after the predictions are confirmed by Kafka, so the messages scorer-2 had read but not finished are re-read by the new owner.

## 2. DB writer down → lag grows → catch-up without duplicates

```bash
docker compose stop db-writer
```
* Grafana: *Consumer lag per group* — `db-writer` climbs; business panels stop updating.
```bash
docker compose start db-writer
```
* Lag drops back to ~0 within seconds.
```sql
SELECT COUNT(*) AS rows, COUNT(DISTINCT (order_id, stage)) AS distinct_rows FROM predictions;   -- equal
```
Why: offsets committed only after the DB transaction; inserts are `ON CONFLICT DO NOTHING`.

## 3. Window aggregator crash in the middle of a window → identical totals

```sql
SELECT customer_state, window_start, orders_placed, deliveries FROM state_daily_windows
ORDER BY window_start DESC, customer_state LIMIT 5;
```
```bash
docker kill olist-delay-streaming-window-aggregator-1      # hard crash, no clean shutdown
docker compose start window-aggregator
docker compose logs --tail 5 window-aggregator             # "RESTORED window state saved at … | offsets {…}"
```
After the replay has moved on, the window rows keep counting correctly. Proof that nothing was double-counted:
```sql
SELECT SUM(orders_placed) FROM state_daily_windows;
SELECT COUNT(*) FROM predictions WHERE stage = 'placed';     -- the same number
```
Why: open windows + Kafka offsets are saved in **one** PostgreSQL transaction; on restart the aggregator
loads that state and seeks to exactly those offsets.

## 4. Bad messages → rejected, nothing crashes

```bash
docker compose run --rm --entrypoint python scorer-1 ops/inject_bad_message.py
```
```sql
SELECT source, reason, detail, topic FROM rejected_events ORDER BY id DESC LIMIT 5;
```
* Grafana: *Rejected messages and rebalances* shows the spike; all scorers keep running (`docker compose ps`).

## 5. Producer stopped → resume without gaps

Press **Ctrl+C** in the producer terminal, then:
```bash
docker compose run --rm --name producer producer --resume
```
It prints `resuming run … at event N (sim time …) with its original settings start=… end=… speed=…`
and continues from the checkpoint (saved every 10 s) on the SAME timeline. The few events sent after
the last checkpoint are sent again: the database keys (`order_id`, `stage`) ignore them and the window
aggregator skips prediction ids it has already counted.

## 6. Kafka broker restart

```bash
docker compose restart kafka
```
Producer and consumers log connection errors for a few seconds, then continue on their own
(producer: retries + idempotence; consumers: resume from committed offsets).

---

### What to say (one line each)
* **At-least-once processing**: commit after the work is safe → nothing lost; **idempotent writes** → repeats harmless → results are effectively exactly-once.
* **Consumer group**: 3 scorer copies share 6 partitions (3 per input topic); one dies → its partitions move to the others.
* **Stateful recovery**: window state and offsets are saved atomically, so a crash never double-counts.
