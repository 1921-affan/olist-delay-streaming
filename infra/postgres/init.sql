-- Olist delay streaming — database schema. Runs once, on the first start of the postgres container.

-- Every stage result for every order (written by db_writer from `delay-predictions`).
CREATE TABLE IF NOT EXISTS predictions (
    order_id                TEXT        NOT NULL,
    stage                   TEXT        NOT NULL,          -- placed / approved / handoff / delivered
    event_time              TIMESTAMP   NOT NULL,          -- simulated 2018 time
    model                   TEXT,                          -- model1 / model3 / none
    raw_score               DOUBLE PRECISION,
    late_probability        DOUBLE PRECISION,
    alert                   BOOLEAN     NOT NULL DEFAULT FALSE,
    threshold               DOUBLE PRECISION,
    reason_tags             TEXT[]      NOT NULL DEFAULT '{}',
    customer_state          TEXT,
    seller_id               TEXT,
    estimated_delivery_date TIMESTAMP,
    is_late_actual          SMALLINT,
    scorer_instance         TEXT,
    scored_at               TIMESTAMPTZ,
    processed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, stage)
);
CREATE INDEX IF NOT EXISTS idx_pred_event_time ON predictions (event_time);
CREATE INDEX IF NOT EXISTS idx_pred_stage_time ON predictions (stage, event_time);

-- All alerts (order risk from the scorer, region surge from the window aggregator).
CREATE TABLE IF NOT EXISTS alerts (
    alert_id        TEXT        PRIMARY KEY,
    alert_type      TEXT        NOT NULL,                  -- order_risk / region_surge
    order_id        TEXT,
    stage           TEXT,
    severity        TEXT,
    score           DOUBLE PRECISION,
    reasons         TEXT[]      NOT NULL DEFAULT '{}',
    customer_state  TEXT,
    seller_id       TEXT,
    event_time      TIMESTAMP   NOT NULL,
    source          TEXT,
    created_at      TIMESTAMPTZ,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_alerts_event_time ON alerts (event_time);

-- Latest known state of each order (one row per order, kept up to date by db_writer).
CREATE TABLE IF NOT EXISTS order_status (
    order_id                TEXT        PRIMARY KEY,
    customer_state          TEXT,
    seller_id               TEXT,
    estimated_delivery_date TIMESTAMP,
    placed_time             TIMESTAMP,
    current_stage           TEXT,
    stage_rank              SMALLINT    NOT NULL DEFAULT 0,
    last_event_time         TIMESTAMP,
    m1_probability          DOUBLE PRECISION,
    m1_alert                BOOLEAN,
    m3_probability          DOUBLE PRECISION,
    m3_alert                BOOLEAN,
    latest_probability      DOUBLE PRECISION,
    any_alert               BOOLEAN     NOT NULL DEFAULT FALSE,
    reason_tags             TEXT[]      NOT NULL DEFAULT '{}',
    is_late_actual          SMALLINT,
    delivered_time          TIMESTAMP,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_status_stage ON order_status (stage_rank);

-- 1-day tumbling windows per customer state (window_aggregator).
CREATE TABLE IF NOT EXISTS state_daily_windows (
    customer_state      TEXT        NOT NULL,
    window_start        TIMESTAMP   NOT NULL,
    window_end          TIMESTAMP   NOT NULL,
    orders_placed       INTEGER     NOT NULL DEFAULT 0,
    avg_m1_probability  DOUBLE PRECISION,
    m1_alerts           INTEGER     NOT NULL DEFAULT 0,
    approvals           INTEGER     NOT NULL DEFAULT 0,
    r1_slow_approvals   INTEGER     NOT NULL DEFAULT 0,
    handoffs            INTEGER     NOT NULL DEFAULT 0,
    avg_m3_probability  DOUBLE PRECISION,
    m3_alerts           INTEGER     NOT NULL DEFAULT 0,
    deliveries          INTEGER     NOT NULL DEFAULT 0,
    late_deliveries     INTEGER     NOT NULL DEFAULT 0,
    late_rate           DOUBLE PRECISION,
    rolling7_deliveries INTEGER,
    rolling7_late_rate  DOUBLE PRECISION,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_state, window_start)
);

-- 1-day tumbling windows per seller (window_aggregator) — source of "top delay-prone sellers".
CREATE TABLE IF NOT EXISTS seller_daily_windows (
    seller_id           TEXT        NOT NULL,
    window_start        TIMESTAMP   NOT NULL,
    handoffs            INTEGER     NOT NULL DEFAULT 0,
    avg_m3_probability  DOUBLE PRECISION,
    r2_missed_deadline  INTEGER     NOT NULL DEFAULT 0,
    deliveries          INTEGER     NOT NULL DEFAULT 0,
    late_deliveries     INTEGER     NOT NULL DEFAULT 0,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (seller_id, window_start)
);

-- Window aggregator checkpoint: open-window state + Kafka offsets, saved in one transaction.
CREATE TABLE IF NOT EXISTS aggregator_state (
    id          TEXT        PRIMARY KEY,
    state       JSONB       NOT NULL,
    offsets     JSONB       NOT NULL,
    saved_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Messages that could not be used (bad JSON, missing fields, event dated before its order, ...).
CREATE TABLE IF NOT EXISTS rejected_events (
    id              BIGSERIAL   PRIMARY KEY,
    source          TEXT        NOT NULL,
    reason          TEXT        NOT NULL,
    detail          TEXT,
    topic           TEXT,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    payload         TEXT,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic, kafka_partition, kafka_offset)
);

-- One row per replay run.
CREATE TABLE IF NOT EXISTS replay_runs (
    run_id          TEXT        PRIMARY KEY,
    speed           DOUBLE PRECISION,
    sim_start       TIMESTAMP,
    sim_end         TIMESTAMP,
    events_planned  INTEGER,
    events_sent     INTEGER     DEFAULT 0,
    sim_clock       TIMESTAMP,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

-- ---------------------------------------------------------------- views for Grafana

-- "Now" in simulated time = latest event stored.
CREATE OR REPLACE VIEW v_sim_now AS
SELECT COALESCE(MAX(event_time), TIMESTAMP '2018-05-01') AS sim_now FROM predictions;

-- Open orders (not delivered yet) ranked by their latest late probability.
CREATE OR REPLACE VIEW v_live_risk AS
SELECT s.order_id, s.customer_state, s.seller_id, s.current_stage,
       ROUND(s.latest_probability::numeric * 100, 1)        AS late_prob_pct,
       ROUND(s.m1_probability::numeric * 100, 1)            AS model1_pct,
       ROUND(s.m3_probability::numeric * 100, 1)            AS model3_pct,
       s.any_alert, array_to_string(s.reason_tags, ', ')    AS reasons,
       s.placed_time, s.estimated_delivery_date, s.last_event_time
FROM order_status s
WHERE s.stage_rank < 4 AND s.latest_probability IS NOT NULL;

-- Per simulated day: mean predicted late probability (per model) vs actual late rate,
-- grouped by the day the order was PLACED, for orders that are already delivered.
CREATE OR REPLACE VIEW v_predicted_vs_actual AS
SELECT date_trunc('day', s.placed_time)           AS day,
       COUNT(*)                                   AS delivered_orders,
       AVG(s.m1_probability)                      AS model1_mean_prob,
       AVG(s.m3_probability)                      AS model3_mean_prob,
       AVG(s.is_late_actual::double precision)    AS actual_late_rate
FROM order_status s
WHERE s.stage_rank = 4 AND s.placed_time IS NOT NULL
GROUP BY 1;

-- Top delay-prone sellers over the last 7 simulated days.
CREATE OR REPLACE VIEW v_top_sellers_7d AS
SELECT w.seller_id,
       SUM(w.deliveries)                                            AS deliveries,
       SUM(w.late_deliveries)                                       AS late_deliveries,
       ROUND((SUM(w.late_deliveries)::numeric / NULLIF(SUM(w.deliveries), 0)) * 100, 1) AS late_rate_pct,
       SUM(w.handoffs)                                              AS handoffs,
       SUM(w.r2_missed_deadline)                                    AS missed_deadlines,
       ROUND((SUM(w.avg_m3_probability * w.handoffs) / NULLIF(SUM(w.handoffs), 0))::numeric * 100, 1) AS avg_model3_pct
FROM seller_daily_windows w, v_sim_now n
WHERE w.window_start >= date_trunc('day', n.sim_now) - INTERVAL '7 days'
GROUP BY w.seller_id
HAVING SUM(w.deliveries) >= 3;

-- Stage funnel: how many orders are at each stage right now.
CREATE OR REPLACE VIEW v_stage_funnel AS
SELECT current_stage, stage_rank, COUNT(*) AS orders
FROM order_status GROUP BY current_stage, stage_rank;
