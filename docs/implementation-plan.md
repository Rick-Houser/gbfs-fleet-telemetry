# Implementation Plan & Design Decisions

## Build History

### 1. Infrastructure
Docker Compose running Postgres, Redis, and Grafana, with
`condition: service_healthy` dependency gating and Docker log
rotation configured on Redis. Schema is delivered as numbered
migrations under `infrastructure/init/` (`001_schema.sql`,
`002_ingest_run_log.sql`), run in order by Postgres's
`docker-entrypoint-initdb.d` convention — no migration framework
(Alembic/Flyway), since this is a dev-only Compose setup that's
recreated from scratch rather than migrated in place.

### 2. Ingestion
`src/poller/ingest.py` polls Lime's GBFS v2.2 `free_bike_status`
endpoint for San Francisco.

**Data source switch:** originally built against Lyft's Bay Wheels
GBFS feed. Bay Wheels' live dockless fleet turned out to have no
electric vehicles (no battery data — `current_fuel_percent` was
always null) and almost no disabled-vehicle events, leaving two of
the planned Grafana panels empty against real data. Rather than
seed synthetic data to fill that gap, switched to Lime, whose SF
fleet is fully motorized (scooters + e-assist bikes). Lime doesn't
publish a fuel-percent field directly — battery level is derived by
comparing each vehicle's `current_range_meters` against its
`max_range_meters` from the separate `vehicle_types` feed. All prior
Bay Wheels data was wiped (`docker compose down -v`) rather than
mixed with Lime data in the same tables, since blending two
operators' fleets silently would corrupt fleet-wide metrics like
availability rate.

### 3. Integration
Each cycle writes to two sinks with different guarantees:
* **Redis** — best-effort cache of current vehicle state, TTL'd
  slightly beyond the poll interval. A Redis failure degrades read
  latency only; it never blocks the Postgres write.
* **Postgres** — transactional source of truth. `dim_vehicle` is
  upserted idempotently; `fact_vehicle_status` is appended as an
  event ledger. Both writes commit or roll back together.

### 4. Scheduling & Resilience
Polling runs via **cron**, not an in-process loop. A continuous
scheduler (with signal handling and exponential backoff) was built
and tested, then deliberately replaced: at a 1-minute interval, cron
already provides scheduling, per-run isolation, and automatic
restart — a long-running daemon adds a process to keep alive without
adding real capability at this cadence. The loop pattern would be
justified for sub-minute intervals or state that needs to persist
across cycles, neither of which applies here.

### 5. Pipeline Observability
`ingest_run_log` records one row per cycle — vehicles fetched,
Redis/Postgres success flags, duration, and error message on
failure — written on its own database connection so a failed
fleet-data transaction can't also prevent the pipeline from
reporting on itself. Only a fetch failure triggers a nonzero exit
code (what cron sees); write failures degrade independently and are
visible in the log row instead.

### 6. Visualization
Grafana connects to Postgres directly (via the `postgres:5432` Docker
service name, not `localhost` — Grafana runs inside the Compose
network, unlike the Python script which runs on the host). Two
dashboards:

* **Fleet Status** — availability rate (gauge, current snapshot, not
  time-range scoped, so it always answers "right now"), battery
  health distribution, a critically-low-battery stat (≤5%, flagged
  separately since a dead vehicle is operationally different from a
  low-but-working one), and top 10 recurring problem vehicles (bar
  gauge ranked by disabled-event count) — a lightweight, honest
  stand-in for full MTBF analysis without building the batch engine.
* **Pipeline Health** — mapped explicitly to the four golden signals:
  freshness (seconds since last successful run, also unscoped by
  time range for the same "right now" reason as availability),
  errors (failure count + a detail table of actual error messages),
  latency (`duration_ms` over time), and traffic (`vehicles_fetched`
  over time — catches the empty-feed case even when both writes
  otherwise succeed, and reflects normal churn as vehicles enter/exit
  the rentable pool via rentals, not a partial-update artifact).

All time-scoped panels use Grafana's `$__timeFilter` macro
consistently, so every panel (except the two "right now" exceptions
above, which are labeled as such) respects the dashboard's time
range picker.

**Battery Health Distribution is a manually-bucketed Bar chart, not
Grafana's Histogram panel.** Histogram's fixed bucket width couldn't
isolate a 0% (dead battery) bucket from a 1-10% (low but working)
bucket, and its half-open `[start, end)` binning produced a
misleading "100-110%" bucket for vehicles at exactly 100% charge —
not a data error, just a labeling artifact of automatic binning. The
bar chart's buckets are computed explicitly in SQL (`CASE WHEN`),
giving full control over both boundaries and tooltip labels.

**Dashboards and the datasource are provisioned from files**, not
configured through the UI — see Operational Lessons below for why
that matters and what it took to get right.

## Operational Lessons

A few real problems came up building this that are worth being able
to speak to directly, since they reflect genuine production-relevant
judgment rather than being incidental bugs:

* **`docker compose down -v` deletes ALL named volumes, not just
  Postgres's.** Using it to reset the schema during development also
  silently deleted Grafana's own internal database — including both
  dashboards — since Grafana's config lived only in its `grafana_data`
  volume. Fix: **Grafana provisioning**. The Postgres datasource
  (`infrastructure/grafana/provisioning/datasources/postgres.yaml`)
  and both dashboards (`infrastructure/grafana/dashboards/*.json`,
  loaded per `infrastructure/grafana/provisioning/dashboards/dashboards.yaml`)
  are now version-controlled files, mounted read-only into the
  container and auto-loaded on startup — a `down -v` no longer loses
  either. Deliberately not a bigger migration framework, matching the
  standard already set for `infrastructure/init/`: file-based
  provisioning is the right amount of tooling for a dev-only stack
  that's recreated, not incrementally migrated.
* **Grafana's provisioned `database` field must live under `jsonData`,
  not as a top-level key.** Placing it at the top level (matching a
  reasonable reading of some older examples) is silently ignored —
  `Save & Test` passes, but every actual query then fails with "you
  do not currently have a default database configured." Nothing in
  the UI or the successful connection test surfaces this; it only
  shows up the moment a real query runs.
* **Unbounded table growth is a real operational risk, not just a
  theoretical one.** A week of uncapped 1-minute polling grew
  `fact_vehicle_status` to 7+GB, which measurably strained local
  Postgres (multi-minute checkpoints) and contributed to a full
  system freeze. Fixed by trimming to a rolling window and rebuilding
  indexes; the underlying lesson — a retention policy isn't optional
  once a system runs unattended — is now factored into how this
  project would be described if extended (see Scope Decisions).

## Scope Decisions

The original design considered a fuller Lambda architecture:
Redis Pub/Sub feeding a real-time anomaly-triage service, and a
scheduled Pandas job computing MTBF across hardware generations with
an executive HTML report. Both are intentionally out of scope for
this iteration — see the README's **Out of Scope** section for what
that means concretely and why. The data model was built to support
them without a schema change if they're picked up later:
`fact_vehicle_status` is already an append-only ledger keyed by
`vehicle_type_id`, and the Redis cache already holds the current
state a Pub/Sub subscriber would need. A data retention policy
(rolling window or archival rollup) would also be a natural addition
if this ran unattended long-term — see Operational Lessons.

## Current State
Ingestion, integration, scheduling, pipeline observability, and
visualization are all built and running against real, verified
Lime data. The pipeline is complete as scoped; remaining work
(streaming alerts, batch MTBF) is intentionally deferred, not
outstanding.