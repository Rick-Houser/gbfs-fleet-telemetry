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
`src/poller/ingest.py` polls the Lyft Bay Wheels GBFS v2.3
`free_bike_status.json` endpoint.

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
state a Pub/Sub subscriber would need.

## Next Step
Connect Grafana to Postgres for two dashboards: fleet status
(battery levels, disabled/reserved counts over time) and pipeline
health (`ingest_run_log`: success rate, duration, vehicle counts).