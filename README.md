# gbfs-fleet-telemetry

> 🚧 **Work in Progress:** Ingestion, integration, scheduling, and
> pipeline observability are live and running against real data.
> Visualization (Grafana) is the last step to a complete pipeline —
> see Status below.

A telemetry pipeline for edge hardware fleets, using the live GBFS
(General Bikeshare Feed Specification) feed from Bay Wheels as a
stand-in for the scale and noise of a real fleet. Demonstrates
ingestion reliability, a dual-sink integration layer (cache +
source of truth), and pipeline observability — the core discipline
of a production telemetry system, end to end.

## Status

| Layer | Status | Notes |
|---|---|---|
| Infrastructure (Docker Compose: Postgres, Redis, Grafana) | ✅ Built | Healthchecks, log rotation, `.env`-based secrets |
| Ingestion (`src/poller/ingest.py`) | ✅ Built | Polls Bay Wheels GBFS every 1 min via cron |
| Integration (writes to Redis + Postgres) | ✅ Built | Redis: best-effort cache. Postgres: transactional, source of truth |
| Scheduling & resilience | ✅ Built | Cron-driven, not an in-process loop — see [Implementation Plan](./docs/implementation-plan.md) for the reasoning |
| Pipeline observability (`ingest_run_log`) | ✅ Built | Per-cycle success/failure, duration, error tracking |
| Visualization (Grafana dashboards) | 🔜 Next | Fleet status + pipeline health, both queryable now |

See **Out of Scope** below for what this project deliberately does
not include.

## System Architecture

```mermaid
flowchart TD
    subgraph Ingestion["Ingestion Layer — Built"]
        A[Bay Wheels GBFS API] -->|Polls JSON every 1 min, via cron| B[Python Ingestion Script]
    end

    subgraph Integration["Integration Layer — Built"]
        B -->|Cache current state| C[(Redis)]
        B -->|Append + upsert| F[(PostgreSQL)]
        B -->|Per-cycle health| I[(ingest_run_log)]
    end

    subgraph Visualization["Visualization — Next"]
        F -.->|Not yet built| J[Grafana: Fleet Status]
        I -.->|Not yet built| K[Grafana: Pipeline Health]
    end
```

## Out of Scope (By Design)

This project intentionally stops at a working, observable
ingestion → storage → visualization pipeline rather than building
every layer of a full Lambda architecture thin. Not built:

* **Streaming alert service** — the Redis cache already holds each
  cycle's current state, so a subscriber evaluating it for anomalies
  (e.g. disabled + battery drop) is a natural extension, but
  real-time triage wasn't the priority for this iteration.
* **Batch MTBF / reliability analytics** — `fact_vehicle_status` is
  structured as an append-only ledger specifically so this kind of
  analysis is possible later without a schema change; the batch
  script itself doesn't exist yet.

See [Implementation Plan](./docs/implementation-plan.md) for the
full reasoning behind this scope.

## Documentation
* 🛠️ [Implementation Plan & Status](./docs/implementation-plan.md) — full build history, design decisions (including where the implementation deviates from the original plan and why), and remaining work.

## Quickstart

```bash
# 1. Clone and configure
git clone https://github.com/Rick-Houser/gbfs-fleet-telemetry.git
cd gbfs-fleet-telemetry
cp .env.example .env   # fill in POSTGRES_USER / POSTGRES_PASSWORD / POSTGRES_DB / Grafana admin creds

# 2. Bring up the stack
docker compose up -d
docker compose ps       # confirm postgres, redis, grafana all show healthy

# 3. Set up the Python environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. Run one ingestion cycle manually
python src/poller/ingest.py

# 5. Verify data landed
docker exec -it telemetry-postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \
  -c "SELECT * FROM ingest_run_log ORDER BY run_at DESC LIMIT 1;"

# 6. (Optional) Schedule it via cron for continuous polling
crontab -e
# * * * * * cd /path/to/gbfs-fleet-telemetry && .venv/bin/python src/poller/ingest.py >> logs/ingest.log 2>&1
```