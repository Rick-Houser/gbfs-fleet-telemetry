"""GBFS ingestion pipeline for Bay Wheels vehicle telemetry.

Polls the Lyft GBFS free_bike_status feed and writes each vehicle
record to two sinks with different guarantees: Redis (best-effort
cache of current state) and Postgres (source of truth, append-only
event history).
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any

import psycopg2
import psycopg2.extras
import redis
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(process)d - %(message)s",
)
logger = logging.getLogger(__name__)

GBFS_URL = "https://gbfs.lyft.com/gbfs/2.3/bay/en/free_bike_status.json"
REQUEST_TIMEOUT_SECONDS = 10

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
# TTL exceeds the polling interval so an entry only goes stale if a
# cycle is actually missed, not between two consecutive polls.
REDIS_CACHE_TTL_SECONDS = 120

# Reuses the same POSTGRES_USER/PASSWORD/DB vars docker-compose.yml
# already loads from .env, so credentials have one source of truth
# instead of drifting between the compose file and a separate DSN string.
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DSN = (
    f"dbname={os.environ['POSTGRES_DB']} "
    f"user={os.environ['POSTGRES_USER']} "
    f"password={os.environ['POSTGRES_PASSWORD']} "
    f"host={POSTGRES_HOST} port={POSTGRES_PORT}"
)


def fetch_fleet_status() -> dict[str, Any]:
    """Fetch the current GBFS free_bike_status payload.

    Exits with a nonzero code on failure — this runs as a cron job,
    so cron (and any log/exit-code monitoring around it) owns retry
    cadence, not the script itself.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    try:
        response = requests.get(GBFS_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException:
        logger.exception("Failed to fetch GBFS data")
        sys.exit(1)


def normalize_bike(bike: dict[str, Any], reported_at: datetime) -> dict[str, Any]:
    """Map a raw GBFS bike record onto the dim_vehicle/fact_vehicle_status schema."""
    fuel_fraction = bike.get("current_fuel_percent")
    return {
        "vehicle_id": bike["bike_id"],
        "vehicle_type": bike.get("vehicle_type_id", "unknown"),
        "is_disabled": bool(bike.get("is_disabled", 0)),
        "is_reserved": bool(bike.get("is_reserved", 0)),
        # GBFS reports fuel/battery as a 0.0-1.0 fraction; null for
        # non-electric vehicles, so preserve None rather than coercing to 0.
        "battery_level": round(fuel_fraction * 100) if fuel_fraction is not None else None,
        # GBFS 2.3 has no per-vehicle timestamp; feed-level last_updated applies to all.
        "reported_at": reported_at,
    }


def write_to_redis(client: redis.Redis, records: list[dict[str, Any]]) -> None:
    """Cache current vehicle state. Best-effort: Postgres is the system of record,
    so a Redis outage should degrade read latency, not halt ingestion."""
    try:
        pipeline = client.pipeline()
        for record in records:
            key = f"vehicle:{record['vehicle_id']}"
            pipeline.hset(key, mapping={k: str(v) for k, v in record.items()})
            pipeline.expire(key, REDIS_CACHE_TTL_SECONDS)
        pipeline.execute()
        logger.info("Cached %d vehicle records to Redis", len(records))
    except redis.RedisError:
        logger.exception("Redis write failed; continuing to Postgres")


def write_to_postgres(conn: psycopg2.extensions.connection, records: list[dict[str, Any]]) -> None:
    """Upsert dim_vehicle and append fact_vehicle_status in one transaction,
    so a partial-fleet failure can't corrupt the history table."""
    try:
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO dim_vehicle (vehicle_id, vehicle_type)
                VALUES %s
                ON CONFLICT (vehicle_id) DO NOTHING
                """,
                [(r["vehicle_id"], r["vehicle_type"]) for r in records],
            )
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO fact_vehicle_status
                    (vehicle_id, is_disabled, is_reserved, battery_level, reported_at)
                VALUES %s
                """,
                [
                    (
                        r["vehicle_id"],
                        r["is_disabled"],
                        r["is_reserved"],
                        r["battery_level"],
                        r["reported_at"],
                    )
                    for r in records
                ],
            )
        conn.commit()
        logger.info("Persisted %d vehicle status events to Postgres", len(records))
    except psycopg2.Error:
        conn.rollback()
        logger.exception("Postgres write failed; transaction rolled back")


def run_ingest_cycle() -> None:
    """Fetch, normalize, and write one poll of the GBFS feed."""
    payload = fetch_fleet_status()
    bikes = payload.get("data", {}).get("bikes", [])
    # GBFS reports last_updated as a Unix epoch int; the timestamptz
    # column needs a real datetime, not the raw integer.
    reported_at = datetime.fromtimestamp(payload["last_updated"], tz=timezone.utc)

    if not bikes:
        logger.warning("GBFS feed returned zero vehicles; skipping writes this cycle")
        return

    records = [normalize_bike(bike, reported_at) for bike in bikes]
    logger.info("Fetched %d vehicles from Bay Wheels", len(records))

    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    write_to_redis(redis_client, records)

    pg_conn = psycopg2.connect(POSTGRES_DSN)
    try:
        write_to_postgres(pg_conn, records)
    finally:
        pg_conn.close()


if __name__ == "__main__":
    run_ingest_cycle()