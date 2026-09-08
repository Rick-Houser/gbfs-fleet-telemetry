"""GBFS ingestion pipeline for Lime vehicle telemetry (San Francisco).

Polls Lime's GBFS free_bike_status feed and writes each vehicle
record to two sinks with different guarantees: Redis (best-effort
cache of current state) and Postgres (source of truth, append-only
event history).
"""

from __future__ import annotations

import logging
import os
import sys
import time
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

GBFS_URL = "https://data.lime.bike/api/partners/v2/gbfs/san_francisco/free_bike_status"
# Slow-changing (ttl=86400 in Lime's feed): maps vehicle_type_id to
# max_range_meters, needed to convert current_range_meters into a
# battery percentage. Refetched every cycle for simplicity — it's a
# small file, and there's no in-memory state across cron invocations
# to cache it in anyway.
VEHICLE_TYPES_URL = "https://data.lime.bike/api/partners/v2/gbfs/san_francisco/vehicle_types"
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

    Raises requests.exceptions.RequestException on failure — the caller
    logs the run outcome before deciding how to exit, so this no longer
    exits the process directly.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(GBFS_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.json()


def fetch_vehicle_type_ranges() -> dict[str, float]:
    """Fetch vehicle_types.json and return {vehicle_type_id: max_range_meters}.

    Only motorized types publish max_range_meters — human-powered
    bikes are absent from the returned mapping, which is how
    normalize_bike knows to report battery_level as None for them.
    Raises requests.exceptions.RequestException on failure, same as
    fetch_fleet_status — the caller decides how to handle it.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(VEHICLE_TYPES_URL, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    vehicle_types = response.json().get("data", {}).get("vehicle_types", [])
    return {
        vt["vehicle_type_id"]: vt["max_range_meters"]
        for vt in vehicle_types
        if "max_range_meters" in vt
    }


def normalize_bike(
    bike: dict[str, Any], reported_at: datetime, type_ranges: dict[str, float]
) -> dict[str, Any]:
    """Map a raw GBFS bike record onto the dim_vehicle/fact_vehicle_status schema.

    Lime doesn't publish a fuel-percent field directly — battery level
    is derived from current_range_meters against that vehicle type's
    max_range_meters (from vehicle_types.json). Human-powered bikes
    have no entry in type_ranges, so battery_level is correctly None
    for them rather than a fabricated 0.
    """
    current_range = bike.get("current_range_meters")
    max_range = type_ranges.get(bike.get("vehicle_type_id"))
    battery_level = None
    if current_range is not None and max_range:
        # min(100, ...) guards against sensor noise reporting slightly
        # over max_range_meters.
        battery_level = min(100, round(100 * current_range / max_range))

    return {
        "vehicle_id": bike["bike_id"],
        # Lime includes this human-readable form_factor as a
        # convenience field beyond the GBFS spec; fall back to the
        # numeric ID if it's ever absent.
        "vehicle_type": bike.get("vehicle_type", bike.get("vehicle_type_id", "unknown")),
        "is_disabled": bool(bike.get("is_disabled", 0)),
        "is_reserved": bool(bike.get("is_reserved", 0)),
        "battery_level": battery_level,
        # GBFS 2.2 has no per-vehicle timestamp in this feed; feed-level
        # last_updated applies to all records in the same poll cycle.
        "reported_at": reported_at,
    }


def write_to_redis(client: redis.Redis, records: list[dict[str, Any]]) -> bool:
    """Cache current vehicle state. Best-effort: Postgres is the system of record,
    so a Redis outage should degrade read latency, not halt ingestion.

    Returns whether the write succeeded, for the run-summary log.
    """
    try:
        pipeline = client.pipeline()
        for record in records:
            key = f"vehicle:{record['vehicle_id']}"
            pipeline.hset(key, mapping={k: str(v) for k, v in record.items()})
            pipeline.expire(key, REDIS_CACHE_TTL_SECONDS)
        pipeline.execute()
        logger.info("Cached %d vehicle records to Redis", len(records))
        return True
    except redis.RedisError:
        logger.exception("Redis write failed; continuing to Postgres")
        return False


def write_to_postgres(conn: psycopg2.extensions.connection, records: list[dict[str, Any]]) -> bool:
    """Upsert dim_vehicle and append fact_vehicle_status in one transaction,
    so a partial-fleet failure can't corrupt the history table.

    Returns whether the write succeeded, for the run-summary log.
    """
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
        return True
    except psycopg2.Error:
        conn.rollback()
        logger.exception("Postgres write failed; transaction rolled back")
        return False


def log_run_summary(
    vehicles_fetched: int,
    redis_success: bool,
    postgres_success: bool,
    duration_ms: int,
    error_message: str | None = None,
) -> None:
    """Record one row summarizing this cycle's outcome.

    Uses its own connection, separate from the main vehicle-data write,
    so a problem with the fleet data transaction never prevents the
    pipeline from reporting on itself. Best-effort: if even this fails,
    log it and move on rather than raising out of a cycle that may have
    otherwise succeeded.
    """
    try:
        conn = psycopg2.connect(POSTGRES_DSN)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO ingest_run_log
                        (vehicles_fetched, redis_success, postgres_success, duration_ms, error_message)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (vehicles_fetched, redis_success, postgres_success, duration_ms, error_message),
                )
            conn.commit()
        finally:
            conn.close()
    except psycopg2.Error:
        logger.exception("Failed to write run summary to ingest_run_log")


def run_ingest_cycle() -> None:
    """Fetch, normalize, and write one poll of the GBFS feed.

    Always records a summary row to ingest_run_log, success or failure,
    then exits nonzero if the fetch itself failed — cron uses that
    exit code, ingest_run_log carries the detail behind it.
    """
    start = time.monotonic()
    vehicles_fetched = 0
    redis_success = False
    postgres_success = False
    error_message: str | None = None

    try:
        payload = fetch_fleet_status()
        bikes = payload.get("data", {}).get("bikes", [])
        # GBFS reports last_updated as a Unix epoch int; the timestamptz
        # column needs a real datetime, not the raw integer.
        reported_at = datetime.fromtimestamp(payload["last_updated"], tz=timezone.utc)

        if not bikes:
            logger.warning("GBFS feed returned zero vehicles; skipping writes this cycle")
            error_message = "empty feed"
            return

        type_ranges = fetch_vehicle_type_ranges()
        records = [normalize_bike(bike, reported_at, type_ranges) for bike in bikes]
        vehicles_fetched = len(records)
        logger.info("Fetched %d vehicles from Lime", vehicles_fetched)

        redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_success = write_to_redis(redis_client, records)

        pg_conn = psycopg2.connect(POSTGRES_DSN)
        try:
            postgres_success = write_to_postgres(pg_conn, records)
        finally:
            pg_conn.close()
    except requests.exceptions.RequestException as e:
        logger.exception("Failed to fetch GBFS data")
        error_message = f"fetch failed: {e}"
    finally:
        duration_ms = int((time.monotonic() - start) * 1000)
        log_run_summary(vehicles_fetched, redis_success, postgres_success, duration_ms, error_message)

    if error_message is not None and error_message.startswith("fetch failed"):
        sys.exit(1)


if __name__ == "__main__":
    run_ingest_cycle()