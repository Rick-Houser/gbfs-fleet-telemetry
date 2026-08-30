-- One row per ingest cycle; independent of fleet data so pipeline
-- health can be queried without joining against vehicle tables
CREATE TABLE IF NOT EXISTS ingest_run_log (
    run_id BIGSERIAL PRIMARY KEY,
    run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    vehicles_fetched INTEGER NOT NULL DEFAULT 0,
    redis_success BOOLEAN NOT NULL DEFAULT FALSE,
    postgres_success BOOLEAN NOT NULL DEFAULT FALSE,
    duration_ms INTEGER NOT NULL,
    error_message TEXT
);

-- Supports "last N runs" / time-windowed health queries
CREATE INDEX idx_ingest_run_log_run_at ON ingest_run_log (run_at DESC);