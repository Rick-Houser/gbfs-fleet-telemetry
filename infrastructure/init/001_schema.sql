-- Tracks static hardware attributes; slowly changing dimension
CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_id VARCHAR(50) PRIMARY KEY,
    vehicle_type VARCHAR(50) NOT NULL,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Append-only ledger for edge device state changes
CREATE TABLE IF NOT EXISTS fact_vehicle_status (
    event_id BIGSERIAL PRIMARY KEY,
    vehicle_id VARCHAR(50) NOT NULL REFERENCES dim_vehicle(vehicle_id),
    is_disabled BOOLEAN NOT NULL,
    is_reserved BOOLEAN NOT NULL,
    battery_level INTEGER, 
    reported_at TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes optimized for batch MTBF Window Functions (LAG/LEAD)
CREATE INDEX idx_fact_reported_at ON fact_vehicle_status (reported_at DESC);
CREATE INDEX idx_fact_vehicle_time ON fact_vehicle_status (vehicle_id, reported_at DESC);
CREATE INDEX idx_fact_disabled ON fact_vehicle_status (is_disabled) WHERE is_disabled = TRUE;