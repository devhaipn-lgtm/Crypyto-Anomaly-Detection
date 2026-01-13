CREATE DATABASE IF NOT EXISTS crypto;

USE crypto;

CREATE TABLE IF NOT EXISTS market_data (
    event_time DateTime,
    symbol String,
    price Float64,
    volume Float64,
    is_anomaly UInt8,
    is_alerted UInt8 DEFAULT 0
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time);