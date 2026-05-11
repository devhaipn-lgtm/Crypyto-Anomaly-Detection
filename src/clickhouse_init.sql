CREATE DATABASE IF NOT EXISTS crypto;

USE crypto;

CREATE TABLE IF NOT EXISTS market_data (
    event_time DateTime,
    symbol String,
    price Float64,
    volume Float64,
    is_anomaly UInt8,
    is_alerted UInt8 DEFAULT 0,
    volume_score Float64 DEFAULT 0,
    sentiment_avg Float64 DEFAULT 0,
    combined_score Float64 DEFAULT 0
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time);

CREATE TABLE IF NOT EXISTS raw_trades (
    event_time DateTime64(3),
    symbol String,
    price Float64,
    volume Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time);

CREATE TABLE IF NOT EXISTS sentiment_data (
    event_time DateTime,
    symbol String,
    source String,
    post_id String,
    title String,
    kind String,
    sentiment_score Float64,
    sentiment_label String,
    votes_positive UInt32,
    votes_negative UInt32,
    votes_important UInt32,
    url String,
    published_at String
) ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY (symbol, event_time, post_id);