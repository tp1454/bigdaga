CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.equities_prices (
    symbol String,
    ts DateTime64(3, 'UTC'),
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64
)
ENGINE = MergeTree()
ORDER BY (symbol, ts);
