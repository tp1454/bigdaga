
CREATE DATABASE IF NOT EXISTS analytics;

-- 1. Ho so doanh nghiep SEC EDGAR
CREATE TABLE IF NOT EXISTS analytics.company_filings
(
    cik String,
    entity_name Nullable(String),
    sic Nullable(String),
    accession_number String,
    filing_date Date,
    form String,
    primary_document Nullable(String),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (cik, accession_number);

-- 2. Giao dich Binance
CREATE TABLE IF NOT EXISTS analytics.crypto_trades
(
    symbol String,
    timestamp DateTime64(3, 'UTC'),
    price Float64,
    quantity Float64,
    trade_id UInt64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (source, symbol, timestamp, trade_id);

-- 3. Du lieu thi truong CoinGecko
CREATE TABLE IF NOT EXISTS analytics.crypto_market_data
(
    symbol String,
    timestamp DateTime64(3, 'UTC'),
    price Float64,
    market_cap Nullable(Float64),
    volume_24h Nullable(Float64),
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (symbol, timestamp);

-- 4. Ty gia Forex
CREATE TABLE IF NOT EXISTS analytics.forex_rates
(
    pair String,
    timestamp DateTime64(3, 'UTC'),
    rate Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree()
ORDER BY (pair, timestamp);

-- 5. Chi so kinh te BLS va Treasury
CREATE TABLE IF NOT EXISTS analytics.macro_indicators
(
    indicator LowCardinality(String),
    timestamp Date,
    value Float64,
    source LowCardinality(String),
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (source, indicator, timestamp);
  