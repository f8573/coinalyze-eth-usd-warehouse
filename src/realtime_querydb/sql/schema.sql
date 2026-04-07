CREATE TABLE IF NOT EXISTS provider (
    provider_code text PRIMARY KEY,
    provider_name text NOT NULL,
    last_discovered_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS market (
    market_id bigserial PRIMARY KEY,
    coinalyze_symbol text NOT NULL UNIQUE,
    provider_code text NOT NULL REFERENCES provider(provider_code),
    market_type text NOT NULL CHECK (market_type IN ('spot', 'perp')),
    base_asset text NOT NULL,
    quote_asset text NOT NULL,
    symbol_on_exchange text NOT NULL,
    is_perpetual boolean NOT NULL,
    margined text NULL,
    oi_unit text NULL,
    has_buy_sell_data boolean NOT NULL,
    has_ohlcv_data boolean NOT NULL DEFAULT true,
    last_discovered_at timestamptz NOT NULL DEFAULT now(),
    required_granularities text[] NOT NULL DEFAULT ARRAY['15min', '1hour', 'daily'],
    activation_status text NOT NULL DEFAULT 'candidate' CHECK (activation_status IN ('candidate', 'active', 'excluded')),
    eligibility_checked_at timestamptz NULL,
    shared_complete_from timestamptz NULL,
    shared_complete_to timestamptz NULL,
    exclusion_reason text NULL
);

ALTER TABLE market
    ADD COLUMN IF NOT EXISTS last_discovered_at timestamptz NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_market_scope
    ON market (provider_code, market_type, quote_asset, activation_status);

CREATE TABLE IF NOT EXISTS ohlcv_fact (
    market_id bigint NOT NULL REFERENCES market(market_id) ON DELETE CASCADE,
    granularity text NOT NULL CHECK (granularity IN ('15min', '1hour', 'daily')),
    bucket_start timestamptz NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    volume double precision NOT NULL,
    buy_volume double precision NULL,
    trade_count double precision NULL,
    buy_trade_count double precision NULL,
    PRIMARY KEY (market_id, granularity, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_bucket
    ON ohlcv_fact (bucket_start);

CREATE TABLE IF NOT EXISTS open_interest_fact (
    market_id bigint NOT NULL REFERENCES market(market_id) ON DELETE CASCADE,
    granularity text NOT NULL CHECK (granularity IN ('15min', '1hour', 'daily')),
    bucket_start timestamptz NOT NULL,
    native_open double precision NOT NULL,
    native_high double precision NOT NULL,
    native_low double precision NOT NULL,
    native_close double precision NOT NULL,
    usd_open double precision NOT NULL,
    usd_high double precision NOT NULL,
    usd_low double precision NOT NULL,
    usd_close double precision NOT NULL,
    PRIMARY KEY (market_id, granularity, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_open_interest_bucket
    ON open_interest_fact (bucket_start);

CREATE TABLE IF NOT EXISTS funding_rate_fact (
    market_id bigint NOT NULL REFERENCES market(market_id) ON DELETE CASCADE,
    granularity text NOT NULL CHECK (granularity IN ('15min', '1hour', 'daily')),
    bucket_start timestamptz NOT NULL,
    open double precision NOT NULL,
    high double precision NOT NULL,
    low double precision NOT NULL,
    close double precision NOT NULL,
    PRIMARY KEY (market_id, granularity, bucket_start)
);

CREATE INDEX IF NOT EXISTS idx_funding_rate_bucket
    ON funding_rate_fact (bucket_start);

CREATE TABLE IF NOT EXISTS granularity_sync_state (
    market_id bigint NOT NULL REFERENCES market(market_id) ON DELETE CASCADE,
    granularity text NOT NULL CHECK (granularity IN ('15min', '1hour', 'daily')),
    metric_name text NOT NULL CHECK (metric_name IN ('ohlcv', 'oi_native', 'oi_usd', 'funding')),
    oldest_backfilled_bucket timestamptz NULL,
    newest_loaded_bucket timestamptz NULL,
    retry_after_until timestamptz NULL,
    last_attempt_at timestamptz NOT NULL,
    last_error text NULL,
    PRIMARY KEY (market_id, granularity, metric_name)
);
