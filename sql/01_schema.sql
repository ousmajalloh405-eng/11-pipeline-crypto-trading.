-- =====================================================================
-- Schéma de la base "crypto"
--   crypto.*  : données métier (référentiel, bougies, marché, social)
--   ops.*     : suivi d'exécution des pipelines (monitoring)
-- Toutes les écritures sont idempotentes (PRIMARY KEY + ON CONFLICT)
-- => relancer un DAG ne crée jamais de doublons.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS crypto;
CREATE SCHEMA IF NOT EXISTS ops;

-- Référentiel des cryptos suivies (alimenté depuis config/coins.yaml)
CREATE TABLE IF NOT EXISTS crypto.assets (
    symbol          TEXT PRIMARY KEY,          -- ex: BTC
    name            TEXT NOT NULL,             -- ex: Bitcoin
    binance_symbol  TEXT NOT NULL UNIQUE,      -- ex: BTCUSDT
    coingecko_id    TEXT NOT NULL UNIQUE,      -- ex: bitcoin
    active          BOOLEAN NOT NULL DEFAULT TRUE
);

-- Bougies 1 minute (source : Binance) — table de faits principale
CREATE TABLE IF NOT EXISTS crypto.klines_1m (
    symbol        TEXT        NOT NULL REFERENCES crypto.assets(symbol),
    open_time     TIMESTAMPTZ NOT NULL,        -- début de la bougie (UTC)
    open          NUMERIC(24, 10) NOT NULL,
    high          NUMERIC(24, 10) NOT NULL,
    low           NUMERIC(24, 10) NOT NULL,
    close         NUMERIC(24, 10) NOT NULL,
    volume        NUMERIC(30, 10) NOT NULL,    -- volume en crypto (base asset)
    quote_volume  NUMERIC(30, 10) NOT NULL,    -- volume en USDT
    trades        INTEGER     NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, open_time),
    CONSTRAINT klines_1m_ohlc_valid CHECK (high >= low AND low > 0)
);
CREATE INDEX IF NOT EXISTS idx_klines_1m_time ON crypto.klines_1m (open_time DESC);

-- Bougies agrégées (5m, 15m, 1h, 1d) calculées par le job de processing
CREATE TABLE IF NOT EXISTS crypto.klines_agg (
    symbol        TEXT        NOT NULL REFERENCES crypto.assets(symbol),
    interval      TEXT        NOT NULL CHECK (interval IN ('5m', '15m', '1h', '1d')),
    open_time     TIMESTAMPTZ NOT NULL,
    open          NUMERIC(24, 10) NOT NULL,
    high          NUMERIC(24, 10) NOT NULL,
    low           NUMERIC(24, 10) NOT NULL,
    close         NUMERIC(24, 10) NOT NULL,
    volume        NUMERIC(30, 10) NOT NULL,
    quote_volume  NUMERIC(30, 10) NOT NULL,
    trades        INTEGER     NOT NULL,
    candles       INTEGER     NOT NULL,        -- nb de bougies 1m utilisées
    PRIMARY KEY (symbol, interval, open_time)
);

-- Instantanés de marché (source : CoinGecko) : prix, market cap, volume 24h
CREATE TABLE IF NOT EXISTS crypto.market_snapshots (
    symbol                  TEXT        NOT NULL REFERENCES crypto.assets(symbol),
    fetched_at              TIMESTAMPTZ NOT NULL,
    price_usd               NUMERIC(24, 10),
    market_cap_usd          NUMERIC(30, 2),
    market_cap_rank         INTEGER,
    total_volume_usd        NUMERIC(30, 2),
    price_change_24h_pct    NUMERIC(12, 4),
    circulating_supply      NUMERIC(34, 4),
    PRIMARY KEY (symbol, fetched_at)
);

-- Métriques sociales quotidiennes (source : CoinGecko /coins/{id})
CREATE TABLE IF NOT EXISTS crypto.social_metrics (
    symbol                  TEXT NOT NULL REFERENCES crypto.assets(symbol),
    metric_date             DATE NOT NULL,
    twitter_followers       BIGINT,
    reddit_subscribers      BIGINT,
    telegram_channel_users  BIGINT,
    PRIMARY KEY (symbol, metric_date)
);

-- Monitoring : une ligne par exécution de tâche
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    id               BIGSERIAL PRIMARY KEY,
    pipeline         TEXT        NOT NULL,     -- ex: binance_klines
    status           TEXT        NOT NULL CHECK (status IN ('success', 'partial', 'failed')),
    started_at       TIMESTAMPTZ NOT NULL,
    finished_at      TIMESTAMPTZ NOT NULL,
    rows_written     INTEGER     NOT NULL DEFAULT 0,
    symbols_ok       INTEGER     NOT NULL DEFAULT 0,
    symbols_failed   INTEGER     NOT NULL DEFAULT 0,
    error            TEXT
);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_lookup
    ON ops.pipeline_runs (pipeline, finished_at DESC);
