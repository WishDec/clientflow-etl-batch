-- ClientFlow ETL (Batch) - schema for pricing & margin analytics
-- PostgreSQL-style DDL (works as a reference model even if not executed)

-- =========================
-- RAW LAYER (ingested data)
-- =========================
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.marketplace_offers (
    ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    source             TEXT      NOT NULL,          -- e.g., "kaspi"
    product_sku        TEXT      NOT NULL,
    merchant_id        TEXT      NOT NULL,
    merchant_name      TEXT      NULL,
    offer_title        TEXT      NULL,
    price_kzt          NUMERIC(12,2) NOT NULL,
    currency           TEXT      NOT NULL DEFAULT 'KZT',
    rating             NUMERIC(3,2)  NULL,
    reviews_qty        INT          NULL,
    purchase_count     INT          NULL,
    delivery_ts        TIMESTAMP    NULL,
    raw_payload        JSONB        NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_offers_ingested_at ON raw.marketplace_offers (ingested_at);
CREATE INDEX IF NOT EXISTS ix_raw_offers_sku ON raw.marketplace_offers (product_sku);
CREATE INDEX IF NOT EXISTS ix_raw_offers_merchant ON raw.marketplace_offers (merchant_id);


-- =========================
-- STAGING LAYER (cleaned)
-- =========================
CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.offers_clean (
    dt                DATE      NOT NULL,           -- business date (snapshot)
    source            TEXT      NOT NULL,
    product_sku       TEXT      NOT NULL,
    merchant_id       TEXT      NOT NULL,
    price_kzt         NUMERIC(12,2) NOT NULL,
    rating            NUMERIC(3,2)  NULL,
    reviews_qty       INT          NULL,
    purchase_count    INT          NULL,
    is_valid          BOOLEAN      NOT NULL DEFAULT TRUE,
    invalid_reason    TEXT         NULL,
    created_at        TIMESTAMP    NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dt, source, product_sku, merchant_id)
);

CREATE INDEX IF NOT EXISTS ix_stg_offers_clean_dt ON stg.offers_clean (dt);
CREATE INDEX IF NOT EXISTS ix_stg_offers_clean_sku ON stg.offers_clean (product_sku);


-- =========================
-- DIMENSIONS (reference)
-- =========================
CREATE SCHEMA IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.products (
    product_sku       TEXT PRIMARY KEY,
    product_name      TEXT NULL,
    category          TEXT NULL,
    brand             TEXT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim.merchants (
    merchant_id       TEXT PRIMARY KEY,
    merchant_name     TEXT NULL,
    segment           TEXT NULL,
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);


-- =========================
-- FACTS / MART (analytics)
-- =========================
CREATE SCHEMA IF NOT EXISTS mart;

-- Daily snapshot per product (best price, client price, competitor stats)
CREATE TABLE IF NOT EXISTS mart.product_price_daily (
    dt                    DATE NOT NULL,
    source                TEXT NOT NULL,
    product_sku           TEXT NOT NULL,

    best_price_kzt         NUMERIC(12,2) NULL,
    best_merchant_id       TEXT NULL,

    avg_price_kzt          NUMERIC(12,2) NULL,
    offers_count           INT NULL,

    client_price_kzt       NUMERIC(12,2) NULL,   -- placeholder for client's own offer
    client_cost_kzt        NUMERIC(12,2) NULL,   -- placeholder for product cost
    client_margin_kzt      NUMERIC(12,2) NULL,
    client_margin_pct      NUMERIC(6,2)  NULL,

    created_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dt, source, product_sku)
);

CREATE INDEX IF NOT EXISTS ix_mart_price_daily_dt ON mart.product_price_daily (dt);
CREATE INDEX IF NOT EXISTS ix_mart_price_daily_sku ON mart.product_price_daily (product_sku);

-- Anomaly flags (e.g., sudden price drops)
CREATE TABLE IF NOT EXISTS mart.price_anomalies (
    dt                  DATE NOT NULL,
    source              TEXT NOT NULL,
    product_sku         TEXT NOT NULL,
    anomaly_type        TEXT NOT NULL,        -- e.g., "spike_down", "spike_up"
    severity            INT  NOT NULL DEFAULT 1,
    details             JSONB NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dt, source, product_sku, anomaly_type)
);
