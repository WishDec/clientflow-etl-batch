-- ClientFlow ETL (Batch) - transformations
-- RAW -> STG -> MART

-- =========================
-- 1) RAW -> STG (cleaning)
-- =========================
-- Assumption: We create a daily snapshot using the ingestion date.
-- We also validate price values and mark invalid rows.

INSERT INTO stg.offers_clean (
    dt, source, product_sku, merchant_id,
    price_kzt, rating, reviews_qty, purchase_count,
    is_valid, invalid_reason
)
SELECT
    DATE(r.ingested_at) AS dt,
    r.source,
    TRIM(r.product_sku) AS product_sku,
    TRIM(r.merchant_id) AS merchant_id,
    r.price_kzt,
    r.rating,
    r.reviews_qty,
    r.purchase_count,
    CASE
        WHEN r.price_kzt IS NULL THEN FALSE
        WHEN r.price_kzt <= 0 THEN FALSE
        WHEN r.price_kzt > 100000000 THEN FALSE
        ELSE TRUE
    END AS is_valid,
    CASE
        WHEN r.price_kzt IS NULL THEN 'price_is_null'
        WHEN r.price_kzt <= 0 THEN 'price_non_positive'
        WHEN r.price_kzt > 100000000 THEN 'price_outlier'
        ELSE NULL
    END AS invalid_reason
FROM raw.marketplace_offers r
WHERE r.ingested_at >= NOW() - INTERVAL '7 days'
ON CONFLICT (dt, source, product_sku, merchant_id)
DO UPDATE SET
    price_kzt = EXCLUDED.price_kzt,
    rating = EXCLUDED.rating,
    reviews_qty = EXCLUDED.reviews_qty,
    purchase_count = EXCLUDED.purchase_count,
    is_valid = EXCLUDED.is_valid,
    invalid_reason = EXCLUDED.invalid_reason,
    created_at = NOW();


-- =========================
-- 2) STG -> MART (daily metrics)
-- =========================
-- Builds per-product daily aggregates:
-- best price, avg price, offers count.
-- client_* fields are placeholders for future enrichment (ClientFlow integration).

WITH valid_offers AS (
    SELECT
        dt, source, product_sku, merchant_id, price_kzt
    FROM stg.offers_clean
    WHERE is_valid = TRUE
),
best_offer AS (
    SELECT
        dt, source, product_sku,
        MIN(price_kzt) AS best_price_kzt
    FROM valid_offers
    GROUP BY dt, source, product_sku
),
best_merchant AS (
    SELECT dt, source, product_sku, best_merchant_id
    FROM (
        SELECT
            v.dt, v.source, v.product_sku,
            v.merchant_id AS best_merchant_id,
            ROW_NUMBER() OVER (
                PARTITION BY v.dt, v.source, v.product_sku
                ORDER BY v.merchant_id
            ) AS rn
        FROM valid_offers v
        JOIN best_offer b
          ON b.dt = v.dt
         AND b.source = v.source
         AND b.product_sku = v.product_sku
         AND b.best_price_kzt = v.price_kzt
    ) t
    WHERE rn = 1
),
agg AS (
    SELECT
        dt, source, product_sku,
        AVG(price_kzt) AS avg_price_kzt,
        COUNT(*) AS offers_count
    FROM valid_offers
    GROUP BY dt, source, product_sku
)
INSERT INTO mart.product_price_daily (
    dt, source, product_sku,
    best_price_kzt, best_merchant_id,
    avg_price_kzt, offers_count
)
SELECT
    a.dt, a.source, a.product_sku,
    b.best_price_kzt,
    bm.best_merchant_id,
    a.avg_price_kzt,
    a.offers_count
FROM agg a
LEFT JOIN best_offer b
  ON b.dt = a.dt AND b.source = a.source AND b.product_sku = a.product_sku
LEFT JOIN best_merchant bm
  ON bm.dt = a.dt AND bm.source = a.source AND bm.product_sku = a.product_sku
ON CONFLICT (dt, source, product_sku)
DO UPDATE SET
    best_price_kzt = EXCLUDED.best_price_kzt,
    best_merchant_id = EXCLUDED.best_merchant_id,
    avg_price_kzt = EXCLUDED.avg_price_kzt,
    offers_count = EXCLUDED.offers_count,
    created_at = NOW();


-- =========================
-- 3) MART anomalies (simple rules)
-- =========================
-- Example: detect sudden drops vs previous day (>= 15% down)

WITH cur AS (
    SELECT dt, source, product_sku, best_price_kzt
    FROM mart.product_price_daily
    WHERE dt = CURRENT_DATE
),
prev AS (
    SELECT dt, source, product_sku, best_price_kzt
    FROM mart.product_price_daily
    WHERE dt = CURRENT_DATE - INTERVAL '1 day'
),
joined AS (
    SELECT
        c.dt, c.source, c.product_sku,
        c.best_price_kzt AS cur_price,
        p.best_price_kzt AS prev_price,
        CASE
            WHEN p.best_price_kzt IS NULL THEN NULL
            WHEN p.best_price_kzt = 0 THEN NULL
            ELSE (c.best_price_kzt - p.best_price_kzt) / p.best_price_kzt
        END AS pct_change
    FROM cur c
    LEFT JOIN prev p
      ON p.source = c.source AND p.product_sku = c.product_sku
)
INSERT INTO mart.price_anomalies (
    dt, source, product_sku,
    anomaly_type, severity, details
)
SELECT
    dt, source, product_sku,
    'spike_down' AS anomaly_type,
    2 AS severity,
    jsonb_build_object(
        'cur_price', cur_price,
        'prev_price', prev_price,
        'pct_change', pct_change
    )
FROM joined
WHERE pct_change IS NOT NULL
  AND pct_change <= -0.15
ON CONFLICT (dt, source, product_sku, anomaly_type)
DO UPDATE SET
    severity = EXCLUDED.severity,
    details = EXCLUDED.details,
    created_at = NOW();
