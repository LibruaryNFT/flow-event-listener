-- =================================================================================
-- 03_pinnacle_edition_market_data.sql
-- Per-edition analytics: floor prices, sales velocity, grail scores, etc.
--
-- Ported from BigQuery: 4_PINNACLE_edition_market_data.sql
-- Target: PostgreSQL 16+
--
-- PREREQUISITE: 02_pinnacle_tables.sql must have been run first
-- SCHEDULE: After each pinnacle_tables refresh
-- =================================================================================

-- ---------------------------------------------------------------------------------
-- TABLE
-- ---------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pinnacle_edition_market_data (
  edition_id              bigint PRIMARY KEY,
  series_id               bigint,
  set_id                  bigint,
  edition_type_id         bigint,
  edition_type_name       text,
  is_limited              boolean,
  is_maturing             boolean,
  character_name          text,
  shape_name              text,
  series_name             text,
  set_name                text,
  description             text,
  shape_id                bigint,
  variant                 text,
  printing                text,
  is_chaser               boolean,
  render_id               text,

  -- Supply
  max_mint_size           bigint,
  burn_count              bigint,
  existing_supply         bigint,
  in_vaults_count         bigint,

  -- Holder analytics
  unique_holders          bigint,
  largest_holder_count    bigint,
  concentration_pct       double precision,
  holder_diversity_pct    double precision,

  -- Listing analytics
  listed_count            bigint,
  floor_price             double precision,
  median_listing_price    double precision,
  listings_at_floor       bigint,
  listing_price_ratio     double precision,

  -- Sales: 7d
  total_sales_7d          bigint,
  total_volume_7d         double precision,
  asp_7d                  double precision,

  -- Sales: 30d
  total_sales_30d         bigint,
  total_volume_30d        double precision,
  asp_30d                 double precision,
  asp_30d_raw             double precision,
  special_sales_30d_count bigint,
  outlier_sales_30d_count bigint,

  -- Sales: 180d
  total_sales_180d        bigint,
  total_volume_180d       double precision,
  asp_180d                double precision,
  asp_180d_raw            double precision,
  special_sales_180d_count bigint,
  outlier_sales_180d_count bigint,

  -- Sales: lifetime
  total_sales_lifetime    bigint,
  total_volume_lifetime   double precision,
  asp_lifetime            double precision,
  asp_dapper              double precision,

  -- Temporal metadata
  first_sale_timestamp    timestamp,
  last_sale_timestamp     timestamp,
  last_sale_price         double precision,
  days_since_last_sale    integer,

  -- Derived metrics
  floating_supply_pct     double precision,
  price_change_pct        double precision,
  price_trend_7d_vs_30d   double precision,
  price_trend_30d_vs_180d double precision,
  daily_volume_delta      double precision,

  -- Estimated value
  estimated_value         double precision,
  value_confidence        text,

  -- Market cap
  market_cap              double precision,

  -- Scores
  sales_activity_score    integer,
  sales_activity_confidence text,
  market_freshness_score  integer,
  sales_momentum_score    integer,
  sales_momentum_confidence text,
  liquidity_score         integer,
  sell_through_rate       double precision,
  grail_score             double precision,

  data_generated_at       timestamp
);

CREATE INDEX IF NOT EXISTS idx_pemd_edition_id  ON pinnacle_edition_market_data (edition_id);
CREATE INDEX IF NOT EXISTS idx_pemd_grail_score ON pinnacle_edition_market_data (grail_score);

-- ---------------------------------------------------------------------------------
-- REFRESH FUNCTION
-- ---------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION refresh_pinnacle_edition_market_data()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN

TRUNCATE pinnacle_edition_market_data;

INSERT INTO pinnacle_edition_market_data
WITH
  -- ============================================================
  -- EDITION BASE: Master edition info enriched with metadata
  -- ============================================================
  editions_deduped AS (
    SELECT
      e.edition_id,
      e.series_id,
      e.set_id,
      e.edition_type_id,
      et.edition_type_name,
      et.is_limited::boolean,
      et.is_maturing::boolean,
      COALESCE(e.character_name, sh.character_name) AS character_name,
      sh.shape_name,
      se.series_name,
      s.set_name,
      e.shape_id,
      e.variant,
      e.printing,
      e.is_chaser,
      e.render_id,
      e.max_mint_size,
      e.description,
      ROW_NUMBER() OVER (PARTITION BY e.edition_id ORDER BY e.edition_id) AS rn
    FROM pinnacle_editions e
    LEFT JOIN pinnacle_shapes sh ON e.shape_id = sh.shape_id
    LEFT JOIN pinnacle_sets s ON e.set_id = s.set_id
    LEFT JOIN pinnacle_series se ON e.series_id = se.series_id
    LEFT JOIN pinnacle_edition_types et ON e.edition_type_id = et.edition_type_id
  ),

  editions AS (
    SELECT
      edition_id, series_id, set_id, edition_type_id, edition_type_name,
      is_limited, is_maturing, character_name, shape_name, series_name,
      set_name, shape_id, variant, printing, is_chaser, render_id,
      max_mint_size, description
    FROM editions_deduped
    WHERE rn = 1
  ),

  -- Mint counts per edition (from on-chain minting events)
  mint_counts AS (
    SELECT edition_id, COUNT(*) AS mint_count
    FROM pinnacle_nfts
    GROUP BY edition_id
  ),

  -- ============================================================
  -- ACTIVE OFFERS: Current state + historical timeframes
  -- ============================================================
  offer_stats AS (
    SELECT
      edition_id,
      MAX(offer_amount) AS highest_offer,
      MIN(offer_amount) AS lowest_offer,
      AVG(offer_amount) AS avg_offer,
      COUNT(*) AS edition_offer_count,
      COUNT(DISTINCT offer_address) AS unique_bidders,
      SUM(offer_amount) AS total_offer_volume
    FROM pinnacle_active_offers
    WHERE edition_id IS NOT NULL AND offer_amount > 0
    GROUP BY edition_id
  ),

  -- Offer history: 7d
  offers_7d AS (
    SELECT
      edition_id,
      COUNT(DISTINCT (block_timestamp::date)) AS days_with_offers_7d,
      AVG(offer_amount) AS avg_offer_7d
    FROM pinnacle_offers
    WHERE event_type = 'OfferAvailable'
      AND (block_timestamp::date) >= CURRENT_DATE - INTERVAL '7 days'
      AND offer_amount > 0 AND edition_id IS NOT NULL
    GROUP BY edition_id
  ),

  -- Offer history: 30d
  offers_30d AS (
    SELECT
      edition_id,
      COUNT(DISTINCT (block_timestamp::date)) AS days_with_offers_30d,
      AVG(offer_amount) AS avg_offer_30d
    FROM pinnacle_offers
    WHERE event_type = 'OfferAvailable'
      AND (block_timestamp::date) >= CURRENT_DATE - INTERVAL '30 days'
      AND offer_amount > 0 AND edition_id IS NOT NULL
    GROUP BY edition_id
  ),

  -- Offer history: 180d
  offers_180d AS (
    SELECT
      edition_id,
      COUNT(DISTINCT (block_timestamp::date)) AS days_with_offers_180d,
      AVG(offer_amount) AS avg_offer_180d
    FROM pinnacle_offers
    WHERE event_type = 'OfferAvailable'
      AND (block_timestamp::date) >= CURRENT_DATE - INTERVAL '180 days'
      AND offer_amount > 0 AND edition_id IS NOT NULL
    GROUP BY edition_id
  ),

  -- ============================================================
  -- LIVE ACTIVE LISTINGS: Computed from event state machine at query time.
  -- ============================================================
  live_nft_status AS (
    SELECT
      nft_id,
      price,
      event_type,
      (NULLIF(TRIM(raw_data->>'expiry'), ''))::bigint AS expiry_epoch,
      ROW_NUMBER() OVER (
        PARTITION BY nft_id
        ORDER BY block_timestamp DESC, transaction_hash DESC, block_height DESC
      ) AS rn
    FROM pinnacle_events
    WHERE event_type IN (
        'ListingAvailable',
        'PurchasedDapper',
        'PurchasedFlowty',
        'ListingWithdrawn',
        'PinNFTBurned'
      )
      AND nft_id IS NOT NULL
  ),

  live_active_listings AS (
    SELECT
      l.nft_id,
      n.edition_id,
      l.price
    FROM live_nft_status l
    JOIN pinnacle_nfts n ON l.nft_id = n.nft_id
    WHERE l.rn = 1
      AND l.event_type = 'ListingAvailable'
      AND l.price IS NOT NULL
      AND l.price > 0
      -- Expiry check at query time (not table build time)
      AND (l.expiry_epoch IS NULL OR l.expiry_epoch > EXTRACT(EPOCH FROM NOW())::bigint)
  ),

  -- ============================================================
  -- RAW SALES: Simple median for ghost listing anchor
  -- ============================================================
  raw_sales_180d AS (
    SELECT
      edition_id,
      COUNT(*) AS raw_sales_count_180d,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS raw_median_price_180d
    FROM pinnacle_sales
    WHERE price > 0
      AND block_timestamp >= NOW() - INTERVAL '180 days'
    GROUP BY edition_id
  ),

  -- ============================================================
  -- LISTING ANALYTICS: Ghost detection + floor computation
  -- Pre-compute edition medians for ghost detection (PG has no percentile window fn)
  -- ============================================================
  edition_listing_medians AS (
    SELECT
      edition_id,
      COUNT(*) AS listing_count,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price
    FROM live_active_listings
    WHERE price > 0
    GROUP BY edition_id
  ),

  verified_listings AS (
    SELECT
      al.edition_id,
      al.price,
      CASE
        WHEN elm.listing_count >= 3
         AND al.price < elm.median_price * 0.1
        THEN TRUE
        WHEN os.highest_offer > 0 AND al.price < os.highest_offer
        THEN TRUE
        WHEN rs.raw_sales_count_180d >= 5 AND rs.raw_median_price_180d > 0
         AND al.price > rs.raw_median_price_180d * 100
        THEN TRUE
        ELSE FALSE
      END AS is_ghost
    FROM live_active_listings al
    LEFT JOIN edition_listing_medians elm ON al.edition_id = elm.edition_id
    LEFT JOIN offer_stats os ON al.edition_id = os.edition_id
    LEFT JOIN raw_sales_180d rs ON al.edition_id = rs.edition_id
    WHERE al.price > 0
  ),

  floor_prices AS (
    SELECT
      edition_id,
      MIN(CASE WHEN NOT is_ghost THEN price END) AS floor_price
    FROM verified_listings
    GROUP BY edition_id
  ),

  listing_analytics AS (
    SELECT
      vl.edition_id,
      COUNT(*) AS total_listings,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY vl.price) AS median_listing_price,
      SUM(CASE
        WHEN fp.floor_price IS NOT NULL AND vl.price <= fp.floor_price * 1.1
        THEN 1 ELSE 0
      END) AS listings_at_floor
    FROM verified_listings vl
    LEFT JOIN floor_prices fp ON vl.edition_id = fp.edition_id
    WHERE NOT vl.is_ghost
    GROUP BY vl.edition_id
  ),

  -- ============================================================
  -- SALES DATA: Multi-timeframe with wash trade + special serial cleaning
  -- ============================================================
  tagged_sales_180d AS (
    SELECT
      s.edition_id,
      s.price,
      s.block_timestamp,
      s.serial_number,
      e.max_mint_size,
      COALESCE(
        s.serial_number = 1
        OR (e.max_mint_size IS NOT NULL AND s.serial_number = e.max_mint_size),
        false
      ) AS is_special_serial,
      (
        COALESCE(fp.floor_price, 0) > 0
        AND s.price > 10 * fp.floor_price
      ) AS is_price_outlier
    FROM pinnacle_sales s
    LEFT JOIN editions e ON s.edition_id = e.edition_id
    LEFT JOIN floor_prices fp ON s.edition_id = fp.edition_id
    WHERE s.price > 0
      AND s.block_timestamp >= NOW() - INTERVAL '180 days'
  ),

  q180 AS (
    SELECT
      edition_id,
      percentile_cont(0.05) WITHIN GROUP (ORDER BY price) AS p05,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY price) AS p95
    FROM tagged_sales_180d
    WHERE NOT is_special_serial AND NOT is_price_outlier
    GROUP BY edition_id
  ),

  sales_180d_clean AS (
    SELECT
      t.edition_id,
      COUNT(*) AS total_sales_180d,
      SUM(t.price) AS total_volume_180d,
      AVG(t.price) AS asp_180d_raw,
      COUNT(*) FILTER (WHERE t.is_special_serial) AS special_sales_180d_count,
      COUNT(*) FILTER (WHERE t.is_price_outlier) AS outlier_sales_180d_count,
      COUNT(*) FILTER (WHERE NOT t.is_special_serial AND NOT t.is_price_outlier) AS non_special_sales_180d_count,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY t.price)
        FILTER (WHERE NOT t.is_special_serial AND NOT t.is_price_outlier) AS asp_180d,
      q.p05 AS p05_ex_special_180d,
      q.p95 AS p95_ex_special_180d
    FROM tagged_sales_180d t
    LEFT JOIN q180 q USING (edition_id)
    GROUP BY t.edition_id, q.p05, q.p95
  ),

  tagged_sales_30d AS (
    SELECT
      s.edition_id,
      s.price,
      s.block_timestamp,
      s.serial_number,
      e.max_mint_size,
      COALESCE(
        s.serial_number = 1
        OR (e.max_mint_size IS NOT NULL AND s.serial_number = e.max_mint_size),
        false
      ) AS is_special_serial,
      (
        COALESCE(fp.floor_price, 0) > 0
        AND s.price > 10 * fp.floor_price
      ) AS is_price_outlier
    FROM pinnacle_sales s
    LEFT JOIN editions e ON s.edition_id = e.edition_id
    LEFT JOIN floor_prices fp ON s.edition_id = fp.edition_id
    WHERE s.price > 0
      AND s.block_timestamp >= NOW() - INTERVAL '30 days'
  ),

  q30 AS (
    SELECT
      edition_id,
      percentile_cont(0.05) WITHIN GROUP (ORDER BY price) AS p05,
      percentile_cont(0.95) WITHIN GROUP (ORDER BY price) AS p95
    FROM tagged_sales_30d
    WHERE NOT is_special_serial AND NOT is_price_outlier
    GROUP BY edition_id
  ),

  sales_30d_clean AS (
    SELECT
      t.edition_id,
      COUNT(*) AS total_sales_30d,
      SUM(t.price) AS total_volume_30d,
      AVG(t.price) AS asp_30d_raw,
      COUNT(*) FILTER (WHERE t.is_special_serial) AS special_sales_30d_count,
      COUNT(*) FILTER (WHERE t.is_price_outlier) AS outlier_sales_30d_count,
      COUNT(*) FILTER (WHERE NOT t.is_special_serial AND NOT t.is_price_outlier) AS non_special_sales_30d_count,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY t.price)
        FILTER (WHERE NOT t.is_special_serial AND NOT t.is_price_outlier) AS asp_30d,
      q.p05 AS p05_ex_special_30d,
      q.p95 AS p95_ex_special_30d
    FROM tagged_sales_30d t
    LEFT JOIN q30 q USING (edition_id)
    GROUP BY t.edition_id, q.p05, q.p95
  ),

  sales_7d AS (
    SELECT
      edition_id,
      COUNT(*) AS total_sales_7d,
      SUM(price) AS total_volume_7d,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS asp_7d
    FROM pinnacle_sales
    WHERE price > 0 AND block_timestamp >= NOW() - INTERVAL '7 days'
    GROUP BY edition_id
  ),

  sales_lifetime AS (
    SELECT
      edition_id,
      COUNT(*) AS total_sales_lifetime,
      SUM(price) AS total_volume_lifetime,
      AVG(price) AS asp_lifetime,
      MIN(block_timestamp) AS first_sale_timestamp
    FROM pinnacle_sales
    WHERE price > 0
    GROUP BY edition_id
  ),

  edition_last_sale AS (
    SELECT
      edition_id,
      price AS last_sale_price,
      block_timestamp AS last_sale_timestamp
    FROM (
      SELECT
        edition_id, price, block_timestamp,
        ROW_NUMBER() OVER (PARTITION BY edition_id ORDER BY block_timestamp DESC) AS rn
      FROM pinnacle_sales
      WHERE price > 0
    ) sub
    WHERE rn = 1
  ),

  -- ============================================================
  -- DAPPER ASP: Average of last 10 sales
  -- ============================================================
  dapper_asp AS (
    SELECT
      edition_id,
      ROUND(AVG(price)::numeric, 2)::double precision AS asp_dapper
    FROM (
      SELECT
        edition_id,
        price,
        ROW_NUMBER() OVER (PARTITION BY edition_id ORDER BY block_timestamp DESC) AS rn
      FROM pinnacle_sales
      WHERE price > 0
    ) sub
    WHERE rn <= 10
    GROUP BY edition_id
  ),

  -- ============================================================
  -- NORMALIZATION FACTORS: Market-wide percentiles for scoring
  -- ============================================================
  bidder_percentiles AS (
    SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY bidder_count) AS bidders_p90
    FROM (
      SELECT edition_id, COUNT(DISTINCT offer_address) AS bidder_count
      FROM pinnacle_active_offers
      WHERE edition_id IS NOT NULL AND offer_amount > 0
      GROUP BY edition_id
    ) sub
  ),

  normalization_factors AS (
    SELECT
      percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(s7.total_sales_7d, 0)) AS sales_7d_p90,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(s30.total_sales_30d, 0)) AS sales_30d_p90,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(s180.total_sales_180d, 0)) AS sales_180d_p90,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(o7.days_with_offers_7d, 0)) AS offers_7d_p90,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(o30.days_with_offers_30d, 0)) AS offers_30d_p90,
      percentile_cont(0.9) WITHIN GROUP (ORDER BY COALESCE(o180.days_with_offers_180d, 0)) AS offers_180d_p90,
      MAX(bp.bidders_p90) AS bidders_p90
    FROM editions ed
    LEFT JOIN sales_7d s7 ON ed.edition_id = s7.edition_id
    LEFT JOIN sales_30d_clean s30 ON ed.edition_id = s30.edition_id
    LEFT JOIN sales_180d_clean s180 ON ed.edition_id = s180.edition_id
    LEFT JOIN offers_7d o7 ON ed.edition_id = o7.edition_id
    LEFT JOIN offers_30d o30 ON ed.edition_id = o30.edition_id
    LEFT JOIN offers_180d o180 ON ed.edition_id = o180.edition_id
    CROSS JOIN bidder_percentiles bp
  )

-- ============================================================
-- FINAL SELECT: All fields assembled
-- ============================================================
SELECT
  e.edition_id,
  e.series_id,
  e.set_id,
  e.edition_type_id,
  e.edition_type_name,
  e.is_limited,
  e.is_maturing,
  e.character_name,
  e.shape_name,
  e.series_name,
  e.set_name,
  e.description,
  e.shape_id,
  e.variant,
  e.printing,
  e.is_chaser,
  e.render_id,

  -- Supply
  COALESCE(mc.mint_count, e.max_mint_size, 0) AS max_mint_size,
  COALESCE(es.burn_count, 0) AS burn_count,
  COALESCE(es.existing_supply, mc.mint_count, e.max_mint_size, 0) AS existing_supply,
  COALESCE(es.in_vaults_count, 0) AS in_vaults_count,

  -- Holder analytics
  COALESCE(es.unique_holders, 0) AS unique_holders,
  COALESCE(es.largest_holder_count, 0) AS largest_holder_count,
  COALESCE(es.concentration_pct, 0) AS concentration_pct,
  ROUND(
    CASE
      WHEN NULLIF(COALESCE(es.existing_supply, 0), 0) IS NOT NULL
      THEN (COALESCE(es.unique_holders, 0)::double precision / es.existing_supply) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS holder_diversity_pct,

  -- ============================================================
  -- LISTING ANALYTICS
  -- ============================================================
  COALESCE(la.total_listings, 0) AS listed_count,
  ROUND(COALESCE(fp.floor_price, 0)::numeric, 2)::double precision AS floor_price,
  ROUND(COALESCE(la.median_listing_price, 0)::numeric, 2)::double precision AS median_listing_price,
  COALESCE(la.listings_at_floor, 0) AS listings_at_floor,

  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price, 0) IS NOT NULL AND COALESCE(la.median_listing_price, 0) > 0
      THEN la.median_listing_price / fp.floor_price
      ELSE NULL
    END::numeric, 2
  )::double precision AS listing_price_ratio,

  -- ============================================================
  -- SALES: Multi-timeframe
  -- ============================================================
  COALESCE(s7.total_sales_7d, 0) AS total_sales_7d,
  ROUND(COALESCE(s7.total_volume_7d, 0)::numeric, 2)::double precision AS total_volume_7d,
  ROUND(COALESCE(s7.asp_7d, 0)::numeric, 2)::double precision AS asp_7d,

  COALESCE(s30.total_sales_30d, 0) AS total_sales_30d,
  ROUND(COALESCE(s30.total_volume_30d, 0)::numeric, 2)::double precision AS total_volume_30d,
  ROUND(COALESCE(s30.asp_30d, 0)::numeric, 2)::double precision AS asp_30d,
  ROUND(COALESCE(s30.asp_30d_raw, 0)::numeric, 2)::double precision AS asp_30d_raw,
  COALESCE(s30.special_sales_30d_count, 0) AS special_sales_30d_count,
  COALESCE(s30.outlier_sales_30d_count, 0) AS outlier_sales_30d_count,

  COALESCE(s180.total_sales_180d, 0) AS total_sales_180d,
  ROUND(COALESCE(s180.total_volume_180d, 0)::numeric, 2)::double precision AS total_volume_180d,
  ROUND(COALESCE(s180.asp_180d, 0)::numeric, 2)::double precision AS asp_180d,
  ROUND(COALESCE(s180.asp_180d_raw, 0)::numeric, 2)::double precision AS asp_180d_raw,
  COALESCE(s180.special_sales_180d_count, 0) AS special_sales_180d_count,
  COALESCE(s180.outlier_sales_180d_count, 0) AS outlier_sales_180d_count,

  COALESCE(sl.total_sales_lifetime, 0) AS total_sales_lifetime,
  ROUND(COALESCE(sl.total_volume_lifetime, 0)::numeric, 2)::double precision AS total_volume_lifetime,
  ROUND(COALESCE(sl.asp_lifetime, 0)::numeric, 2)::double precision AS asp_lifetime,
  ROUND(COALESCE(da.asp_dapper, 0)::numeric, 2)::double precision AS asp_dapper,

  -- Temporal metadata
  sl.first_sale_timestamp,
  els.last_sale_timestamp,
  ROUND(COALESCE(els.last_sale_price, 0)::numeric, 2)::double precision AS last_sale_price,
  CASE
    WHEN els.last_sale_timestamp IS NOT NULL
    THEN (CURRENT_DATE - els.last_sale_timestamp::date)
    ELSE NULL
  END AS days_since_last_sale,

  -- ============================================================
  -- DERIVED METRICS
  -- ============================================================
  ROUND(
    CASE
      WHEN NULLIF(COALESCE(es.existing_supply, mc.mint_count, e.max_mint_size, 0), 0) IS NOT NULL
      THEN (COALESCE(la.total_listings, 0)::double precision / COALESCE(es.existing_supply, mc.mint_count, e.max_mint_size)) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS floating_supply_pct,

  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price, 0) IS NOT NULL AND COALESCE(s180.asp_180d, 0) > 0
      THEN ((s180.asp_180d - fp.floor_price) / fp.floor_price) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS price_change_pct,

  -- ============================================================
  -- PRICE TRENDS
  -- ============================================================
  ROUND(
    CASE
      WHEN s30.asp_30d IS NOT NULL AND s30.asp_30d > 0 AND COALESCE(s7.asp_7d, 0) > 0
      THEN ((s7.asp_7d - s30.asp_30d) / s30.asp_30d) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS price_trend_7d_vs_30d,

  ROUND(
    CASE
      WHEN s180.asp_180d IS NOT NULL AND s180.asp_180d > 0
           AND s30.asp_30d IS NOT NULL AND s30.asp_30d > 0
      THEN ((s30.asp_30d - s180.asp_180d) / s180.asp_180d) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS price_trend_30d_vs_180d,

  ROUND(
    CASE
      WHEN NULLIF(s30.total_volume_30d / 30.0, 0) IS NOT NULL
           AND s7.total_volume_7d IS NOT NULL
      THEN (((s7.total_volume_7d / 7.0) - (s30.total_volume_30d / 30.0))
            / (s30.total_volume_30d / 30.0)) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS daily_volume_delta,

  -- ============================================================
  -- ESTIMATED VALUE: Fair market value via recency-weighted ASP + floor blend
  -- Model 5 (floor-blend)
  -- ============================================================
  ROUND(
    CASE
      -- HIGH VOLUME (10+ sales): recency-weighted ASP
      WHEN COALESCE(s180.total_sales_180d, 0) >= 10 AND s180.asp_180d IS NOT NULL
        THEN
          CASE
            -- Guardrail: FLOOR_CAP
            WHEN COALESCE(la.total_listings, 0) >= 2
              AND COALESCE(fp.floor_price, 0) > 0
              AND COALESCE(CASE
                WHEN COALESCE(s30.asp_30d, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) >= 3
                  THEN (s30.asp_30d * 0.70) + (s180.asp_180d * 0.30)
                WHEN COALESCE(s30.asp_30d, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) BETWEEN 1 AND 2
                  THEN (s30.asp_30d * 0.50) + (s180.asp_180d * 0.50)
                ELSE s180.asp_180d
              END, s180.asp_180d) > fp.floor_price * 2
            THEN fp.floor_price
            -- Standard: recency blend
            ELSE COALESCE(CASE
              WHEN COALESCE(s30.asp_30d, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) >= 3
                THEN (s30.asp_30d * 0.70) + (s180.asp_180d * 0.30)
              WHEN COALESCE(s30.asp_30d, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) BETWEEN 1 AND 2
                THEN (s30.asp_30d * 0.50) + (s180.asp_180d * 0.50)
              ELSE s180.asp_180d
            END, s180.asp_180d)
          END
      -- MEDIUM (3-9 sales) with ASP and floor: blend 70/30 ASP/floor
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9
        AND s180.asp_180d IS NOT NULL
        AND COALESCE(fp.floor_price, 0) > 0
        AND fp.floor_price <= s180.asp_180d * 3
        THEN (s180.asp_180d * 0.70) + (fp.floor_price * 0.30)
      -- MEDIUM (3-9 sales) with ASP, no floor: ASP only
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9
        AND s180.asp_180d IS NOT NULL
        THEN s180.asp_180d
      -- LOW (1-2 sales) with ASP and floor: blend 50/50 ASP/floor
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
        AND s180.asp_180d IS NOT NULL
        AND COALESCE(fp.floor_price, 0) > 0
        AND fp.floor_price <= s180.asp_180d * 3
        THEN (s180.asp_180d * 0.50) + (fp.floor_price * 0.50)
      -- LOW (1-2 sales) with ASP, no floor: ASP only
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
        AND s180.asp_180d IS NOT NULL
        THEN s180.asp_180d
      -- Fallbacks
      WHEN COALESCE(s180.asp_180d_raw, 0) > 0 AND fp.floor_price > 0
        THEN LEAST(s180.asp_180d_raw, fp.floor_price)
      WHEN COALESCE(els.last_sale_price, 0) > 0 AND fp.floor_price > 0
        THEN LEAST(els.last_sale_price, fp.floor_price)
      WHEN COALESCE(els.last_sale_price, 0) > 0
        THEN els.last_sale_price
      ELSE NULL
    END::numeric, 2
  )::double precision AS estimated_value,

  CASE
    WHEN COALESCE(s180.total_sales_180d, 0) >= 10 THEN 'high'
    WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9 THEN 'medium'
    WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2 THEN 'low'
    WHEN COALESCE(sl.total_sales_lifetime, 0) > 0 THEN 'lifetime'
    ELSE 'none'
  END AS value_confidence,

  -- ============================================================
  -- MARKET CAP (mirrors estimated_value logic)
  -- ============================================================
  ROUND(
    (CASE
      WHEN COALESCE(s180.total_sales_180d, 0) >= 10 AND s180.asp_180d IS NOT NULL
        THEN
          CASE
            WHEN COALESCE(la.total_listings, 0) >= 2
              AND COALESCE(fp.floor_price, 0) > 0
              AND COALESCE(CASE
                WHEN COALESCE(s30.asp_30d, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) >= 3
                  THEN (s30.asp_30d * 0.70) + (s180.asp_180d * 0.30)
                WHEN COALESCE(s30.asp_30d, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) BETWEEN 1 AND 2
                  THEN (s30.asp_30d * 0.50) + (s180.asp_180d * 0.50)
                ELSE s180.asp_180d
              END, s180.asp_180d) > fp.floor_price * 2
            THEN fp.floor_price
            ELSE COALESCE(CASE
              WHEN COALESCE(s30.asp_30d, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) >= 3
                THEN (s30.asp_30d * 0.70) + (s180.asp_180d * 0.30)
              WHEN COALESCE(s30.asp_30d, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) BETWEEN 1 AND 2
                THEN (s30.asp_30d * 0.50) + (s180.asp_180d * 0.50)
              ELSE s180.asp_180d
            END, s180.asp_180d)
          END
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9
        AND s180.asp_180d IS NOT NULL
        AND COALESCE(fp.floor_price, 0) > 0
        AND fp.floor_price <= s180.asp_180d * 3
        THEN (s180.asp_180d * 0.70) + (fp.floor_price * 0.30)
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9
        AND s180.asp_180d IS NOT NULL
        THEN s180.asp_180d
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
        AND s180.asp_180d IS NOT NULL
        AND COALESCE(fp.floor_price, 0) > 0
        AND fp.floor_price <= s180.asp_180d * 3
        THEN (s180.asp_180d * 0.50) + (fp.floor_price * 0.50)
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
        AND s180.asp_180d IS NOT NULL
        THEN s180.asp_180d
      WHEN COALESCE(s180.asp_180d_raw, 0) > 0 AND fp.floor_price > 0
        THEN LEAST(s180.asp_180d_raw, fp.floor_price)
      WHEN COALESCE(els.last_sale_price, 0) > 0 AND fp.floor_price > 0
        THEN LEAST(els.last_sale_price, fp.floor_price)
      WHEN COALESCE(els.last_sale_price, 0) > 0
        THEN els.last_sale_price
      ELSE NULL
    END) * COALESCE(es.existing_supply, mc.mint_count, e.max_mint_size, 0)::numeric, 0
  )::double precision AS market_cap,

  -- ============================================================
  -- NORMALIZED SCORES (0-100)
  -- ============================================================
  LEAST(100, (
    (LEAST(1.0, COALESCE(s7.total_sales_7d, 0)::double precision /
      NULLIF((SELECT sales_7d_p90 FROM normalization_factors), 0)) * 50)
    + (LEAST(1.0, COALESCE(s30.total_sales_30d, 0)::double precision /
      NULLIF((SELECT sales_30d_p90 FROM normalization_factors), 0)) * 30)
    + (LEAST(1.0, COALESCE(s180.total_sales_180d, 0)::double precision /
      NULLIF((SELECT sales_180d_p90 FROM normalization_factors), 0)) * 20)
  )::integer) AS sales_activity_score,

  CASE
    WHEN COALESCE(s30.total_sales_30d, 0) >= 5 THEN 'high'
    WHEN COALESCE(s30.total_sales_30d, 0) BETWEEN 2 AND 4 THEN 'medium'
    WHEN COALESCE(s30.total_sales_30d, 0) = 1 THEN 'low'
    ELSE 'none'
  END AS sales_activity_confidence,

  CASE
    WHEN els.last_sale_timestamp IS NULL THEN 0
    WHEN (CURRENT_DATE - els.last_sale_timestamp::date) <= 7 THEN 100
    WHEN (CURRENT_DATE - els.last_sale_timestamp::date) <= 30 THEN 75
    WHEN (CURRENT_DATE - els.last_sale_timestamp::date) <= 90 THEN 50
    WHEN (CURRENT_DATE - els.last_sale_timestamp::date) <= 180 THEN 25
    ELSE 0
  END AS market_freshness_score,

  LEAST(100, GREATEST(-100, (
    CASE
      WHEN COALESCE(s30.total_sales_30d, 0) >= 3
      THEN (((COALESCE(s7.total_sales_7d, 0)::double precision / 7.0) - (s30.total_sales_30d::double precision / 30.0)) /
            NULLIF(s30.total_sales_30d::double precision / 30.0, 0)) * 50
      ELSE 0
    END
    + CASE
      WHEN COALESCE(s180.total_sales_180d, 0) >= 5
      THEN (((COALESCE(s30.total_sales_30d, 0)::double precision / 30.0) - (s180.total_sales_180d::double precision / 180.0)) /
            NULLIF(s180.total_sales_180d::double precision / 180.0, 0)) * 30
      ELSE 0
    END
    + CASE
      WHEN COALESCE(sl.total_sales_lifetime, 0) >= 10
        AND sl.first_sale_timestamp IS NOT NULL
      THEN (((COALESCE(s180.total_sales_180d, 0)::double precision / 180.0) -
             (sl.total_sales_lifetime::double precision / GREATEST(1, (CURRENT_DATE - sl.first_sale_timestamp::date)))) /
            NULLIF(sl.total_sales_lifetime::double precision / GREATEST(1, (CURRENT_DATE - sl.first_sale_timestamp::date)), 0)) * 20
      ELSE 0
    END
  )::integer)) AS sales_momentum_score,

  CASE
    WHEN COALESCE(s7.total_sales_7d, 0) >= 3 AND COALESCE(s30.total_sales_30d, 0) >= 3 THEN 'high'
    WHEN COALESCE(s7.total_sales_7d, 0) >= 1 AND COALESCE(s30.total_sales_30d, 0) >= 1 THEN 'medium'
    WHEN COALESCE(s7.total_sales_7d, 0) + COALESCE(s30.total_sales_30d, 0) > 0 THEN 'low'
    ELSE 'none'
  END AS sales_momentum_confidence,

  LEAST(100, (
    (LEAST(1.0, COALESCE(s180.total_sales_180d, 0)::double precision /
      NULLIF((SELECT sales_180d_p90 FROM normalization_factors), 0)) * 40)
    + (LEAST(1.0, COALESCE(os.unique_bidders, 0)::double precision /
      NULLIF((SELECT bidders_p90 FROM normalization_factors), 0)) * 30)
    + (CASE
        WHEN NULLIF(COALESCE(es.existing_supply, mc.mint_count, e.max_mint_size, 0), 0) IS NOT NULL AND COALESCE(la.total_listings, 0) > 0
        THEN LEAST(20.0, (la.total_listings::double precision / COALESCE(es.existing_supply, mc.mint_count, e.max_mint_size)) * 200)
        ELSE 0
      END)
    + (CASE
        WHEN NULLIF(fp.floor_price, 0) IS NOT NULL AND COALESCE(os.highest_offer, 0) > 0
        THEN LEAST(10, (1 - ((fp.floor_price - os.highest_offer) / fp.floor_price)) * 10)
        ELSE 0
      END)
  )::integer) AS liquidity_score,

  ROUND(
    CASE
      WHEN COALESCE(la.total_listings, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) > 0
      THEN s30.total_sales_30d::double precision / la.total_listings::double precision
      ELSE NULL
    END::numeric, 2
  )::double precision AS sell_through_rate,

  ROUND(
    GREATEST(
      COALESCE(s180.asp_180d, 0),
      COALESCE(os.highest_offer, 0)
    )::numeric, 2
  )::double precision AS grail_score,

  NOW() AS data_generated_at

FROM editions e
LEFT JOIN mint_counts mc ON e.edition_id = mc.edition_id
LEFT JOIN offer_stats os ON e.edition_id = os.edition_id
LEFT JOIN floor_prices fp ON e.edition_id = fp.edition_id
LEFT JOIN listing_analytics la ON e.edition_id = la.edition_id
LEFT JOIN edition_last_sale els ON e.edition_id = els.edition_id
LEFT JOIN sales_7d s7 ON e.edition_id = s7.edition_id
LEFT JOIN sales_30d_clean s30 ON e.edition_id = s30.edition_id
LEFT JOIN sales_180d_clean s180 ON e.edition_id = s180.edition_id
LEFT JOIN sales_lifetime sl ON e.edition_id = sl.edition_id
LEFT JOIN offers_7d o7 ON e.edition_id = o7.edition_id
LEFT JOIN offers_30d o30 ON e.edition_id = o30.edition_id
LEFT JOIN offers_180d o180 ON e.edition_id = o180.edition_id
LEFT JOIN dapper_asp da ON e.edition_id = da.edition_id
LEFT JOIN pinnacle_edition_supply es ON e.edition_id = es.edition_id
WHERE COALESCE(mc.mint_count, e.max_mint_size, 0) > 0;

END;
$$;
