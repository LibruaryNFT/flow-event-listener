-- =================================================================================
-- 03_allday_edition_market_data.sql
-- Per-edition analytics: floor prices, sales velocity, grail scores, etc.
--
-- Ported from BigQuery: vaultopolis/google/queries/pipeline/4_ALLDAY_edition_market_data.sql
-- PREREQUISITE: 02_allday_tables.sql must have been run first
-- SCHEDULE: After each allday_tables refresh
-- =================================================================================

-- ---------------------------------------------------------------------------------
-- TABLE
-- ---------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS allday_edition_market_data (
  edition_id              TEXT        NOT NULL,
  series_id               TEXT,
  set_id                  TEXT,
  play_id                 TEXT,
  tier                    TEXT,
  player_name             TEXT,
  team                    TEXT,
  play_category           TEXT,
  set_name                TEXT,
  series_name             TEXT,

  -- Supply
  mint_count              INTEGER     NOT NULL DEFAULT 0,
  burn_count              INTEGER     NOT NULL DEFAULT 0,
  existing_supply         INTEGER     NOT NULL DEFAULT 0,
  in_packs_count          INTEGER     NOT NULL DEFAULT 0,

  -- Holder analytics
  unique_holders          INTEGER     NOT NULL DEFAULT 0,
  largest_holder_count    INTEGER     NOT NULL DEFAULT 0,
  concentration_pct       DOUBLE PRECISION NOT NULL DEFAULT 0,
  holder_diversity_pct    DOUBLE PRECISION,

  -- Listing analytics
  listed_count            INTEGER     NOT NULL DEFAULT 0,
  floor_price             DOUBLE PRECISION NOT NULL DEFAULT 0,
  median_listing_price    DOUBLE PRECISION NOT NULL DEFAULT 0,
  listings_at_floor       INTEGER     NOT NULL DEFAULT 0,
  listing_price_ratio     DOUBLE PRECISION,

  -- Sales: 7d
  total_sales_7d          INTEGER     NOT NULL DEFAULT 0,
  total_volume_7d         DOUBLE PRECISION NOT NULL DEFAULT 0,
  asp_7d                  DOUBLE PRECISION NOT NULL DEFAULT 0,

  -- Sales: 30d
  total_sales_30d         INTEGER     NOT NULL DEFAULT 0,
  total_volume_30d        DOUBLE PRECISION NOT NULL DEFAULT 0,
  asp_30d                 DOUBLE PRECISION NOT NULL DEFAULT 0,
  asp_30d_raw             DOUBLE PRECISION NOT NULL DEFAULT 0,
  special_sales_30d_count INTEGER     NOT NULL DEFAULT 0,
  outlier_sales_30d_count INTEGER     NOT NULL DEFAULT 0,

  -- Sales: 180d
  total_sales_180d        INTEGER     NOT NULL DEFAULT 0,
  total_volume_180d       DOUBLE PRECISION NOT NULL DEFAULT 0,
  asp_180d                DOUBLE PRECISION NOT NULL DEFAULT 0,
  asp_180d_raw            DOUBLE PRECISION NOT NULL DEFAULT 0,
  special_sales_180d_count INTEGER    NOT NULL DEFAULT 0,
  outlier_sales_180d_count INTEGER    NOT NULL DEFAULT 0,

  -- Sales: lifetime
  total_sales_lifetime    INTEGER     NOT NULL DEFAULT 0,
  total_volume_lifetime   DOUBLE PRECISION NOT NULL DEFAULT 0,
  asp_lifetime            DOUBLE PRECISION NOT NULL DEFAULT 0,
  asp_dapper              DOUBLE PRECISION NOT NULL DEFAULT 0,

  -- Temporal metadata
  first_sale_timestamp    TIMESTAMPTZ,
  last_sale_timestamp     TIMESTAMPTZ,
  last_sale_price         DOUBLE PRECISION NOT NULL DEFAULT 0,
  days_since_last_sale    INTEGER,

  -- Offer analytics
  edition_offer_count     INTEGER     NOT NULL DEFAULT 0,
  highest_offer           DOUBLE PRECISION NOT NULL DEFAULT 0,
  lowest_offer            DOUBLE PRECISION NOT NULL DEFAULT 0,
  avg_offer               DOUBLE PRECISION NOT NULL DEFAULT 0,
  total_offer_volume      DOUBLE PRECISION NOT NULL DEFAULT 0,
  unique_bidders          INTEGER     NOT NULL DEFAULT 0,

  -- Offer history timeframes
  days_with_offers_7d     INTEGER     NOT NULL DEFAULT 0,
  avg_offer_7d            DOUBLE PRECISION NOT NULL DEFAULT 0,
  days_with_offers_30d    INTEGER     NOT NULL DEFAULT 0,
  avg_offer_30d           DOUBLE PRECISION NOT NULL DEFAULT 0,
  days_with_offers_180d   INTEGER     NOT NULL DEFAULT 0,
  avg_offer_180d          DOUBLE PRECISION NOT NULL DEFAULT 0,

  -- Derived metrics
  liquidity_spread_pct    DOUBLE PRECISION,
  floating_supply_pct     DOUBLE PRECISION,
  offer_depth_score       DOUBLE PRECISION NOT NULL DEFAULT 0,
  price_change_pct        DOUBLE PRECISION,
  price_trend_7d_vs_30d   DOUBLE PRECISION,
  price_trend_30d_vs_180d DOUBLE PRECISION,
  daily_volume_delta      DOUBLE PRECISION,

  -- Estimated value
  estimated_value         DOUBLE PRECISION,
  value_confidence        TEXT,

  -- Market cap
  market_cap              DOUBLE PRECISION,

  -- Scores
  sales_activity_score    INTEGER,
  sales_activity_confidence TEXT,
  offer_frequency_score   INTEGER,
  offer_frequency_confidence TEXT,
  market_freshness_score  INTEGER,
  sales_momentum_score    INTEGER,
  sales_momentum_confidence TEXT,
  liquidity_score         INTEGER,
  sell_through_rate       DOUBLE PRECISION,
  grail_score             DOUBLE PRECISION NOT NULL DEFAULT 0,

  data_generated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_allday_emd_edition_id  ON allday_edition_market_data (edition_id);
CREATE INDEX IF NOT EXISTS idx_allday_emd_grail_score ON allday_edition_market_data (grail_score DESC);

-- ---------------------------------------------------------------------------------
-- REFRESH FUNCTION
-- ---------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION refresh_allday_edition_market_data()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN

TRUNCATE allday_edition_market_data;

INSERT INTO allday_edition_market_data
WITH
  -- ============================================================
  -- EDITION BASE: Master edition info enriched with metadata
  -- ============================================================
  editions AS (
    SELECT
      e.edition_id,
      e.series_id,
      e.set_id,
      e.play_id,
      e.tier,
      p.player_name,
      p.team,
      p.play_category,
      p.jersey_number,
      s.set_name,
      se.series_name
    FROM allday_editions e
    LEFT JOIN allday_plays p ON e.play_id = p.play_id
    LEFT JOIN allday_sets s ON e.set_id = s.set_id
    LEFT JOIN allday_series se ON e.series_id = se.series_id
  ),

  -- Mint counts per edition
  mint_counts AS (
    SELECT edition_id, COUNT(*) AS mint_count, MAX(serial_number) AS max_serial
    FROM allday_moments
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
    FROM allday_active_offers
    WHERE edition_id IS NOT NULL AND offer_amount > 0
    GROUP BY edition_id
  ),

  -- Offer history: 7d
  offers_7d AS (
    SELECT
      edition_id,
      COUNT(DISTINCT (block_timestamp::date)) AS days_with_offers_7d,
      AVG(offer_amount) AS avg_offer_7d
    FROM allday_offers
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
    FROM allday_offers
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
    FROM allday_offers
    WHERE event_type = 'OfferAvailable'
      AND (block_timestamp::date) >= CURRENT_DATE - INTERVAL '180 days'
      AND offer_amount > 0 AND edition_id IS NOT NULL
    GROUP BY edition_id
  ),

  -- ============================================================
  -- LIVE ACTIVE LISTINGS: Computed from event state machine at query time.
  -- Bypasses allday_active_listings (pre-built -- stale between pipeline runs).
  -- Expiry check runs at query time so expired Flowty/Dapper V2 listings are
  -- always excluded, not just at table build time.
  -- ============================================================
  live_moment_status AS (
    SELECT
      nft_id AS moment_id,
      price,
      event_type,
      raw_data,
      ROW_NUMBER() OVER (
        PARTITION BY nft_id
        ORDER BY block_timestamp DESC, transaction_hash DESC
      ) AS rn
    FROM allday_events
    WHERE event_type IN (
        'ListingAvailable',
        'ListingCompleted',
        'Withdrawn',
        'MomentNFTBurned'
      )
      AND nft_id IS NOT NULL
  ),

  live_active_listings AS (
    SELECT
      l.moment_id,
      m.edition_id,
      l.price
    FROM live_moment_status l
    JOIN allday_moments m ON l.moment_id = m.moment_id
    WHERE l.rn = 1
      AND l.event_type = 'ListingAvailable'
      AND l.price IS NOT NULL
      AND l.price > 0
      -- Expiry check at query time (not table build time):
      -- Dapper V1 has no $.expiry (NULL -> passes). Dapper V2 and Flowty have $.expiry.
      AND (
        (NULLIF(TRIM(l.raw_data->>'expiry'), ''))::bigint IS NULL
        OR (NULLIF(TRIM(l.raw_data->>'expiry'), ''))::bigint > EXTRACT(EPOCH FROM NOW())::bigint
      )
  ),

  -- ============================================================
  -- RAW SALES: Simple median for ghost listing anchor (no dependency on floor_prices)
  -- ============================================================
  raw_sales_180d AS (
    SELECT
      edition_id,
      COUNT(*) AS raw_sales_count_180d,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS raw_median_price_180d
    FROM allday_sales
    WHERE price > 0
      AND block_timestamp >= NOW() - INTERVAL '180 days'
    GROUP BY edition_id
  ),

  -- ============================================================
  -- LISTING ANALYTICS: Ghost detection + floor computation
  -- ============================================================
  -- Pre-compute edition medians (PostgreSQL cannot use percentile_cont as window fn)
  edition_listing_medians AS (
    SELECT
      edition_id,
      COUNT(*) AS listing_count,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price
    FROM live_active_listings
    WHERE price > 0
    GROUP BY edition_id
  ),

  -- Ghost listing protection:
  --   1. Statistical outlier: price < 10% of edition median (when 3+ listings)
  --   2. Listing below highest edition offer (should auto-match on Dapper)
  --   3. Wildly overpriced: price > 100x recent sales median (when 5+ sales)
  --      Prevents overpriced spam from inflating the median and causing
  --      legitimate cheap listings to be incorrectly flagged as ghosts
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
  -- Exclude: serial #1, last mint, jersey match; median of remaining
  -- ============================================================
  tagged_sales_180d AS (
    SELECT
      s.edition_id,
      s.price,
      s.block_timestamp,
      s.serial_number,
      mc.max_serial,
      ed.jersey_number,
      (
        s.serial_number = 1
        OR (mc.max_serial IS NOT NULL AND s.serial_number = mc.max_serial)
        OR (ed.jersey_number IS NOT NULL AND s.serial_number = ed.jersey_number)
      ) AS is_special_serial,
      (
        COALESCE(fp.floor_price, 0) > 0
        AND s.price > 10 * fp.floor_price
      ) AS is_price_outlier
    FROM allday_sales s
    LEFT JOIN mint_counts mc ON s.edition_id = mc.edition_id
    LEFT JOIN editions ed ON s.edition_id = ed.edition_id
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
      mc.max_serial,
      ed.jersey_number,
      (
        s.serial_number = 1
        OR (mc.max_serial IS NOT NULL AND s.serial_number = mc.max_serial)
        OR (ed.jersey_number IS NOT NULL AND s.serial_number = ed.jersey_number)
      ) AS is_special_serial,
      (
        COALESCE(fp.floor_price, 0) > 0
        AND s.price > 10 * fp.floor_price
      ) AS is_price_outlier
    FROM allday_sales s
    LEFT JOIN mint_counts mc ON s.edition_id = mc.edition_id
    LEFT JOIN editions ed ON s.edition_id = ed.edition_id
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

  -- 7d sales (raw median -- too few sales for cleaning)
  sales_7d AS (
    SELECT
      edition_id,
      COUNT(*) AS total_sales_7d,
      SUM(price) AS total_volume_7d,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS asp_7d
    FROM allday_sales
    WHERE price > 0 AND block_timestamp >= NOW() - INTERVAL '7 days'
    GROUP BY edition_id
  ),

  -- Lifetime sales
  sales_lifetime AS (
    SELECT
      edition_id,
      COUNT(*) AS total_sales_lifetime,
      SUM(price) AS total_volume_lifetime,
      AVG(price) AS asp_lifetime,
      MIN(block_timestamp) AS first_sale_timestamp
    FROM allday_sales
    WHERE price > 0
    GROUP BY edition_id
  ),

  -- Last sale per edition
  edition_last_sale AS (
    SELECT
      edition_id,
      price AS last_sale_price,
      block_timestamp AS last_sale_timestamp
    FROM (
      SELECT
        edition_id, price, block_timestamp,
        ROW_NUMBER() OVER (PARTITION BY edition_id ORDER BY block_timestamp DESC) AS rn
      FROM allday_sales
      WHERE price > 0
    ) sub
    WHERE rn = 1
  ),

  -- ============================================================
  -- DAPPER ASP: Average of last 10 sales (Dapper's own formula)
  -- ============================================================
  dapper_asp AS (
    SELECT
      edition_id,
      ROUND(AVG(price)::numeric, 2) AS asp_dapper
    FROM (
      SELECT
        edition_id,
        price,
        ROW_NUMBER() OVER (PARTITION BY edition_id ORDER BY block_timestamp DESC) AS rn
      FROM allday_sales
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
      FROM allday_active_offers
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
  e.play_id,
  e.tier,
  e.player_name,
  e.team,
  e.play_category,
  e.set_name,
  e.series_name,

  -- Supply
  COALESCE(mc.mint_count, 0) AS mint_count,
  COALESCE(es.burn_count, 0) AS burn_count,
  COALESCE(es.existing_supply, mc.mint_count, 0) AS existing_supply,
  COALESCE(es.in_packs_count, 0) AS in_packs_count,

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

  -- Ratio of median listing price to floor (1.0 = all at floor)
  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price, 0) IS NOT NULL AND COALESCE(la.median_listing_price, 0) > 0
      THEN la.median_listing_price / fp.floor_price
      ELSE NULL
    END::numeric, 2
  )::double precision AS listing_price_ratio,

  -- ============================================================
  -- SALES: Multi-timeframe (clean = excludes wash trades)
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
  -- OFFER ANALYTICS: Current + Historical
  -- ============================================================
  COALESCE(os.edition_offer_count, 0) AS edition_offer_count,
  ROUND(COALESCE(os.highest_offer, 0)::numeric, 2)::double precision AS highest_offer,
  ROUND(COALESCE(os.lowest_offer, 0)::numeric, 2)::double precision AS lowest_offer,
  ROUND(COALESCE(os.avg_offer, 0)::numeric, 2)::double precision AS avg_offer,
  ROUND(COALESCE(os.total_offer_volume, 0)::numeric, 2)::double precision AS total_offer_volume,
  COALESCE(os.unique_bidders, 0) AS unique_bidders,

  -- Offer history timeframes
  COALESCE(o7.days_with_offers_7d, 0) AS days_with_offers_7d,
  ROUND(COALESCE(o7.avg_offer_7d, 0)::numeric, 2)::double precision AS avg_offer_7d,
  COALESCE(o30.days_with_offers_30d, 0) AS days_with_offers_30d,
  ROUND(COALESCE(o30.avg_offer_30d, 0)::numeric, 2)::double precision AS avg_offer_30d,
  COALESCE(o180.days_with_offers_180d, 0) AS days_with_offers_180d,
  ROUND(COALESCE(o180.avg_offer_180d, 0)::numeric, 2)::double precision AS avg_offer_180d,

  -- ============================================================
  -- DERIVED METRICS: Spread, Supply, Depth
  -- ============================================================

  -- Liquidity spread: % gap between floor and highest offer
  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price, 0) IS NOT NULL AND COALESCE(os.highest_offer, 0) > 0
      THEN ((fp.floor_price - os.highest_offer) / fp.floor_price) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS liquidity_spread_pct,

  -- Floating supply: % of existing supply currently listed
  ROUND(
    CASE
      WHEN NULLIF(COALESCE(es.existing_supply, mc.mint_count), 0) IS NOT NULL
      THEN (COALESCE(la.total_listings, 0)::double precision / COALESCE(es.existing_supply, mc.mint_count)) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS floating_supply_pct,

  -- Offer depth: total capital committed vs floor price
  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price, 0) IS NOT NULL AND COALESCE(os.total_offer_volume, 0) > 0
      THEN os.total_offer_volume / fp.floor_price
      ELSE 0
    END::numeric, 2
  )::double precision AS offer_depth_score,

  -- Premium/discount: how much clean ASP exceeds or trails floor
  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price, 0) IS NOT NULL AND COALESCE(s180.asp_180d, 0) > 0
      THEN ((s180.asp_180d - fp.floor_price) / fp.floor_price) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS price_change_pct,

  -- ============================================================
  -- PRICE TRENDS (clean ASP based)
  -- ============================================================

  -- 7d vs 30d price trend
  ROUND(
    CASE
      WHEN s30.asp_30d IS NOT NULL AND s30.asp_30d > 0 AND COALESCE(s7.asp_7d, 0) > 0
      THEN ((s7.asp_7d - s30.asp_30d) / s30.asp_30d) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS price_trend_7d_vs_30d,

  -- 30d vs 180d price trend
  ROUND(
    CASE
      WHEN s180.asp_180d IS NOT NULL AND s180.asp_180d > 0
           AND s30.asp_30d IS NOT NULL AND s30.asp_30d > 0
      THEN ((s30.asp_30d - s180.asp_180d) / s180.asp_180d) * 100
      ELSE NULL
    END::numeric, 2
  )::double precision AS price_trend_30d_vs_180d,

  -- Daily volume change: % diff between 7d and 30d daily averages
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
  -- Model 5 (floor-blend): replaces offer blending with floor blending
  -- Guardrails:
  --   FLOOR_CAP (HIGH): cap at floor when blend > 2x floor + 2+ listings
  --   FLOOR_SANITY (MEDIUM/LOW): only blend with floor if floor <= 3x ASP
  -- ============================================================
  ROUND(
    CASE
      -- HIGH VOLUME (10+ sales): recency-weighted ASP
      WHEN COALESCE(s180.total_sales_180d, 0) >= 10 AND s180.asp_180d IS NOT NULL
        THEN
          CASE
            -- Guardrail: FLOOR_CAP -- cap at floor when blend > 2x floor and floor has supply
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
            -- Standard: recency blend (no offer floor)
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
        AND fp.floor_price <= s180.asp_180d * 3  -- GUARDRAIL: reject insane floors
        THEN (s180.asp_180d * 0.70) + (fp.floor_price * 0.30)
      -- MEDIUM (3-9 sales) with ASP, no floor: ASP only
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9
        AND s180.asp_180d IS NOT NULL
        THEN s180.asp_180d
      -- LOW (1-2 sales) with ASP and floor: blend 50/50 ASP/floor
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
        AND s180.asp_180d IS NOT NULL
        AND COALESCE(fp.floor_price, 0) > 0
        AND fp.floor_price <= s180.asp_180d * 3  -- GUARDRAIL: reject insane floors
        THEN (s180.asp_180d * 0.50) + (fp.floor_price * 0.50)
      -- LOW (1-2 sales) with ASP, no floor: ASP only
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
        AND s180.asp_180d IS NOT NULL
        THEN s180.asp_180d
      -- Fallbacks: raw 180d ASP capped at floor -> last sale price capped at floor -> last sale price
      WHEN COALESCE(s180.asp_180d_raw, 0) > 0 AND fp.floor_price > 0
        THEN LEAST(s180.asp_180d_raw, fp.floor_price)
      WHEN COALESCE(els.last_sale_price, 0) > 0 AND fp.floor_price > 0
        THEN LEAST(els.last_sale_price, fp.floor_price)
      WHEN COALESCE(els.last_sale_price, 0) > 0
        THEN els.last_sale_price
      -- No sales data: NULL (floor alone doesn't establish value)
      ELSE NULL
    END::numeric, 2
  )::double precision AS estimated_value,

  -- Value confidence level
  CASE
    WHEN COALESCE(s180.total_sales_180d, 0) >= 10 THEN 'high'
    WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9 THEN 'medium'
    WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2 THEN 'low'
    WHEN COALESCE(sl.total_sales_lifetime, 0) > 0 THEN 'lifetime'
    ELSE 'none'
  END AS value_confidence,

  -- ============================================================
  -- MARKET CAP (mirrors estimated_value logic -- Model 5 floor-blend)
  -- ============================================================
  ROUND(
    (CASE
      -- HIGH VOLUME: recency-weighted ASP with floor cap
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
        AND fp.floor_price <= s180.asp_180d * 3  -- GUARDRAIL: reject insane floors
        THEN (s180.asp_180d * 0.70) + (fp.floor_price * 0.30)
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9
        AND s180.asp_180d IS NOT NULL
        THEN s180.asp_180d
      WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
        AND s180.asp_180d IS NOT NULL
        AND COALESCE(fp.floor_price, 0) > 0
        AND fp.floor_price <= s180.asp_180d * 3  -- GUARDRAIL: reject insane floors
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
    END) * COALESCE(es.existing_supply, mc.mint_count, 0)::numeric, 0
  )::double precision AS market_cap,

  -- ============================================================
  -- NORMALIZED SCORES (0-100) -- mirrors TopShot scoring
  -- ============================================================

  -- Sales Activity Score: weighted blend of 7d/30d/180d velocity
  LEAST(100, (
    (LEAST(1.0, COALESCE(s7.total_sales_7d, 0) /
      NULLIF((SELECT sales_7d_p90 FROM normalization_factors), 0)) * 50)
    + (LEAST(1.0, COALESCE(s30.total_sales_30d, 0) /
      NULLIF((SELECT sales_30d_p90 FROM normalization_factors), 0)) * 30)
    + (LEAST(1.0, COALESCE(s180.total_sales_180d, 0) /
      NULLIF((SELECT sales_180d_p90 FROM normalization_factors), 0)) * 20)
  )::bigint) AS sales_activity_score,

  CASE
    WHEN COALESCE(s30.total_sales_30d, 0) >= 5 THEN 'high'
    WHEN COALESCE(s30.total_sales_30d, 0) BETWEEN 2 AND 4 THEN 'medium'
    WHEN COALESCE(s30.total_sales_30d, 0) = 1 THEN 'low'
    ELSE 'none'
  END AS sales_activity_confidence,

  -- Offer Frequency Score: weighted blend of offer days + bidder depth
  LEAST(100, (
    (LEAST(1.0, COALESCE(o7.days_with_offers_7d, 0) /
      NULLIF((SELECT offers_7d_p90 FROM normalization_factors), 0)) * 40)
    + (LEAST(1.0, COALESCE(o30.days_with_offers_30d, 0) /
      NULLIF((SELECT offers_30d_p90 FROM normalization_factors), 0)) * 30)
    + (LEAST(1.0, COALESCE(os.unique_bidders, 0) /
      NULLIF((SELECT bidders_p90 FROM normalization_factors), 0)) * 30)
  )::bigint) AS offer_frequency_score,

  CASE
    WHEN COALESCE(o180.days_with_offers_180d, 0) >= 10 THEN 'high'
    WHEN COALESCE(o180.days_with_offers_180d, 0) BETWEEN 3 AND 9 THEN 'medium'
    WHEN COALESCE(o180.days_with_offers_180d, 0) BETWEEN 1 AND 2 THEN 'low'
    ELSE 'none'
  END AS offer_frequency_confidence,

  -- Market Freshness Score: based on recency of last sale
  CASE
    WHEN els.last_sale_timestamp IS NULL THEN 0
    WHEN (CURRENT_DATE - els.last_sale_timestamp::date) <= 7 THEN 100
    WHEN (CURRENT_DATE - els.last_sale_timestamp::date) <= 30 THEN 75
    WHEN (CURRENT_DATE - els.last_sale_timestamp::date) <= 90 THEN 50
    WHEN (CURRENT_DATE - els.last_sale_timestamp::date) <= 180 THEN 25
    ELSE 0
  END AS market_freshness_score,

  -- Sales Momentum Score (-100 to +100): multi-tier velocity trend
  LEAST(100, GREATEST(-100, (
    CASE
      WHEN COALESCE(s30.total_sales_30d, 0) >= 3
      THEN (((COALESCE(s7.total_sales_7d, 0) / 7.0) - (s30.total_sales_30d / 30.0)) /
            NULLIF(s30.total_sales_30d / 30.0, 0)) * 50
      ELSE 0
    END
    + CASE
      WHEN COALESCE(s180.total_sales_180d, 0) >= 5
      THEN (((COALESCE(s30.total_sales_30d, 0) / 30.0) - (s180.total_sales_180d / 180.0)) /
            NULLIF(s180.total_sales_180d / 180.0, 0)) * 30
      ELSE 0
    END
    + CASE
      WHEN COALESCE(sl.total_sales_lifetime, 0) >= 10
        AND sl.first_sale_timestamp IS NOT NULL
      THEN (((COALESCE(s180.total_sales_180d, 0) / 180.0) -
             (sl.total_sales_lifetime / GREATEST(1, (CURRENT_DATE - sl.first_sale_timestamp::date))::double precision)) /
            NULLIF(sl.total_sales_lifetime / GREATEST(1, (CURRENT_DATE - sl.first_sale_timestamp::date))::double precision, 0)) * 20
      ELSE 0
    END
  )::bigint)) AS sales_momentum_score,

  CASE
    WHEN COALESCE(s7.total_sales_7d, 0) >= 3 AND COALESCE(s30.total_sales_30d, 0) >= 3 THEN 'high'
    WHEN COALESCE(s7.total_sales_7d, 0) >= 1 AND COALESCE(s30.total_sales_30d, 0) >= 1 THEN 'medium'
    WHEN COALESCE(s7.total_sales_7d, 0) + COALESCE(s30.total_sales_30d, 0) > 0 THEN 'low'
    ELSE 'none'
  END AS sales_momentum_confidence,

  -- Liquidity Score (0-100): composite market health
  LEAST(100, (
    -- 40%: sales volume relative to market
    (LEAST(1.0, COALESCE(s180.total_sales_180d, 0) /
      NULLIF((SELECT sales_180d_p90 FROM normalization_factors), 0)) * 40)
    -- 30%: unique bidders relative to market
    + (LEAST(1.0, COALESCE(os.unique_bidders, 0) /
      NULLIF((SELECT bidders_p90 FROM normalization_factors), 0)) * 30)
    -- 20%: sell-side depth (proportional to floating supply, capped)
    + (CASE
        WHEN NULLIF(COALESCE(es.existing_supply, mc.mint_count), 0) IS NOT NULL AND COALESCE(la.total_listings, 0) > 0
        THEN LEAST(20.0, (la.total_listings::double precision / COALESCE(es.existing_supply, mc.mint_count)) * 200)
        ELSE 0
      END)
    -- 10%: bid-ask spread tightness
    + (CASE
        WHEN NULLIF(fp.floor_price, 0) IS NOT NULL AND COALESCE(os.highest_offer, 0) > 0
        THEN LEAST(10, (1 - ((fp.floor_price - os.highest_offer) / fp.floor_price)) * 10)
        ELSE 0
      END)
  )::bigint) AS liquidity_score,

  -- Sell-through rate: 30d sales / listed count
  ROUND(
    CASE
      WHEN COALESCE(la.total_listings, 0) > 0 AND COALESCE(s30.total_sales_30d, 0) > 0
      THEN s30.total_sales_30d::double precision / la.total_listings::double precision
      ELSE NULL
    END::numeric, 2
  )::double precision AS sell_through_rate,

  -- Grail score (ranking metric): higher of clean ASP and highest offer
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
LEFT JOIN allday_edition_supply es ON e.edition_id = es.edition_id
WHERE COALESCE(mc.mint_count, 0) > 0;

END;
$$;
