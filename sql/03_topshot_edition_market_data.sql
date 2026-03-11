-- =================================================================================
-- 03_topshot_edition_market_data.sql
-- Per-edition analytics: floor prices, sales velocity, grail scores, etc.
--
-- Ported from BigQuery: vaultopolis/google/queries/pipeline/4_TOPSHOT_edition_data.sql
-- Target: PostgreSQL 16+
--
-- PREREQUISITE: 02_topshot_tables.sql must have been run first
-- SCHEDULE: After each topshot_tables refresh
-- =================================================================================

-- ---------------------------------------------------------------------------
-- Table definition
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS topshot_edition_market_data (
    edition_id                    TEXT PRIMARY KEY,

    -- Core price metrics
    grail_score                   NUMERIC,
    highest_edition_offer         NUMERIC,
    highest_moment_offer          NUMERIC,
    asp_180d                      NUMERIC,
    asp_180d_raw                  NUMERIC,
    special_sales_180d_count      INTEGER,
    outlier_sales_180d_count      INTEGER,
    non_special_sales_180d_count  INTEGER,
    p05_ex_special_180d           NUMERIC,
    p95_ex_special_180d           NUMERIC,

    -- Edition metadata
    player_name                   TEXT,
    series                        TEXT,
    play_category                 TEXT,
    team                          TEXT,
    existing_supply               INTEGER,
    has_parallels                 BOOLEAN,

    -- Image CDN moment IDs
    nft_id_serial1                TEXT,
    nft_id_recent                 TEXT,

    -- Holder analytics
    unique_holders                INTEGER,
    largest_holder_count          INTEGER,
    concentration_pct             NUMERIC,

    -- ASPs
    asp_lifetime                  NUMERIC,
    asp_dapper                    NUMERIC,

    -- Floor prices
    floor_price                   NUMERIC,
    floor_price_dapper            NUMERIC,
    floor_price_flowty            NUMERIC,
    floor_price_any               NUMERIC,

    -- Sales counts
    total_sales_180d              INTEGER,
    total_sales_lifetime          INTEGER,

    -- 7-day metrics
    total_sales_7d                INTEGER,
    total_volume_7d               NUMERIC,
    asp_7d                        NUMERIC,
    days_with_offers_7d           INTEGER,
    avg_offer_7d                  NUMERIC,

    -- 30-day metrics
    total_sales_30d               INTEGER,
    total_volume_30d              NUMERIC,
    asp_30d                       NUMERIC,
    asp_30d_raw                   NUMERIC,
    special_sales_30d_count       INTEGER,
    outlier_sales_30d_count       INTEGER,
    non_special_sales_30d_count   INTEGER,
    p05_ex_special_30d            NUMERIC,
    p95_ex_special_30d            NUMERIC,
    days_with_offers_30d          INTEGER,
    avg_offer_30d                 NUMERIC,

    -- 180-day volume/offers
    total_volume_180d             NUMERIC,
    days_with_offers_180d         INTEGER,
    avg_offer_180d                NUMERIC,
    avg_daily_high_offer_180d     NUMERIC,

    -- Lifetime volume
    total_volume_lifetime         NUMERIC,

    -- Temporal metadata
    first_sale_timestamp          TIMESTAMPTZ,
    last_sale_timestamp           TIMESTAMPTZ,
    days_since_last_sale          INTEGER,

    -- Offer & listing analytics
    edition_offer_count           INTEGER,
    total_edition_offer_volume    NUMERIC,
    avg_edition_offer             NUMERIC,
    lowest_edition_offer          NUMERIC,
    unique_edition_bidders        INTEGER,
    last_sale_price               NUMERIC,
    price_change_pct              NUMERIC,
    price_trend_7d_vs_30d        NUMERIC,
    price_trend_30d_vs_180d      NUMERIC,

    -- Listing counts
    total_listings                INTEGER,
    total_listings_dapper         INTEGER,
    total_listings_flowty         INTEGER,
    median_listing_price          NUMERIC,
    median_listing_price_dapper   NUMERIC,
    listings_at_floor             INTEGER,
    listings_at_floor_dapper      INTEGER,
    listing_price_ratio           NUMERIC,

    -- Basic derived metrics
    liquidity_spread_pct          NUMERIC,
    floating_supply_pct           NUMERIC,
    offer_depth_score             NUMERIC,

    -- Normalized scores
    sales_activity_score          INTEGER,
    sales_activity_confidence     TEXT,
    offer_frequency_score         INTEGER,
    offer_frequency_confidence    TEXT,
    market_freshness_score        INTEGER,
    sales_momentum_score          INTEGER,
    sales_momentum_confidence     TEXT,
    daily_volume_delta            NUMERIC,
    liquidity_score               INTEGER,

    -- Valuation
    estimated_value               NUMERIC,
    value_confidence              TEXT,
    market_cap                    NUMERIC,
    sell_through_rate             NUMERIC,
    holder_diversity_pct          NUMERIC,

    data_generated_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_temd_edition_id   ON topshot_edition_market_data (edition_id);
CREATE INDEX IF NOT EXISTS idx_temd_grail_score  ON topshot_edition_market_data (grail_score DESC);

-- ---------------------------------------------------------------------------
-- Refresh function
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION refresh_topshot_edition_market_data()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN

TRUNCATE topshot_edition_market_data;

INSERT INTO topshot_edition_market_data
WITH
  -- Identify which base editions have multiple subedition variants (parallels)
  editions_with_parallels AS (
    SELECT
      (regexp_match(edition_id, '^([0-9]+_[0-9]+)'))[1] AS base_edition
    FROM topshot_moments
    WHERE subedition_id IS NOT NULL
    GROUP BY base_edition
    HAVING COUNT(DISTINCT subedition_id) > 1
  ),

  edition_base AS (
    SELECT
      e.edition_id,
      e.player_name,
      e.series,
      e.play_category,
      e.team,
      e.existing_supply,
      e.listed_count,

      -- Holder analytics from topshot_editions
      e.unique_holders,
      e.largest_holder_count,
      e.concentration_pct,

      CASE
        WHEN (regexp_match(e.edition_id, '^([0-9]+_[0-9]+)'))[1] IN (SELECT base_edition FROM editions_with_parallels)
        THEN TRUE
        ELSE FALSE
      END AS has_parallels
    FROM topshot_editions e
  ),

  -- Offer stats (current active offers) - mint-backed only (prevents unmapped leakage)
  offer_stats AS (
    SELECT
      ao.edition_id,

      MAX(CASE WHEN ao.target_nft_id IS NULL THEN ao.offer_amount ELSE NULL END) AS highest_edition_offer,
      MIN(CASE WHEN ao.target_nft_id IS NULL THEN ao.offer_amount ELSE NULL END) AS lowest_edition_offer,
      AVG(CASE WHEN ao.target_nft_id IS NULL THEN ao.offer_amount ELSE NULL END) AS avg_edition_offer,
      COUNT(CASE WHEN ao.target_nft_id IS NULL THEN 1 ELSE NULL END) AS edition_offer_count,
      COUNT(DISTINCT CASE WHEN ao.target_nft_id IS NULL THEN ao.offer_address ELSE NULL END) AS unique_edition_bidders,
      SUM(CASE WHEN ao.target_nft_id IS NULL THEN ao.offer_amount ELSE NULL END) AS total_edition_offer_volume,

      MAX(CASE WHEN ao.target_nft_id IS NOT NULL THEN ao.offer_amount ELSE NULL END) AS highest_moment_offer,

      MAX(ao.offer_amount) AS highest_offer_overall,
      COUNT(*) AS all_offers_count,
      COUNT(DISTINCT ao.offer_address) AS all_unique_bidders
    FROM topshot_active_offers ao
    JOIN edition_base eb
      ON ao.edition_id = eb.edition_id
    GROUP BY ao.edition_id
  ),

  -- ============================================================
  -- RAW SALES: Simple median for ghost listing anchor (no dependency on floor_prices)
  -- ============================================================
  raw_sales_180d AS (
    SELECT
      m.edition_id,
      COUNT(*) AS raw_sales_count_180d,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY s.price) AS raw_median_price_180d
    FROM topshot_sales s
    JOIN topshot_moments m ON s.moment_id = m.moment_id
    WHERE s.price > 0
      AND s.block_timestamp >= NOW() - INTERVAL '180 days'
    GROUP BY m.edition_id
  ),

  -- Pre-compute median price per edition for ghost detection
  -- (PostgreSQL does not support percentile_cont as a window function)
  listing_median_prices AS (
    SELECT
      al.edition_id,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY al.price) AS median_price
    FROM topshot_active_listings al
    JOIN topshot_events e
      ON e.transaction_hash = al.transaction_hash
     AND e.nft_id = al.moment_id
     AND e.event_type = 'MomentListed'
    WHERE al.price > 0
      AND al.block_timestamp >= NOW() - INTERVAL '365 days'
      AND (
        CASE WHEN TRIM(e.raw_data->>'expiry') ~ '^[0-9]+$' THEN (TRIM(e.raw_data->>'expiry'))::bigint ELSE NULL END IS NULL
        OR CASE WHEN TRIM(e.raw_data->>'expiry') ~ '^[0-9]+$' THEN (TRIM(e.raw_data->>'expiry'))::bigint ELSE NULL END > EXTRACT(EPOCH FROM NOW())::bigint
      )
    GROUP BY al.edition_id
  ),

  listing_edition_counts AS (
    SELECT
      al.edition_id,
      COUNT(*) AS listing_count
    FROM topshot_active_listings al
    JOIN topshot_events e
      ON e.transaction_hash = al.transaction_hash
     AND e.nft_id = al.moment_id
     AND e.event_type = 'MomentListed'
    WHERE al.price > 0
      AND al.block_timestamp >= NOW() - INTERVAL '365 days'
      AND (
        CASE WHEN TRIM(e.raw_data->>'expiry') ~ '^[0-9]+$' THEN (TRIM(e.raw_data->>'expiry'))::bigint ELSE NULL END IS NULL
        OR CASE WHEN TRIM(e.raw_data->>'expiry') ~ '^[0-9]+$' THEN (TRIM(e.raw_data->>'expiry'))::bigint ELSE NULL END > EXTRACT(EPOCH FROM NOW())::bigint
      )
    GROUP BY al.edition_id
  ),

  -- All active listings with marketplace source and ghost detection.
  -- GHOST LISTING PROTECTION:
  --   1. Exclude listings older than 365 days (stale/orphaned)
  --   2. Statistical outlier: price < 10% of edition median (when 3+ listings)
  --   3. Dapper ghost detection: Dapper listing below highest Dapper edition
  --      offer. On Dapper, offers auto-match the lowest listing -- if a listing
  --      sits unfilled below the offer, the moment isn't from that edition
  --      (bad minting events from 2020/2021 Flow blockchain).
  --      Flowty listings below Dapper offers are normal (can't cross-match).
  --   4. Wildly overpriced: price > 100x recent sales median (when 5+ sales)
  --      Prevents overpriced spam from inflating the median and causing
  --      legitimate cheap listings to be incorrectly flagged as ghosts
  verified_listings AS (
    SELECT
      listings.edition_id,
      listings.price,
      listings.listing_source,
      CASE
        -- Rule 1: Statistical outlier (price < 10% of edition median, 3+ listings)
        WHEN lec.listing_count >= 3
         AND listings.price < lmp.median_price * 0.1
        THEN TRUE
        -- Rule 2: Dapper listing below Dapper edition offer (should auto-match)
        WHEN listings.listing_source = 'Dapper_Market'
         AND os.highest_edition_offer > 0
         AND listings.price < os.highest_edition_offer
        THEN TRUE
        -- Rule 3: Wildly overpriced compared to recent sales
        WHEN rs.raw_sales_count_180d >= 5 AND rs.raw_median_price_180d > 0
         AND listings.price > rs.raw_median_price_180d * 100
        THEN TRUE
        ELSE FALSE
      END AS is_ghost
    FROM (
      SELECT
        al.edition_id,
        al.price,
        CASE
          WHEN EXISTS (
            SELECT 1 FROM unnest(e.topics) AS t
            WHERE t IN (
              'A.c1e4f4f4c4257510.TopShotMarketV3.MomentListed',
              'A.c1e4f4f4c4257510.TopShotMarketV2.MomentListed'
            )
          ) THEN 'Dapper_Market'
          WHEN EXISTS (
            SELECT 1 FROM unnest(e.topics) AS t
            WHERE t = 'A.3cdbb3d569211ff3.NFTStorefrontV2.ListingAvailable'
          ) THEN 'Flowty_NFTStorefrontV2'
          ELSE 'Unknown'
        END AS listing_source
      FROM topshot_active_listings al
      JOIN topshot_events e
        ON e.transaction_hash = al.transaction_hash
       AND e.nft_id = al.moment_id
       AND e.event_type = 'MomentListed'
      WHERE al.price > 0
        AND al.block_timestamp >= NOW() - INTERVAL '365 days'
        -- Expiry check at query time (not table build time).
        -- Dapper V2/V3 listings have no $.expiry -> NULL IS NULL -> kept.
        -- Flowty NFTStorefrontV2 listings carry a $.expiry UNIX epoch -> checked live.
        AND (
          CASE WHEN TRIM(e.raw_data->>'expiry') ~ '^[0-9]+$' THEN (TRIM(e.raw_data->>'expiry'))::bigint ELSE NULL END IS NULL
          OR CASE WHEN TRIM(e.raw_data->>'expiry') ~ '^[0-9]+$' THEN (TRIM(e.raw_data->>'expiry'))::bigint ELSE NULL END > EXTRACT(EPOCH FROM NOW())::bigint
        )
    ) listings
    LEFT JOIN offer_stats os ON listings.edition_id = os.edition_id
    LEFT JOIN raw_sales_180d rs ON listings.edition_id = rs.edition_id
    LEFT JOIN listing_median_prices lmp ON listings.edition_id = lmp.edition_id
    LEFT JOIN listing_edition_counts lec ON listings.edition_id = lec.edition_id
  ),

  -- Floors: compute by listing source; keep Dapper floor as canonical
  floor_prices AS (
    SELECT
      edition_id,
      MIN(CASE WHEN listing_source = 'Dapper_Market' AND NOT is_ghost THEN price END) AS floor_price_dapper,
      MIN(CASE WHEN listing_source = 'Flowty_NFTStorefrontV2' AND NOT is_ghost THEN price END) AS floor_price_flowty,
      MIN(CASE WHEN NOT is_ghost THEN price END) AS floor_price_any
    FROM verified_listings
    GROUP BY edition_id
  ),

  -- Listing analytics (all marketplaces + per-marketplace breakdowns)
  -- Ghost listings excluded from counts and averages

  -- Pre-compute median listing prices for listing_analytics
  listing_median_all AS (
    SELECT
      vl.edition_id,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY vl.price) AS median_listing_price_all
    FROM verified_listings vl
    WHERE NOT vl.is_ghost
    GROUP BY vl.edition_id
  ),

  listing_median_dapper AS (
    SELECT
      vl.edition_id,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY vl.price) AS median_listing_price_dapper
    FROM verified_listings vl
    WHERE NOT vl.is_ghost
      AND vl.listing_source = 'Dapper_Market'
    GROUP BY vl.edition_id
  ),

  listing_analytics AS (
    SELECT
      vl.edition_id,
      -- Combined (all marketplaces)
      COUNT(*) AS total_listings_all,
      lma.median_listing_price_all,
      SUM(
        CASE
          WHEN fp.floor_price_any IS NOT NULL AND vl.price <= fp.floor_price_any * 1.1
          THEN 1 ELSE 0
        END
      ) AS listings_at_floor_all,
      -- Dapper only
      COUNT(CASE WHEN vl.listing_source = 'Dapper_Market' THEN 1 END) AS total_listings_dapper,
      lmd.median_listing_price_dapper,
      SUM(
        CASE
          WHEN vl.listing_source = 'Dapper_Market'
            AND fp.floor_price_dapper IS NOT NULL AND vl.price <= fp.floor_price_dapper * 1.1
          THEN 1 ELSE 0
        END
      ) AS listings_at_floor_dapper,
      -- Flowty only
      COUNT(CASE WHEN vl.listing_source = 'Flowty_NFTStorefrontV2' THEN 1 END) AS total_listings_flowty
    FROM verified_listings vl
    LEFT JOIN floor_prices fp ON vl.edition_id = fp.edition_id
    LEFT JOIN listing_median_all lma ON vl.edition_id = lma.edition_id
    LEFT JOIN listing_median_dapper lmd ON vl.edition_id = lmd.edition_id
    WHERE NOT vl.is_ghost
    GROUP BY vl.edition_id, lma.median_listing_price_all, lmd.median_listing_price_dapper
  ),

  -- Last sale info (most recent sale per moment)
  last_sale_info AS (
    SELECT
      m.edition_id,
      s.price AS last_sale_price,
      s.block_timestamp AS last_sale_timestamp_detail
    FROM (
      SELECT
        moment_id,
        price,
        block_timestamp,
        ROW_NUMBER() OVER(PARTITION BY moment_id ORDER BY block_timestamp DESC) AS rn
      FROM topshot_sales
      WHERE price > 0
    ) s
    JOIN topshot_moments m ON s.moment_id = m.moment_id
    WHERE s.rn = 1
  ),

  -- Aggregate last sale to edition level
  edition_last_sale AS (
    SELECT
      edition_id,
      MAX(last_sale_timestamp_detail) AS most_recent_sale_time,
      (ARRAY_AGG(last_sale_price ORDER BY last_sale_timestamp_detail DESC))[1] AS last_sale_price
    FROM last_sale_info
    GROUP BY edition_id
  ),

  -- ============================================================
  -- SALES DATA: Multi-timeframe (RAW ASP)
  -- ============================================================

  sales_7d AS (
    SELECT
      m.edition_id,
      SUM(s.price) AS total_volume_7d,
      COUNT(*) AS total_sales_7d,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY s.price) AS asp_7d
    FROM topshot_sales s
    JOIN topshot_moments m ON s.moment_id = m.moment_id
    WHERE s.block_timestamp >= NOW() - INTERVAL '7 days'
      AND s.price > 0
    GROUP BY m.edition_id
  ),

  sales_30d AS (
    SELECT
      m.edition_id,
      SUM(s.price) AS total_volume_30d,
      COUNT(*) AS total_sales_30d,
      AVG(s.price) AS asp_30d_raw
    FROM topshot_sales s
    JOIN topshot_moments m ON s.moment_id = m.moment_id
    WHERE s.block_timestamp >= NOW() - INTERVAL '30 days'
      AND s.price > 0
    GROUP BY m.edition_id
  ),

  sales_180d AS (
    SELECT
      m.edition_id,
      SUM(s.price) AS total_volume_180d,
      COUNT(*) AS total_sales_180d,
      AVG(s.price) AS asp_180d_raw
    FROM topshot_sales s
    JOIN topshot_moments m ON s.moment_id = m.moment_id
    WHERE s.block_timestamp >= NOW() - INTERVAL '180 days'
      AND s.price > 0
    GROUP BY m.edition_id
  ),

  sales_lifetime AS (
    SELECT
      m.edition_id,
      SUM(s.price) AS total_volume_lifetime,
      COUNT(*) AS total_sales_lifetime,
      AVG(s.price) AS asp_lifetime,
      MIN(s.block_timestamp) AS first_sale_timestamp
    FROM topshot_sales s
    JOIN topshot_moments m ON s.moment_id = m.moment_id
    WHERE s.price > 0
    GROUP BY m.edition_id
  ),

  -- ============================================================
  -- SPECIAL SERIAL CLEANING
  -- Exclude: #1, jersey match, last mint; median of remaining
  -- (p05/p95 retained for diagnostic output only)
  -- ============================================================

  mint_serial AS (
    SELECT DISTINCT ON (e.nft_id)
      e.nft_id AS moment_id,
      CASE WHEN TRIM(e.raw_data->>'serialNumber') ~ '^[0-9]+$' THEN (TRIM(e.raw_data->>'serialNumber'))::bigint ELSE NULL END AS serial_number
    FROM topshot_events e
    WHERE e.event_type = 'MomentMinted'
    ORDER BY e.nft_id, e.block_timestamp ASC, e.event_order ASC, e.transaction_hash ASC
  ),

  play_jersey AS (
    SELECT
      play_id,
      CASE WHEN NULLIF(NULLIF(jersey_number, 'N/A'), '') ~ '^[0-9]+$' THEN (NULLIF(NULLIF(jersey_number, 'N/A'), ''))::bigint ELSE NULL END AS jersey_number_int
    FROM topshot_plays
  ),

  tagged_sales_30d AS (
    SELECT
      m.edition_id,
      s.price,
      ms.serial_number,
      pj.jersey_number_int,
      ed.mint_count,
      COALESCE(
        ms.serial_number = 1
        OR (pj.jersey_number_int IS NOT NULL AND ms.serial_number = pj.jersey_number_int)
        OR (ed.mint_count IS NOT NULL AND ms.serial_number = ed.mint_count),
        false
      ) AS is_special_serial,
      -- WASH TRADE / FRAUD DETECTION: sale price > 10x floor = outlier
      (
        COALESCE(fp.floor_price_any, 0) > 0
        AND s.price > 10 * fp.floor_price_any
      ) AS is_price_outlier
    FROM topshot_sales s
    JOIN topshot_moments m
      ON s.moment_id = m.moment_id
    LEFT JOIN mint_serial ms
      ON s.moment_id = ms.moment_id
    LEFT JOIN play_jersey pj
      ON m.play_id = pj.play_id
    LEFT JOIN topshot_editions ed
      ON m.edition_id = ed.edition_id
    LEFT JOIN floor_prices fp
      ON m.edition_id = fp.edition_id
    WHERE s.block_timestamp >= NOW() - INTERVAL '30 days'
      AND s.price > 0
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

  -- Pre-compute 30d clean median (can't use percentile_cont with FILTER as aggregate + WITHIN GROUP)
  clean_median_30d AS (
    SELECT
      edition_id,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS asp_30d_clean
    FROM tagged_sales_30d
    WHERE NOT is_special_serial AND NOT is_price_outlier
    GROUP BY edition_id
  ),

  sales_30d_clean AS (
    SELECT
      t.edition_id,
      COUNT(*) AS sales_30d_count,
      COUNT(*) FILTER (WHERE t.is_special_serial) AS special_sales_30d_count,
      COUNT(*) FILTER (WHERE t.is_price_outlier) AS outlier_sales_30d_count,
      COUNT(*) FILTER (WHERE NOT t.is_special_serial AND NOT t.is_price_outlier) AS non_special_sales_30d_count,
      AVG(t.price) AS asp_30d_raw_check,
      -- Median of clean prices: excludes special serials AND wash trade outliers
      cm.asp_30d_clean,
      q.p05 AS p05_ex_special_30d,
      q.p95 AS p95_ex_special_30d
    FROM tagged_sales_30d t
    LEFT JOIN q30 q USING (edition_id)
    LEFT JOIN clean_median_30d cm USING (edition_id)
    GROUP BY t.edition_id, q.p05, q.p95, cm.asp_30d_clean
  ),

  tagged_sales_180d AS (
    SELECT
      m.edition_id,
      s.price,
      ms.serial_number,
      pj.jersey_number_int,
      ed.mint_count,
      COALESCE(
        ms.serial_number = 1
        OR (pj.jersey_number_int IS NOT NULL AND ms.serial_number = pj.jersey_number_int)
        OR (ed.mint_count IS NOT NULL AND ms.serial_number = ed.mint_count),
        false
      ) AS is_special_serial,
      -- WASH TRADE / FRAUD DETECTION: sale price > 10x floor = outlier
      (
        COALESCE(fp.floor_price_any, 0) > 0
        AND s.price > 10 * fp.floor_price_any
      ) AS is_price_outlier
    FROM topshot_sales s
    JOIN topshot_moments m
      ON s.moment_id = m.moment_id
    LEFT JOIN mint_serial ms
      ON s.moment_id = ms.moment_id
    LEFT JOIN play_jersey pj
      ON m.play_id = pj.play_id
    LEFT JOIN topshot_editions ed
      ON m.edition_id = ed.edition_id
    LEFT JOIN floor_prices fp
      ON m.edition_id = fp.edition_id
    WHERE s.block_timestamp >= NOW() - INTERVAL '180 days'
      AND s.price > 0
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

  -- Pre-compute 180d clean median
  clean_median_180d AS (
    SELECT
      edition_id,
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS asp_180d_clean
    FROM tagged_sales_180d
    WHERE NOT is_special_serial AND NOT is_price_outlier
    GROUP BY edition_id
  ),

  sales_180d_clean AS (
    SELECT
      t.edition_id,
      COUNT(*) AS sales_180d_count,
      COUNT(*) FILTER (WHERE t.is_special_serial) AS special_sales_180d_count,
      COUNT(*) FILTER (WHERE t.is_price_outlier) AS outlier_sales_180d_count,
      COUNT(*) FILTER (WHERE NOT t.is_special_serial AND NOT t.is_price_outlier) AS non_special_sales_180d_count,
      AVG(t.price) AS asp_180d_raw_check,
      -- Median of clean prices: excludes special serials AND wash trade outliers
      cm.asp_180d_clean,
      q.p05 AS p05_ex_special_180d,
      q.p95 AS p95_ex_special_180d
    FROM tagged_sales_180d t
    LEFT JOIN q180 q USING (edition_id)
    LEFT JOIN clean_median_180d cm USING (edition_id)
    GROUP BY t.edition_id, q.p05, q.p95, cm.asp_180d_clean
  ),

  -- ============================================================
  -- OFFERS DATA: Multi-timeframe (offer history) - mint-backed only
  -- ============================================================

  offers_7d AS (
    SELECT
      o.edition_id,
      COUNT(DISTINCT o.block_timestamp::date) AS days_with_offers_7d,
      AVG(o.offer_amount) AS avg_offer_7d
    FROM topshot_offers o
    JOIN edition_base eb
      ON o.edition_id = eb.edition_id
    WHERE o.event_type = 'OfferAvailable'
      AND o.block_timestamp::date >= CURRENT_DATE - INTERVAL '7 days'
      AND o.offer_amount > 0
    GROUP BY o.edition_id
  ),

  offers_30d AS (
    SELECT
      o.edition_id,
      COUNT(DISTINCT o.block_timestamp::date) AS days_with_offers_30d,
      AVG(o.offer_amount) AS avg_offer_30d
    FROM topshot_offers o
    JOIN edition_base eb
      ON o.edition_id = eb.edition_id
    WHERE o.event_type = 'OfferAvailable'
      AND o.block_timestamp::date >= CURRENT_DATE - INTERVAL '30 days'
      AND o.offer_amount > 0
    GROUP BY o.edition_id
  ),

  offers_180d AS (
    SELECT
      o.edition_id,
      COUNT(DISTINCT o.block_timestamp::date) AS days_with_offers_180d,
      AVG(o.offer_amount) AS avg_offer_180d
    FROM topshot_offers o
    JOIN edition_base eb
      ON o.edition_id = eb.edition_id
    WHERE o.event_type = 'OfferAvailable'
      AND o.block_timestamp::date >= CURRENT_DATE - INTERVAL '180 days'
      AND o.offer_amount > 0
    GROUP BY o.edition_id
  ),

  avg_daily_high_offer_180d AS (
    SELECT
      edition_id,
      AVG(daily_high) AS avg_daily_high_offer_180d
    FROM (
      SELECT
        o.edition_id,
        o.block_timestamp::date AS offer_date,
        MAX(o.offer_amount) AS daily_high
      FROM topshot_offers o
      JOIN edition_base eb ON o.edition_id = eb.edition_id
      WHERE o.event_type = 'OfferAvailable'
        AND o.offer_amount > 0
        AND o.block_timestamp::date >= CURRENT_DATE - INTERVAL '180 days'
      GROUP BY o.edition_id, o.block_timestamp::date
    ) sub
    GROUP BY edition_id
  ),

  -- ============================================================
  -- NORMALIZATION FACTORS: Market-wide percentiles
  -- ============================================================

  bidder_percentiles AS (
    SELECT percentile_cont(0.9) WITHIN GROUP (ORDER BY bidder_count) AS bidders_p90
    FROM (
      SELECT
        ao.edition_id,
        COUNT(DISTINCT ao.offer_address) AS bidder_count
      FROM topshot_active_offers ao
      JOIN edition_base eb
        ON ao.edition_id = eb.edition_id
      WHERE ao.target_nft_id IS NULL
      GROUP BY ao.edition_id
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
    FROM edition_base eb
    LEFT JOIN sales_7d s7 ON eb.edition_id = s7.edition_id
    LEFT JOIN sales_30d s30 ON eb.edition_id = s30.edition_id
    LEFT JOIN sales_180d s180 ON eb.edition_id = s180.edition_id
    LEFT JOIN offers_7d o7 ON eb.edition_id = o7.edition_id
    LEFT JOIN offers_30d o30 ON eb.edition_id = o30.edition_id
    LEFT JOIN offers_180d o180 ON eb.edition_id = o180.edition_id
    CROSS JOIN bidder_percentiles bp
  ),

  -- Sample moment IDs per edition for Dapper media gateway image URLs
  -- Serial #1 is the most iconic; recent moment_id as fallback if #1 is burned
  sample_moments AS (
    SELECT
      m.edition_id,
      (ARRAY_AGG(m.moment_id ORDER BY ms.serial_number ASC))[1] AS nft_id_serial1,
      (ARRAY_AGG(m.moment_id ORDER BY m.moment_id DESC))[1] AS nft_id_recent
    FROM topshot_moments m
    LEFT JOIN mint_serial ms ON m.moment_id = ms.moment_id
    GROUP BY m.edition_id
  ),

  -- ============================================================
  -- DAPPER ASP: Average of last 10 sales (Dapper's own formula)
  -- ============================================================
  dapper_asp AS (
    SELECT
      edition_id,
      ROUND(AVG(price), 2) AS asp_dapper
    FROM (
      SELECT
        m.edition_id,
        s.price,
        ROW_NUMBER() OVER (PARTITION BY m.edition_id ORDER BY s.block_timestamp DESC) AS rn
      FROM topshot_sales s
      JOIN topshot_moments m ON s.moment_id = m.moment_id
      WHERE s.price > 0
    ) sub
    WHERE rn <= 10
    GROUP BY edition_id
  ),

  -- ============================================================
  -- ESTIMATED VALUE helper CTE: extract Model 5 floor-blend logic once
  -- to avoid duplication between estimated_value and market_cap
  -- ============================================================
  estimated_value_cte AS (
    SELECT
      eb.edition_id,
      CASE
        -- Tier 1: HIGH VOLUME (10+ sales) -- recency-weighted ASP
        WHEN COALESCE(s180.total_sales_180d, 0) >= 10
          AND s180c.asp_180d_clean IS NOT NULL
          THEN
            CASE
              -- Guardrail: FLOOR_CAP -- cap at floor when blend > 2x floor and floor has supply
              WHEN COALESCE(la.total_listings_all, 0) >= 2
                AND COALESCE(fp.floor_price_any, 0) > 0
                AND COALESCE(CASE
                  WHEN COALESCE(s30dc.asp_30d_clean, 0) > 0 AND COALESCE(s30d.total_sales_30d, 0) >= 3
                    THEN (s30dc.asp_30d_clean * 0.70) + (s180c.asp_180d_clean * 0.30)
                  WHEN COALESCE(s30dc.asp_30d_clean, 0) > 0 AND COALESCE(s30d.total_sales_30d, 0) BETWEEN 1 AND 2
                    THEN (s30dc.asp_30d_clean * 0.50) + (s180c.asp_180d_clean * 0.50)
                  ELSE s180c.asp_180d_clean
                END, s180c.asp_180d_clean) > fp.floor_price_any * 2
              THEN fp.floor_price_any
              -- Standard: recency blend (no offer floor)
              ELSE COALESCE(CASE
                WHEN COALESCE(s30dc.asp_30d_clean, 0) > 0 AND COALESCE(s30d.total_sales_30d, 0) >= 3
                  THEN (s30dc.asp_30d_clean * 0.70) + (s180c.asp_180d_clean * 0.30)
                WHEN COALESCE(s30dc.asp_30d_clean, 0) > 0 AND COALESCE(s30d.total_sales_30d, 0) BETWEEN 1 AND 2
                  THEN (s30dc.asp_30d_clean * 0.50) + (s180c.asp_180d_clean * 0.50)
                ELSE s180c.asp_180d_clean
              END, s180c.asp_180d_clean)
            END
        -- Tier 2: MEDIUM (3-9 sales) with clean ASP and floor -- blend 70/30 ASP/floor
        WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9
          AND s180c.asp_180d_clean IS NOT NULL
          AND COALESCE(fp.floor_price_any, 0) > 0
          AND fp.floor_price_any <= s180c.asp_180d_clean * 3  -- GUARDRAIL: reject insane floors
          THEN (s180c.asp_180d_clean * 0.70) + (fp.floor_price_any * 0.30)
        -- Tier 2b: MEDIUM (3-9 sales) with clean ASP, no floor -- ASP only
        WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9
          AND s180c.asp_180d_clean IS NOT NULL
          THEN s180c.asp_180d_clean
        -- Tier 3: LOW (1-2 sales) with clean ASP and floor -- blend 50/50 ASP/floor
        WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
          AND s180c.asp_180d_clean IS NOT NULL
          AND COALESCE(fp.floor_price_any, 0) > 0
          AND fp.floor_price_any <= s180c.asp_180d_clean * 3  -- GUARDRAIL: reject insane floors
          THEN (s180c.asp_180d_clean * 0.50) + (fp.floor_price_any * 0.50)
        -- Tier 3b: LOW (1-2 sales) with clean ASP, no floor -- ASP only
        WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2
          AND s180c.asp_180d_clean IS NOT NULL
          THEN s180c.asp_180d_clean
        -- Fallbacks: no clean ASP available
        -- When sales exist but all excluded (special/outlier), use LEAST of raw ASP and floor
        WHEN COALESCE(s180.asp_180d_raw, 0) > 0 AND COALESCE(fp.floor_price_any, 0) > 0
          THEN LEAST(s180.asp_180d_raw, fp.floor_price_any)
        WHEN COALESCE(s180.asp_180d_raw, 0) > 0
          THEN s180.asp_180d_raw
        -- No 180d sales: use last sale price (capped at floor if both exist)
        WHEN COALESCE(els.last_sale_price, 0) > 0 AND COALESCE(fp.floor_price_any, 0) > 0
          THEN LEAST(els.last_sale_price, fp.floor_price_any)
        WHEN COALESCE(els.last_sale_price, 0) > 0
          THEN els.last_sale_price
        -- No sales data: NULL (floor alone doesn't establish value)
        ELSE NULL
      END AS estimated_value
    FROM edition_base eb
    LEFT JOIN floor_prices fp ON eb.edition_id = fp.edition_id
    LEFT JOIN listing_analytics la ON eb.edition_id = la.edition_id
    LEFT JOIN edition_last_sale els ON eb.edition_id = els.edition_id
    LEFT JOIN sales_30d s30d ON eb.edition_id = s30d.edition_id
    LEFT JOIN sales_30d_clean s30dc ON eb.edition_id = s30dc.edition_id
    LEFT JOIN sales_180d s180 ON eb.edition_id = s180.edition_id
    LEFT JOIN sales_180d_clean s180c ON eb.edition_id = s180c.edition_id
  )

SELECT
  eb.edition_id,

  -- ============================================================
  -- CORE PRICE METRICS
  -- ============================================================
  ROUND(
    GREATEST(
      COALESCE(s180c.asp_180d_clean, 0),
      COALESCE(os.highest_edition_offer, 0)
    ), 2
  ) AS grail_score,

  ROUND(COALESCE(os.highest_edition_offer, 0), 2) AS highest_edition_offer,
  ROUND(COALESCE(os.highest_moment_offer, 0), 2) AS highest_moment_offer,

  -- CLEAN (median, excludes special serials + wash trades) + RAW (mean) ASPs
  ROUND(COALESCE(s180c.asp_180d_clean, 0), 2) AS asp_180d,
  ROUND(COALESCE(s180.asp_180d_raw, 0), 2) AS asp_180d_raw,
  COALESCE(s180c.special_sales_180d_count, 0) AS special_sales_180d_count,
  COALESCE(s180c.outlier_sales_180d_count, 0) AS outlier_sales_180d_count,
  COALESCE(s180c.non_special_sales_180d_count, 0) AS non_special_sales_180d_count,
  ROUND(COALESCE(s180c.p05_ex_special_180d, 0), 2) AS p05_ex_special_180d,
  ROUND(COALESCE(s180c.p95_ex_special_180d, 0), 2) AS p95_ex_special_180d,

  eb.player_name,
  eb.series,
  eb.play_category,
  eb.team,
  eb.existing_supply,
  eb.has_parallels,

  -- Image CDN moment IDs (for https://assets.nbatopshot.com/media/{id}/image)
  sm.nft_id_serial1,
  sm.nft_id_recent,

  -- holder analytics
  eb.unique_holders,
  eb.largest_holder_count,
  eb.concentration_pct,

  ROUND(COALESCE(sl.asp_lifetime, 0), 2) AS asp_lifetime,
  ROUND(COALESCE(da.asp_dapper, 0), 2) AS asp_dapper,

  ROUND(COALESCE(fp.floor_price_any, 0), 2) AS floor_price,
  ROUND(COALESCE(fp.floor_price_dapper, 0), 2) AS floor_price_dapper,
  ROUND(COALESCE(fp.floor_price_flowty, 0), 2) AS floor_price_flowty,
  ROUND(COALESCE(fp.floor_price_any, 0), 2) AS floor_price_any,

  COALESCE(s180.total_sales_180d, 0) AS total_sales_180d,
  COALESCE(sl.total_sales_lifetime, 0) AS total_sales_lifetime,

  -- ============================================================
  -- MULTI-TIMEFRAME RAW DATA
  -- ============================================================

  COALESCE(s7d.total_sales_7d, 0) AS total_sales_7d,
  ROUND(COALESCE(s7d.total_volume_7d, 0), 2) AS total_volume_7d,
  ROUND(COALESCE(s7d.asp_7d, 0), 2) AS asp_7d,
  COALESCE(o7d.days_with_offers_7d, 0) AS days_with_offers_7d,
  ROUND(COALESCE(o7d.avg_offer_7d, 0), 2) AS avg_offer_7d,

  COALESCE(s30d.total_sales_30d, 0) AS total_sales_30d,
  ROUND(COALESCE(s30d.total_volume_30d, 0), 2) AS total_volume_30d,

  -- CLEAN (median, excludes special serials + wash trades) + RAW (mean) 30d ASP
  ROUND(COALESCE(s30dc.asp_30d_clean, 0), 2) AS asp_30d,
  ROUND(COALESCE(s30d.asp_30d_raw, 0), 2) AS asp_30d_raw,
  COALESCE(s30dc.special_sales_30d_count, 0) AS special_sales_30d_count,
  COALESCE(s30dc.outlier_sales_30d_count, 0) AS outlier_sales_30d_count,
  COALESCE(s30dc.non_special_sales_30d_count, 0) AS non_special_sales_30d_count,
  ROUND(COALESCE(s30dc.p05_ex_special_30d, 0), 2) AS p05_ex_special_30d,
  ROUND(COALESCE(s30dc.p95_ex_special_30d, 0), 2) AS p95_ex_special_30d,

  COALESCE(o30d.days_with_offers_30d, 0) AS days_with_offers_30d,
  ROUND(COALESCE(o30d.avg_offer_30d, 0), 2) AS avg_offer_30d,

  ROUND(COALESCE(s180.total_volume_180d, 0), 2) AS total_volume_180d,
  COALESCE(o180.days_with_offers_180d, 0) AS days_with_offers_180d,
  ROUND(COALESCE(o180.avg_offer_180d, 0), 2) AS avg_offer_180d,
  ROUND(COALESCE(adh180.avg_daily_high_offer_180d, 0), 2) AS avg_daily_high_offer_180d,

  ROUND(COALESCE(sl.total_volume_lifetime, 0), 2) AS total_volume_lifetime,

  -- Temporal metadata
  sl.first_sale_timestamp,
  els.most_recent_sale_time AS last_sale_timestamp,
  CASE
    WHEN els.most_recent_sale_time IS NOT NULL
    THEN (CURRENT_DATE - els.most_recent_sale_time::date)
    ELSE NULL
  END AS days_since_last_sale,

  -- ============================================================
  -- OFFER & LISTING ANALYTICS
  -- ============================================================

  COALESCE(os.edition_offer_count, 0) AS edition_offer_count,
  ROUND(COALESCE(os.total_edition_offer_volume, 0), 2) AS total_edition_offer_volume,
  ROUND(COALESCE(os.avg_edition_offer, 0), 2) AS avg_edition_offer,
  ROUND(COALESCE(os.lowest_edition_offer, 0), 2) AS lowest_edition_offer,
  COALESCE(os.unique_edition_bidders, 0) AS unique_edition_bidders,

  ROUND(COALESCE(els.last_sale_price, 0), 2) AS last_sale_price,

  -- Premium/discount: how much the clean median sale price exceeds (or trails) floor
  ROUND(
    CASE
      WHEN NULLIF(COALESCE(fp.floor_price_any, 0), 0) IS NOT NULL
           AND COALESCE(s180c.asp_180d_clean, 0) > 0
      THEN ((s180c.asp_180d_clean - fp.floor_price_any) / fp.floor_price_any) * 100
      ELSE NULL
    END, 2
  ) AS price_change_pct,

  -- Price Trend Indicators (7d vs CLEAN 30d; CLEAN 30d vs CLEAN 180d)
  ROUND(
    CASE
      WHEN s30dc.asp_30d_clean IS NOT NULL AND s30dc.asp_30d_clean > 0
           AND COALESCE(s7d.asp_7d, 0) > 0
      THEN ((s7d.asp_7d - s30dc.asp_30d_clean) / s30dc.asp_30d_clean) * 100
      ELSE NULL
    END, 2
  ) AS price_trend_7d_vs_30d,

  ROUND(
    CASE
      WHEN s180c.asp_180d_clean IS NOT NULL AND s180c.asp_180d_clean > 0
           AND s30dc.asp_30d_clean IS NOT NULL AND s30dc.asp_30d_clean > 0
      THEN ((s30dc.asp_30d_clean - s180c.asp_180d_clean) / s180c.asp_180d_clean) * 100
      ELSE NULL
    END, 2
  ) AS price_trend_30d_vs_180d,

  COALESCE(la.total_listings_all, 0) AS total_listings,
  COALESCE(la.total_listings_dapper, 0) AS total_listings_dapper,
  COALESCE(la.total_listings_flowty, 0) AS total_listings_flowty,
  ROUND(COALESCE(la.median_listing_price_all, 0), 2) AS median_listing_price,
  ROUND(COALESCE(la.median_listing_price_dapper, 0), 2) AS median_listing_price_dapper,
  COALESCE(la.listings_at_floor_all, 0) AS listings_at_floor,
  COALESCE(la.listings_at_floor_dapper, 0) AS listings_at_floor_dapper,

  -- Ratio of median listing price to floor (1.0 = all listings at floor)
  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price_any, 0) IS NOT NULL
        AND COALESCE(la.median_listing_price_all, 0) > 0
      THEN la.median_listing_price_all / fp.floor_price_any
      ELSE NULL
    END, 2
  ) AS listing_price_ratio,

  -- ============================================================
  -- BASIC DERIVED METRICS
  -- ============================================================

  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price_dapper, 0) IS NOT NULL AND COALESCE(os.highest_edition_offer, 0) > 0
      THEN ((fp.floor_price_dapper - os.highest_edition_offer) / fp.floor_price_dapper) * 100
      ELSE NULL
    END, 2
  ) AS liquidity_spread_pct,

  ROUND(
    CASE
      WHEN NULLIF(eb.existing_supply, 0) IS NOT NULL
      THEN (eb.listed_count::double precision / eb.existing_supply) * 100
      ELSE NULL
    END, 2
  ) AS floating_supply_pct,

  -- Offer depth: total capital committed in offers relative to floor price
  ROUND(
    CASE
      WHEN NULLIF(fp.floor_price_dapper, 0) IS NOT NULL
        AND COALESCE(os.total_edition_offer_volume, 0) > 0
      THEN os.total_edition_offer_volume / fp.floor_price_dapper
      ELSE 0
    END, 2
  ) AS offer_depth_score,

  -- ============================================================
  -- NORMALIZED SCORES (0-100)
  -- ============================================================

  LEAST(100, (
    (LEAST(1.0, COALESCE(s7d.total_sales_7d, 0) /
      NULLIF((SELECT sales_7d_p90 FROM normalization_factors), 0)) * 50)
    + (LEAST(1.0, COALESCE(s30d.total_sales_30d, 0) /
      NULLIF((SELECT sales_30d_p90 FROM normalization_factors), 0)) * 30)
    + (LEAST(1.0, COALESCE(s180.total_sales_180d, 0) /
      NULLIF((SELECT sales_180d_p90 FROM normalization_factors), 0)) * 20)
  )::bigint) AS sales_activity_score,

  CASE
    WHEN COALESCE(s30d.total_sales_30d, 0) >= 5 THEN 'high'
    WHEN COALESCE(s30d.total_sales_30d, 0) BETWEEN 2 AND 4 THEN 'medium'
    WHEN COALESCE(s30d.total_sales_30d, 0) = 1 THEN 'low'
    ELSE 'none'
  END AS sales_activity_confidence,

  LEAST(100, (
    (LEAST(1.0, COALESCE(o7d.days_with_offers_7d, 0) /
      NULLIF((SELECT offers_7d_p90 FROM normalization_factors), 0)) * 40)
    + (LEAST(1.0, COALESCE(o30d.days_with_offers_30d, 0) /
      NULLIF((SELECT offers_30d_p90 FROM normalization_factors), 0)) * 30)
    + (LEAST(1.0, COALESCE(os.unique_edition_bidders, 0) /
      NULLIF((SELECT bidders_p90 FROM normalization_factors), 0)) * 30)
  )::bigint) AS offer_frequency_score,

  CASE
    WHEN COALESCE(o180.days_with_offers_180d, 0) >= 10 THEN 'high'
    WHEN COALESCE(o180.days_with_offers_180d, 0) BETWEEN 3 AND 9 THEN 'medium'
    WHEN COALESCE(o180.days_with_offers_180d, 0) BETWEEN 1 AND 2 THEN 'low'
    ELSE 'none'
  END AS offer_frequency_confidence,

  CASE
    WHEN els.most_recent_sale_time IS NULL THEN 0
    WHEN (CURRENT_DATE - els.most_recent_sale_time::date) <= 7 THEN 100
    WHEN (CURRENT_DATE - els.most_recent_sale_time::date) <= 30 THEN 75
    WHEN (CURRENT_DATE - els.most_recent_sale_time::date) <= 90 THEN 50
    WHEN (CURRENT_DATE - els.most_recent_sale_time::date) <= 180 THEN 25
    ELSE 0
  END AS market_freshness_score,

  LEAST(100, GREATEST(-100, (
    CASE
      WHEN COALESCE(s30d.total_sales_30d, 0) >= 3
      THEN (((COALESCE(s7d.total_sales_7d, 0) / 7.0) - (s30d.total_sales_30d / 30.0)) /
            NULLIF(s30d.total_sales_30d / 30.0, 0)) * 50
      ELSE 0
    END
    + CASE
      WHEN COALESCE(s180.total_sales_180d, 0) >= 5
      THEN (((COALESCE(s30d.total_sales_30d, 0) / 30.0) - (s180.total_sales_180d / 180.0)) /
            NULLIF(s180.total_sales_180d / 180.0, 0)) * 30
      ELSE 0
    END
    + CASE
      WHEN COALESCE(sl.total_sales_lifetime, 0) >= 10
        AND sl.first_sale_timestamp IS NOT NULL
      THEN (((COALESCE(s180.total_sales_180d, 0) / 180.0) -
             (sl.total_sales_lifetime / GREATEST(1, (CURRENT_DATE - sl.first_sale_timestamp::date)))) /
            NULLIF(sl.total_sales_lifetime / GREATEST(1, (CURRENT_DATE - sl.first_sale_timestamp::date)), 0)) * 20
      ELSE 0
    END
  )::bigint)) AS sales_momentum_score,

  CASE
    WHEN COALESCE(s7d.total_sales_7d, 0) >= 3 AND COALESCE(s30d.total_sales_30d, 0) >= 3 THEN 'high'
    WHEN COALESCE(s7d.total_sales_7d, 0) >= 1 AND COALESCE(s30d.total_sales_30d, 0) >= 1 THEN 'medium'
    WHEN COALESCE(s7d.total_sales_7d, 0) + COALESCE(s30d.total_sales_30d, 0) > 0 THEN 'low'
    ELSE 'none'
  END AS sales_momentum_confidence,

  -- Daily volume change: % difference between 7d and 30d daily averages
  ROUND(
    CASE
      WHEN NULLIF(s30d.total_volume_30d / 30.0, 0) IS NOT NULL
           AND s7d.total_volume_7d IS NOT NULL
      THEN (((s7d.total_volume_7d / 7.0) - (s30d.total_volume_30d / 30.0))
            / (s30d.total_volume_30d / 30.0)) * 100
      ELSE NULL
    END, 2
  ) AS daily_volume_delta,

  LEAST(100, (
    -- 40%: sales volume relative to market
    (LEAST(1.0, COALESCE(s180.total_sales_180d, 0) /
      NULLIF((SELECT sales_180d_p90 FROM normalization_factors), 0)) * 40)
    -- 30%: unique bidders relative to market
    + (LEAST(1.0, COALESCE(os.unique_edition_bidders, 0) /
      NULLIF((SELECT bidders_p90 FROM normalization_factors), 0)) * 30)
    -- 20%: sell-side depth (proportional to floating supply, capped at 20)
    + (CASE
        WHEN NULLIF(eb.existing_supply, 0) IS NOT NULL AND COALESCE(eb.listed_count, 0) > 0
        THEN LEAST(20.0, (eb.listed_count::double precision / eb.existing_supply) * 200)
        ELSE 0
      END)
    -- 10%: bid-ask spread tightness
    + (CASE
        WHEN NULLIF(fp.floor_price_dapper, 0) IS NOT NULL AND COALESCE(os.highest_edition_offer, 0) > 0
        THEN LEAST(10, (1 - ((fp.floor_price_dapper - os.highest_edition_offer) / fp.floor_price_dapper)) * 10)
        ELSE 0
      END)
  )::bigint) AS liquidity_score,

  -- ============================================================
  -- ESTIMATED VALUE: Fair market value via recency-weighted ASP + floor blend
  -- Model 5 (floor-blend) -- uses pre-computed CTE to avoid duplication
  -- ============================================================
  ROUND(ev.estimated_value, 2) AS estimated_value,

  CASE
    WHEN COALESCE(s180.total_sales_180d, 0) >= 10 THEN 'high'
    WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 3 AND 9 THEN 'medium'
    WHEN COALESCE(s180.total_sales_180d, 0) BETWEEN 1 AND 2 THEN 'low'
    WHEN COALESCE(sl.total_sales_lifetime, 0) > 0 THEN 'lifetime'
    ELSE 'none'
  END AS value_confidence,

  -- ============================================================
  -- ADDITIONAL DERIVED METRICS
  -- ============================================================

  -- Market cap: total edition value (estimated_value x supply)
  ROUND(ev.estimated_value * eb.existing_supply, 0) AS market_cap,

  -- Sell-through rate: how many times current listed inventory would turn over in 30d
  ROUND(
    CASE
      WHEN COALESCE(eb.listed_count, 0) > 0 AND COALESCE(s30d.total_sales_30d, 0) > 0
      THEN s30d.total_sales_30d::double precision / eb.listed_count::double precision
      ELSE NULL
    END, 2
  ) AS sell_through_rate,

  -- Holder diversity: % of supply held by unique holders (100% = max diversity)
  ROUND(
    CASE
      WHEN NULLIF(eb.existing_supply, 0) IS NOT NULL
      THEN (eb.unique_holders::double precision / eb.existing_supply) * 100
      ELSE NULL
    END, 2
  ) AS holder_diversity_pct,

  NOW() AS data_generated_at

FROM edition_base eb
LEFT JOIN offer_stats os ON eb.edition_id = os.edition_id
LEFT JOIN floor_prices fp ON eb.edition_id = fp.edition_id
LEFT JOIN listing_analytics la ON eb.edition_id = la.edition_id
LEFT JOIN edition_last_sale els ON eb.edition_id = els.edition_id
LEFT JOIN sales_7d s7d ON eb.edition_id = s7d.edition_id
LEFT JOIN sales_30d s30d ON eb.edition_id = s30d.edition_id
LEFT JOIN sales_30d_clean s30dc ON eb.edition_id = s30dc.edition_id
LEFT JOIN sales_180d s180 ON eb.edition_id = s180.edition_id
LEFT JOIN sales_180d_clean s180c ON eb.edition_id = s180c.edition_id
LEFT JOIN sales_lifetime sl ON eb.edition_id = sl.edition_id
LEFT JOIN offers_7d o7d ON eb.edition_id = o7d.edition_id
LEFT JOIN offers_30d o30d ON eb.edition_id = o30d.edition_id
LEFT JOIN offers_180d o180 ON eb.edition_id = o180.edition_id
LEFT JOIN avg_daily_high_offer_180d adh180 ON eb.edition_id = adh180.edition_id
LEFT JOIN sample_moments sm ON eb.edition_id = sm.edition_id
LEFT JOIN dapper_asp da ON eb.edition_id = da.edition_id
LEFT JOIN estimated_value_cte ev ON eb.edition_id = ev.edition_id
;

END;
$$;
