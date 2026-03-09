-- =================================================================================
-- 03_packnft_analytics.sql
-- Per-drop EV and summary statistics for pack distributions (PostgreSQL)
--
-- Ported from BigQuery: vaultopolis/google/queries/pipeline/4_PACKNFT_analytics.sql
--
-- PREREQUISITE: All of the following must be complete before running:
--   - 02_packnft_tables.sql  (provides pack_distributions and pack_contents)
--   - topshot_edition_market_data table (provides EV values)
--
-- TABLES MAINTAINED:
--   pack_distribution_analytics — Per-drop pack EV, min/max/avg, confidence level
-- =================================================================================

-- ---------------------------------------------------------------------------
-- Table definition
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pack_distribution_analytics (
    dist_id              text PRIMARY KEY,
    first_mint_timestamp timestamptz,
    last_mint_timestamp  timestamptz,
    total_packs          bigint,
    sealed_count         bigint,
    revealed_count       bigint,
    opened_count         bigint,
    burned_count         bigint,
    revealed_with_values bigint,
    avg_pack_value       numeric(12,2),
    median_pack_value    numeric(12,2),
    min_pack_value       numeric(12,2),
    max_pack_value       numeric(12,2),
    avg_moments_per_pack numeric(8,1),
    ev_confidence        text
);

-- ---------------------------------------------------------------------------
-- Refresh function (TRUNCATE + INSERT)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION refresh_packnft_analytics()
RETURNS void
LANGUAGE sql
AS $$
    TRUNCATE pack_distribution_analytics;

    INSERT INTO pack_distribution_analytics (
        dist_id,
        first_mint_timestamp,
        last_mint_timestamp,
        total_packs,
        sealed_count,
        revealed_count,
        opened_count,
        burned_count,
        revealed_with_values,
        avg_pack_value,
        median_pack_value,
        min_pack_value,
        max_pack_value,
        avg_moments_per_pack,
        ev_confidence
    )
    WITH
    -- Value each moment using the freshly updated edition market data
    MomentValues AS (
        SELECT
            pc.pack_id,
            pc.dist_id,
            pc.moment_id,
            m.edition_id,
            COALESCE(emd.estimated_value, emd.floor_price, 0) AS moment_value
        FROM pack_contents pc
        LEFT JOIN topshot_moments m
            ON pc.moment_id = m.moment_id
        LEFT JOIN topshot_edition_market_data emd
            ON m.edition_id = emd.edition_id
    ),
    -- Sum value per pack
    PackValues AS (
        SELECT
            pack_id,
            dist_id,
            SUM(moment_value) AS pack_total_value,
            COUNT(*) AS moments_in_pack
        FROM MomentValues
        GROUP BY pack_id, dist_id
    ),
    -- Aggregate per distribution
    DistributionStats AS (
        SELECT
            dist_id,
            COUNT(*) AS revealed_pack_count,
            AVG(pack_total_value) AS avg_pack_value,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY pack_total_value) AS median_pack_value,
            MIN(pack_total_value) AS min_pack_value,
            MAX(pack_total_value) AS max_pack_value,
            AVG(moments_in_pack) AS avg_moments_per_pack
        FROM PackValues
        WHERE dist_id IS NOT NULL
        GROUP BY dist_id
    )
    SELECT
        pd.dist_id,
        pd.first_mint_timestamp,
        pd.last_mint_timestamp,
        pd.total_packs,
        pd.sealed_count,
        pd.revealed_count,
        pd.opened_count,
        pd.burned_count,

        -- EV stats from revealed packs (using fresh topshot_edition_market_data)
        COALESCE(ds.revealed_pack_count, 0)                    AS revealed_with_values,
        ROUND(COALESCE(ds.avg_pack_value, 0), 2)               AS avg_pack_value,
        ROUND(COALESCE(ds.median_pack_value, 0)::numeric, 2)   AS median_pack_value,
        ROUND(COALESCE(ds.min_pack_value, 0), 2)               AS min_pack_value,
        ROUND(COALESCE(ds.max_pack_value, 0), 2)               AS max_pack_value,
        ROUND(COALESCE(ds.avg_moments_per_pack, 0)::numeric, 1) AS avg_moments_per_pack,

        -- Confidence level based on revealed sample size
        CASE
            WHEN COALESCE(ds.revealed_pack_count, 0) >= 50 THEN 'high'
            WHEN COALESCE(ds.revealed_pack_count, 0) >= 10 THEN 'medium'
            WHEN COALESCE(ds.revealed_pack_count, 0) >= 1  THEN 'low'
            ELSE 'none'
        END AS ev_confidence

    FROM pack_distributions pd
    LEFT JOIN DistributionStats ds ON pd.dist_id = ds.dist_id;
$$;
