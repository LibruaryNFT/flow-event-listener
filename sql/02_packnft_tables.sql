-- =================================================================================
-- 02_packnft_tables.sql
-- Derived tables for Pack NFT analytics (PostgreSQL/TimescaleDB)
--
-- Ported from BigQuery: vaultopolis/google/queries/pipeline/3_PACKNFT_tables.sql
--
-- PREREQUISITE: packnft_events must already exist, maintained by
--               01_packnft_events.sql
--
-- TABLES MAINTAINED:
--   1. pack_distributions    — One row per drop (grouped by dist_id)   [TRUNCATE + rebuild]
--   2. pack_nfts             — Current status of every pack NFT        [TRUNCATE + rebuild]
--   3. pack_contents         — Pack → moment mapping (from Revealed)   [INCREMENTAL INSERT]
--
-- NOTE: pack_distribution_analytics lives in a separate file.
--       It must run AFTER topshot_edition_data because it joins
--       topshot_edition_market_data for EV values.
-- =================================================================================

-- ---------------------------------------------------------------------------
-- Table definitions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS pack_distributions (
    dist_id              text PRIMARY KEY,
    first_mint_timestamp timestamptz,
    last_mint_timestamp  timestamptz,
    total_packs          bigint,
    sealed_count         bigint,
    revealed_count       bigint,
    opened_count         bigint,
    burned_count         bigint
);

CREATE TABLE IF NOT EXISTS pack_nfts (
    pack_id          bigint PRIMARY KEY,
    dist_id          text,
    status           text,
    owner_address    text,
    status_timestamp timestamptz
);

CREATE TABLE IF NOT EXISTS pack_contents (
    pack_id            bigint,
    dist_id            text,
    reveal_timestamp   timestamptz,
    nft_identifier     text,
    moment_id          bigint
);

-- Index for the incremental NOT IN check
CREATE INDEX IF NOT EXISTS idx_pack_contents_pack_id ON pack_contents (pack_id);

-- ---------------------------------------------------------------------------
-- Refresh function
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION refresh_packnft_tables() RETURNS void
LANGUAGE plpgsql AS $$
BEGIN

-- ==========================================================================
-- Part 1: pack_distributions — One row per distribution (drop)
-- ==========================================================================
TRUNCATE pack_distributions;

INSERT INTO pack_distributions
    (dist_id, first_mint_timestamp, last_mint_timestamp,
     total_packs, sealed_count, revealed_count, opened_count, burned_count)
WITH
MintedPacks AS (
    -- Deduplicate: some packs have multiple Minted events
    SELECT
        dist_id,
        pack_id,
        MIN(block_timestamp) AS mint_timestamp
    FROM packnft_events
    WHERE event_type = 'PackMinted'
      AND dist_id IS NOT NULL
    GROUP BY dist_id, pack_id
),
PackStatuses AS (
    -- Get latest status-changing event per pack (excludes transfers)
    -- Lifecycle tiebreaker handles Revealed+Opened sharing same timestamp/event_order
    SELECT
        pack_id,
        event_type,
        ROW_NUMBER() OVER (
            PARTITION BY pack_id
            ORDER BY block_timestamp DESC, event_order DESC,
                CASE event_type
                    WHEN 'PackBurned'   THEN 1
                    WHEN 'PackOpened'   THEN 2
                    WHEN 'PackRevealed' THEN 3
                    WHEN 'PackMinted'   THEN 4
                END ASC
        ) AS rn
    FROM packnft_events
    WHERE event_type IN ('PackMinted', 'PackRevealed', 'PackOpened', 'PackBurned')
),
PackCurrentStatus AS (
    SELECT
        pack_id,
        CASE
            WHEN event_type = 'PackBurned'   THEN 'burned'
            WHEN event_type = 'PackOpened'   THEN 'opened'
            WHEN event_type = 'PackRevealed' THEN 'revealed'
            ELSE 'sealed'
        END AS status
    FROM PackStatuses
    WHERE rn = 1
)
SELECT
    mp.dist_id,
    MIN(mp.mint_timestamp)                                AS first_mint_timestamp,
    MAX(mp.mint_timestamp)                                AS last_mint_timestamp,
    COUNT(DISTINCT mp.pack_id)                            AS total_packs,
    COUNT(*) FILTER (WHERE ps.status = 'sealed')          AS sealed_count,
    COUNT(*) FILTER (WHERE ps.status = 'revealed')        AS revealed_count,
    COUNT(*) FILTER (WHERE ps.status = 'opened')          AS opened_count,
    COUNT(*) FILTER (WHERE ps.status = 'burned')          AS burned_count
FROM MintedPacks mp
LEFT JOIN PackCurrentStatus ps ON mp.pack_id = ps.pack_id
GROUP BY mp.dist_id;

-- ==========================================================================
-- Part 2: pack_nfts — Current status and owner of every pack NFT
-- ==========================================================================
TRUNCATE pack_nfts;

INSERT INTO pack_nfts
    (pack_id, dist_id, status, owner_address, status_timestamp)
WITH
-- Get latest status-changing event per pack
LatestStatusEvent AS (
    SELECT
        pack_id,
        event_type,
        block_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY pack_id
            ORDER BY block_timestamp DESC, event_order DESC,
                CASE event_type
                    WHEN 'PackBurned'   THEN 1
                    WHEN 'PackOpened'   THEN 2
                    WHEN 'PackRevealed' THEN 3
                    WHEN 'PackMinted'   THEN 4
                END ASC
        ) AS rn
    FROM packnft_events
    WHERE event_type IN ('PackMinted', 'PackRevealed', 'PackOpened', 'PackBurned')
),
-- Get current owner (latest Deposit)
LatestOwner AS (
    SELECT
        pack_id,
        owner_address,
        ROW_NUMBER() OVER (
            PARTITION BY pack_id
            ORDER BY block_timestamp DESC, event_order DESC
        ) AS rn
    FROM packnft_events
    WHERE event_type = 'PackDeposit'
      AND owner_address IS NOT NULL
),
-- Get dist_id from Minted event
PackDistribution AS (
    SELECT DISTINCT
        pack_id,
        dist_id
    FROM packnft_events
    WHERE event_type = 'PackMinted'
      AND dist_id IS NOT NULL
)
SELECT
    ls.pack_id,
    pd.dist_id,
    CASE
        WHEN ls.event_type = 'PackBurned'   THEN 'burned'
        WHEN ls.event_type = 'PackOpened'   THEN 'opened'
        WHEN ls.event_type = 'PackRevealed' THEN 'revealed'
        ELSE 'sealed'
    END AS status,
    lo.owner_address,
    ls.block_timestamp AS status_timestamp
FROM LatestStatusEvent ls
LEFT JOIN LatestOwner lo ON ls.pack_id = lo.pack_id AND lo.rn = 1
LEFT JOIN PackDistribution pd ON ls.pack_id = pd.pack_id
WHERE ls.rn = 1;

-- ==========================================================================
-- Part 3: pack_contents — Pack → moment mapping (INCREMENTAL)
--
-- The Revealed event's nfts_raw field contains comma-separated NFT identifiers:
--   "A.0b2a3299cc857e29.TopShot.12345,A.0b2a3299cc857e29.TopShot.67890"
-- We split these and extract the moment_id (last segment after the final dot).
--
-- Some distributions are "pack bundles" containing nested PackNFT references
-- (e.g. "A.0b2a3299cc857e29.PackNFT.999999") instead of TopShot moments.
-- These are filtered out — only TopShot moment identifiers produce valid moment_ids.
--
-- INCREMENTAL: Only inserts newly revealed packs not already in pack_contents.
-- Pack reveals are permanent on-chain — no need to ever re-derive them.
-- ==========================================================================
INSERT INTO pack_contents
    (pack_id, dist_id, reveal_timestamp, nft_identifier, moment_id)
WITH
NewlyRevealed AS (
    SELECT
        pack_id,
        dist_id,
        nfts_raw,
        block_timestamp AS reveal_timestamp
    FROM packnft_events
    WHERE event_type = 'PackRevealed'
      AND nfts_raw IS NOT NULL
      AND nfts_raw != ''
      -- Only process packs not already stored
      AND pack_id NOT IN (
          SELECT DISTINCT pack_id FROM pack_contents
      )
),
-- Get dist_id from Minted if not on Revealed event
PackDistribution AS (
    SELECT DISTINCT pack_id, dist_id
    FROM packnft_events
    WHERE event_type = 'PackMinted'
      AND dist_id IS NOT NULL
),
-- Split the comma-separated nfts_raw string into individual NFT identifiers
SplitNFTs AS (
    SELECT
        rp.pack_id,
        COALESCE(rp.dist_id, pd.dist_id) AS dist_id,
        rp.reveal_timestamp,
        TRIM(nft_identifier) AS nft_identifier
    FROM NewlyRevealed rp
    LEFT JOIN PackDistribution pd ON rp.pack_id = pd.pack_id
    CROSS JOIN unnest(string_to_array(rp.nfts_raw, ',')) AS nft_identifier
)
SELECT
    sn.pack_id,
    sn.dist_id,
    sn.reveal_timestamp,
    sn.nft_identifier,
    -- Extract moment_id: last segment after the final dot
    -- e.g. "A.0b2a3299cc857e29.TopShot.12345" → 12345
    (regexp_match(sn.nft_identifier, '\.(\d+)$'))[1]::bigint AS moment_id
FROM SplitNFTs sn
WHERE sn.nft_identifier IS NOT NULL
  AND TRIM(sn.nft_identifier) != ''
  -- Exclude PackNFT references (pack bundles) — only keep TopShot moments
  AND sn.nft_identifier LIKE 'A.0b2a3299cc857e29.TopShot.%';

END;
$$;
