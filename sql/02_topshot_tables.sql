-- 02_topshot_tables.sql
-- Port of BigQuery 3_TOPSHOT_tables.sql to PostgreSQL/TimescaleDB
--
-- Derived foundation tables for TopShot analytics.
-- PREREQUISITE: topshot_events must already exist (01_topshot_events.sql).
--
-- Creates 9 tables, then a single refresh_topshot_tables() function that
-- truncates and rebuilds them all in dependency order.

-- ══════════════════════════════════════════════════════════════
-- Table definitions
-- ══════════════════════════════════════════════════════════════

-- Part 1: topshot_plays (play metadata)
CREATE TABLE IF NOT EXISTS topshot_plays (
    play_id         BIGINT PRIMARY KEY,
    player_name     TEXT NOT NULL DEFAULT 'Unknown Player',
    team            TEXT NOT NULL DEFAULT 'Unknown Team',
    play_category   TEXT NOT NULL DEFAULT 'Unknown Category',
    jersey_number   TEXT NOT NULL DEFAULT 'N/A',
    play_type       TEXT NOT NULL DEFAULT 'N/A',
    date_of_moment  TEXT NOT NULL DEFAULT 'N/A',
    first_name      TEXT NOT NULL DEFAULT 'N/A',
    last_name       TEXT NOT NULL DEFAULT 'N/A'
);

-- Part 2: topshot_sets (set metadata)
CREATE TABLE IF NOT EXISTS topshot_sets (
    set_id  BIGINT PRIMARY KEY,
    series  BIGINT NOT NULL DEFAULT 0
);

-- Part 3: topshot_moments (deduplicated mints)
CREATE TABLE IF NOT EXISTS topshot_moments (
    transaction_hash TEXT NOT NULL,
    moment_id        BIGINT PRIMARY KEY,
    play_id          BIGINT,
    set_id           BIGINT,
    subedition_id    BIGINT,
    edition_id       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_moments_edition ON topshot_moments (edition_id);
CREATE INDEX IF NOT EXISTS idx_moments_set_play ON topshot_moments (set_id, play_id);

-- Part 4: topshot_subedition_definitions
CREATE TABLE IF NOT EXISTS topshot_subedition_definitions (
    subedition_id    BIGINT,
    subedition_name  TEXT,
    metadata_json    JSONB
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_subedition_def_id
    ON topshot_subedition_definitions ((COALESCE(subedition_id, -1)));

-- Part 5: topshot_sales (purchase transactions)
CREATE TABLE IF NOT EXISTS topshot_sales (
    transaction_hash   TEXT NOT NULL,
    moment_id          BIGINT NOT NULL,
    block_timestamp    TIMESTAMPTZ NOT NULL,
    price              NUMERIC,
    buyer_address      TEXT,
    seller_address     TEXT,
    marketplace_source TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sales_moment ON topshot_sales (moment_id);
CREATE INDEX IF NOT EXISTS idx_sales_timestamp ON topshot_sales (block_timestamp);

-- Part 6: topshot_offers (offer events with edition metadata)
CREATE TABLE IF NOT EXISTS topshot_offers (
    transaction_hash TEXT NOT NULL,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    event_order      INTEGER,
    offer_id         BIGINT,
    offer_address    TEXT,
    offer_amount     NUMERIC,
    target_nft_id    BIGINT,
    edition_id       TEXT,
    event_type       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_offers_edition ON topshot_offers (edition_id);
CREATE INDEX IF NOT EXISTS idx_offers_offer_id ON topshot_offers (offer_id);

-- Part 7: topshot_active_listings (current state: one row per listed moment)
CREATE TABLE IF NOT EXISTS topshot_active_listings (
    transaction_hash TEXT NOT NULL,
    moment_id        BIGINT PRIMARY KEY,
    edition_id       TEXT,
    price            NUMERIC,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    seller_address   TEXT,
    marketplace      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_active_listings_edition ON topshot_active_listings (edition_id);

-- Part 8: topshot_active_offers (current state: one row per open offer)
CREATE TABLE IF NOT EXISTS topshot_active_offers (
    transaction_hash TEXT NOT NULL,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    offer_id         BIGINT PRIMARY KEY,
    offer_address    TEXT,
    offer_amount     NUMERIC,
    target_nft_id    BIGINT,
    edition_id       TEXT
);
CREATE INDEX IF NOT EXISTS idx_active_offers_edition ON topshot_active_offers (edition_id);

-- Part 9: topshot_editions (supply analysis / edition dashboard)
CREATE TABLE IF NOT EXISTS topshot_editions (
    edition_id          TEXT PRIMARY KEY,
    set_id              BIGINT,
    play_id             BIGINT,
    subedition_id       BIGINT,
    subedition_name     TEXT,
    subedition_metadata JSONB,
    edition_status      TEXT,
    player_name         TEXT,
    jersey_number       TEXT,
    play_type           TEXT,
    date_of_moment      TEXT,
    first_name          TEXT,
    last_name           TEXT,
    series              BIGINT,
    play_category       TEXT,
    team                TEXT,
    mint_count          BIGINT,
    burn_count          BIGINT,
    existing_supply     BIGINT,
    listed_count        BIGINT,
    locked_count        BIGINT,
    in_packs_count      BIGINT,
    held_count          BIGINT,
    unique_holders      BIGINT DEFAULT 0,
    largest_holder_count BIGINT DEFAULT 0,
    concentration_pct   NUMERIC DEFAULT 0
);

-- ══════════════════════════════════════════════════════════════
-- Refresh function — rebuilds all 9 tables in dependency order
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION refresh_topshot_tables() RETURNS void AS $$
BEGIN

-- ─────────────────────────────────────────────────────────────
-- 1. topshot_plays
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_plays;
INSERT INTO topshot_plays (play_id, player_name, team, play_category,
                           jersey_number, play_type, date_of_moment,
                           first_name, last_name)
SELECT
    nft_id AS play_id,
    COALESCE(MAX(raw_data->'metadata'->>'FullName'), 'Unknown Player') AS player_name,
    COALESCE(MAX(raw_data->'metadata'->>'TeamAtMoment'), 'Unknown Team') AS team,
    COALESCE(MAX(raw_data->'metadata'->>'PlayCategory'), 'Unknown Category') AS play_category,
    COALESCE(MAX(raw_data->'metadata'->>'JerseyNumber'), 'N/A') AS jersey_number,
    COALESCE(MAX(raw_data->'metadata'->>'PlayType'), 'N/A') AS play_type,
    COALESCE(MAX(raw_data->'metadata'->>'DateOfMoment'), 'N/A') AS date_of_moment,
    COALESCE(MAX(raw_data->'metadata'->>'FirstName'), 'N/A') AS first_name,
    COALESCE(MAX(raw_data->'metadata'->>'LastName'), 'N/A') AS last_name
FROM topshot_events
WHERE event_type = 'PlayCreated'
GROUP BY nft_id;

-- ─────────────────────────────────────────────────────────────
-- 2. topshot_sets
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_sets;
INSERT INTO topshot_sets (set_id, series)
SELECT
    set_id,
    COALESCE(MAX((NULLIF(TRIM(raw_data->>'series'), ''))::bigint), 0) AS series
FROM topshot_events
WHERE event_type = 'SetCreated'
GROUP BY set_id;

-- ─────────────────────────────────────────────────────────────
-- 3. topshot_moments (deduplicated mints)
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_moments;
INSERT INTO topshot_moments (transaction_hash, moment_id, play_id, set_id,
                             subedition_id, edition_id)
WITH DeduplicatedMints AS (
    SELECT
        transaction_hash,
        nft_id AS moment_id,
        play_id,
        set_id,
        subedition_id,
        ROW_NUMBER() OVER (
            PARTITION BY nft_id
            ORDER BY block_timestamp ASC, event_order ASC, transaction_hash ASC
        ) AS rn
    FROM topshot_events
    WHERE event_type = 'MomentMinted'
)
SELECT
    m.transaction_hash,
    m.moment_id,
    m.play_id,
    m.set_id,
    m.subedition_id,
    CASE
        WHEN m.subedition_id IS NOT NULL THEN
            m.set_id::text || '_' || m.play_id::text || '_' || m.subedition_id::text
        ELSE
            m.set_id::text || '_' || m.play_id::text
    END AS edition_id
FROM DeduplicatedMints AS m
WHERE m.rn = 1;

-- ─────────────────────────────────────────────────────────────
-- 4. topshot_subedition_definitions
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_subedition_definitions;
INSERT INTO topshot_subedition_definitions (subedition_id, subedition_name, metadata_json)
WITH SubeditionEvents AS (
    SELECT
        raw_data->>'subeditionID' AS subedition_id_str,
        raw_data->>'name' AS subedition_name,
        raw_data->'metadata' AS metadata_json,
        block_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data->>'subeditionID'
            ORDER BY block_timestamp ASC
        ) AS rn
    FROM topshot_events
    WHERE event_type = 'SubeditionCreated'
)
SELECT
    (NULLIF(TRIM(subedition_id_str), ''))::bigint AS subedition_id,
    subedition_name,
    metadata_json
FROM SubeditionEvents
WHERE rn = 1

UNION ALL

-- Standard edition (no create event)
SELECT 0 AS subedition_id, 'Standard' AS subedition_name, NULL::jsonb AS metadata_json

UNION ALL

-- Legacy edition (pre-dates subeditions; distinct from subedition_id=0)
SELECT NULL::bigint AS subedition_id, 'Legacy' AS subedition_name, NULL::jsonb AS metadata_json;

-- ─────────────────────────────────────────────────────────────
-- 5. topshot_sales
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_sales;
INSERT INTO topshot_sales (transaction_hash, moment_id, block_timestamp,
                           price, buyer_address, seller_address, marketplace_source)
SELECT DISTINCT
    transaction_hash,
    nft_id AS moment_id,
    block_timestamp,
    price,
    buyer_address,
    seller_address,
    CASE
        WHEN event_type = 'MomentPurchasedV1' THEN 'Dapper_Marketplace_V1'
        WHEN event_type = 'MomentPurchasedV2' THEN 'Dapper_Marketplace_V2'
        WHEN event_type = 'MomentPurchasedV3' THEN 'Dapper_Marketplace_V3'
        WHEN event_type = 'MomentPurchasedFlowty' THEN 'Flowty_NFTStorefrontV2'
        WHEN event_type = 'OfferCompleted' THEN 'Offers_V1_V2'
        ELSE 'Unknown'
    END AS marketplace_source
FROM topshot_events
WHERE
    (
        event_type IN ('MomentPurchasedV1', 'MomentPurchasedV2', 'MomentPurchasedV3', 'MomentPurchasedFlowty')
        OR
        (event_type = 'OfferCompleted' AND raw_data->>'purchased' = 'true')
    )
    AND nft_id IS NOT NULL
    -- EXCLUDE Dapper buyback accounts (not real market sales)
    AND buyer_address NOT IN (
        '0xe1f2a091f7bb5245',  -- Dapper reserve/buyback
        '0x8c543d2b27c81a56',  -- Dapper packs
        '0xfa57101aa0d55954'   -- Dapper reserve
    )
    -- EXCLUDE non-USD Flowty sales (FlowToken, FlowUtilityToken, SloppyStakes, etc.)
    AND (
        event_type != 'MomentPurchasedFlowty'
        OR raw_data->>'salePaymentVaultType' IN (
            'A.ead892083b3e2c6c.DapperUtilityCoin.Vault',
            'A.f1ab99c82dee3526.USDCFlow.Vault',
            'A.b19436aae4d94622.FiatToken.Vault'
        )
    );

-- ─────────────────────────────────────────────────────────────
-- 6. topshot_offers
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_offers;
INSERT INTO topshot_offers (transaction_hash, block_timestamp, event_order,
                            offer_id, offer_address, offer_amount,
                            target_nft_id, edition_id, event_type)
WITH
EditionsWithSubeditions AS (
    SELECT DISTINCT set_id, play_id
    FROM topshot_moments
    WHERE subedition_id IS NOT NULL
),
RawOffers AS (
    SELECT
        e.transaction_hash,
        e.block_timestamp,
        e.event_order,
        (NULLIF(TRIM(e.raw_data->>'offerId'), ''))::bigint AS offer_id,
        e.buyer_address AS offer_address,
        e.price AS offer_amount,
        e.nft_id AS target_nft_id,
        COALESCE(m.set_id, e.set_id) AS set_id,
        COALESCE(m.play_id, e.play_id) AS play_id,
        COALESCE(m.subedition_id, e.subedition_id) AS subedition_id,
        e.event_type
    FROM topshot_events e
    LEFT JOIN topshot_moments m ON e.nft_id = m.moment_id
    WHERE e.event_type IN ('OfferAvailable', 'OfferCompleted')
)
SELECT
    ro.transaction_hash,
    ro.block_timestamp,
    ro.event_order,
    ro.offer_id,
    ro.offer_address,
    ro.offer_amount,
    ro.target_nft_id,
    CASE
        WHEN ro.set_id IS NOT NULL AND ro.play_id IS NOT NULL THEN
            CASE
                WHEN ro.subedition_id IS NOT NULL THEN
                    ro.set_id::text || '_' || ro.play_id::text || '_' || ro.subedition_id::text
                WHEN ro.subedition_id IS NULL AND ews.set_id IS NOT NULL THEN
                    ro.set_id::text || '_' || ro.play_id::text || '_0'
                ELSE
                    ro.set_id::text || '_' || ro.play_id::text
            END
        ELSE NULL
    END AS edition_id,
    ro.event_type
FROM RawOffers ro
LEFT JOIN EditionsWithSubeditions ews
    ON ro.set_id = ews.set_id AND ro.play_id = ews.play_id;

-- ─────────────────────────────────────────────────────────────
-- 7. topshot_active_listings
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_active_listings;
INSERT INTO topshot_active_listings (transaction_hash, moment_id, edition_id,
                                     price, block_timestamp, seller_address,
                                     marketplace)
WITH LatestMarketEvent AS (
    SELECT
        transaction_hash,
        nft_id AS moment_id,
        price,
        event_type,
        block_timestamp,
        event_order,
        raw_data,
        seller_address,
        ROW_NUMBER() OVER (
            PARTITION BY nft_id
            ORDER BY block_timestamp DESC, transaction_hash DESC, event_order DESC
        ) AS rn
    FROM topshot_events
    WHERE
        (
            event_type IN (
                'MomentListed',
                'MomentPurchasedV1',
                'MomentPurchasedV2',
                'MomentPurchasedV3',
                'MomentPurchasedFlowty',
                'MomentWithdrawn',
                'MomentWithdrawnFromCollection',
                'MomentDeposited',
                'MomentDestroyed'
            )
            OR (event_type = 'OfferCompleted' AND raw_data->>'purchased' = 'true')
        )
        AND nft_id IS NOT NULL
)
SELECT
    l.transaction_hash,
    l.moment_id,
    m.edition_id,
    l.price,
    l.block_timestamp,
    l.seller_address,
    CASE
        WHEN l.raw_data->>'salePaymentVaultType' IS NULL THEN 'Dapper'
        ELSE 'Flowty'
    END AS marketplace
FROM LatestMarketEvent l
JOIN topshot_moments m ON l.moment_id = m.moment_id
WHERE l.rn = 1
    AND l.event_type = 'MomentListed'
    -- EXCLUDE non-USD Flowty listings
    AND (
        l.raw_data->>'salePaymentVaultType' IS NULL
        OR l.raw_data->>'salePaymentVaultType' IN (
            'A.ead892083b3e2c6c.DapperUtilityCoin.Vault',
            'A.f1ab99c82dee3526.USDCFlow.Vault',
            'A.b19436aae4d94622.FiatToken.Vault'
        )
    )
    -- EXCLUDE expired Flowty listings (NFTStorefrontV2 listings carry $.expiry UNIX epoch)
    AND (
        CASE WHEN TRIM(l.raw_data->>'expiry') ~ '^[0-9]+$' THEN (TRIM(l.raw_data->>'expiry'))::bigint ELSE NULL END IS NULL
        OR CASE WHEN TRIM(l.raw_data->>'expiry') ~ '^[0-9]+$' THEN (TRIM(l.raw_data->>'expiry'))::bigint ELSE NULL END > EXTRACT(EPOCH FROM NOW())::bigint
    );

-- ─────────────────────────────────────────────────────────────
-- 8. topshot_active_offers
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_active_offers;
INSERT INTO topshot_active_offers (transaction_hash, block_timestamp, offer_id,
                                    offer_address, offer_amount, target_nft_id,
                                    edition_id)
WITH LatestOfferEvent AS (
    SELECT
        transaction_hash,
        block_timestamp,
        event_order,
        offer_id,
        offer_address,
        offer_amount,
        target_nft_id,
        edition_id,
        event_type,
        ROW_NUMBER() OVER (
            PARTITION BY offer_id
            ORDER BY block_timestamp DESC, transaction_hash DESC, event_order DESC
        ) AS rn
    FROM topshot_offers
    WHERE offer_id IS NOT NULL
)
SELECT
    transaction_hash,
    block_timestamp,
    offer_id,
    offer_address,
    offer_amount,
    target_nft_id,
    edition_id
FROM LatestOfferEvent
WHERE rn = 1
    AND event_type = 'OfferAvailable'
    -- EXCLUDE Flow token offers (not DUC)
    AND offer_address != '0xd69b6ce48815d4ad';

-- ─────────────────────────────────────────────────────────────
-- 9. topshot_editions (supply analysis + holder analytics)
-- ─────────────────────────────────────────────────────────────
TRUNCATE topshot_editions;
INSERT INTO topshot_editions (
    edition_id, set_id, play_id, subedition_id,
    subedition_name, subedition_metadata, edition_status,
    player_name, jersey_number, play_type, date_of_moment, first_name, last_name,
    series, play_category, team,
    mint_count, burn_count, existing_supply,
    listed_count, locked_count, in_packs_count, held_count,
    unique_holders, largest_holder_count, concentration_pct
)
WITH
-- Edition status: locked set or retired play
EditionStatus AS (
    WITH
    LatestSetEvent AS (
        SELECT set_id, 'SetLocked' AS set_status
        FROM (
            SELECT
                set_id,
                event_type,
                block_timestamp,
                ROW_NUMBER() OVER (PARTITION BY set_id ORDER BY block_timestamp DESC) AS rn
            FROM topshot_events
            WHERE event_type = 'SetLocked'
        ) sub
        WHERE rn = 1
    ),
    LatestPlayEvent AS (
        SELECT set_id, play_id, 'PlayRetired' AS play_status
        FROM (
            SELECT
                set_id,
                play_id,
                event_type,
                block_timestamp,
                ROW_NUMBER() OVER (PARTITION BY set_id, play_id ORDER BY block_timestamp DESC) AS rn
            FROM topshot_events
            WHERE event_type = 'PlayRetiredFromSet'
        ) sub
        WHERE rn = 1
    )
    SELECT
        dm.set_id,
        dm.play_id,
        COALESCE(s.set_status, p.play_status, 'Active') AS edition_status
    FROM (SELECT DISTINCT set_id, play_id FROM topshot_moments) AS dm
    LEFT JOIN LatestSetEvent s ON dm.set_id = s.set_id
    LEFT JOIN LatestPlayEvent p ON dm.set_id = p.set_id AND dm.play_id = p.play_id
),

-- Moment status: burned / listed / in_packs / locked / held
MomentStatus AS (
    WITH
    CurrentOwners AS (
        SELECT nft_id AS moment_id, owner_address
        FROM (
            SELECT
                nft_id,
                buyer_address AS owner_address,
                ROW_NUMBER() OVER (
                    PARTITION BY nft_id
                    ORDER BY block_timestamp DESC, transaction_hash DESC, event_order DESC
                ) AS rn
            FROM topshot_events
            WHERE (
                event_type IN (
                    'MomentMinted',
                    'MomentPurchasedV1',
                    'MomentPurchasedV2',
                    'MomentPurchasedV3',
                    'MomentDeposited',
                    'MomentPurchasedFlowty'
                )
                OR (event_type = 'OfferCompleted' AND raw_data->>'purchased' = 'true')
            )
            AND nft_id IS NOT NULL AND buyer_address IS NOT NULL
        ) sub
        WHERE rn = 1
    ),
    CurrentLockStatus AS (
        SELECT nft_id AS moment_id
        FROM (
            SELECT
                nft_id,
                event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY nft_id
                    ORDER BY block_timestamp DESC, transaction_hash DESC, event_order DESC
                ) AS rn
            FROM topshot_events
            WHERE (
                event_type IN (
                    'MomentLocked',
                    'MomentUnlocked',
                    'MomentDestroyed',
                    'MomentWithdrawnFromCollection',
                    'MomentPurchasedV1',
                    'MomentPurchasedV2',
                    'MomentPurchasedV3',
                    'MomentPurchasedFlowty'
                )
                OR (event_type = 'OfferCompleted' AND raw_data->>'purchased' = 'true')
            )
        ) sub
        WHERE rn = 1 AND event_type = 'MomentLocked'
    ),
    BurnedMoments AS (
        SELECT DISTINCT nft_id AS moment_id, TRUE AS is_burned
        FROM topshot_events
        WHERE event_type = 'MomentDestroyed'
    )
    SELECT
        m.moment_id,
        m.edition_id,
        co.owner_address,
        CASE
            WHEN bm.is_burned IS TRUE THEN 'burned'
            WHEN al.moment_id IS NOT NULL THEN 'listed'
            WHEN co.owner_address IN ('0x8c543d2b27c81a56', '0xe1f2a091f7bb5245', '0xfa57101aa0d55954') THEN 'in_packs'
            WHEN cls.moment_id IS NOT NULL THEN 'locked'
            ELSE 'held'
        END AS status
    FROM topshot_moments AS m
    LEFT JOIN CurrentOwners AS co ON m.moment_id = co.moment_id
    LEFT JOIN CurrentLockStatus AS cls ON m.moment_id = cls.moment_id
    LEFT JOIN topshot_active_listings AS al ON m.moment_id = al.moment_id
    LEFT JOIN BurnedMoments AS bm ON m.moment_id = bm.moment_id
),

-- Holder analytics from circulating moments
HolderCounts AS (
    SELECT
        edition_id,
        owner_address,
        COUNT(*) AS moment_count
    FROM MomentStatus
    WHERE status IN ('held', 'listed', 'locked')
        AND owner_address IS NOT NULL
        AND owner_address NOT IN (
            '0x8c543d2b27c81a56',
            '0xe1f2a091f7bb5245',
            '0xfa57101aa0d55954'
        )
    GROUP BY 1, 2
),

HolderAgg AS (
    SELECT
        edition_id,
        COUNT(*) AS unique_holders,
        MAX(moment_count) AS largest_holder_count,
        ROUND(MAX(moment_count)::numeric * 100 / NULLIF(SUM(moment_count), 0), 2) AS concentration_pct
    FROM HolderCounts
    GROUP BY edition_id
),

AggregatedSupply AS (
    SELECT
        edition_id,
        COUNT(*) AS mint_count,
        COUNT(*) FILTER (WHERE status = 'burned') AS burn_count,
        COUNT(*) FILTER (WHERE status = 'listed') AS listed_count,
        COUNT(*) FILTER (WHERE status = 'locked') AS locked_count,
        COUNT(*) FILTER (WHERE status = 'in_packs') AS in_packs_count,
        COUNT(*) FILTER (WHERE status = 'held') AS held_count
    FROM MomentStatus
    GROUP BY 1
)

SELECT
    agg.edition_id,
    m.set_id,
    m.play_id,
    m.subedition_id,

    def.subedition_name,
    def.metadata_json AS subedition_metadata,
    stat.edition_status,

    p.player_name,
    p.jersey_number,
    p.play_type,
    p.date_of_moment,
    p.first_name,
    p.last_name,

    s.series,
    p.play_category,
    p.team,

    agg.mint_count,
    agg.burn_count,
    (agg.mint_count - agg.burn_count) AS existing_supply,
    agg.listed_count,
    agg.locked_count,
    agg.in_packs_count,
    agg.held_count,

    COALESCE(ha.unique_holders, 0) AS unique_holders,
    COALESCE(ha.largest_holder_count, 0) AS largest_holder_count,
    COALESCE(ha.concentration_pct, 0) AS concentration_pct

FROM AggregatedSupply AS agg
LEFT JOIN (
    SELECT DISTINCT edition_id, play_id, set_id, subedition_id
    FROM topshot_moments
) AS m ON agg.edition_id = m.edition_id
LEFT JOIN topshot_plays AS p ON m.play_id = p.play_id
LEFT JOIN topshot_sets AS s ON m.set_id = s.set_id
LEFT JOIN topshot_subedition_definitions AS def ON m.subedition_id IS NOT DISTINCT FROM def.subedition_id
LEFT JOIN EditionStatus AS stat ON m.set_id = stat.set_id AND m.play_id = stat.play_id
LEFT JOIN HolderAgg AS ha ON agg.edition_id = ha.edition_id;

END;
$$ LANGUAGE plpgsql;
