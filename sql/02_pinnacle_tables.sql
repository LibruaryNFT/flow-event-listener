-- =================================================================================
-- 02_pinnacle_tables.sql
-- Derived foundation tables for Pinnacle analytics (PostgreSQL/TimescaleDB)
--
-- Ported from BigQuery: vaultopolis/google/queries/pipeline/3_PINNACLE_tables.sql
--
-- PREREQUISITE: pinnacle_events must already exist, maintained by
--               01_pinnacle_events.sql
--
-- USAGE: SELECT refresh_pinnacle_tables();
-- =================================================================================

-- =========================================================
-- Table definitions
-- =========================================================

CREATE TABLE IF NOT EXISTS pinnacle_edition_types (
    edition_type_id   bigint PRIMARY KEY,
    edition_type_name text,
    is_limited        text,
    is_maturing       text
);

CREATE TABLE IF NOT EXISTS pinnacle_series (
    series_id   bigint PRIMARY KEY,
    series_name text
);

CREATE TABLE IF NOT EXISTS pinnacle_sets (
    set_id       bigint PRIMARY KEY,
    set_name     text,
    render_id    text,
    series_id    bigint,
    edition_type text
);

CREATE TABLE IF NOT EXISTS pinnacle_shapes (
    shape_id       bigint PRIMARY KEY,
    render_id      text,
    set_id         bigint,
    shape_name     text,
    edition_type   text,
    character_name text
);

CREATE TABLE IF NOT EXISTS pinnacle_editions (
    edition_id      bigint PRIMARY KEY,
    render_id       text,
    series_id       bigint,
    set_id          bigint,
    shape_id        bigint,
    variant         text,
    printing        bigint,
    edition_type_id bigint,
    description     text,
    is_chaser       boolean,
    max_mint_size   bigint,
    character_name  text
);

CREATE TABLE IF NOT EXISTS pinnacle_nfts (
    nft_id          bigint PRIMARY KEY,
    edition_id      bigint,
    serial_number   bigint,
    minting_txn     text,
    block_timestamp timestamptz
);
CREATE INDEX IF NOT EXISTS idx_pinnacle_nfts_edition ON pinnacle_nfts (edition_id);

CREATE TABLE IF NOT EXISTS pinnacle_sales (
    transaction_hash   text,
    block_timestamp    timestamptz,
    nft_id             bigint,
    edition_id         bigint,
    serial_number      bigint,
    price              numeric,
    buyer_address      text,
    seller_address     text,
    marketplace_source text
);
CREATE INDEX IF NOT EXISTS idx_pinnacle_sales_edition ON pinnacle_sales (edition_id);
CREATE INDEX IF NOT EXISTS idx_pinnacle_sales_ts ON pinnacle_sales (block_timestamp);

CREATE TABLE IF NOT EXISTS pinnacle_active_listings (
    nft_id     bigint PRIMARY KEY,
    edition_id bigint,
    price      numeric
);
CREATE INDEX IF NOT EXISTS idx_pinnacle_active_listings_edition ON pinnacle_active_listings (edition_id);

CREATE TABLE IF NOT EXISTS pinnacle_offers (
    block_timestamp timestamptz,
    offer_id        bigint,
    offer_address   text,
    offer_amount    numeric,
    edition_id      bigint,
    target_nft_id   bigint,
    offer_type      text,
    event_type      text
);
CREATE INDEX IF NOT EXISTS idx_pinnacle_offers_offer_id ON pinnacle_offers (offer_id);

CREATE TABLE IF NOT EXISTS pinnacle_active_offers (
    block_timestamp timestamptz,
    offer_id        bigint PRIMARY KEY,
    offer_address   text,
    offer_amount    numeric,
    edition_id      bigint,
    target_nft_id   bigint,
    offer_type      text,
    event_type      text
);

CREATE TABLE IF NOT EXISTS pinnacle_edition_supply (
    edition_id            bigint PRIMARY KEY,
    mint_count            bigint,
    burn_count            bigint,
    existing_supply       bigint,
    listed_count          bigint,
    in_vaults_count       bigint,
    held_count            bigint,
    unique_holders        bigint,
    largest_holder_count  bigint,
    concentration_pct     numeric
);

-- =========================================================
-- Refresh function — rebuilds all tables in dependency order
-- =========================================================

CREATE OR REPLACE FUNCTION refresh_pinnacle_tables() RETURNS void AS $$
BEGIN

    -- 1. pinnacle_edition_types
    TRUNCATE pinnacle_edition_types;
    INSERT INTO pinnacle_edition_types (edition_type_id, edition_type_name, is_limited, is_maturing)
    SELECT DISTINCT
        nft_id                          AS edition_type_id,
        raw_data->>'name'               AS edition_type_name,
        raw_data->>'isLimited'          AS is_limited,
        raw_data->>'isMaturing'         AS is_maturing
    FROM pinnacle_events
    WHERE event_type = 'EditionTypeCreated';

    -- 2. pinnacle_series
    TRUNCATE pinnacle_series;
    INSERT INTO pinnacle_series (series_id, series_name)
    SELECT DISTINCT
        nft_id              AS series_id,
        raw_data->>'name'   AS series_name
    FROM pinnacle_events
    WHERE event_type = 'SeriesCreated';

    -- 3. pinnacle_sets
    TRUNCATE pinnacle_sets;
    INSERT INTO pinnacle_sets (set_id, set_name, render_id, series_id, edition_type)
    SELECT DISTINCT
        nft_id                                                      AS set_id,
        raw_data->>'name'                                           AS set_name,
        raw_data->>'renderID'                                       AS render_id,
        (NULLIF(TRIM(raw_data->>'seriesID'), ''))::bigint           AS series_id,
        raw_data->>'editionType'                                    AS edition_type
    FROM pinnacle_events
    WHERE event_type = 'SetCreated';

    -- 4. pinnacle_shapes (carries character metadata)
    TRUNCATE pinnacle_shapes;
    INSERT INTO pinnacle_shapes (shape_id, render_id, set_id, shape_name, edition_type, character_name)
    SELECT DISTINCT
        nft_id                                                      AS shape_id,
        raw_data->>'renderID'                                       AS render_id,
        (NULLIF(TRIM(raw_data->>'setID'), ''))::bigint              AS set_id,
        raw_data->>'name'                                           AS shape_name,
        raw_data->>'editionType'                                    AS edition_type,
        -- Character name from metadata (Cadence {String: [String]} → JSON object with array values)
        COALESCE(
            raw_data->'metadata'->'Characters'->>0,
            raw_data->'metadata'->'Character'->>0
        )                                                           AS character_name
    FROM pinnacle_events
    WHERE event_type = 'ShapeCreated';

    -- 5. pinnacle_editions (from EditionCreated events, has traits)
    TRUNCATE pinnacle_editions;
    INSERT INTO pinnacle_editions (edition_id, render_id, series_id, set_id, shape_id,
                                   variant, printing, edition_type_id, description,
                                   is_chaser, max_mint_size, character_name)
    SELECT DISTINCT
        nft_id                                                          AS edition_id,
        raw_data->>'renderID'                                           AS render_id,
        (NULLIF(TRIM(raw_data->>'seriesID'), ''))::bigint               AS series_id,
        (NULLIF(TRIM(raw_data->>'setID'), ''))::bigint                  AS set_id,
        (NULLIF(TRIM(raw_data->>'shapeID'), ''))::bigint                AS shape_id,
        raw_data->>'variant'                                            AS variant,
        (NULLIF(TRIM(raw_data->>'printing'), ''))::bigint               AS printing,
        (NULLIF(TRIM(raw_data->>'editionTypeID'), ''))::bigint          AS edition_type_id,
        raw_data->>'description'                                        AS description,
        (NULLIF(TRIM(raw_data->>'isChaser'), ''))::boolean              AS is_chaser,
        (NULLIF(TRIM(raw_data->>'maxMintSize'), ''))::bigint            AS max_mint_size,
        -- Character name from traits (fallback extraction)
        COALESCE(
            raw_data->'traits'->'Characters'->>0,
            raw_data->'traits'->'Character'->>0
        )                                                               AS character_name
    FROM pinnacle_events
    WHERE event_type = 'EditionCreated';

    -- 6. pinnacle_nfts (all minted Pinnacle NFTs)
    TRUNCATE pinnacle_nfts;
    INSERT INTO pinnacle_nfts (nft_id, edition_id, serial_number, minting_txn, block_timestamp)
    SELECT
        nft_id,
        edition_id,
        serial_number,
        transaction_hash    AS minting_txn,
        block_timestamp
    FROM pinnacle_events
    WHERE event_type = 'PinNFTMinted'
      AND nft_id IS NOT NULL
      AND edition_id IS NOT NULL;

    -- 7. pinnacle_sales (marketplace purchases enriched with edition/serial)
    TRUNCATE pinnacle_sales;
    INSERT INTO pinnacle_sales (transaction_hash, block_timestamp, nft_id, edition_id,
                                serial_number, price, buyer_address, seller_address,
                                marketplace_source)
    WITH sales_txns AS (
        SELECT
            transaction_hash,
            block_timestamp,
            nft_id,
            price,
            buyer_address,
            seller_address,
            CASE
                WHEN event_type = 'PurchasedDapper' THEN 'Dapper'
                WHEN event_type = 'PurchasedFlowty' THEN 'Flowty'
                ELSE 'Unknown'
            END AS marketplace_source
        FROM pinnacle_events
        WHERE event_type IN ('PurchasedDapper', 'PurchasedFlowty')
          AND nft_id IS NOT NULL
          AND price IS NOT NULL
          AND price > 0
    )
    SELECT
        s.transaction_hash,
        s.block_timestamp,
        s.nft_id,
        n.edition_id,
        n.serial_number,
        s.price,
        s.buyer_address,
        s.seller_address,
        s.marketplace_source
    FROM sales_txns s
    LEFT JOIN pinnacle_nfts n ON s.nft_id = n.nft_id
    WHERE n.edition_id IS NOT NULL;

    -- 8. pinnacle_active_listings (latest status per nft_id, filters expired)
    TRUNCATE pinnacle_active_listings;
    INSERT INTO pinnacle_active_listings (nft_id, edition_id, price)
    WITH LatestNFTStatus AS (
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
    )
    SELECT
        l.nft_id,
        n.edition_id,
        l.price
    FROM LatestNFTStatus l
    JOIN pinnacle_nfts n ON l.nft_id = n.nft_id
    WHERE l.rn = 1
      AND l.event_type = 'ListingAvailable'
      AND l.price IS NOT NULL
      AND l.price > 0
      -- Filter out expired listings (NFTStorefrontV2 listings have an expiry epoch)
      AND (l.expiry_epoch IS NULL OR l.expiry_epoch > EXTRACT(EPOCH FROM NOW())::bigint);

    -- 9. pinnacle_offers (all offer history)
    TRUNCATE pinnacle_offers;
    INSERT INTO pinnacle_offers (block_timestamp, offer_id, offer_address, offer_amount,
                                 edition_id, target_nft_id, offer_type, event_type)
    SELECT
        block_timestamp,
        (NULLIF(TRIM(raw_data->>'offerId'), ''))::bigint        AS offer_id,
        raw_data->>'offerAddress'                                AS offer_address,
        price                                                    AS offer_amount,
        -- For edition-level offers, edition_id comes from the offer params
        COALESCE(
            edition_id,
            (NULLIF(TRIM(raw_data->'offerParamsString'->>'editionId'), ''))::bigint,
            (NULLIF(TRIM(raw_data->'offerParamsUInt64'->>'editionId'), ''))::bigint
        )                                                        AS edition_id,
        nft_id                                                   AS target_nft_id,
        raw_data->'offerParamsString'->>'_type'                  AS offer_type,
        event_type
    FROM pinnacle_events
    WHERE event_type IN ('OfferAvailable', 'OfferCompleted');

    -- 10. pinnacle_active_offers (latest per offer_id)
    TRUNCATE pinnacle_active_offers;
    INSERT INTO pinnacle_active_offers (block_timestamp, offer_id, offer_address, offer_amount,
                                        edition_id, target_nft_id, offer_type, event_type)
    WITH LatestOfferEvent AS (
        SELECT
            block_timestamp,
            offer_id,
            offer_address,
            offer_amount,
            edition_id,
            target_nft_id,
            offer_type,
            event_type,
            ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY block_timestamp DESC) AS rn
        FROM pinnacle_offers
        WHERE offer_id IS NOT NULL
    )
    SELECT
        block_timestamp,
        offer_id,
        offer_address,
        offer_amount,
        edition_id,
        target_nft_id,
        offer_type,
        event_type
    FROM LatestOfferEvent
    WHERE rn = 1 AND event_type = 'OfferAvailable';

    -- 11. pinnacle_edition_supply (holder analytics + supply aggregation)
    TRUNCATE pinnacle_edition_supply;
    INSERT INTO pinnacle_edition_supply (edition_id, mint_count, burn_count, existing_supply,
                                         listed_count, in_vaults_count, held_count,
                                         unique_holders, largest_holder_count, concentration_pct)
    WITH
        -- Determine current owner of each NFT from the latest ownership-changing event
        CurrentOwners AS (
            SELECT nft_id, owner_address
            FROM (
                SELECT
                    nft_id,
                    buyer_address AS owner_address,
                    ROW_NUMBER() OVER (
                        PARTITION BY nft_id
                        ORDER BY block_timestamp DESC, transaction_hash DESC, block_height DESC
                    ) AS rn
                FROM pinnacle_events
                WHERE event_type IN (
                    'PinNFTMinted',
                    'PurchasedDapper',
                    'PurchasedFlowty',
                    'NFTDeposited'
                )
                AND nft_id IS NOT NULL AND buyer_address IS NOT NULL
            ) sub
            WHERE rn = 1
        ),

        -- Identify burned NFTs
        BurnedNFTs AS (
            SELECT DISTINCT nft_id, TRUE AS is_burned
            FROM pinnacle_events
            WHERE event_type = 'PinNFTBurned'
        ),

        -- Assign status to each NFT
        NFTStatus AS (
            SELECT
                n.nft_id,
                n.edition_id,
                co.owner_address,
                CASE
                    WHEN bn.is_burned IS TRUE THEN 'burned'
                    WHEN al.nft_id IS NOT NULL THEN 'listed'
                    WHEN co.owner_address IN (
                        '0xb6f2481eba4df97b',  -- Pinnacle vault 2
                        '0xd708cc29ee72b8ee'   -- Pinnacle vault 3
                    ) THEN 'in_vaults'
                    ELSE 'held'
                END AS status
            FROM pinnacle_nfts AS n
            LEFT JOIN CurrentOwners AS co ON n.nft_id = co.nft_id
            LEFT JOIN pinnacle_active_listings AS al ON n.nft_id = al.nft_id
            LEFT JOIN BurnedNFTs AS bn ON n.nft_id = bn.nft_id
        ),

        -- Holder analytics: count NFTs per owner per edition (circulating only)
        HolderCounts AS (
            SELECT
                edition_id,
                owner_address,
                COUNT(*) AS nft_count
            FROM NFTStatus
            WHERE status IN ('held', 'listed')
              AND owner_address IS NOT NULL
              AND owner_address NOT IN (
                  '0xb6f2481eba4df97b',  -- Pinnacle vault 2
                  '0xd708cc29ee72b8ee'   -- Pinnacle vault 3
              )
            GROUP BY edition_id, owner_address
        ),

        HolderAgg AS (
            SELECT
                edition_id,
                COUNT(*)           AS unique_holders,
                MAX(nft_count)     AS largest_holder_count,
                ROUND(MAX(nft_count)::numeric * 100 / NULLIF(SUM(nft_count), 0), 2) AS concentration_pct
            FROM HolderCounts
            GROUP BY edition_id
        ),

        -- Aggregate supply per edition
        AggregatedSupply AS (
            SELECT
                edition_id,
                COUNT(*)                                        AS mint_count,
                COUNT(*) FILTER (WHERE status = 'burned')       AS burn_count,
                COUNT(*) FILTER (WHERE status = 'listed')       AS listed_count,
                COUNT(*) FILTER (WHERE status = 'in_vaults')    AS in_vaults_count,
                COUNT(*) FILTER (WHERE status = 'held')         AS held_count
            FROM NFTStatus
            GROUP BY edition_id
        )

    SELECT
        agg.edition_id,
        agg.mint_count,
        agg.burn_count,
        (agg.mint_count - agg.burn_count)           AS existing_supply,
        agg.listed_count,
        agg.in_vaults_count,
        agg.held_count,
        COALESCE(ha.unique_holders, 0)              AS unique_holders,
        COALESCE(ha.largest_holder_count, 0)         AS largest_holder_count,
        COALESCE(ha.concentration_pct, 0)            AS concentration_pct
    FROM AggregatedSupply AS agg
    LEFT JOIN HolderAgg AS ha ON agg.edition_id = ha.edition_id;

END;
$$ LANGUAGE plpgsql;
