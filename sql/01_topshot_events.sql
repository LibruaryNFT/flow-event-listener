-- 01_topshot_events.sql
-- Port of BigQuery 2_TOPSHOT_events_incremental.sql to PostgreSQL/TimescaleDB
--
-- Most complex Layer 2 query: buyer address enrichment via transaction recipients,
-- offer price/buyer correlation, event type standardization, QA filtering.

-- ══════════════════════════════════════════════════════════════
-- Target table
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS topshot_events (
    transaction_hash TEXT NOT NULL,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    event_order      INTEGER,
    topics           TEXT[],
    event_type       TEXT NOT NULL,
    nft_id           BIGINT,
    play_id          BIGINT,
    set_id           BIGINT,
    subedition_id    BIGINT,
    price            NUMERIC,
    seller_address   TEXT,
    buyer_address    TEXT,
    raw_data         JSONB
);

SELECT create_hypertable('topshot_events', 'block_timestamp',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_topshot_events_type ON topshot_events (event_type);
CREATE INDEX IF NOT EXISTS idx_topshot_events_nft ON topshot_events (nft_id);
CREATE INDEX IF NOT EXISTS idx_topshot_events_play ON topshot_events (play_id);

-- ══════════════════════════════════════════════════════════════
-- Incremental refresh function
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION refresh_topshot_events()
RETURNS void AS $$
DECLARE
    overlap_ts TIMESTAMPTZ;
BEGIN
    SELECT COALESCE(MAX(block_timestamp) - INTERVAL '6 hours', '2019-01-01'::timestamptz)
    INTO overlap_ts
    FROM topshot_events;

    DELETE FROM topshot_events
    WHERE block_timestamp >= overlap_ts;

    INSERT INTO topshot_events
    (transaction_hash, block_timestamp, event_order, topics, event_type,
     nft_id, play_id, set_id, subedition_id, price, seller_address, buyer_address, raw_data)

    WITH
    -- 1. Transaction recipients for buyer identification
    TransactionRecipients AS (
        SELECT
            evt.transaction_hash,
            (NULLIF(TRIM(evt.data->>'id'), ''))::bigint AS nft_id,
            evt.data->>'to' AS recipient_address
        FROM flow_raw_events AS evt
        WHERE topics @> ARRAY['A.0b2a3299cc857e29.TopShot.Deposit']
          AND evt.data->>'to' IS NOT NULL
          AND evt.block_timestamp >= overlap_ts
    ),

    -- 2. Main extraction
    ExtractedEvents AS (
        SELECT
            evt.transaction_hash,
            evt.block_timestamp,
            evt.log_index AS event_order,
            evt.topics,

            CASE
                -- Dapper Marketplace V1
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.Market.MomentListed'] THEN 'MomentListedV1'
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.Market.MomentWithdrawn'] THEN 'MomentWithdrawnV1'
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.Market.MomentPurchased'] THEN 'MomentPurchasedV1'
                -- V2/V3
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.TopShotMarketV3.MomentListed'] THEN 'MomentListedV3'
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.TopShotMarketV2.MomentListed'] THEN 'MomentListedV2'
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.TopShotMarketV3.MomentWithdrawn'] THEN 'MomentWithdrawnV3'
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.TopShotMarketV2.MomentWithdrawn'] THEN 'MomentWithdrawnV2'
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.TopShotMarketV3.MomentPurchased'] THEN 'MomentPurchasedV3'
                WHEN evt.topics @> ARRAY['A.c1e4f4f4c4257510.TopShotMarketV2.MomentPurchased'] THEN 'MomentPurchasedV2'
                -- Offers V2
                WHEN evt.topics @> ARRAY['A.b8ea91944fd51c43.OffersV2.OfferAvailable'] THEN 'OfferAvailableV2'
                WHEN evt.topics @> ARRAY['A.b8ea91944fd51c43.OffersV2.OfferCompleted'] THEN 'OfferCompletedV2'
                -- Offers V1
                WHEN evt.topics @> ARRAY['A.b8ea91944fd51c43.Offers.OfferAvailable'] THEN 'OfferAvailableV1'
                WHEN evt.topics @> ARRAY['A.b8ea91944fd51c43.Offers.OfferCompleted'] THEN 'OfferCompletedV1'
                -- Flowty
                WHEN evt.topics @> ARRAY['A.3cdbb3d569211ff3.NFTStorefrontV2.ListingAvailable'] THEN 'FlowtyListingAvailable'
                WHEN evt.topics @> ARRAY['A.3cdbb3d569211ff3.NFTStorefrontV2.ListingCompleted'] THEN 'FlowtyListingCompleted'
                -- TopShot Core
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.PlayCreated'] THEN 'PlayCreated'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.NewSeriesStarted'] THEN 'NewSeriesStarted'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.SetCreated'] THEN 'SetCreated'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.PlayAddedToSet'] THEN 'PlayAddedToSet'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.PlayRetiredFromSet'] THEN 'PlayRetiredFromSet'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.SetLocked'] THEN 'SetLocked'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.MomentMinted'] THEN 'MomentMinted'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.Withdraw'] THEN 'MomentWithdrawnFromCollection'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.Deposit'] THEN 'MomentDeposited'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.MomentDestroyed'] THEN 'MomentDestroyed'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.NFT.ResourceDestroyed'] THEN 'MomentDestroyed'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.SubeditionCreated'] THEN 'SubeditionCreated'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.SubeditionAddedToMoment'] THEN 'SubeditionAddedToMoment'
                -- Locking
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShotLocking.MomentLocked'] THEN 'MomentLocked'
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShotLocking.MomentUnlocked'] THEN 'MomentUnlocked'
                ELSE NULL
            END AS event_type_internal,

            -- NFT ID
            COALESCE(
                (NULLIF(TRIM(evt.data->>'id'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'momentID'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'momentId'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftId'), ''))::bigint,
                (NULLIF(TRIM(evt.data->'offerParamsString'->>'nftId'), ''))::bigint,
                (NULLIF(TRIM(evt.data->'offerParamsUInt64'->>'nftId'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftID'), ''))::bigint
            ) AS nft_id,

            -- Offer ID
            (NULLIF(TRIM(evt.data->>'offerId'), ''))::bigint AS offer_id,

            -- Play ID
            COALESCE(
                (NULLIF(TRIM(evt.data->>'playID'), ''))::bigint,
                (NULLIF(TRIM(evt.data->'offerParamsString'->>'playId'), ''))::bigint
            ) AS play_id,

            -- Set ID
            COALESCE(
                (NULLIF(TRIM(evt.data->>'setID'), ''))::bigint,
                (NULLIF(TRIM(evt.data->'offerParamsString'->>'setId'), ''))::bigint
            ) AS set_id,

            -- Subedition ID
            CASE
                WHEN evt.topics @> ARRAY['A.0b2a3299cc857e29.TopShot.MomentMinted']
                    THEN (NULLIF(TRIM(evt.data->>'subeditionID'), ''))::bigint
                WHEN evt.topics @> ARRAY['A.b8ea91944fd51c43.OffersV2.OfferAvailable']
                  OR evt.topics @> ARRAY['A.b8ea91944fd51c43.OffersV2.OfferCompleted']
                    THEN (NULLIF(TRIM(evt.data->'offerParamsString'->>'subeditionId'), ''))::bigint
                ELSE NULL
            END AS subedition_id,

            -- Price
            COALESCE(
                (NULLIF(evt.data->>'price', ''))::numeric,
                (NULLIF(evt.data->>'offerAmount', ''))::numeric,
                (NULLIF(evt.data->>'salePrice', ''))::numeric
            ) AS price_raw,

            -- Seller
            COALESCE(
                evt.data->>'from',
                evt.data->>'seller',
                evt.data->>'acceptingAddress',
                evt.data->>'storefrontAddress'
            ) AS seller_address,

            -- Buyer
            COALESCE(
                tr.recipient_address,
                evt.data->>'to',
                evt.data->>'buyer',
                evt.data->>'offerAddress'
            ) AS buyer_address_raw,

            evt.data AS raw_data

        FROM flow_raw_events AS evt
        LEFT JOIN TransactionRecipients AS tr
            ON evt.transaction_hash = tr.transaction_hash
            AND COALESCE(
                (NULLIF(TRIM(evt.data->>'id'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'momentID'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'momentId'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftId'), ''))::bigint,
                (NULLIF(TRIM(evt.data->'offerParamsString'->>'nftId'), ''))::bigint,
                (NULLIF(TRIM(evt.data->'offerParamsUInt64'->>'nftId'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftID'), ''))::bigint
            ) = tr.nft_id
        WHERE evt.block_timestamp >= overlap_ts
          AND EXISTS (
              SELECT 1 FROM unnest(evt.topics) AS topic
              WHERE
                  topic LIKE 'A.0b2a3299cc857e29.TopShot%'
                  OR topic LIKE 'A.c1e4f4f4c4257510.Market.%'
                  OR topic LIKE 'A.c1e4f4f4c4257510.TopShotMarket%'
                  OR (topic LIKE 'A.b8ea91944fd51c43.Offers%'
                      AND evt.data->>'nftType' LIKE 'A.0b2a3299cc857e29.TopShot.NFT%')
                  OR (topic LIKE 'A.3cdbb3d569211ff3.NFTStorefrontV2%'
                      AND evt.data->>'nftType' LIKE 'A.0b2a3299cc857e29.TopShot.NFT%')
          )
    ),

    -- 3. Standardize event types + enrich offers
    EnrichedEvents AS (
        SELECT
            E.transaction_hash,
            E.block_timestamp,
            E.event_order,
            E.topics,
            E.nft_id,
            E.play_id,
            E.set_id,
            E.subedition_id,
            E.seller_address,
            E.raw_data,

            -- Standardize event type
            CASE
                WHEN E.event_type_internal IN ('MomentListedV3', 'MomentListedV2', 'MomentListedV1') THEN 'MomentListed'
                WHEN E.event_type_internal IN ('MomentWithdrawnV3', 'MomentWithdrawnV2', 'MomentWithdrawnV1') THEN 'MomentWithdrawn'
                WHEN E.event_type_internal IN ('OfferAvailableV1', 'OfferAvailableV2') THEN 'OfferAvailable'
                WHEN E.event_type_internal IN ('OfferCompletedV1', 'OfferCompletedV2') THEN 'OfferCompleted'
                WHEN E.event_type_internal = 'FlowtyListingAvailable' THEN 'MomentListed'
                WHEN E.event_type_internal = 'FlowtyListingCompleted' THEN
                    CASE
                        WHEN E.raw_data->>'purchased' = 'true' THEN 'MomentPurchasedFlowty'
                        ELSE 'MomentWithdrawn'
                    END
                ELSE E.event_type_internal
            END AS event_type,

            -- Price enrichment (offers)
            CASE
                WHEN E.event_type_internal IN ('OfferAvailableV1', 'OfferAvailableV2', 'OfferCompletedV1', 'OfferCompletedV2')
                     AND E.offer_id IS NOT NULL
                    THEN MAX(E.price_raw) OVER (PARTITION BY E.offer_id)
                ELSE E.price_raw
            END AS price,

            -- Buyer enrichment (offers)
            CASE
                WHEN E.event_type_internal IN ('OfferAvailableV1', 'OfferAvailableV2', 'OfferCompletedV1', 'OfferCompletedV2')
                     AND E.offer_id IS NOT NULL
                    THEN FIRST_VALUE(E.buyer_address_raw) OVER (
                        PARTITION BY E.offer_id
                        ORDER BY E.block_timestamp ASC, E.event_order ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    )
                ELSE E.buyer_address_raw
            END AS buyer_address

        FROM ExtractedEvents E
        WHERE E.event_type_internal IS NOT NULL
    )

    -- 4. Final with QA filter
    SELECT
        transaction_hash, block_timestamp, event_order, topics, event_type,
        nft_id, play_id, set_id, subedition_id, price, seller_address, buyer_address, raw_data
    FROM EnrichedEvents
    WHERE NOT (
        (
            event_type IN ('MomentPurchasedV1', 'MomentPurchasedV2', 'MomentPurchasedV3', 'MomentPurchasedFlowty')
            OR (event_type = 'OfferCompleted' AND raw_data->>'purchased' = 'true')
        )
        AND (price IS NULL OR seller_address IS NULL OR buyer_address IS NULL OR nft_id IS NULL)
    );
END;
$$ LANGUAGE plpgsql;
