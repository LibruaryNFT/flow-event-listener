-- 01_pinnacle_events.sql
-- Port of BigQuery 2_PINNACLE_events_incremental.sql to PostgreSQL/TimescaleDB
--
-- Complex Layer 2 query: buyer address enrichment via NonFungibleToken.Deposited,
-- marketplace type standardization, offer price/buyer correlation, QA filtering.

-- ══════════════════════════════════════════════════════════════
-- Target table
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pinnacle_events (
    transaction_hash TEXT NOT NULL,
    block_height     BIGINT,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    event_type       TEXT NOT NULL,
    nft_id           BIGINT,
    edition_id       BIGINT,
    serial_number    BIGINT,
    price            NUMERIC,
    seller_address   TEXT,
    buyer_address    TEXT,
    offer_id         BIGINT,
    raw_data         JSONB
);

SELECT create_hypertable('pinnacle_events', 'block_timestamp',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_pinnacle_events_type ON pinnacle_events (event_type);
CREATE INDEX IF NOT EXISTS idx_pinnacle_events_nft ON pinnacle_events (nft_id);
CREATE INDEX IF NOT EXISTS idx_pinnacle_events_edition ON pinnacle_events (edition_id);

-- ══════════════════════════════════════════════════════════════
-- Incremental refresh function
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION refresh_pinnacle_events()
RETURNS void AS $$
DECLARE
    overlap_ts TIMESTAMPTZ;
BEGIN
    SELECT COALESCE(MAX(block_timestamp) - INTERVAL '6 hours', '2019-01-01'::timestamptz)
    INTO overlap_ts
    FROM pinnacle_events;

    DELETE FROM pinnacle_events
    WHERE block_timestamp >= overlap_ts;

    INSERT INTO pinnacle_events
    (transaction_hash, block_height, block_timestamp, event_type, nft_id, edition_id,
     serial_number, price, seller_address, buyer_address, offer_id, raw_data)

    WITH
    -- 1. Transaction recipients for buyer identification (via NonFungibleToken.Deposited)
    -- Pinnacle uses the newer Flow NFT standard with $.type on Deposited events
    PinnacleTransactionRecipients AS (
        SELECT
            evt.transaction_hash,
            (NULLIF(TRIM(evt.data->>'id'), ''))::bigint AS nft_id,
            evt.data->>'to' AS recipient_address
        FROM flow_raw_events AS evt
        WHERE topics @> ARRAY['A.1d7e57aa55817448.NonFungibleToken.Deposited']
          AND evt.data->>'to' IS NOT NULL
          AND evt.data->>'type' LIKE 'A.edf9df96c92f4595.Pinnacle.NFT%'
          AND evt.block_timestamp >= overlap_ts
    ),

    -- 2. Main extraction
    ExtractedEvents AS (
        SELECT
            evt.transaction_hash,
            evt.block_height,
            evt.block_timestamp,

            CASE
                -- Pinnacle Core: NFT events
                WHEN evt.topics @> ARRAY['A.edf9df96c92f4595.Pinnacle.PinNFTMinted'] THEN 'PinNFTMinted'
                WHEN evt.topics @> ARRAY['A.edf9df96c92f4595.Pinnacle.PinNFTBurned'] THEN 'PinNFTBurned'
                -- Pinnacle Core: Metadata creation events
                WHEN evt.topics @> ARRAY['A.edf9df96c92f4595.Pinnacle.SeriesCreated'] THEN 'SeriesCreated'
                WHEN evt.topics @> ARRAY['A.edf9df96c92f4595.Pinnacle.SetCreated'] THEN 'SetCreated'
                WHEN evt.topics @> ARRAY['A.edf9df96c92f4595.Pinnacle.ShapeCreated'] THEN 'ShapeCreated'
                WHEN evt.topics @> ARRAY['A.edf9df96c92f4595.Pinnacle.EditionCreated'] THEN 'EditionCreated'
                WHEN evt.topics @> ARRAY['A.edf9df96c92f4595.Pinnacle.EditionClosed'] THEN 'EditionClosed'
                WHEN evt.topics @> ARRAY['A.edf9df96c92f4595.Pinnacle.EditionTypeCreated'] THEN 'EditionTypeCreated'
                -- Dapper NFTStorefrontV2 (primary marketplace)
                WHEN evt.topics @> ARRAY['A.4eb8a10cb9f87357.NFTStorefrontV2.ListingAvailable'] THEN 'ListingAvailable'
                WHEN evt.topics @> ARRAY['A.4eb8a10cb9f87357.NFTStorefrontV2.ListingCompleted'] THEN 'ListingCompleted'
                -- Flowty / alt NFTStorefrontV2
                WHEN evt.topics @> ARRAY['A.3cdbb3d569211ff3.NFTStorefrontV2.ListingAvailable'] THEN 'ListingAvailableFlowty'
                WHEN evt.topics @> ARRAY['A.3cdbb3d569211ff3.NFTStorefrontV2.ListingCompleted'] THEN 'ListingCompletedFlowty'
                -- OffersV2
                WHEN evt.topics @> ARRAY['A.b8ea91944fd51c43.OffersV2.OfferAvailable'] THEN 'OfferAvailable'
                WHEN evt.topics @> ARRAY['A.b8ea91944fd51c43.OffersV2.OfferCompleted'] THEN 'OfferCompleted'
                -- NonFungibleToken transfers
                WHEN evt.topics @> ARRAY['A.1d7e57aa55817448.NonFungibleToken.Deposited'] THEN 'NFTDeposited'
                WHEN evt.topics @> ARRAY['A.1d7e57aa55817448.NonFungibleToken.Withdrawn'] THEN 'NFTWithdrawn'
                -- Swaps
                WHEN evt.topics @> ARRAY['A.15f55a75d7843780.Swap.ProposalExecuted'] THEN 'SwapExecuted'
                ELSE NULL
            END AS event_type_internal,

            -- NFT ID
            COALESCE(
                (NULLIF(TRIM(evt.data->>'id'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftID'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftId'), ''))::bigint
            ) AS nft_id,

            -- Edition ID (from minting events)
            (NULLIF(TRIM(evt.data->>'editionID'), ''))::bigint AS edition_id,

            -- Serial number (from minting events)
            (NULLIF(TRIM(evt.data->>'serialNumber'), ''))::bigint AS serial_number,

            -- Offer ID
            (NULLIF(TRIM(evt.data->>'offerId'), ''))::bigint AS offer_id,

            -- Price
            COALESCE(
                (NULLIF(evt.data->>'salePrice', ''))::numeric,
                (NULLIF(evt.data->>'price', ''))::numeric,
                (NULLIF(evt.data->>'offerAmount', ''))::numeric
            ) AS price_raw,

            -- Seller
            COALESCE(
                evt.data->>'storefrontAddress',
                evt.data->>'from',
                evt.data->>'seller',
                evt.data->>'acceptingAddress'
            ) AS seller_address,

            -- Buyer
            COALESCE(
                tr.recipient_address,
                evt.data->>'to',
                evt.data->>'buyer',
                evt.data->>'offerAddress'
            ) AS buyer_address_raw,

            -- Purchased flag (for ListingCompleted)
            evt.data->>'purchased' AS purchased,

            evt.data AS raw_data

        FROM flow_raw_events AS evt
        LEFT JOIN PinnacleTransactionRecipients AS tr
            ON evt.transaction_hash = tr.transaction_hash
            AND COALESCE(
                (NULLIF(TRIM(evt.data->>'id'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftID'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftId'), ''))::bigint
            ) = tr.nft_id
        WHERE evt.block_timestamp >= overlap_ts
          AND EXISTS (
              SELECT 1 FROM unnest(evt.topics) AS topic
              WHERE
                  -- Pinnacle Core contract
                  topic LIKE 'A.edf9df96c92f4595.Pinnacle%'
                  -- OffersV2 (only Pinnacle NFTs)
                  OR (topic LIKE 'A.b8ea91944fd51c43.OffersV2%'
                      AND evt.data->>'nftType' LIKE 'A.edf9df96c92f4595.Pinnacle.NFT%')
                  -- Dapper NFTStorefrontV2 (only Pinnacle NFTs)
                  OR (topic LIKE 'A.4eb8a10cb9f87357.NFTStorefrontV2%'
                      AND evt.data->>'nftType' LIKE 'A.edf9df96c92f4595.Pinnacle.NFT%')
                  -- Flowty NFTStorefrontV2 (only Pinnacle NFTs)
                  OR (topic LIKE 'A.3cdbb3d569211ff3.NFTStorefrontV2%'
                      AND evt.data->>'nftType' LIKE 'A.edf9df96c92f4595.Pinnacle.NFT%')
                  -- Swaps involving Pinnacle NFTs
                  OR (topic = 'A.15f55a75d7843780.Swap.ProposalExecuted'
                      AND evt.data->>'nftType' LIKE 'A.edf9df96c92f4595.Pinnacle.NFT%')
                  -- NonFungibleToken transfers for Pinnacle NFTs (uses $.type field)
                  OR (topic LIKE 'A.1d7e57aa55817448.NonFungibleToken%'
                      AND evt.data->>'type' LIKE 'A.edf9df96c92f4595.Pinnacle.NFT%')
          )
    ),

    -- 3. Standardize event types + enrich offers
    EnrichedEvents AS (
        SELECT
            E.transaction_hash,
            E.block_height,
            E.block_timestamp,
            E.nft_id,
            E.edition_id,
            E.serial_number,
            E.seller_address,
            E.purchased,
            E.raw_data,

            -- Standardize event type
            CASE
                -- Marketplace listings
                WHEN E.event_type_internal = 'ListingAvailable' THEN 'ListingAvailable'
                WHEN E.event_type_internal = 'ListingAvailableFlowty' THEN 'ListingAvailable'
                -- Marketplace purchases vs delists
                WHEN E.event_type_internal = 'ListingCompleted' AND E.purchased = 'true' THEN 'PurchasedDapper'
                WHEN E.event_type_internal = 'ListingCompleted' THEN 'ListingWithdrawn'
                WHEN E.event_type_internal = 'ListingCompletedFlowty' AND E.purchased = 'true' THEN 'PurchasedFlowty'
                WHEN E.event_type_internal = 'ListingCompletedFlowty' THEN 'ListingWithdrawn'
                -- Offers (keep as-is)
                WHEN E.event_type_internal IN ('OfferAvailable', 'OfferCompleted') THEN E.event_type_internal
                -- Keep other types as-is
                ELSE E.event_type_internal
            END AS event_type,

            -- Price enrichment (offers)
            CASE
                WHEN E.event_type_internal IN ('OfferAvailable', 'OfferCompleted')
                     AND E.offer_id IS NOT NULL
                    THEN MAX(E.price_raw) OVER (PARTITION BY E.offer_id)
                ELSE E.price_raw
            END AS price,

            -- Buyer enrichment (offers)
            -- Use FIRST_VALUE (chronological) not MAX (lexicographic)
            CASE
                WHEN E.event_type_internal IN ('OfferAvailable', 'OfferCompleted')
                     AND E.offer_id IS NOT NULL
                    THEN FIRST_VALUE(E.buyer_address_raw) OVER (
                        PARTITION BY E.offer_id
                        ORDER BY E.block_timestamp ASC, E.block_height ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    )
                ELSE E.buyer_address_raw
            END AS buyer_address,

            E.offer_id

        FROM ExtractedEvents E
        WHERE E.event_type_internal IS NOT NULL
    )

    -- 4. Final with QA filter
    SELECT
        transaction_hash, block_height, block_timestamp, event_type,
        nft_id, edition_id, serial_number, price, seller_address, buyer_address,
        offer_id, raw_data
    FROM EnrichedEvents
    WHERE NOT (
        event_type IN ('PurchasedDapper', 'PurchasedFlowty')
        AND (price IS NULL OR nft_id IS NULL)
    );
END;
$$ LANGUAGE plpgsql;
