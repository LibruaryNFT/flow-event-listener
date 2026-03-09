-- 01_allday_events.sql
-- Port of BigQuery 2_ALLDAY_events_incremental.sql to PostgreSQL/TimescaleDB
--
-- Creates the allday_events table and a function to incrementally refresh it.

-- ══════════════════════════════════════════════════════════════
-- Target table
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS allday_events (
    transaction_hash TEXT NOT NULL,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    event_type       TEXT NOT NULL,
    nft_id           BIGINT,
    edition_id       BIGINT,
    series_id        BIGINT,
    set_id           BIGINT,
    play_id          BIGINT,
    tier             TEXT,
    serial_number    BIGINT,
    price            NUMERIC,
    raw_data         JSONB
);

SELECT create_hypertable('allday_events', 'block_timestamp',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_allday_events_type ON allday_events (event_type);
CREATE INDEX IF NOT EXISTS idx_allday_events_nft ON allday_events (nft_id);
CREATE INDEX IF NOT EXISTS idx_allday_events_edition ON allday_events (edition_id);

-- ══════════════════════════════════════════════════════════════
-- Incremental refresh function
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION refresh_allday_events()
RETURNS void AS $$
DECLARE
    overlap_ts TIMESTAMPTZ;
BEGIN
    SELECT COALESCE(MAX(block_timestamp) - INTERVAL '6 hours', '2021-01-01'::timestamptz)
    INTO overlap_ts
    FROM allday_events;

    DELETE FROM allday_events
    WHERE block_timestamp >= overlap_ts;

    INSERT INTO allday_events
    (transaction_hash, block_timestamp, event_type, nft_id, edition_id, series_id,
     set_id, play_id, tier, serial_number, price, raw_data)

    WITH
    AllDayRawEvents AS (
        SELECT transaction_hash, block_timestamp, log_index, topics, data
        FROM flow_raw_events
        WHERE block_timestamp >= overlap_ts
          AND (
              EXISTS(SELECT 1 FROM unnest(topics) t WHERE t LIKE 'A.e4cf4bdc1751c65d.AllDay.%')
              OR EXISTS(SELECT 1 FROM unnest(topics) t WHERE t LIKE 'A.4eb8a10cb9f87357.NFTStorefront.%')
              OR EXISTS(SELECT 1 FROM unnest(topics) t WHERE t LIKE 'A.4eb8a10cb9f87357.NFTStorefrontV2.%')
              OR EXISTS(SELECT 1 FROM unnest(topics) t WHERE t LIKE 'A.3cdbb3d569211ff3.NFTStorefrontV2.%')
              OR EXISTS(SELECT 1 FROM unnest(topics) t WHERE t LIKE 'A.b8ea91944fd51c43.OffersV2.%')
              OR EXISTS(SELECT 1 FROM unnest(topics) t WHERE t LIKE 'A.1d7e57aa55817448.NonFungibleToken.%')
          )
    ),

    ExtractedEvents AS (
        SELECT
            evt.transaction_hash,
            evt.block_timestamp,

            CASE
                -- AllDay Core
                WHEN topics @> ARRAY['A.e4cf4bdc1751c65d.AllDay.SeriesCreated'] THEN 'SeriesCreated'
                WHEN topics @> ARRAY['A.e4cf4bdc1751c65d.AllDay.SetCreated'] THEN 'SetCreated'
                WHEN topics @> ARRAY['A.e4cf4bdc1751c65d.AllDay.PlayCreated'] THEN 'PlayCreated'
                WHEN topics @> ARRAY['A.e4cf4bdc1751c65d.AllDay.EditionCreated'] THEN 'EditionCreated'
                WHEN topics @> ARRAY['A.e4cf4bdc1751c65d.AllDay.MomentNFTMinted'] THEN 'MomentNFTMinted'
                WHEN topics @> ARRAY['A.e4cf4bdc1751c65d.AllDay.MomentNFTBurned'] THEN 'MomentNFTBurned'

                -- Dapper Storefront V1
                WHEN topics @> ARRAY['A.4eb8a10cb9f87357.NFTStorefront.ListingAvailable']
                  AND evt.data->>'nftType' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'ListingAvailable'
                WHEN topics @> ARRAY['A.4eb8a10cb9f87357.NFTStorefront.ListingCompleted']
                  AND evt.data->>'nftType' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'ListingCompleted'

                -- Dapper Storefront V2
                WHEN topics @> ARRAY['A.4eb8a10cb9f87357.NFTStorefrontV2.ListingAvailable']
                  AND evt.data->>'nftType' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'ListingAvailable'
                WHEN topics @> ARRAY['A.4eb8a10cb9f87357.NFTStorefrontV2.ListingCompleted']
                  AND evt.data->>'nftType' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'ListingCompleted'

                -- Flowty Storefront V2
                WHEN topics @> ARRAY['A.3cdbb3d569211ff3.NFTStorefrontV2.ListingAvailable']
                  AND evt.data->>'nftType' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'ListingAvailable'
                WHEN topics @> ARRAY['A.3cdbb3d569211ff3.NFTStorefrontV2.ListingCompleted']
                  AND evt.data->>'nftType' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'ListingCompleted'

                -- Offers V2
                WHEN topics @> ARRAY['A.b8ea91944fd51c43.OffersV2.OfferAvailable']
                  AND evt.data->>'nftType' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'OfferAvailable'
                WHEN topics @> ARRAY['A.b8ea91944fd51c43.OffersV2.OfferCompleted']
                  AND evt.data->>'nftType' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'OfferCompleted'

                -- NonFungibleToken
                WHEN topics @> ARRAY['A.1d7e57aa55817448.NonFungibleToken.Deposited']
                  AND evt.data->>'type' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'Deposited'
                WHEN topics @> ARRAY['A.1d7e57aa55817448.NonFungibleToken.Withdrawn']
                  AND evt.data->>'type' LIKE 'A.e4cf4bdc1751c65d.AllDay.NFT%'
                  THEN 'Withdrawn'

                ELSE NULL
            END AS event_type,

            -- NFT ID
            COALESCE(
                (NULLIF(TRIM(evt.data->>'id'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftID'), ''))::bigint,
                (NULLIF(TRIM(evt.data->>'nftId'), ''))::bigint
            ) AS nft_id,

            -- Edition ID
            COALESCE(
                (NULLIF(TRIM(evt.data->>'editionID'), ''))::bigint,
                (NULLIF(TRIM(evt.data->'offerParamsString'->>'editionId'), ''))::bigint
            ) AS edition_id,

            (NULLIF(TRIM(evt.data->>'seriesID'), ''))::bigint AS series_id,
            (NULLIF(TRIM(evt.data->>'setID'), ''))::bigint AS set_id,
            (NULLIF(TRIM(evt.data->>'playID'), ''))::bigint AS play_id,
            evt.data->>'tier' AS tier,
            (NULLIF(TRIM(evt.data->>'serialNumber'), ''))::bigint AS serial_number,

            -- Price
            COALESCE(
                (NULLIF(evt.data->>'salePrice', ''))::numeric,
                (NULLIF(evt.data->>'price', ''))::numeric,
                (NULLIF(evt.data->>'offerAmount', ''))::numeric
            ) AS price,

            evt.data AS raw_data
        FROM AllDayRawEvents AS evt
    )

    SELECT
        transaction_hash, block_timestamp, event_type, nft_id, edition_id,
        series_id, set_id, play_id, tier, serial_number, price, raw_data
    FROM ExtractedEvents
    WHERE event_type IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
