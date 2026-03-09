-- =================================================================================
-- 02_allday_tables.sql
-- Derived foundation tables for AllDay analytics (PostgreSQL/TimescaleDB)
--
-- Ported from BigQuery: vaultopolis/google/queries/pipeline/3_ALLDAY_tables.sql
--
-- PREREQUISITE: allday_events must already exist
-- USAGE:        SELECT refresh_allday_tables();
-- =================================================================================

-- ---------------------------------------------------------------------------------
-- Table definitions
-- ---------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS allday_series (
    series_id   bigint PRIMARY KEY,
    series_name text
);

CREATE TABLE IF NOT EXISTS allday_sets (
    set_id   bigint PRIMARY KEY,
    set_name text
);

CREATE TABLE IF NOT EXISTS allday_plays (
    play_id       bigint PRIMARY KEY,
    player_name   text,
    team          text,
    play_category text,
    jersey_number bigint
);

CREATE TABLE IF NOT EXISTS allday_editions (
    edition_id bigint PRIMARY KEY,
    series_id  bigint,
    set_id     bigint,
    play_id    bigint,
    tier       text
);

CREATE TABLE IF NOT EXISTS allday_moments (
    moment_id   bigint PRIMARY KEY,
    edition_id  bigint,
    serial_number bigint,
    play_id     bigint,
    set_id      bigint,
    series_id   bigint,
    player_name text,
    team        text,
    play_category text,
    set_name    text,
    series_name text,
    tier        text
);
CREATE INDEX IF NOT EXISTS idx_allday_moments_edition ON allday_moments (edition_id);

CREATE TABLE IF NOT EXISTS allday_sales (
    block_timestamp  timestamptz,
    transaction_hash text,
    moment_id        bigint,
    price            numeric,
    edition_id       bigint,
    serial_number    bigint,
    player_name      text,
    team             text,
    play_category    text,
    set_name         text,
    series_name      text,
    tier             text
);
CREATE INDEX IF NOT EXISTS idx_allday_sales_timestamp ON allday_sales (block_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_allday_sales_player    ON allday_sales (player_name);
CREATE INDEX IF NOT EXISTS idx_allday_sales_set       ON allday_sales (set_name);

CREATE TABLE IF NOT EXISTS allday_active_listings (
    moment_id  bigint PRIMARY KEY,
    edition_id bigint,
    price      numeric
);
CREATE INDEX IF NOT EXISTS idx_allday_active_listings_edition ON allday_active_listings (edition_id);

CREATE TABLE IF NOT EXISTS allday_offers (
    block_timestamp timestamptz,
    offer_id        bigint,
    offer_address   text,
    offer_amount    numeric,
    edition_id      bigint,
    target_nft_id   bigint,
    offer_type      text,
    event_type      text
);
CREATE INDEX IF NOT EXISTS idx_allday_offers_offer_id ON allday_offers (offer_id);

CREATE TABLE IF NOT EXISTS allday_active_offers (
    block_timestamp timestamptz,
    offer_id        bigint PRIMARY KEY,
    offer_address   text,
    offer_amount    numeric,
    edition_id      bigint,
    target_nft_id   bigint,
    offer_type      text,
    event_type      text
);

CREATE TABLE IF NOT EXISTS allday_edition_supply (
    edition_id            bigint PRIMARY KEY,
    mint_count            bigint,
    burn_count            bigint,
    existing_supply       bigint,
    listed_count          bigint,
    in_packs_count        bigint,
    held_count            bigint,
    unique_holders        bigint,
    largest_holder_count  bigint,
    concentration_pct     numeric
);

-- ---------------------------------------------------------------------------------
-- Refresh function — rebuilds all tables in dependency order
-- ---------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION refresh_allday_tables() RETURNS void AS $$
BEGIN

    -- 1. allday_series
    TRUNCATE allday_series;
    INSERT INTO allday_series (series_id, series_name)
    SELECT
        nft_id                         AS series_id,
        raw_data->>'name'              AS series_name
    FROM allday_events
    WHERE event_type = 'SeriesCreated';

    -- 2. allday_sets
    TRUNCATE allday_sets;
    INSERT INTO allday_sets (set_id, set_name)
    SELECT
        nft_id                         AS set_id,
        raw_data->>'name'              AS set_name
    FROM allday_events
    WHERE event_type = 'SetCreated';

    -- 3. allday_plays
    TRUNCATE allday_plays;
    INSERT INTO allday_plays (play_id, player_name, team, play_category, jersey_number)
    SELECT
        nft_id AS play_id,
        (raw_data->'metadata'->>'playerFirstName') || ' ' || (raw_data->'metadata'->>'playerLastName') AS player_name,
        raw_data->'metadata'->>'teamName'       AS team,
        raw_data->'metadata'->>'playType'        AS play_category,
        (NULLIF(TRIM(raw_data->'metadata'->>'playerNumber'), ''))::bigint AS jersey_number
    FROM allday_events
    WHERE event_type = 'PlayCreated';

    -- 4. allday_editions (from EditionCreated + MomentNFTMinted)
    TRUNCATE allday_editions;
    INSERT INTO allday_editions (edition_id, series_id, set_id, play_id, tier)
    WITH
        EditionIDsFromCreation AS (
            SELECT DISTINCT nft_id AS edition_id, series_id, set_id, play_id, tier
            FROM allday_events
            WHERE event_type = 'EditionCreated' AND nft_id IS NOT NULL
        ),
        EditionIDsFromMints AS (
            SELECT DISTINCT edition_id
            FROM allday_events
            WHERE event_type = 'MomentNFTMinted' AND edition_id IS NOT NULL
        ),
        AllEditionIDs AS (
            SELECT edition_id, series_id, set_id, play_id, tier FROM EditionIDsFromCreation
            UNION
            SELECT edition_id, NULL, NULL, NULL, NULL FROM EditionIDsFromMints
        )
    SELECT
        edition_id,
        MAX(series_id) AS series_id,
        MAX(set_id)    AS set_id,
        MAX(play_id)   AS play_id,
        MAX(tier)      AS tier
    FROM AllEditionIDs
    GROUP BY edition_id;

    -- 5. allday_moments (enriched with metadata joins)
    TRUNCATE allday_moments;
    INSERT INTO allday_moments (moment_id, edition_id, serial_number, play_id, set_id, series_id,
                                player_name, team, play_category, set_name, series_name, tier)
    SELECT
        m.nft_id AS moment_id,
        m.edition_id,
        m.serial_number,
        e.play_id,
        e.set_id,
        e.series_id,
        p.player_name,
        p.team,
        p.play_category,
        s.set_name,
        se.series_name,
        e.tier
    FROM allday_events        AS m
    LEFT JOIN allday_editions AS e  ON m.edition_id = e.edition_id
    LEFT JOIN allday_plays    AS p  ON e.play_id    = p.play_id
    LEFT JOIN allday_sets     AS s  ON e.set_id     = s.set_id
    LEFT JOIN allday_series   AS se ON e.series_id  = se.series_id
    WHERE m.event_type = 'MomentNFTMinted';

    -- 6. allday_sales (ListingCompleted joined with ListingAvailable for prices)
    TRUNCATE allday_sales;
    INSERT INTO allday_sales (block_timestamp, transaction_hash, moment_id, price,
                              edition_id, serial_number, player_name, team,
                              play_category, set_name, series_name, tier)
    WITH
        ListingPrices AS (
            SELECT
                raw_data->>'listingResourceID' AS listing_resource_id,
                nft_id,
                price
            FROM allday_events
            WHERE event_type = 'ListingAvailable'
              AND price > 0
              AND raw_data->>'listingResourceID' IS NOT NULL
        ),
        SaleEvents AS (
            SELECT
                block_timestamp,
                transaction_hash,
                raw_data->>'listingResourceID' AS listing_resource_id,
                nft_id                         AS sale_nft_id,
                price                          AS sale_price
            FROM allday_events
            WHERE event_type = 'ListingCompleted'
              AND raw_data->>'purchased' = 'true'
              AND raw_data->>'listingResourceID' IS NOT NULL
        )
    SELECT
        s.block_timestamp,
        s.transaction_hash,
        COALESCE(lp.nft_id, s.sale_nft_id) AS moment_id,
        COALESCE(
            CASE WHEN s.sale_price IS NOT NULL AND s.sale_price > 0 THEN s.sale_price END,
            lp.price
        ) AS price,
        m.edition_id,
        m.serial_number,
        m.player_name,
        m.team,
        m.play_category,
        m.set_name,
        m.series_name,
        m.tier
    FROM SaleEvents s
    JOIN ListingPrices lp ON s.listing_resource_id = lp.listing_resource_id
    JOIN allday_moments m ON COALESCE(lp.nft_id, s.sale_nft_id) = m.moment_id
    WHERE m.edition_id IS NOT NULL
      AND COALESCE(
            CASE WHEN s.sale_price IS NOT NULL AND s.sale_price > 0 THEN s.sale_price END,
            lp.price
          ) > 0;

    -- 7. allday_active_listings (latest status per nft_id, filters expired)
    TRUNCATE allday_active_listings;
    INSERT INTO allday_active_listings (moment_id, edition_id, price)
    WITH
        LatestMomentStatus AS (
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
        )
    SELECT
        l.moment_id,
        m.edition_id,
        l.price
    FROM LatestMomentStatus l
    JOIN allday_moments m ON l.moment_id = m.moment_id
    WHERE l.rn = 1
      AND l.event_type = 'ListingAvailable'
      AND l.price IS NOT NULL
      AND l.price > 0
      AND (
          (NULLIF(TRIM(l.raw_data->>'expiry'), ''))::bigint IS NULL
          OR (NULLIF(TRIM(l.raw_data->>'expiry'), ''))::bigint > EXTRACT(EPOCH FROM NOW())::bigint
      );

    -- 8. allday_offers (all offer history)
    TRUNCATE allday_offers;
    INSERT INTO allday_offers (block_timestamp, offer_id, offer_address, offer_amount,
                               edition_id, target_nft_id, offer_type, event_type)
    SELECT
        block_timestamp,
        (NULLIF(TRIM(raw_data->>'offerId'), ''))::bigint        AS offer_id,
        raw_data->>'offerAddress'                                AS offer_address,
        price                                                    AS offer_amount,
        edition_id,
        nft_id                                                   AS target_nft_id,
        raw_data->'offerParamsString'->>'_type'                  AS offer_type,
        event_type
    FROM allday_events
    WHERE event_type IN ('OfferAvailable', 'OfferCompleted');

    -- 9. allday_active_offers (latest per offer_id)
    TRUNCATE allday_active_offers;
    INSERT INTO allday_active_offers (block_timestamp, offer_id, offer_address, offer_amount,
                                      edition_id, target_nft_id, offer_type, event_type)
    WITH
        LatestOfferEvent AS (
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
            FROM allday_offers
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

    -- 10. allday_edition_supply (holder analytics + supply aggregation)
    TRUNCATE allday_edition_supply;
    INSERT INTO allday_edition_supply (edition_id, mint_count, burn_count, existing_supply,
                                       listed_count, in_packs_count, held_count,
                                       unique_holders, largest_holder_count, concentration_pct)
    WITH
        CurrentOwners AS (
            SELECT nft_id AS moment_id, owner_address
            FROM (
                SELECT
                    nft_id,
                    COALESCE(
                        raw_data->>'to',
                        raw_data->>'buyer',
                        raw_data->>'offerAddress'
                    ) AS owner_address,
                    ROW_NUMBER() OVER (
                        PARTITION BY nft_id
                        ORDER BY block_timestamp DESC, transaction_hash DESC
                    ) AS rn
                FROM allday_events
                WHERE event_type IN (
                    'MomentNFTMinted',
                    'Deposited',
                    'ListingCompleted',
                    'OfferCompleted'
                )
                AND nft_id IS NOT NULL
            ) sub
            WHERE rn = 1 AND owner_address IS NOT NULL
        ),

        BurnedMoments AS (
            SELECT DISTINCT nft_id AS moment_id, TRUE AS is_burned
            FROM allday_events
            WHERE event_type = 'MomentNFTBurned'
        ),

        MomentStatus AS (
            SELECT
                m.moment_id,
                m.edition_id,
                co.owner_address,
                CASE
                    WHEN bm.is_burned IS TRUE THEN 'burned'
                    WHEN al.moment_id IS NOT NULL THEN 'listed'
                    WHEN co.owner_address IN (
                        '0x8c543d2b27c81a56',
                        '0xe1f2a091f7bb5245',
                        '0xfa57101aa0d55954'
                    ) THEN 'in_packs'
                    ELSE 'held'
                END AS status
            FROM allday_moments            AS m
            LEFT JOIN CurrentOwners        AS co ON m.moment_id = co.moment_id
            LEFT JOIN allday_active_listings AS al ON m.moment_id = al.moment_id
            LEFT JOIN BurnedMoments        AS bm ON m.moment_id = bm.moment_id
        ),

        HolderCounts AS (
            SELECT
                edition_id,
                owner_address,
                COUNT(*) AS moment_count
            FROM MomentStatus
            WHERE status IN ('held', 'listed')
              AND owner_address IS NOT NULL
              AND owner_address NOT IN (
                  '0x8c543d2b27c81a56',
                  '0xe1f2a091f7bb5245',
                  '0xfa57101aa0d55954'
              )
            GROUP BY edition_id, owner_address
        ),

        HolderAgg AS (
            SELECT
                edition_id,
                COUNT(*)                AS unique_holders,
                MAX(moment_count)       AS largest_holder_count,
                ROUND(MAX(moment_count)::numeric * 100 / NULLIF(SUM(moment_count), 0), 2) AS concentration_pct
            FROM HolderCounts
            GROUP BY edition_id
        ),

        AggregatedSupply AS (
            SELECT
                edition_id,
                COUNT(*)                                       AS mint_count,
                COUNT(*) FILTER (WHERE status = 'burned')      AS burn_count,
                COUNT(*) FILTER (WHERE status = 'listed')      AS listed_count,
                COUNT(*) FILTER (WHERE status = 'in_packs')    AS in_packs_count,
                COUNT(*) FILTER (WHERE status = 'held')        AS held_count
            FROM MomentStatus
            GROUP BY edition_id
        )

    SELECT
        agg.edition_id,
        agg.mint_count,
        agg.burn_count,
        (agg.mint_count - agg.burn_count)    AS existing_supply,
        agg.listed_count,
        agg.in_packs_count,
        agg.held_count,
        COALESCE(ha.unique_holders, 0)       AS unique_holders,
        COALESCE(ha.largest_holder_count, 0)  AS largest_holder_count,
        COALESCE(ha.concentration_pct, 0)     AS concentration_pct
    FROM AggregatedSupply AS agg
    LEFT JOIN HolderAgg   AS ha ON agg.edition_id = ha.edition_id;

END;
$$ LANGUAGE plpgsql;
