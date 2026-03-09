-- 01_packnft_events.sql
-- Port of BigQuery 2_PACKNFT_events_incremental.sql to PostgreSQL/TimescaleDB
--
-- Creates the packnft_events table and a function to incrementally refresh it.
-- The refresh uses a 6-hour overlap window (same as BigQuery version).

-- ══════════════════════════════════════════════════════════════
-- Target table
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS packnft_events (
    transaction_hash TEXT NOT NULL,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    event_order      INTEGER,
    event_type       TEXT NOT NULL,
    pack_id          BIGINT,
    dist_id          BIGINT,
    hash_value       TEXT,
    salt             TEXT,
    nfts_raw         TEXT,
    owner_address    TEXT,
    open_request     TEXT,
    raw_data         JSONB
);

SELECT create_hypertable('packnft_events', 'block_timestamp',
    chunk_time_interval => INTERVAL '3 months',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_packnft_events_type
    ON packnft_events (event_type);
CREATE INDEX IF NOT EXISTS idx_packnft_events_pack
    ON packnft_events (pack_id);

-- ══════════════════════════════════════════════════════════════
-- Incremental refresh function
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION refresh_packnft_events()
RETURNS void AS $$
DECLARE
    overlap_ts TIMESTAMPTZ;
BEGIN
    -- Step 1: Calculate overlap window (6 hours back from latest event)
    SELECT COALESCE(MAX(block_timestamp) - INTERVAL '6 hours', '2019-01-01'::timestamptz)
    INTO overlap_ts
    FROM packnft_events;

    -- Step 2: Delete events in overlap window
    DELETE FROM packnft_events
    WHERE block_timestamp >= overlap_ts;

    -- Step 3: Re-insert from raw events
    INSERT INTO packnft_events
    (transaction_hash, block_timestamp, event_order, event_type, pack_id, dist_id,
     hash_value, salt, nfts_raw, owner_address, open_request, raw_data)

    WITH ExtractedEvents AS (
        SELECT
            evt.transaction_hash,
            evt.block_timestamp,
            evt.log_index AS event_order,

            -- Event Type Classification
            CASE
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.Minted'] THEN 'PackMinted'
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.Revealed'] THEN 'PackRevealed'
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.Opened'] THEN 'PackOpened'
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.Deposit'] THEN 'PackDeposit'
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.Withdraw'] THEN 'PackWithdraw'
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.Burned'] THEN 'PackBurned'
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.NFT.ResourceDestroyed'] THEN 'PackBurned'
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.RevealRequest'] THEN 'PackRevealRequest'
                WHEN topics @> ARRAY['A.0b2a3299cc857e29.PackNFT.OpenRequest'] THEN 'PackOpenRequest'
                ELSE NULL
            END AS event_type,

            -- Pack ID
            (NULLIF(TRIM(evt.data->>'id'), ''))::bigint AS pack_id,

            -- Distribution ID (only in Minted)
            (NULLIF(TRIM(evt.data->>'distId'), ''))::bigint AS dist_id,

            -- Hash (only in Minted)
            evt.data->>'hash' AS hash_value,

            -- Salt (only in Revealed)
            evt.data->>'salt' AS salt,

            -- NFTs string (only in Revealed)
            evt.data->>'nfts' AS nfts_raw,

            -- Owner address
            COALESCE(evt.data->>'to', evt.data->>'from') AS owner_address,

            -- Open request flag
            evt.data->>'openRequest' AS open_request,

            evt.data AS raw_data

        FROM flow_raw_events AS evt
        WHERE evt.block_timestamp >= overlap_ts
          AND EXISTS (
              SELECT 1 FROM unnest(evt.topics) AS topic
              WHERE topic LIKE 'A.0b2a3299cc857e29.PackNFT.%'
          )
    )
    SELECT *
    FROM ExtractedEvents
    WHERE event_type IS NOT NULL
      AND pack_id IS NOT NULL;
END;
$$ LANGUAGE plpgsql;
