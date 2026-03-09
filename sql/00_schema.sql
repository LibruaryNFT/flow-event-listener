-- 00_schema.sql — TimescaleDB schema for flow_raw_events
--
-- Run this on the dedicated Hetzner server after installing PostgreSQL 16 + TimescaleDB.
-- Database: vaultopolis_events
--
-- Usage:
--   createdb vaultopolis_events
--   psql vaultopolis_events < sql/00_schema.sql

-- ══════════════════════════════════════════════════════════════
-- Extensions
-- ══════════════════════════════════════════════════════════════

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ══════════════════════════════════════════════════════════════
-- Core event table — mirrors BigQuery flow.flow_raw_events
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS flow_raw_events (
    block_height     BIGINT NOT NULL,
    block_timestamp  TIMESTAMPTZ NOT NULL,
    transaction_hash TEXT NOT NULL,
    log_index        INTEGER NOT NULL,
    topics           TEXT[] NOT NULL,      -- array of event type strings
    data             JSONB,               -- event payload

    -- TimescaleDB requires the partition column in the primary key
    PRIMARY KEY (block_timestamp, block_height, transaction_hash, log_index)
);

-- Convert to hypertable (auto-partitions by time)
-- chunk_time_interval: 1 month balances chunk size vs count
SELECT create_hypertable('flow_raw_events', 'block_timestamp',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- ══════════════════════════════════════════════════════════════
-- Compression policy — compress chunks older than 3 months
-- Reduces storage by 90-95% on historical data
-- ══════════════════════════════════════════════════════════════

ALTER TABLE flow_raw_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'block_height',
    timescaledb.compress_orderby = 'block_timestamp DESC'
);

SELECT add_compression_policy('flow_raw_events', INTERVAL '3 months',
    if_not_exists => TRUE
);

-- ══════════════════════════════════════════════════════════════
-- Indexes for common query patterns
-- ══════════════════════════════════════════════════════════════

-- Block height lookups (for gap detection, range queries)
CREATE INDEX IF NOT EXISTS idx_raw_events_block
    ON flow_raw_events (block_height);

-- Topic filtering (GIN index for array containment queries)
-- e.g., WHERE topics @> ARRAY['A.0b2a3299cc857e29.TopShot.MomentMinted']
CREATE INDEX IF NOT EXISTS idx_raw_events_topics
    ON flow_raw_events USING GIN (topics);

-- Transaction hash lookups
CREATE INDEX IF NOT EXISTS idx_raw_events_tx
    ON flow_raw_events (transaction_hash);

-- ══════════════════════════════════════════════════════════════
-- Block sync log — tracks which blocks have been processed
-- Used by gap-detector to find missing block ranges
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS block_sync_log (
    block_height BIGINT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    event_count  INTEGER NOT NULL DEFAULT 0
);

-- Index for finding the latest processed block quickly
CREATE INDEX IF NOT EXISTS idx_sync_log_processed
    ON block_sync_log (processed_at DESC);

-- ══════════════════════════════════════════════════════════════
-- Indexer state — key-value store for operational state
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS indexer_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ══════════════════════════════════════════════════════════════
-- Verify setup
-- ══════════════════════════════════════════════════════════════

-- Show hypertable info
SELECT * FROM timescaledb_information.hypertables
WHERE hypertable_name = 'flow_raw_events';

-- Show compression settings
SELECT * FROM timescaledb_information.compression_settings
WHERE hypertable_name = 'flow_raw_events';
