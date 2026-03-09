-- refresh.sql — Orchestrates all derived table refreshes in dependency order
--
-- Run after the indexer has caught up with the chain head.
-- Each layer depends on the previous layer's tables being up-to-date.
--
-- Usage:
--   psql vaultopolis_events < sql/refresh.sql
--   -- or from a cron job / scheduled function
--
-- Dependency graph:
--
--   Layer 0: flow_raw_events (populated by indexer/index.cjs)
--       │
--   Layer 1: Event extraction (from flow_raw_events → product event tables)
--       ├── refresh_topshot_events()
--       ├── refresh_allday_events()
--       ├── refresh_pinnacle_events()
--       └── refresh_packnft_events()
--       │
--   Layer 2: Derived tables (metadata, sales, listings, offers, supply)
--       ├── refresh_topshot_tables()
--       ├── refresh_allday_tables()
--       ├── refresh_pinnacle_tables()
--       └── refresh_packnft_tables()
--       │
--   Layer 3: Edition market data (floor prices, ASP, scoring, estimated value)
--       ├── refresh_topshot_edition_market_data()
--       ├── refresh_allday_edition_market_data()
--       ├── refresh_pinnacle_edition_market_data()
--       └── refresh_packnft_analytics()  ← depends on topshot_edition_market_data
--

-- ══════════════════════════════════════════════════════════════
-- Master refresh function — calls everything in order
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION refresh_all_derived_tables()
RETURNS void AS $$
DECLARE
    t0 TIMESTAMPTZ;
    layer_start TIMESTAMPTZ;
BEGIN
    t0 := clock_timestamp();

    -- ── Layer 1: Event extraction ─────────────────────────────
    RAISE NOTICE 'Layer 1: Extracting events from flow_raw_events...';
    layer_start := clock_timestamp();

    PERFORM refresh_topshot_events();
    RAISE NOTICE '  topshot_events refreshed (% ms)', EXTRACT(MILLISECOND FROM clock_timestamp() - layer_start)::int;

    PERFORM refresh_allday_events();
    RAISE NOTICE '  allday_events refreshed';

    PERFORM refresh_pinnacle_events();
    RAISE NOTICE '  pinnacle_events refreshed';

    PERFORM refresh_packnft_events();
    RAISE NOTICE '  packnft_events refreshed';

    RAISE NOTICE 'Layer 1 complete (% ms)', EXTRACT(MILLISECOND FROM clock_timestamp() - layer_start)::int;

    -- ── Layer 2: Derived tables ───────────────────────────────
    RAISE NOTICE 'Layer 2: Building derived tables...';
    layer_start := clock_timestamp();

    PERFORM refresh_topshot_tables();
    RAISE NOTICE '  topshot tables refreshed';

    PERFORM refresh_allday_tables();
    RAISE NOTICE '  allday tables refreshed';

    PERFORM refresh_pinnacle_tables();
    RAISE NOTICE '  pinnacle tables refreshed';

    PERFORM refresh_packnft_tables();
    RAISE NOTICE '  packnft tables refreshed';

    RAISE NOTICE 'Layer 2 complete (% ms)', EXTRACT(MILLISECOND FROM clock_timestamp() - layer_start)::int;

    -- ── Layer 3: Edition market data ──────────────────────────
    RAISE NOTICE 'Layer 3: Computing edition market data...';
    layer_start := clock_timestamp();

    PERFORM refresh_topshot_edition_market_data();
    RAISE NOTICE '  topshot edition market data refreshed';

    PERFORM refresh_allday_edition_market_data();
    RAISE NOTICE '  allday edition market data refreshed';

    PERFORM refresh_pinnacle_edition_market_data();
    RAISE NOTICE '  pinnacle edition market data refreshed';

    -- PackNFT analytics depends on topshot_edition_market_data (for EV values)
    PERFORM refresh_packnft_analytics();
    RAISE NOTICE '  packnft analytics refreshed';

    RAISE NOTICE 'Layer 3 complete (% ms)', EXTRACT(MILLISECOND FROM clock_timestamp() - layer_start)::int;

    RAISE NOTICE 'All derived tables refreshed in % ms', EXTRACT(MILLISECOND FROM clock_timestamp() - t0)::int;
END;
$$ LANGUAGE plpgsql;

-- ══════════════════════════════════════════════════════════════
-- Convenience: Run a single layer
-- ══════════════════════════════════════════════════════════════

CREATE OR REPLACE FUNCTION refresh_layer1_events()
RETURNS void AS $$
BEGIN
    PERFORM refresh_topshot_events();
    PERFORM refresh_allday_events();
    PERFORM refresh_pinnacle_events();
    PERFORM refresh_packnft_events();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_layer2_tables()
RETURNS void AS $$
BEGIN
    PERFORM refresh_topshot_tables();
    PERFORM refresh_allday_tables();
    PERFORM refresh_pinnacle_tables();
    PERFORM refresh_packnft_tables();
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION refresh_layer3_market_data()
RETURNS void AS $$
BEGIN
    PERFORM refresh_topshot_edition_market_data();
    PERFORM refresh_allday_edition_market_data();
    PERFORM refresh_pinnacle_edition_market_data();
    PERFORM refresh_packnft_analytics();
END;
$$ LANGUAGE plpgsql;

-- ══════════════════════════════════════════════════════════════
-- Run the full refresh now (uncomment to execute on import)
-- ══════════════════════════════════════════════════════════════
-- SELECT refresh_all_derived_tables();
