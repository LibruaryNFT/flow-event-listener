/**
 * api.cjs — Express API server for market data (EX44 PostgreSQL)
 *
 * Serves the same endpoints as the Heroku Express backend for market data:
 *   GET /topshot-market-data
 *   GET /allday-market-data
 *   GET /pinnacle-market-data
 *   GET /api/health
 *
 * Reads from PostgreSQL tables populated by flow-refresh.sh (6-hourly).
 * Designed to run in parallel with Heroku until fully validated.
 *
 * Usage:
 *   POSTGRES_EVENTS_URI=postgres://... node api.cjs
 *   Or: systemd service (flow-api.service)
 */

"use strict";

const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");
const logger = require("./logger.cjs");

// ─── Config ──────────────────────────────────────────────────────────────────

const PORT = parseInt(process.env.API_PORT || "8080", 10);
const CACHE_TTL_MS = 5 * 1000; // 5 seconds — realtime refresh runs inline with indexer

const CORS_ORIGINS = [
  "https://vaultopolis.com",
  "https://www.vaultopolis.com",
  "http://localhost:3000",
  "http://localhost:3001",
  "http://localhost:5173",
];

// ─── PostgreSQL ──────────────────────────────────────────────────────────────

const pool = new Pool({
  connectionString:
    process.env.POSTGRES_EVENTS_URI || "postgres:///flow_events",
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

pool.on("error", (err) => {
  logger.error({ err: err.message }, "Unexpected PostgreSQL pool error");
});

// ─── In-memory cache ─────────────────────────────────────────────────────────

const cache = new Map();

function getCached(key) {
  const entry = cache.get(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > CACHE_TTL_MS) {
    cache.delete(key);
    return null;
  }
  return entry.data;
}

function setCache(key, data) {
  cache.set(key, { data, ts: Date.now() });
}

// ─── Express App ─────────────────────────────────────────────────────────────

const app = express();

app.use(
  cors({
    origin: (origin, cb) => {
      if (!origin || CORS_ORIGINS.includes(origin)) return cb(null, true);
      cb(null, false);
    },
  })
);

app.use(express.json());

// ─── Rate limiting (simple in-memory) ────────────────────────────────────────

const rateLimits = new Map();
const RATE_WINDOW_MS = 15 * 60 * 1000;
const RATE_MAX = 300;

function rateLimit(req, res, next) {
  const ip = req.ip;
  const now = Date.now();
  let entry = rateLimits.get(ip);
  if (!entry || now - entry.windowStart > RATE_WINDOW_MS) {
    entry = { windowStart: now, count: 0 };
    rateLimits.set(ip, entry);
  }
  entry.count++;
  if (entry.count > RATE_MAX) {
    return res.status(429).json({ error: "Too many requests" });
  }
  next();
}

// Clean up rate limit entries periodically
setInterval(() => {
  const now = Date.now();
  for (const [ip, entry] of rateLimits) {
    if (now - entry.windowStart > RATE_WINDOW_MS) rateLimits.delete(ip);
  }
}, 60000);

// ─── Health endpoint ─────────────────────────────────────────────────────────

app.get("/api/health", async (_req, res) => {
  try {
    const result = await pool.query("SELECT 1 AS ok");
    res.json({
      status: "ok",
      uptime: process.uptime(),
      postgres: result.rows[0].ok === 1 ? "connected" : "error",
      timestamp: new Date().toISOString(),
      version: "1.0.0",
      cache_entries: cache.size,
    });
  } catch (err) {
    res.status(500).json({
      status: "error",
      postgres: "disconnected",
      error: err.message,
    });
  }
});

// ─── Market Data Query Builder ───────────────────────────────────────────────

/**
 * Build a parameterized SELECT query for a market data table.
 * Mirrors the Heroku Express backend filtering/sorting/pagination exactly.
 */
function buildMarketDataQuery(table, query, config) {
  const conditions = [];
  const params = [];
  let paramIdx = 1;

  // Apply filters from config
  for (const filter of config.filters) {
    const val = query[filter.param] ?? query[filter.alias];
    if (val === undefined || val === null || val === "") continue;

    if (filter.type === "exact") {
      conditions.push(`${filter.column} = $${paramIdx}`);
      params.push(filter.cast === "number" ? Number(val) : val);
      paramIdx++;
    } else if (filter.type === "ilike") {
      conditions.push(`${filter.column} ILIKE $${paramIdx}`);
      params.push(`%${val}%`);
      paramIdx++;
    } else if (filter.type === "gte") {
      conditions.push(`${filter.column} >= $${paramIdx}`);
      params.push(Number(val));
      paramIdx++;
    } else if (filter.type === "boolean") {
      conditions.push(`${filter.column} = $${paramIdx}`);
      params.push(val === "true");
      paramIdx++;
    } else if (filter.type === "csv") {
      // Comma-separated values (e.g., tier=common,fandom)
      const values = val
        .split(",")
        .map((v) => v.trim().toLowerCase())
        .filter(Boolean);
      if (values.length > 0) {
        const placeholders = values.map((_, i) => `$${paramIdx + i}`);
        conditions.push(
          `LOWER(${filter.column}) IN (${placeholders.join(",")})`
        );
        values.forEach((v) => params.push(v));
        paramIdx += values.length;
      }
    }
  }

  const whereClause =
    conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

  // Sort
  const sortField = config.validSortFields.includes(query.sort)
    ? query.sort
    : config.defaultSort;

  // Map backward-compat field names
  const sortColumn = config.sortAliases?.[sortField] || sortField;

  const sortOrder =
    query.order?.toLowerCase() === "asc"
      ? "ASC"
      : query.order?.toLowerCase() === "desc"
        ? "DESC"
        : config.defaultOrder || "DESC";

  // Pagination
  const limit = Math.min(Math.max(parseInt(query.limit) || 50, 1), 20000);
  const offset = Math.max(parseInt(query.offset) || 0, 0);

  // Count query
  const countSql = `SELECT COUNT(*) AS total FROM ${table} ${whereClause}`;

  // Data query — NULLS LAST for numeric sorts so zero/null rows sink
  const dataSql = `SELECT * FROM ${table} ${whereClause} ORDER BY ${sortColumn} ${sortOrder} NULLS LAST LIMIT $${paramIdx} OFFSET $${paramIdx + 1}`;
  const dataParams = [...params, limit, offset];

  return { countSql, dataSql, countParams: params, dataParams, limit, offset };
}

// ─── TopShot Config ──────────────────────────────────────────────────────────

const TOPSHOT_CONFIG = {
  table: "topshot_edition_market_data",
  defaultSort: "grail_score",
  defaultOrder: "DESC",
  filters: [
    {
      param: "edition_id",
      alias: "editionID",
      column: "edition_id",
      type: "exact",
    },
    {
      param: "player_name",
      alias: "FullName",
      column: "player_name",
      type: "ilike",
    },
    { param: "series", column: "series", type: "exact", cast: "number" },
    {
      param: "has_parallels",
      column: "has_parallels",
      type: "boolean",
    },
    {
      param: "min_grail_score",
      column: "grail_score",
      type: "gte",
    },
    {
      param: "min_floor_price",
      column: "floor_price",
      type: "gte",
    },
    {
      param: "min_estimated_value",
      column: "estimated_value",
      type: "gte",
    },
  ],
  validSortFields: [
    "grail_score",
    "floor_price",
    "existing_supply",
    "player_name",
    "asp_180d",
    "estimated_value",
    "liquidity_score",
    "sales_activity_score",
    "offer_frequency_score",
    "market_freshness_score",
    "sales_momentum_score",
    "asp_lifetime",
    "asp_dapper",
    "total_sales_7d",
    "total_sales_30d",
    "total_sales_180d",
    "total_sales_lifetime",
    "asp_7d",
    "asp_30d",
    "total_volume_7d",
    "total_volume_30d",
    "total_volume_180d",
    "total_volume_lifetime",
    "last_sale_price",
    "price_change_pct",
    "total_edition_offer_volume",
    "highest_edition_offer",
    "highest_moment_offer",
    "price_trend_7d_vs_30d",
    "price_trend_30d_vs_180d",
    "total_listings",
    "total_listings_dapper",
    "total_listings_flowty",
    "floor_price_dapper",
    "floor_price_flowty",
    "floor_price_any",
    "edition_offer_count",
    "avg_edition_offer",
    "lowest_edition_offer",
    "unique_edition_bidders",
    "days_since_last_sale",
    "sell_through_rate",
    "holder_diversity_pct",
    "market_cap",
    // backward compat aliases
    "total_offer_volume",
    "offer_count",
    "average_offer_amount",
    "lowest_offer",
    "unique_bidders",
  ],
  sortAliases: {
    total_offer_volume: "total_edition_offer_volume",
    offer_count: "edition_offer_count",
    average_offer_amount: "avg_edition_offer",
    lowest_offer: "lowest_edition_offer",
    unique_bidders: "unique_edition_bidders",
  },
};

// ─── AllDay Config ───────────────────────────────────────────────────────────

const ALLDAY_CONFIG = {
  table: "allday_edition_market_data",
  defaultSort: "grail_score",
  defaultOrder: "DESC",
  filters: [
    {
      param: "edition_id",
      alias: "editionID",
      column: "edition_id",
      type: "exact",
      cast: "number",
    },
    {
      param: "set_id",
      alias: "setID",
      column: "set_id",
      type: "exact",
      cast: "number",
    },
    {
      param: "play_id",
      alias: "playID",
      column: "play_id",
      type: "exact",
      cast: "number",
    },
    {
      param: "series_id",
      alias: "seriesID",
      column: "series_id",
      type: "exact",
      cast: "number",
    },
    { param: "series", column: "series_id", type: "exact", cast: "number" },
    { param: "tier", column: "tier", type: "csv" },
    {
      param: "player_name",
      alias: "FullName",
      column: "player_name",
      type: "ilike",
    },
    { param: "min_grail_score", column: "grail_score", type: "gte" },
    { param: "min_floor_price", column: "floor_price", type: "gte" },
    { param: "min_estimated_value", column: "estimated_value", type: "gte" },
  ],
  validSortFields: [
    "grail_score",
    "floor_price",
    "mint_count",
    "player_name",
    "listed_count",
    "asp_7d",
    "asp_30d",
    "asp_180d",
    "asp_lifetime",
    "total_sales_7d",
    "total_sales_30d",
    "total_sales_180d",
    "total_sales_lifetime",
    "total_volume_7d",
    "total_volume_30d",
    "total_volume_180d",
    "total_volume_lifetime",
    "last_sale_price",
    "days_since_last_sale",
    "estimated_value",
    "liquidity_score",
    "sales_activity_score",
    "offer_frequency_score",
    "market_freshness_score",
    "sales_momentum_score",
    "offer_depth_score",
    "daily_volume_delta",
    "price_change_pct",
    "price_trend_7d_vs_30d",
    "price_trend_30d_vs_180d",
    "highest_offer",
    "edition_offer_count",
    "total_offer_volume",
    "unique_bidders",
    "market_cap",
    "sell_through_rate",
    "floating_supply_pct",
    "liquidity_spread_pct",
    "median_listing_price",
    "listings_at_floor",
    "edition_id",
    "set_id",
    "series_id",
  ],
  sortAliases: {},
};

// ─── Pinnacle Config ─────────────────────────────────────────────────────────

const PINNACLE_CONFIG = {
  table: "pinnacle_edition_market_data",
  defaultSort: "grail_score",
  defaultOrder: "DESC",
  filters: [
    {
      param: "edition_id",
      alias: "editionID",
      column: "edition_id",
      type: "exact",
      cast: "number",
    },
    {
      param: "set_id",
      alias: "setID",
      column: "set_id",
      type: "exact",
      cast: "number",
    },
    {
      param: "series_id",
      alias: "seriesID",
      column: "series_id",
      type: "exact",
      cast: "number",
    },
    { param: "series", column: "series_id", type: "exact", cast: "number" },
    {
      param: "edition_type",
      column: "edition_type_name",
      type: "ilike",
    },
    {
      param: "character_name",
      column: "character_name",
      type: "ilike",
    },
    { param: "is_chaser", column: "is_chaser", type: "boolean" },
    { param: "min_grail_score", column: "grail_score", type: "gte" },
    { param: "min_floor_price", column: "floor_price", type: "gte" },
    { param: "min_estimated_value", column: "estimated_value", type: "gte" },
  ],
  validSortFields: [
    "grail_score",
    "floor_price",
    "max_mint_size",
    "character_name",
    "listed_count",
    "asp_7d",
    "asp_30d",
    "asp_180d",
    "asp_lifetime",
    "asp_dapper",
    "total_sales_7d",
    "total_sales_30d",
    "total_sales_180d",
    "total_sales_lifetime",
    "total_volume_7d",
    "total_volume_30d",
    "total_volume_180d",
    "total_volume_lifetime",
    "last_sale_price",
    "days_since_last_sale",
    "estimated_value",
    "liquidity_score",
    "sales_activity_score",
    "market_freshness_score",
    "sales_momentum_score",
    "daily_volume_delta",
    "price_change_pct",
    "price_trend_7d_vs_30d",
    "price_trend_30d_vs_180d",
    "market_cap",
    "sell_through_rate",
    "floating_supply_pct",
    "edition_id",
    "set_id",
    "series_id",
  ],
  sortAliases: {},
};

// ─── Generic market data handler ─────────────────────────────────────────────

function marketDataHandler(config) {
  return async (req, res) => {
    try {
      // Cache key from full query string
      const cacheKey = `${config.table}:${req.originalUrl}`;
      const cached = getCached(cacheKey);
      if (cached) return res.json(cached);

      const { countSql, dataSql, countParams, dataParams, limit, offset } =
        buildMarketDataQuery(config.table, req.query, config);

      const [countResult, dataResult] = await Promise.all([
        pool.query(countSql, countParams),
        pool.query(dataSql, dataParams),
      ]);

      const total = parseInt(countResult.rows[0].total, 10);

      // Convert numeric strings and format response to match Heroku output
      const editions = dataResult.rows.map(formatRow);

      const response = {
        editions,
        pagination: {
          limit,
          offset,
          total,
          returned: editions.length,
          has_more: offset + editions.length < total,
        },
        filters_applied: req.query,
      };

      setCache(cacheKey, response);
      res.json(response);
    } catch (err) {
      logger.error({ err: err.message, table: config.table }, "Query error");
      res.status(500).json({ error: "Internal server error" });
    }
  };
}

/**
 * Format a PostgreSQL row to match the JSON shape the frontend expects.
 * PG returns numeric columns as strings; convert them to numbers.
 * Also convert timestamps to ISO strings.
 */
function formatRow(row) {
  const formatted = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null) {
      formatted[key] = null;
    } else if (value instanceof Date) {
      formatted[key] = value.toISOString();
    } else if (typeof value === "string" && /^-?\d+(\.\d+)?$/.test(value)) {
      formatted[key] = Number(value);
    } else {
      formatted[key] = value;
    }
  }
  return formatted;
}

// ─── Routes ──────────────────────────────────────────────────────────────────

app.get("/topshot-market-data", rateLimit, marketDataHandler(TOPSHOT_CONFIG));
app.get("/allday-market-data", rateLimit, marketDataHandler(ALLDAY_CONFIG));
app.get(
  "/pinnacle-market-data",
  rateLimit,
  marketDataHandler(PINNACLE_CONFIG)
);

// ─── TSHOT Swap Events & Wallet Stats ────────────────────────────────────────
// Derived directly from flow_raw_events — replaces MongoDB monitor.cjs pipeline

const DEPOSIT_EVT = "A.05b67ba314000b2d.TSHOTExchange.NFTToTSHOTSwapCompleted";
const WITHDRAW_EVT = "A.05b67ba314000b2d.TSHOTExchange.TSHOTToNFTSwapCompleted";

// GET /wallet-events/:wallet — paginated swap history for a wallet
app.get("/wallet-events/:wallet", rateLimit, async (req, res) => {
  try {
    const wallet = req.params.wallet;
    if (!wallet) return res.status(400).json({ error: "wallet address required" });

    const cacheKey = `wallet-events:${wallet}:${req.originalUrl}`;
    const cached = getCached(cacheKey);
    if (cached) return res.json(cached);

    // TSHOTExchange deployed 2026-03-09 — hard floor so TimescaleDB only scans recent chunks
    const TSHOT_LAUNCH = "2026-03-09T00:00:00Z";
    const conditions = ["topics[1] IN ($1, $2)", "data->>'payer' = $3", "block_timestamp >= $4"];
    const params = [DEPOSIT_EVT, WITHDRAW_EVT, wallet, TSHOT_LAUNCH];
    let paramIdx = 5;

    // Optional type filter
    if (req.query.type === "deposit") {
      conditions.push(`topics[1] = $${paramIdx++}`);
      params.push(DEPOSIT_EVT);
    } else if (req.query.type === "withdraw") {
      conditions.push(`topics[1] = $${paramIdx++}`);
      params.push(WITHDRAW_EVT);
    }

    // Optional date range
    if (req.query.from) {
      conditions.push(`block_timestamp >= $${paramIdx++}`);
      params.push(new Date(req.query.from));
    }
    if (req.query.to) {
      conditions.push(`block_timestamp <= $${paramIdx++}`);
      params.push(new Date(req.query.to));
    }

    const where = `WHERE ${conditions.join(" AND ")}`;
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 25));
    const offset = (page - 1) * limit;

    const [countResult, dataResult] = await Promise.all([
      pool.query(`SELECT COUNT(*) AS total FROM flow_raw_events ${where}`, params),
      pool.query(
        `SELECT block_height AS "blockHeight", block_timestamp AS "blockTimestamp",
                transaction_hash AS "transactionId", topics[1] AS type,
                data->>'numNFTs' AS "numNFTs", data->>'amountBurned' AS "amountBurned",
                data->>'receiptID' AS "receiptID"
         FROM flow_raw_events ${where}
         ORDER BY block_height DESC
         LIMIT $${paramIdx} OFFSET $${paramIdx + 1}`,
        [...params, limit, offset]
      ),
    ]);

    const totalItems = parseInt(countResult.rows[0].total);
    const response = {
      totalItems,
      totalPages: Math.ceil(totalItems / limit),
      currentPage: page,
      items: dataResult.rows,
    };

    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    logger.error({ err: err.message }, "Error in /wallet-events");
    res.status(500).json({ error: "Unable to fetch events" });
  }
});

// GET /wallet-stats — aggregated wallet stats (leaderboard)
// GET /wallet-stats/:wallet — stats for a specific wallet
// Reads from tshot_wallet_stats materialized view (refreshed by flow-refresh.sh)
app.get("/wallet-stats/:wallet?", rateLimit, async (req, res) => {
  try {
    const wallet = req.params.wallet || req.query.wallet;
    const cacheKey = `wallet-stats:${req.originalUrl}`;
    const cached = getCached(cacheKey);
    if (cached) return res.json(cached);

    const conditions = [];
    const params = [];
    let paramIdx = 1;

    if (wallet) {
      conditions.push(`wallet_address = $${paramIdx++}`);
      params.push(wallet);
    }

    if (req.query.minNet) {
      const n = parseInt(req.query.minNet);
      if (!isNaN(n)) {
        conditions.push(`net >= $${paramIdx++}`);
        params.push(n);
      }
    }

    const where = conditions.length ? `WHERE ${conditions.join(" AND ")}` : "";

    // Pagination
    const page = Math.max(1, parseInt(req.query.page) || 1);
    const limit = Math.min(100, Math.max(1, parseInt(req.query.limit) || 20));

    // Sort
    const sortMap = {
      netDesc: "net DESC",
      netAsc: "net ASC",
      depositsDesc: "deposits DESC",
      withdrawDesc: "withdrawals DESC",
    };
    const sortClause = sortMap[req.query.sort] || "net DESC";

    const countResult = await pool.query(
      `SELECT COUNT(*) AS total FROM tshot_wallet_stats ${where}`, params
    );
    const totalItems = parseInt(countResult.rows[0].total);

    const dataResult = await pool.query(
      `SELECT wallet_address AS _id,
              deposits AS "NFTToTSHOTSwapCompleted",
              withdrawals AS "TSHOTToNFTSwapCompleted",
              net,
              first_event AS "firstEvent",
              last_event AS "lastEvent"
       FROM tshot_wallet_stats ${where}
       ORDER BY ${sortClause}
       LIMIT $${paramIdx} OFFSET $${paramIdx + 1}`,
      [...params, limit, (page - 1) * limit]
    );

    const response = {
      totalItems,
      totalPages: Math.ceil(totalItems / limit),
      currentPage: page,
      items: dataResult.rows,
    };

    setCache(cacheKey, response);
    res.json(response);
  } catch (err) {
    logger.error({ err: err.message }, "Error in /wallet-stats");
    res.status(500).json({ error: "Unable to fetch wallet stats" });
  }
});

// GET /tshot-daily-stats — daily aggregation of TSHOT swap activity
// Reads from tshot_daily_stats materialized view (refreshed by flow-refresh.sh)
app.get("/tshot-daily-stats", rateLimit, async (req, res) => {
  try {
    const cacheKey = "tshot-daily-stats";
    const cached = getCached(cacheKey);
    if (cached) return res.json(cached);

    const result = await pool.query(`
      SELECT date,
             daily_incoming AS "dailyIncoming",
             daily_outgoing AS "dailyOutgoing",
             daily_total_exchanged AS "dailyTotalExchanged",
             daily_unique_wallets AS "dailyUniqueWallets"
      FROM tshot_daily_stats
      ORDER BY date ASC
    `);

    setCache(cacheKey, result.rows);
    res.json(result.rows);
  } catch (err) {
    logger.error({ err: err.message }, "Error in /tshot-daily-stats");
    res.status(500).json({ error: "Unable to fetch TSHOT daily stats" });
  }
});

// Root
app.get("/", (_req, res) => {
  res.send("Vaultopolis Market Data API (EX44)");
});

// ─── Start ───────────────────────────────────────────────────────────────────

app.listen(PORT, "0.0.0.0", () => {
  logger.info({ port: PORT }, "Market data API started");
});

// Graceful shutdown
process.on("SIGTERM", async () => {
  logger.info("SIGTERM received, shutting down...");
  await pool.end();
  process.exit(0);
});

process.on("SIGINT", async () => {
  logger.info("SIGINT received, shutting down...");
  await pool.end();
  process.exit(0);
});
