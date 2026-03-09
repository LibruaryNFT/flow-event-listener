/* index.cjs — Flow blockchain event indexer
 *
 * Polls Flow Access Node for events from configured contracts,
 * writes to PostgreSQL/TimescaleDB. Designed to run alongside
 * monitor.cjs (which continues writing TSHOT events to MongoDB).
 *
 * Usage: node indexer/index.cjs
 * Env:   FLOW_ACCESS_NODE, POSTGRES_EVENTS_URI
 */

const http = require("http");
const fcl = require("@onflow/fcl");
require("dotenv").config();
const logger = require("../logger.cjs");
const db = require("./db.cjs");
const gapDetector = require("./gap-detector.cjs");
const config = require("./config.cjs");

/* ───────── env vars ───────── */
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const POSTGRES_EVENTS_URI = process.env.POSTGRES_EVENTS_URI;
if (!FLOW_ACCESS_NODE || !POSTGRES_EVENTS_URI) {
  logger.fatal("FLOW_ACCESS_NODE or POSTGRES_EVENTS_URI missing");
  process.exit(1);
}

/* ───────── state ───────── */
let mode = "STARTING";
let param = { ...config.LIVE };
let lastProcessedBlock = 0;
let currentNodeIdx = 0;
let consecutiveNodeErrors = 0;
const startTime = Date.now();

/* ───────── retry wrapper ───────── */
async function retry(label, fn) {
  for (let i = 0; i < config.RETRIES; i++) {
    try {
      const t0 = Date.now();
      const res = await fn();
      logger.debug({ label, durationMs: Date.now() - t0 }, "RPC ok");
      consecutiveNodeErrors = 0;
      return res;
    } catch (err) {
      const wait = config.BACKOFF_MS * 2 ** i + Math.random() * config.JITTER_MS;
      logger.warn(
        { label, attempt: i + 1, err: err.message.trim(), backoffMs: Math.round(wait) },
        "RPC failed"
      );

      // Node failover after 3 consecutive errors
      consecutiveNodeErrors++;
      if (consecutiveNodeErrors >= 3 && config.ACCESS_NODES.length > 1) {
        currentNodeIdx = (currentNodeIdx + 1) % config.ACCESS_NODES.length;
        const newNode = config.ACCESS_NODES[currentNodeIdx];
        logger.warn({ newNode, previousErrors: consecutiveNodeErrors }, "Failing over to backup node");
        await fcl.config().put("accessNode.api", newNode);
        consecutiveNodeErrors = 0;
        await db.setState("active_node", newNode);
      }

      if (i === config.RETRIES - 1) throw err;
      await new Promise((r) => setTimeout(r, wait));
    }
  }
}

/* ───────── Flow helpers ───────── */
async function getHead() {
  return retry("getBlock", () =>
    fcl
      .send([fcl.getBlock(true)]) // isSealed: true — only finalized blocks
      .then(fcl.decode)
      .then((b) => b.height)
  );
}

function fetchEvents(type, from, to) {
  return retry(`events:${type.split(".").pop()} ${from}-${to}`, () =>
    fcl.send([fcl.getEventsAtBlockHeightRange(type, from, to)]).then(fcl.decode)
  );
}

/**
 * Build the list of event type strings to query for.
 * Flow API requires exact event type names, not prefixes.
 * We query for known event types from each contract.
 *
 * NOTE: Flow's getEventsAtBlockHeightRange requires EXACT event type IDs.
 * We can't do prefix matching at the API level — we need to know the
 * specific event names. For prefix-matched contracts, we maintain a list
 * of known event types in the config and discover new ones via block scanning.
 */
function buildEventTypeList() {
  const types = [];

  // For prefix contracts, we need known event type suffixes
  // These come from the BigQuery analysis + contract ABI
  const KNOWN_EVENTS = {
    "A.0b2a3299cc857e29.TopShot": [
      "PlayCreated", "NewSeriesStarted", "SetCreated", "PlayAddedToSet",
      "PlayRetiredFromSet", "SetLocked", "MomentMinted", "Withdraw",
      "Deposit", "MomentDestroyed", "SubeditionCreated", "SubeditionAddedToMoment",
    ],
    "A.0b2a3299cc857e29.TopShotLocking": [
      "MomentLocked", "MomentUnlocked",
    ],
    "A.0b2a3299cc857e29.PackNFT": [
      "Minted", "Revealed", "Opened", "Deposit", "Withdraw", "Burned",
      "RevealRequest", "OpenRequest",
    ],
    "A.c1e4f4f4c4257510.Market": [
      "MomentListed", "MomentPurchased", "MomentWithdrawn",
    ],
    "A.c1e4f4f4c4257510.TopShotMarketV2": [
      "MomentListed", "MomentPurchased", "MomentWithdrawn",
    ],
    "A.c1e4f4f4c4257510.TopShotMarketV3": [
      "MomentListed", "MomentPurchased", "MomentWithdrawn",
    ],
    "A.e4cf4bdc1751c65d.AllDay": [
      "SeriesCreated", "SetCreated", "PlayCreated", "EditionCreated",
      "MomentNFTMinted", "MomentNFTBurned",
    ],
    "A.edf9df96c92f4595.Pinnacle": [
      "PinNFTMinted", "PinNFTBurned", "SeriesCreated", "SetCreated",
      "ShapeCreated", "EditionCreated", "EditionClosed", "EditionTypeCreated",
    ],
    "A.05b67ba314000b2d.TSHOTExchange": [
      "NFTToTSHOTSwapCompleted", "TSHOTToNFTSwapCompleted",
    ],
    // Filtered contracts — capture all event types, filter at storage time
    "A.b8ea91944fd51c43.Offers": [
      "OfferAvailable", "OfferCompleted", "OfferRemoved",
    ],
    "A.b8ea91944fd51c43.OffersV2": [
      "OfferAvailable", "OfferCompleted", "OfferRemoved",
    ],
    "A.4eb8a10cb9f87357.NFTStorefront": [
      "ListingAvailable", "ListingCompleted",
    ],
    "A.4eb8a10cb9f87357.NFTStorefrontV2": [
      "ListingAvailable", "ListingCompleted",
    ],
    "A.3cdbb3d569211ff3.NFTStorefrontV2": [
      "ListingAvailable", "ListingCompleted",
    ],
    "A.1d7e57aa55817448.NonFungibleToken": [
      "Deposited", "Withdrawn",
    ],
  };

  // Build fully qualified event type IDs
  for (const [contract, events] of Object.entries(KNOWN_EVENTS)) {
    for (const evt of events) {
      types.push(`${contract}.${evt}`);
    }
  }

  // Add exact events
  types.push(...config.EXACT_EVENTS);

  return types;
}

/**
 * Check if an event passes the NFT type filter for filtered contracts.
 * Returns true if the event should be stored.
 */
function passesFilter(event) {
  // Check if this event's contract is in the filtered list
  const eventContract = event.type.split(".").slice(0, 3).join(".");
  const filterDef = config.FILTERED_CONTRACTS.find(
    (f) => f.address === eventContract
  );

  // Not a filtered contract → always store
  if (!filterDef) return true;

  // Check the filter field in the event data
  const filterValue = String(event.data?.[filterDef.filterField] ?? "");
  return config.NFT_TYPE_PREFIXES.some((prefix) =>
    filterValue.startsWith(prefix)
  );
}

/**
 * Transform a Flow FCL event into our DB row format.
 */
function toDbRow(event) {
  return {
    blockHeight: event.blockHeight,
    blockTimestamp: event.blockTimestamp,
    transactionHash: event.transactionId,
    logIndex: event.eventIndex,
    topics: [event.type], // array of event type strings (matches BigQuery schema)
    data: event.data || {},
  };
}

/* ───────── health check server ───────── */
const HEALTH_PORT = Number(process.env.INDEXER_HEALTH_PORT || 8093);

http
  .createServer(async (req, res) => {
    if (req.url === "/health" && (req.method === "GET" || req.method === "HEAD")) {
      const pgHealthy = await db.isHealthy();
      const body = JSON.stringify({
        status: pgHealthy ? "healthy" : "degraded",
        service: "flow-indexer",
        uptime: Math.floor((Date.now() - startTime) / 1000),
        postgres: pgHealthy ? "connected" : "disconnected",
        mode,
        lastProcessedBlock,
        activeNode: config.ACCESS_NODES[currentNodeIdx],
        timestamp: new Date().toISOString(),
      });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(body);
    } else {
      res.writeHead(404);
      res.end("Not found");
    }
  })
  .listen(HEALTH_PORT, () => {
    logger.info({ port: HEALTH_PORT }, "Indexer health check listening");
  });

/* ───────── main loop ───────── */
async function main() {
  // Initialize
  const initialNode = process.env.FLOW_ACCESS_NODE || config.ACCESS_NODES[0];
  await fcl.config().put("accessNode.api", initialNode);
  db.init(POSTGRES_EVENTS_URI);

  // Check database connectivity
  const healthy = await db.isHealthy();
  if (!healthy) {
    logger.fatal("Cannot connect to PostgreSQL — check POSTGRES_EVENTS_URI");
    process.exit(1);
  }

  // Build event type list
  const eventTypes = buildEventTypeList();
  logger.info({ eventTypeCount: eventTypes.length }, "Event types configured");

  // Check for startup gap
  const head = await getHead();
  const gapInfo = await gapDetector.checkStartupGap(head);
  lastProcessedBlock = gapInfo.lastBlock;

  if (gapInfo.lastBlock === 0) {
    // First run — start from current block (historical data comes from BigQuery backfill)
    logger.info({ head }, "First run — starting from current chain height");
    await db.setLastProcessedBlock(head);
    lastProcessedBlock = head;
  }

  logger.info(
    { lastBlock: lastProcessedBlock, chainHead: head, gap: gapInfo.gap },
    "Indexer ready"
  );

  // Main polling loop
  async function loop() {
    let wait = param.DELAY;
    try {
      const head = await getHead();
      const from = lastProcessedBlock + 1;
      const lag = head - from;

      // Mode switching
      if (lag > config.BEHIND_THRESHOLD && mode !== "CATCH") {
        mode = "CATCH";
        param = { ...config.CATCH };
        logger.info({ lag }, "Switching to CATCH mode");
      } else if (lag <= config.BEHIND_THRESHOLD && mode !== "LIVE") {
        mode = "LIVE";
        param = { ...config.LIVE };
        logger.info({ lag }, "Switching to LIVE mode");
      }

      if (lag <= 0) {
        // Caught up — idle
        logger.debug({ head }, "Idle");
        wait = param.IDLE;
      } else {
        const to = Math.min(from + param.RANGE - 1, head);
        logger.info({ mode, from, to, lag, eventTypes: eventTypes.length }, "Processing slice");

        // Fetch events for all types in parallel (batched to avoid rate limits)
        const BATCH_SIZE = 10; // concurrent event type fetches
        const allEvents = [];

        for (let i = 0; i < eventTypes.length; i += BATCH_SIZE) {
          const batch = eventTypes.slice(i, i + BATCH_SIZE);
          const results = await Promise.all(
            batch.map((t) => fetchEvents(t, from, to).catch((err) => {
              logger.debug({ type: t, err: err.message }, "Event fetch failed for type");
              return [];
            }))
          );
          allEvents.push(...results.flat());

          // Small delay between batches to respect rate limits
          if (i + BATCH_SIZE < eventTypes.length && mode === "CATCH") {
            await new Promise((r) => setTimeout(r, param.DELAY));
          }
        }

        // Filter events (for filtered contracts)
        const filtered = allEvents.filter(passesFilter);

        if (filtered.length) {
          // Transform to DB format and insert
          const rows = filtered.map(toDbRow);
          const inserted = await db.insertEvents(rows);
          logger.info(
            { total: allEvents.length, filtered: filtered.length, inserted, from, to },
            "Events stored"
          );
        } else {
          logger.debug({ from, to }, "No matching events in slice");
        }

        // Update tracking
        await db.setLastProcessedBlock(to);
        await db.logBlockSyncRange(from, to, filtered.length);
        lastProcessedBlock = to;
      }
    } catch (err) {
      if (err.message.includes("is currently being indexed") ||
          err.message.includes("not available")) {
        // Chain might be halted (spork transition)
        logger.warn({ err: err.message }, "Chain may be halted — entering WAITING state");
        mode = "WAITING";
        wait = Math.min((wait || param.IDLE) * 2, 300000); // exponential backoff, 5min cap
      } else {
        logger.error({ err: err.message }, "Slice aborted");
        wait = param.IDLE * 2;
      }
    } finally {
      setTimeout(loop, wait);
    }
  }

  loop();
}

main().catch(async (e) => {
  logger.fatal({ err: e.message }, "Fatal error");
  await db.close();
  process.exit(1);
});

/* ───────── graceful shutdown ───────── */
async function shutdown() {
  logger.info("Shutting down...");
  await db.close();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
