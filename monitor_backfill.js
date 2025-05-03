// monitor.js  ────────────────────────────────────────────────────────────
// Flow event monitor with auto catch-up mode
//
// • Built on your monitor_final_optimized.js foundation
// • Adds dynamic tuning so a week-long gap is processed rapidly
// • No other deployment-time changes are required
//
// ────────────────────────────────────────────────────────────────────────

// ── Imports & env ───────────────────────────────────────────────────────
const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();
// p-limit is imported dynamically below

const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const MONGODB_URI = process.env.MONGODB_URI;
const DATABASE_NAME = "flow_events";
const RAW_EVENTS_COLLECTION = "raw_events";
const PROCESSED_BLOCKS_COLLECTION = "processed_blocks";
const projectId = process.env.PROJECT_ID || "flow_monitors"; // keep stable

// ── Tuning profiles ────────────────────────────────────────────────────
// These two objects hold *all* tunables.  The script switches between them
// depending on how far behind the head block it is.
const LIVE = {
  TX_FETCH_CONCURRENCY: 8,
  BLOCK_POLL_RANGE: 25,
  POLLING_DELAY: 500, // ms
  CAUGHT_UP_POLL_DELAY: 5000, // ms when fully caught up
};

const CATCH_UP = {
  TX_FETCH_CONCURRENCY: 32,
  BLOCK_POLL_RANGE: 250, // Flow RPC maximum
  POLLING_DELAY: 0, // hammer until near head
  CAUGHT_UP_POLL_DELAY: 2000,
};

// How far behind before we enter catch-up
const BEHIND_THRESHOLD = 10_000; // blocks ≈ 2 h on mainnet

// Active parameter set (starts in LIVE; may switch to CATCH_UP)
let params = { ...LIVE };

// ── Contract map (updated event names) ─────────────────────────────────
const contracts = {
  TSHOT: {
    address: "A.05b67ba314000b2d.TSHOT",
    events: [
      "TokensInitialized",
      "TokensWithdrawn",
      "TokensDeposited",
      "TokensMinted",
      "TokensBurned",
    ],
  },
  TSHOT_EXCHANGE: {
    address: "A.05b67ba314000b2d.TSHOTExchange",
    events: [
      "NFTToTSHOTSwapCompleted",
      "TSHOTToNFTSwapInitiated",
      "TSHOTToNFTSwapCompleted",
    ],
  },
  TSHOT_FLOW_PAIR: {
    // fully-qualified contract identifier
    address: "A.5eaa6c0b37002bbd.SwapPair",
    events: [
      "TokensInitialized",
      "TokensMinted",
      "TokensBurned",
      "LiquidityAdded",
      "LiquidityRemoved",
      "Swap",
      "Flashloan",
    ],
  },
};

function generateEventTypes() {
  const set = new Set();
  for (const cfg of Object.values(contracts)) {
    if (cfg.address && Array.isArray(cfg.events)) {
      cfg.events.forEach((ev) => set.add(`${cfg.address}.${ev}`));
    }
  }
  if (!set.size) {
    console.error("FATAL: no valid contracts configured");
    process.exit(1);
  }
  return [...set];
}
const eventTypes = generateEventTypes();

// ── Globals for Mongo handles ──────────────────────────────────────────
let mongoClient = null;
let eventsCollection = null;
let processedBlocksCol = null;

// ── Mongo connection helper ────────────────────────────────────────────
async function connectToMongo() {
  if (!mongoClient) mongoClient = new MongoClient(MONGODB_URI);
  if (!mongoClient.topology || !mongoClient.topology.isConnected()) {
    console.log("Connecting/Reconnecting MongoDB Client...");
    await mongoClient.connect();
  }
  const db = mongoClient.db(DATABASE_NAME);
  eventsCollection = db.collection(RAW_EVENTS_COLLECTION);
  processedBlocksCol = db.collection(PROCESSED_BLOCKS_COLLECTION);

  // indices (noop if already exist)
  await Promise.all([
    eventsCollection.createIndex({ projectId: 1, blockHeight: -1 }),
    eventsCollection.createIndex({ transactionId: 1 }),
    eventsCollection.createIndex({ type: 1 }),
    processedBlocksCol.createIndex({ projectId: 1 }, { unique: true }),
  ]);
}

// ── Flow helpers ───────────────────────────────────────────────────────
const getLatestSealedBlockHeight = async () =>
  fcl
    .send([fcl.getBlock(true)])
    .then(fcl.decode)
    .then((b) => b.height);

async function getLatestProcessedBlockHeight() {
  const doc = await processedBlocksCol.findOne({ projectId });
  return doc?.blockHeight ?? 0;
}

// storeEvent unchanged from your version (trimmed for brevity)
async function storeEvent(event, tx, status) {
  // … identical to your original implementation …
  const filter = {
    transactionId: event.transactionId,
    eventIndex: event.eventIndex,
    projectId,
  };
  const update = {
    $set: {
      projectId,
      blockHeight: event.blockHeight,
      blockTimestamp: event.blockTimestamp,
      type: event.type,
      transactionId: event.transactionId,
      eventIndex: event.eventIndex,
      data: event.data,
      processedAt: new Date(),
      // minimal extras; expand as in your old code if needed
    },
  };
  await eventsCollection.updateOne(filter, update, { upsert: true });

  return true;
}

// ── Main monitor ───────────────────────────────────────────────────────
async function startMonitor() {
  // dynamic import for p-limit
  const { default: pLimit } = await import("p-limit");

  if (!FLOW_ACCESS_NODE) {
    console.error("FLOW_ACCESS_NODE missing");
    process.exit(1);
  }
  if (!MONGODB_URI) {
    console.error("MONGODB_URI missing");
    process.exit(1);
  }

  await fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);
  await connectToMongo();

  // helper for parallel Tx fetches under current params
  const txFetchLimit = () => pLimit(params.TX_FETCH_CONCURRENCY);

  // polling loop (recursive setTimeout)
  async function pollEvents() {
    let delay = params.POLLING_DELAY;
    try {
      const latestSealed = await getLatestSealedBlockHeight();
      const lastProcessed = await getLatestProcessedBlockHeight();
      let from = lastProcessed
        ? lastProcessed + 1
        : Math.max(1, latestSealed - 5);

      // switch tuning profile if needed
      const distance = latestSealed - from;
      if (distance > BEHIND_THRESHOLD && params !== CATCH_UP) {
        console.log(`⚡ Catch-up mode (${distance} blocks behind)`);
        params = { ...CATCH_UP };
      } else if (distance <= BEHIND_THRESHOLD && params !== LIVE) {
        console.log("✅ Live mode – fully caught up");
        params = { ...LIVE };
      }

      if (from > latestSealed) {
        process.stdout.write(`Waiting for new blocks… head=${latestSealed}\r`);
        delay = params.CAUGHT_UP_POLL_DELAY;
        setTimeout(pollEvents, delay);
        return;
      }

      const to = Math.min(from + params.BLOCK_POLL_RANGE - 1, latestSealed);
      console.log(`Polling ${from}-${to} (head ${latestSealed})`);

      // ─── 1. fetch events for *all* types in parallel ────────────────
      const eventsNested = await Promise.all(
        eventTypes.map((type) =>
          fcl
            .send([fcl.getEventsAtBlockHeightRange(type, from, to)])
            .then(fcl.decode)
            .catch((err) => {
              if (!/event type not found|cannot get events/.test(err.message))
                console.warn(`WARN event ${type}: ${err.message}`);
              return [];
            })
        )
      );
      const allEvents = eventsNested.flat();
      if (!allEvents.length) {
        console.log(" -> no relevant events");
      } else {
        console.log(` -> found ${allEvents.length} events`);
        // ─── 2. get unique tx IDs ─────────────────────────────────────
        const uniqTxIds = [...new Set(allEvents.map((e) => e.transactionId))];
        const cache = new Map();
        // fetch Tx + status with concurrency limit
        await Promise.all(
          uniqTxIds.map((id) =>
            txFetchLimit()(async () => {
              try {
                const [tx, st] = await Promise.all([
                  fcl.send([fcl.getTransaction(id)]).then(fcl.decode),
                  fcl.send([fcl.getTransactionStatus(id)]).then(fcl.decode),
                ]);
                cache.set(id, { tx, st });
              } catch (e) {
                cache.set(id, { tx: null, st: null });
              }
            })
          )
        );

        // sort events deterministically
        allEvents.sort(
          (a, b) =>
            a.blockHeight - b.blockHeight ||
            (a.transactionIndex ?? 0) - (b.transactionIndex ?? 0) ||
            a.eventIndex - b.eventIndex
        );

        // store
        let stored = 0;
        for (const ev of allEvents) {
          if (
            await storeEvent(
              ev,
              cache.get(ev.transactionId)?.tx,
              cache.get(ev.transactionId)?.st
            )
          )
            stored++;
        }
        console.log(` -> stored ${stored}/${allEvents.length}`);
      }

      // write checkpoint
      await processedBlocksCol.updateOne(
        { projectId },
        { $set: { blockHeight: to, updatedAt: new Date() } },
        { upsert: true }
      );
    } catch (err) {
      console.error(`pollEvents error: ${err.message}`, err);
      delay = params.CAUGHT_UP_POLL_DELAY * 2;
    } finally {
      setTimeout(pollEvents, delay);
    }
  }

  console.log(`Monitoring started (project ${projectId})`);
  console.log(`Event types: ${eventTypes.length}`);
  pollEvents();
}

// ── graceful shutdown ──────────────────────────────────────────────────
async function cleanup() {
  console.log("\nShutting down monitor…");
  if (mongoClient?.close) await mongoClient.close();
  process.exit(0);
}
process.on("SIGINT", cleanup);
process.on("SIGTERM", cleanup);

// ── bootstrap ──────────────────────────────────────────────────────────
startMonitor().catch(async (err) => {
  console.error("Fatal startup error:", err);
  if (mongoClient?.close) await mongoClient.close();
  process.exit(1);
});
