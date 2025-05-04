// monitor.js  ––– Flow event monitor (TSHOT removed, slower polling)

/* eslint-disable no-console */
const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();

const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const MONGODB_URI = process.env.MONGODB_URI;
const DATABASE_NAME = "flow_events";
const RAW_EVENTS_COLLECTION = "raw_events";
const PROCESSED_BLOCKS_COLLECTION = "processed_blocks";
const projectId = process.env.PROJECT_ID || "flow_monitors";

// ── Tuning profiles ────────────────────────────────────────────────
const LIVE = {
  TX_FETCH_CONCURRENCY: 8,
  BLOCK_POLL_RANGE: 25,
  POLLING_DELAY: 2000, // 2 s
  CAUGHT_UP_POLL_DELAY: 10000, // 10 s idle wait
};

const CATCH_UP = {
  TX_FETCH_CONCURRENCY: 32,
  BLOCK_POLL_RANGE: 250,
  POLLING_DELAY: 0,
  CAUGHT_UP_POLL_DELAY: 2000,
};
const BEHIND_THRESHOLD = 10_000; // ≈2 h

let params = { ...LIVE };

// ── Contract map (TSHOT removed) ───────────────────────────────────
const contracts = {
  TSHOT_EXCHANGE: {
    address: "A.05b67ba314000b2d.TSHOTExchange",
    events: [
      "NFTToTSHOTSwapCompleted",
      "TSHOTToNFTSwapInitiated",
      "TSHOTToNFTSwapCompleted",
    ],
  },
  TSHOT_FLOW_PAIR: {
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

const eventTypes = Object.values(contracts).flatMap((c) =>
  c.events.map((ev) => `${c.address}.${ev}`)
);

// ── Mongo handles ─────────────────────────────────────────────────
let mongoClient, eventsCollection, processedBlocksCol;
async function connectToMongo() {
  if (!mongoClient) mongoClient = new MongoClient(MONGODB_URI);
  if (!mongoClient.topology || !mongoClient.topology.isConnected()) {
    console.log("Connecting MongoDB …");
    await mongoClient.connect();
  }
  const db = mongoClient.db(DATABASE_NAME);
  eventsCollection = db.collection(RAW_EVENTS_COLLECTION);
  processedBlocksCol = db.collection(PROCESSED_BLOCKS_COLLECTION);
  await Promise.all([
    eventsCollection.createIndex({ projectId: 1, blockHeight: -1 }),
    eventsCollection.createIndex({ transactionId: 1 }),
    eventsCollection.createIndex({ type: 1 }),
    processedBlocksCol.createIndex({ projectId: 1 }, { unique: true }),
  ]);
}

// ── Flow helpers ──────────────────────────────────────────────────
const headBlock = () =>
  fcl
    .send([fcl.getBlock(true)])
    .then(fcl.decode)
    .then((b) => b.height);

const lastProcessed = () =>
  processedBlocksCol.findOne({ projectId }).then((d) => d?.blockHeight ?? 0);

// storeEvent (unchanged apart from minimal fields)
async function storeEvent(ev) {
  await eventsCollection.updateOne(
    { projectId, transactionId: ev.transactionId, eventIndex: ev.eventIndex },
    {
      $set: {
        projectId,
        blockHeight: ev.blockHeight,
        blockTimestamp: ev.blockTimestamp,
        type: ev.type,
        transactionId: ev.transactionId,
        eventIndex: ev.eventIndex,
        data: ev.data,
        processedAt: new Date(),
      },
    },
    { upsert: true }
  );
}

// ── Monitor loop ─────────────────────────────────────────────────
async function startMonitor() {
  const { default: pLimit } = await import("p-limit");

  if (!FLOW_ACCESS_NODE || !MONGODB_URI) {
    console.error("FLOW_ACCESS_NODE or MONGODB_URI missing");
    process.exit(1);
  }
  await fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);
  await connectToMongo();

  const txLimit = () => pLimit(params.TX_FETCH_CONCURRENCY);

  async function poll() {
    let delay = params.POLLING_DELAY;
    try {
      const head = await headBlock();
      const last = await lastProcessed();
      let from = last ? last + 1 : Math.max(1, head - 5);

      const lag = head - from;
      if (lag > BEHIND_THRESHOLD && params !== CATCH_UP) {
        console.log(`⚡ Catch-up mode (${lag} blocks behind)`);
        params = { ...CATCH_UP };
      } else if (lag <= BEHIND_THRESHOLD && params !== LIVE) {
        console.log("✅ Live mode – fully caught up");
        params = { ...LIVE };
      }

      if (from > head) {
        process.stdout.write(`Waiting … head=${head}\r`);
        delay = params.CAUGHT_UP_POLL_DELAY;
        setTimeout(poll, delay);
        return;
      }

      const to = Math.min(from + params.BLOCK_POLL_RANGE - 1, head);
      console.log(`Polling ${from}-${to}`);

      // 1) fetch events for all types
      const arrays = await Promise.all(
        eventTypes.map((t) =>
          fcl
            .send([fcl.getEventsAtBlockHeightRange(t, from, to)])
            .then(fcl.decode)
            .catch(() => [])
        )
      );
      const events = arrays.flat();
      if (events.length) {
        console.log(` -> ${events.length} events`);
        // upsert in parallel
        await Promise.all(events.map((e) => storeEvent(e)));
      } else {
        console.log(" -> none");
      }

      // checkpoint
      await processedBlocksCol.updateOne(
        { projectId },
        { $set: { blockHeight: to, updatedAt: new Date() } },
        { upsert: true }
      );
    } catch (e) {
      console.error("poll error:", e.message);
      delay = params.CAUGHT_UP_POLL_DELAY * 2;
    } finally {
      setTimeout(poll, delay);
    }
  }

  console.log(`Monitor started – ${eventTypes.length} event types`);
  poll();
}

// ── graceful shutdown ───────────────────────────────────────────
process.on("SIGINT", async () => {
  await mongoClient?.close();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await mongoClient?.close();
  process.exit(0);
});

// ── kick-off ─────────────────────────────────────────────────────
startMonitor().catch(async (e) => {
  console.error("Fatal:", e);
  await mongoClient?.close();
  process.exit(1);
});
