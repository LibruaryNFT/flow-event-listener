// monitor.js  ––– Flow event monitor + lifetime wallet_stats updater
/* eslint-disable no-console */
const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();

/* ────────── ENV & CONSTS ────────── */
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const MONGODB_URI = process.env.MONGODB_URI;
const DATABASE_NAME = "flow_events";
const RAW_EVENTS_COLLECTION = "raw_events";
const PROCESSED_BLOCKS_COLLECTION = "processed_blocks";
const WALLET_STATS_COLLECTION = "wallet_stats"; // NEW
const projectId = process.env.PROJECT_ID || "flow_monitors";

/* ── Progress tuning ── */
const LIVE = {
  TX_FETCH_CONCURRENCY: 8,
  BLOCK_POLL_RANGE: 25,
  POLLING_DELAY: 2000,
  CAUGHT_UP_POLL_DELAY: 10000,
};
const CATCH_UP = {
  TX_FETCH_CONCURRENCY: 32,
  BLOCK_POLL_RANGE: 250,
  POLLING_DELAY: 0,
  CAUGHT_UP_POLL_DELAY: 2000,
};
const BEHIND_THRESHOLD = 10_000; // ≈2h

let params = { ...LIVE };

/* ── Contract event map ── */
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
    /* kept for raw logging */ address: "A.5eaa6c0b37002bbd.SwapPair",
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

/* Full strings of the two swap events we care about for wallet_stats */
const DEPOSIT_EVT = "A.05b67ba314000b2d.TSHOTExchange.NFTToTSHOTSwapCompleted";
const WITHDRAW_EVT = "A.05b67ba314000b2d.TSHOTExchange.TSHOTToNFTSwapCompleted";

/* ── Mongo handles ── */
let mongoClient, eventsCollection, processedBlocksCol, walletStatsCol;
async function connectToMongo() {
  if (!mongoClient) mongoClient = new MongoClient(MONGODB_URI);
  if (!mongoClient.topology || !mongoClient.topology.isConnected()) {
    console.log("Connecting MongoDB …");
    await mongoClient.connect();
  }
  const db = mongoClient.db(DATABASE_NAME);
  eventsCollection = db.collection(RAW_EVENTS_COLLECTION);
  processedBlocksCol = db.collection(PROCESSED_BLOCKS_COLLECTION);
  walletStatsCol = db.collection(WALLET_STATS_COLLECTION); // NEW

  await Promise.all([
    eventsCollection.createIndex({ projectId: 1, blockHeight: -1 }),
    eventsCollection.createIndex({ transactionId: 1 }),
    eventsCollection.createIndex({ type: 1 }),
    processedBlocksCol.createIndex({ projectId: 1 }, { unique: true }),
    walletStatsCol.createIndex({ net: -1 }), // leaderboard
  ]);
}

/* ── Flow helpers ── */
const headBlock = () =>
  fcl
    .send([fcl.getBlock(true)])
    .then(fcl.decode)
    .then((b) => b.height);

const lastProcessed = () =>
  processedBlocksCol.findOne({ projectId }).then((d) => d?.blockHeight ?? 0);

/* ── RAW event upsert  +  wallet_stats update ── */
async function storeEvent(ev) {
  // 1) raw_events
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

  // 2) lifetime wallet_stats  (only for the two swap-completed events)
  if (ev.type !== DEPOSIT_EVT && ev.type !== WITHDRAW_EVT) return;

  const wallet = ev.data?.payer;
  if (!wallet) return; // skip malformed rows

  const num = Number(ev.data?.numNFTs || "0");
  if (!num) return;

  const isDeposit = ev.type === DEPOSIT_EVT;
  const incNet = isDeposit ? num : -num;

  await walletStatsCol.updateOne(
    { _id: wallet },
    {
      $inc: {
        deposits: isDeposit ? num : 0,
        withdrawals: isDeposit ? 0 : num,
        net: incNet,
      },
      $min: { firstEvent: new Date(ev.blockTimestamp) },
      $max: { lastEvent: new Date(ev.blockTimestamp) },
    },
    { upsert: true }
  );
}

/* ── Monitor loop ── */
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

      // fetch events
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
        await Promise.all(events.map((e) => storeEvent(e))); // <- updates both colls
      } else {
        console.log(" -> none");
      }

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

/* ── graceful shutdown ── */
process.on("SIGINT", async () => {
  await mongoClient?.close();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await mongoClient?.close();
  process.exit(0);
});

/* ── kick-off ── */
startMonitor().catch(async (e) => {
  console.error("Fatal:", e);
  await mongoClient?.close();
  process.exit(1);
});
