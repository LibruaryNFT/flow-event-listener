/* monitor.cjs — Flow monitor + wallet_stats with retry/back-off (CommonJS) */
/* eslint-disable no-console */
const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();

/* ───────── env vars ───────── */
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const MONGODB_URI = process.env.MONGODB_URI;
if (!FLOW_ACCESS_NODE || !MONGODB_URI) {
  console.error("FATAL: FLOW_ACCESS_NODE or MONGODB_URI missing");
  process.exit(1);
}

/* ───────── constants ───────── */
const DB = "flow_events";
const RAW_COL = "raw_events";
const PROCESSED_COL = "processed_blocks";
const WALLET_STATS_COL = "wallet_stats";
const projectId = process.env.PROJECT_ID || "flow_monitors";

const LIVE = { RANGE: 25, DELAY: 2000, IDLE: 10000 };
const CATCH = { RANGE: 250, DELAY: 0, IDLE: 2000 };
const BEHIND = 10_000; // blocks
let mode = "LIVE",
  param = { ...LIVE };

/* Swap-completed events */
const DEPOSIT_EVT = "A.05b67ba314000b2d.TSHOTExchange.NFTToTSHOTSwapCompleted";
const WITHDRAW_EVT = "A.05b67ba314000b2d.TSHOTExchange.TSHOTToNFTSwapCompleted";
const EVENTS = [DEPOSIT_EVT, WITHDRAW_EVT];

/* Retry settings */
const RETRIES = 5;
const BACKOFF_MS = 500;
const JITTER_MS = 250;

/* ───────── Mongo handles ───────── */
let mongo, raw, processed, wallet;
async function mongoEnsure() {
  if (!mongo) mongo = new MongoClient(MONGODB_URI);
  if (!mongo.topology || !mongo.topology.isConnected()) await mongo.connect();
  const db = mongo.db(DB);
  raw = db.collection(RAW_COL);
  processed = db.collection(PROCESSED_COL);
  wallet = db.collection(WALLET_STATS_COL);

  await Promise.all([
    raw.createIndex({ projectId: 1, blockHeight: -1 }),
    raw.createIndex({ transactionId: 1 }),
    raw.createIndex({ type: 1 }),
    processed.createIndex({ projectId: 1 }, { unique: true }),
    wallet.createIndex({ net: -1 }),
  ]);
}

/* ───────── retry wrapper ───────── */
async function retry(label, fn) {
  for (let i = 0; i < RETRIES; i++) {
    try {
      const t0 = Date.now();
      const res = await fn();
      console.debug(`[RPC] ${label} ok in ${Date.now() - t0} ms`);
      return res;
    } catch (err) {
      const wait = BACKOFF_MS * 2 ** i + Math.random() * JITTER_MS;
      console.warn(
        `[RPC] ${label} fail ${
          i + 1
        }/${RETRIES}: ${err.message.trim()} — ${Math.round(wait)} ms`
      );
      if (i === RETRIES - 1) throw err;
      await new Promise((r) => setTimeout(r, wait));
    }
  }
}

/* ───────── Flow helpers ───────── */
async function getHead() {
  return retry("getBlock", () =>
    fcl
      .send([fcl.getBlock(true)])
      .then(fcl.decode)
      .then((b) => b.height)
  );
}
function fetchEvents(type, from, to) {
  return retry(`${type} ${from}-${to}`, () =>
    fcl.send([fcl.getEventsAtBlockHeightRange(type, from, to)]).then(fcl.decode)
  );
}
const lastHeight = () =>
  processed.findOne({ projectId }).then((d) => d?.blockHeight ?? 0);

/* ───────── persistence ───────── */
async function store(ev) {
  await raw.updateOne(
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

  if (ev.type !== DEPOSIT_EVT && ev.type !== WITHDRAW_EVT) return;
  const walletId = ev.data?.payer;
  const nfts = Number(ev.data?.numNFTs || 0);
  if (!walletId || !nfts) return;

  const fld =
    ev.type === DEPOSIT_EVT
      ? "NFTToTSHOTSwapCompleted"
      : "TSHOTToNFTSwapCompleted";
  const net = ev.type === DEPOSIT_EVT ? nfts : -nfts;

  await wallet.updateOne(
    { _id: walletId },
    {
      $inc: { [fld]: nfts, net },
      $min: { firstEvent: new Date(ev.blockTimestamp) },
      $max: { lastEvent: new Date(ev.blockTimestamp) },
    },
    { upsert: true }
  );
}

/* ───────── main logic ───────── */
async function main() {
  await fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);
  await mongoEnsure();
  console.log(`Monitor ready (${EVENTS.length} event types)`);

  async function loop() {
    let wait = param.DELAY;
    try {
      const head = await getHead();
      const from = (await lastHeight()) + 1;
      const lag = head - from;

      if (lag > BEHIND && mode !== "CATCH") {
        mode = "CATCH";
        param = { ...CATCH };
      } else if (lag <= BEHIND && mode !== "LIVE") {
        mode = "LIVE";
        param = { ...LIVE };
      }

      if (lag <= 0) {
        process.stdout.write(`Idle … head=${head}\r`);
        wait = param.IDLE;
      } else {
        const to = Math.min(from + param.RANGE - 1, head);
        console.log(`[${mode}] slice ${from}-${to} (lag ${lag})`);

        const arrays = await Promise.all(
          EVENTS.map((t) => fetchEvents(t, from, to).catch(() => []))
        );
        const evs = arrays.flat();

        if (evs.length) {
          console.log(` · ${evs.length} events`);
          await Promise.all(evs.map(store));
        } else console.log(" · none");

        await processed.updateOne(
          { projectId },
          { $set: { blockHeight: to, updatedAt: new Date() } },
          { upsert: true }
        );
      }
    } catch (err) {
      console.error("Slice aborted:", err.message);
      wait = param.IDLE * 2; // heavy back-off
    } finally {
      setTimeout(loop, wait);
    }
  }

  loop();
}

main().catch(async (e) => {
  console.error("Fatal:", e);
  await mongo?.close();
  process.exit(1);
});

/* graceful shutdown */
process.on("SIGINT", async () => {
  await mongo?.close();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await mongo?.close();
  process.exit(0);
});
