/* monitor.cjs — Flow monitor + wallet_stats with retry/back-off (CommonJS) */

const http = require("http");
const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();
const logger = require("./logger.cjs");

/* ───────── env vars ───────── */
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const MONGODB_URI = process.env.MONGODB_URI;
if (!FLOW_ACCESS_NODE || !MONGODB_URI) {
  logger.fatal("FLOW_ACCESS_NODE or MONGODB_URI missing");
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
  try { await mongo.db("admin").command({ ping: 1 }); } catch { await mongo.connect(); }
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
      logger.debug({ label, durationMs: Date.now() - t0 }, "RPC call succeeded");
      return res;
    } catch (err) {
      const wait = BACKOFF_MS * 2 ** i + Math.random() * JITTER_MS;
      logger.warn(
        { label, attempt: i + 1, maxRetries: RETRIES, err: err.message.trim(), backoffMs: Math.round(wait) },
        "RPC call failed, retrying"
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

/* ───────── health check server ───────── */
const HEALTH_PORT = Number(process.env.HEALTH_PORT || 8091);
const monitorStartTime = Date.now();
let lastProcessedBlock = 0;
let currentMode = "STARTING";

http
  .createServer(async (req, res) => {
    if (req.url === "/health" && (req.method === "GET" || req.method === "HEAD")) {
      let mongoStatus = "unknown";
      try {
        if (mongo) {
          await mongo.db("admin").command({ ping: 1 });
          mongoStatus = "connected";
        } else {
          mongoStatus = "disconnected";
        }
      } catch {
        mongoStatus = "disconnected";
      }
      const body = JSON.stringify({
        status: mongoStatus === "connected" ? "healthy" : "degraded",
        service: "flow-event-listener",
        uptime: Math.floor((Date.now() - monitorStartTime) / 1000),
        mongodb: mongoStatus,
        mode: currentMode,
        lastProcessedBlock,
        eventTypes: EVENTS.length,
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
    logger.info({ port: HEALTH_PORT }, "Health check server listening");
  });

/* ───────── main logic ───────── */
async function main() {
  await fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);
  await mongoEnsure();
  logger.info({ eventTypes: EVENTS.length }, "Monitor ready");

  async function loop() {
    let wait = param.DELAY;
    try {
      const head = await getHead();
      const from = (await lastHeight()) + 1;
      const lag = head - from;

      if (lag > BEHIND && mode !== "CATCH") {
        mode = "CATCH";
        currentMode = "CATCH";
        param = { ...CATCH };
      } else if (lag <= BEHIND && mode !== "LIVE") {
        mode = "LIVE";
        currentMode = "LIVE";
        param = { ...LIVE };
      }

      if (lag <= 0) {
        logger.debug({ head }, "Idle, waiting for new blocks");
        wait = param.IDLE;
      } else {
        const to = Math.min(from + param.RANGE - 1, head);
        logger.info({ mode, from, to, lag }, "Processing block slice");

        const arrays = await Promise.all(
          EVENTS.map((t) => fetchEvents(t, from, to).catch(() => []))
        );
        const evs = arrays.flat();

        if (evs.length) {
          logger.info({ count: evs.length }, "Events found in slice");
          await Promise.all(evs.map(store));
        } else logger.debug("No events in slice");

        await processed.updateOne(
          { projectId },
          { $set: { blockHeight: to, updatedAt: new Date() } },
          { upsert: true }
        );
        lastProcessedBlock = to;
      }
    } catch (err) {
      logger.error({ err: err.message }, "Slice aborted");
      wait = param.IDLE * 2; // heavy back-off
    } finally {
      setTimeout(loop, wait);
    }
  }

  loop();
}

main().catch(async (e) => {
  logger.fatal({ err: e.message }, "Fatal error");
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
