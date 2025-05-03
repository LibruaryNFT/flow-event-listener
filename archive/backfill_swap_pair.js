/**
 * Increment-fi TSHOT/FLOW SwapPair back-fill
 * --------------------------------------------------------------
 *   node backfill_swap_pair.js         # auto-resume
 *   node backfill_swap_pair.js reset   # restart from genesis
 * --------------------------------------------------------------
 */

const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();

const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const MONGODB_URI = process.env.MONGODB_URI;
const DATABASE = "flow_events";
const RAW_COLLECTION = "raw_events";
const projectId = process.env.PROJECT_ID || "flow_monitors";

const PAIR_ADDR = "A.5eaa6c0b37002bbd.SwapPair";
const EVTS = [
  "TokensInitialized",
  "TokensMinted",
  "TokensBurned",
  "LiquidityAdded",
  "LiquidityRemoved",
  "Swap",
  "Flashloan",
].map((e) => `${PAIR_ADDR}.${e}`);

const GENESIS = 104_711_985;
const BATCH = 250;

// ── helpers ────────────────────────────────────────────
const head = () =>
  fcl
    .send([fcl.getBlock(true)])
    .then(fcl.decode)
    .then((b) => b.height);
const bar = (pct, len = 28) =>
  "█".repeat(Math.floor(pct * len)).padEnd(len, "░");

// ── main ───────────────────────────────────────────────
(async () => {
  if (!FLOW_ACCESS_NODE || !MONGODB_URI) {
    console.error("env vars");
    process.exit(1);
  }
  await fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);
  const mongo = new MongoClient(MONGODB_URI);
  await mongo.connect();
  const col = mongo.db(DATABASE).collection(RAW_COLLECTION);

  // where to start
  let start = GENESIS;
  if (process.argv[2] !== "reset") {
    const last = await col
      .find({ projectId, type: /^A\.5eaa6c0b37002bbd\.SwapPair\./ })
      .sort({ blockHeight: -1 })
      .limit(1)
      .next();
    if (last) start = last.blockHeight + 1;
  }
  const stop = await head();
  const totalBatches = Math.ceil((stop - start + 1) / BATCH);
  console.log(`Back-filling ${start} → ${stop} (${totalBatches} slices)`);

  const t0 = Date.now();
  let doneBatches = 0;

  for (let from = start; from <= stop; from += BATCH) {
    const to = Math.min(from + BATCH - 1, stop);
    let stored = 0;
    try {
      const arrays = await Promise.all(
        EVTS.map((t) =>
          fcl
            .send([fcl.getEventsAtBlockHeightRange(t, from, to)])
            .then(fcl.decode)
            .catch(() => [])
        )
      );
      const evts = arrays.flat();
      stored = evts.length;
      if (stored)
        await col.bulkWrite(
          evts.map((e) => ({
            updateOne: {
              filter: {
                projectId,
                transactionId: e.transactionId,
                eventIndex: e.eventIndex,
              },
              update: {
                $set: {
                  projectId,
                  blockHeight: e.blockHeight,
                  blockTimestamp: e.blockTimestamp,
                  type: e.type,
                  transactionId: e.transactionId,
                  eventIndex: e.eventIndex,
                  data: e.data,
                  processedAt: new Date(),
                },
              },
              upsert: true,
            },
          })),
          { ordered: false }
        );
    } catch (e) {
      /* skip slice on error */
    }

    // ---- progress display ----
    doneBatches++;
    const pct = doneBatches / totalBatches;
    const speed = (Date.now() - t0) / doneBatches; // ms per batch
    const eta = ((totalBatches - doneBatches) * speed) / 1000; // sec
    process.stdout.write(
      `\r${bar(pct)} ${Math.round(pct * 100)}%  ` +
        `${doneBatches}/${totalBatches}  ` +
        `${stored.toString().padStart(4)} evts  ` +
        `ETA ${eta > 60 ? `${Math.round(eta / 60)}m` : Math.round(eta)}s      `
    );
  }

  console.log("\n✅ Back-fill complete");
  await mongo.close();
  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
