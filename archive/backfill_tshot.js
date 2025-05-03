/**
 * backfill_tshot.js  ––  TSHOT contract (A.05b67ba314000b2d.TSHOT)
 * ------------------------------------------------------------------
 *  node backfill_tshot.js         ⟶ fills gap *before* earliest record
 *  node backfill_tshot.js reset   ⟶ scans entire history since deploy
 * ------------------------------------------------------------------
 */

const fcl = require("@onflow/fcl");
const { MongoClient } = require("mongodb");
require("dotenv").config();

// ── ENV  ───────────────────────────────────────────────────────────────
const FLOW_ACCESS_NODE = process.env.FLOW_ACCESS_NODE;
const MONGODB_URI = process.env.MONGODB_URI;
const DB_NAME = "flow_events";
const RAW_COLL = "raw_events";
const projectId = process.env.PROJECT_ID || "flow_monitors";

// ── CONTRACT & EVENTS ─────────────────────────────────────────────────
const TSHOT_ADDR = "A.05b67ba314000b2d.TSHOT";
const TSHOT_EVENTS = [
  "TokensInitialized",
  "TokensWithdrawn",
  "TokensDeposited",
  "TokensMinted",
  "TokensBurned",
].map((e) => `${TSHOT_ADDR}.${e}`);

// Deployment block (2025-02-12)
const GENESIS = 103_337_840;
const BATCH = 250; // Flow RPC limit

// ── helpers ───────────────────────────────────────────────────────────
const latest = () =>
  fcl
    .send([fcl.getBlock(true)])
    .then(fcl.decode)
    .then((b) => b.height);
const bar = (p, l = 28) => "█".repeat(Math.floor(p * l)).padEnd(l, "░");

// ── main  ─────────────────────────────────────────────────────────────
(async () => {
  if (!FLOW_ACCESS_NODE || !MONGODB_URI) {
    console.error("FLOW_ACCESS_NODE & MONGODB_URI must be set");
    process.exit(1);
  }
  await fcl.config().put("accessNode.api", FLOW_ACCESS_NODE);

  const mongo = new MongoClient(MONGODB_URI);
  await mongo.connect();
  const col = mongo.db(DB_NAME).collection(RAW_COLL);

  // ── Determine range to scan ───────────────────────────────────────
  let start = GENESIS;
  let stop = await latest(); // default (no earlier rows)

  if (process.argv[2] !== "reset") {
    // earliest block we already have for TSHOT
    const earliest = await col
      .find({ projectId, type: new RegExp(`^${TSHOT_ADDR}\\.`) })
      .sort({ blockHeight: 1 })
      .limit(1)
      .next();
    if (earliest) stop = Math.max(GENESIS, earliest.blockHeight - 1);
  }

  if (stop < start) {
    console.log("Nothing to back-fill – earliest record already at genesis.");
    await mongo.close();
    process.exit(0);
  }

  const total = Math.ceil((stop - start + 1) / BATCH);
  console.log(`Back-filling TSHOT events ${start} → ${stop} (${total} slices)`);

  // ── Loop ──────────────────────────────────────────────────────────
  const t0 = Date.now();
  let done = 0;

  for (let from = start; from <= stop; from += BATCH) {
    const to = Math.min(from + BATCH - 1, stop);
    let stored = 0;

    try {
      const arrays = await Promise.all(
        TSHOT_EVENTS.map((t) =>
          fcl
            .send([fcl.getEventsAtBlockHeightRange(t, from, to)])
            .then(fcl.decode)
            .catch(() => [])
        )
      );
      const evts = arrays.flat();
      stored = evts.length;

      if (stored) {
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
      }
    } catch (err) {
      console.warn(
        `⚠️  slice ${from}-${to} failed: ${err.message.split("\n")[0]}`
      );
    }

    // progress bar
    done++;
    const pct = done / total;
    const rate = (Date.now() - t0) / done; // ms per slice
    const eta = ((total - done) * rate) / 1000; // sec
    process.stdout.write(
      `\r${bar(pct)} ${Math.round(pct * 100)}% ` +
        `${done}/${total}  ${stored.toString().padStart(4)} evts  ` +
        `ETA ${
          eta > 60 ? Math.round(eta / 60) + "m" : Math.round(eta) + "s"
        }   `
    );
  }

  console.log("\n✅ Back-fill complete");
  await mongo.close();
  process.exit(0);
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
