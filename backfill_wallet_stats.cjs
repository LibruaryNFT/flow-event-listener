#!/usr/bin/env node
/* backfill_wallet_stats.cjs
   ----------------------------------------------------
   Builds lifetime wallet_stats with flat root fields:
     NFTToTSHOTSwapCompleted   (+numNFTs)
     TSHOTToNFTSwapCompleted   (-numNFTs)
     net                       (deposit – withdraw)
*/

const { MongoClient } = require("mongodb");
const cliProgress = require("cli-progress");
require("dotenv").config();
const logger = require("./logger.cjs");

/* ---------- CONFIG ---------- */
const URI = process.env.MONGODB_URI; // e.g. mongodb://localhost:27017
const DB_NAME = "flow_events";
const RAW_COL = "raw_events";
const STATS_COL = "wallet_stats";

/* Full event strings in raw_events */
const DEPOSIT_EVT_STR =
  "A.05b67ba314000b2d.TSHOTExchange.NFTToTSHOTSwapCompleted";
const WITHDRAW_EVT_STR =
  "A.05b67ba314000b2d.TSHOTExchange.TSHOTToNFTSwapCompleted";

/* Field names we want in wallet_stats */
const FIELD_DEPOSIT = "NFTToTSHOTSwapCompleted";
const FIELD_WITHDRAW = "TSHOTToNFTSwapCompleted";
/* ----------------------------- */

(async () => {
  if (!URI) {
    logger.fatal("MONGODB_URI is not set");
    process.exit(1);
  }

  const client = new MongoClient(URI);
  await client.connect();
  const db = client.db(DB_NAME);
  const raw = db.collection(RAW_COL);
  const ws = db.collection(STATS_COL);

  await ws.deleteMany({});
  logger.info("wallet_stats cleared");

  const total = await raw.countDocuments({
    type: { $in: [DEPOSIT_EVT_STR, WITHDRAW_EVT_STR] },
  });
  logger.info({ total }, "Swap events to process");

  const bar = new cliProgress.SingleBar(
    { format: "Progress |{bar}| {percentage}% {value}/{total}" },
    cliProgress.Presets.shades_classic
  );
  bar.start(total, 0);

  const cur = raw
    .find(
      { type: { $in: [DEPOSIT_EVT_STR, WITHDRAW_EVT_STR] } },
      {
        projection: {
          type: 1,
          "data.numNFTs": 1,
          "data.payer": 1,
          blockTimestamp: 1,
        },
      }
    )
    .sort({ blockHeight: 1 });

  while (await cur.hasNext()) {
    const ev = await cur.next();
    const wallet = ev.data?.payer;
    if (!wallet) {
      bar.increment();
      continue;
    }

    const amt = Number(ev.data?.numNFTs || "0");
    if (!amt) {
      bar.increment();
      continue;
    }

    const isDeposit = ev.type === DEPOSIT_EVT_STR;
    const incField = isDeposit ? FIELD_DEPOSIT : FIELD_WITHDRAW;
    const incNet = isDeposit ? amt : -amt;

    await ws.updateOne(
      { _id: wallet },
      {
        $inc: { [incField]: amt, net: incNet },
        $min: { firstEvent: new Date(ev.blockTimestamp) },
        $max: { lastEvent: new Date(ev.blockTimestamp) },
      },
      { upsert: true }
    );
    bar.increment();
  }

  bar.stop();
  await ws.createIndex({ net: -1 });
  logger.info("Back-fill complete");
  await client.close();
})();
