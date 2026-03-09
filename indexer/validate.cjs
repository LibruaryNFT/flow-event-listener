/* validate.cjs — Compare PostgreSQL event counts vs BigQuery
 *
 * Run manually or via cron during Phase 4 (parallel validation).
 * Connects to PostgreSQL and prints daily event counts for comparison.
 *
 * Usage: node indexer/validate.cjs [--days 7]
 */

require("dotenv").config();
const db = require("./db.cjs");
const logger = require("../logger.cjs");

const POSTGRES_EVENTS_URI = process.env.POSTGRES_EVENTS_URI;
if (!POSTGRES_EVENTS_URI) {
  logger.fatal("POSTGRES_EVENTS_URI missing");
  process.exit(1);
}

async function validate() {
  const days = parseInt(process.argv.find((a, i) => process.argv[i - 1] === "--days") || "7");
  db.init(POSTGRES_EVENTS_URI);

  const pool = db.getPool();

  // Daily event counts
  logger.info({ days }, "Daily event counts (last N days)");
  const dailyCounts = await pool.query(`
    SELECT
      DATE(block_timestamp) AS day,
      COUNT(*) AS event_count,
      COUNT(DISTINCT block_height) AS block_count,
      COUNT(DISTINCT transaction_hash) AS tx_count
    FROM flow_raw_events
    WHERE block_timestamp >= NOW() - $1::int * INTERVAL '1 day'
    GROUP BY DATE(block_timestamp)
    ORDER BY day DESC
  `, [days]);

  console.log("\n=== Daily Event Counts ===");
  console.log("Day          | Events    | Blocks  | Transactions");
  console.log("-------------|-----------|---------|-------------");
  for (const row of dailyCounts.rows) {
    console.log(
      `${row.day.toISOString().slice(0, 10)} | ${String(row.event_count).padStart(9)} | ${String(row.block_count).padStart(7)} | ${String(row.tx_count).padStart(12)}`
    );
  }

  // Event counts by contract (topic prefix)
  const contractCounts = await pool.query(`
    SELECT
      SPLIT_PART(topics[1], '.', 1) || '.' ||
      SPLIT_PART(topics[1], '.', 2) || '.' ||
      SPLIT_PART(topics[1], '.', 3) AS contract,
      COUNT(*) AS event_count
    FROM flow_raw_events
    WHERE block_timestamp >= NOW() - $1::int * INTERVAL '1 day'
    GROUP BY contract
    ORDER BY event_count DESC
  `, [days]);

  console.log("\n=== Event Counts by Contract (last " + days + " days) ===");
  console.log("Contract                              | Events");
  console.log("--------------------------------------|--------");
  for (const row of contractCounts.rows) {
    console.log(`${row.contract.padEnd(37)} | ${row.event_count}`);
  }

  // Sync stats
  const stats = await pool.query(`
    SELECT
      COUNT(*) AS total_events,
      MIN(block_timestamp) AS earliest,
      MAX(block_timestamp) AS latest,
      MIN(block_height) AS min_block,
      MAX(block_height) AS max_block
    FROM flow_raw_events
  `);

  const s = stats.rows[0];
  console.log("\n=== Overall Stats ===");
  console.log(`Total events:   ${s.total_events}`);
  console.log(`Earliest:       ${s.earliest}`);
  console.log(`Latest:         ${s.latest}`);
  console.log(`Block range:    ${s.min_block} — ${s.max_block}`);

  // Block sync gaps
  const syncStats = await pool.query(`
    SELECT
      COUNT(*) AS synced_blocks,
      MIN(block_height) AS first_block,
      MAX(block_height) AS last_block,
      MAX(block_height) - MIN(block_height) + 1 - COUNT(*) AS missing_blocks
    FROM block_sync_log
  `);

  const sync = syncStats.rows[0];
  console.log(`\nSync coverage:  ${sync.synced_blocks} blocks (${sync.first_block} — ${sync.last_block})`);
  console.log(`Missing blocks: ${sync.missing_blocks}`);

  await db.close();
}

validate().catch((err) => {
  logger.fatal({ err: err.message }, "Validation failed");
  process.exit(1);
});
