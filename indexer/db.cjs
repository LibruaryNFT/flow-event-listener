/* db.cjs — PostgreSQL/TimescaleDB connection pool + insert helpers */

const { Pool } = require("pg");
const logger = require("../logger.cjs");

let pool;

/**
 * Initialize the connection pool. Call once at startup.
 * @param {string} connectionString - POSTGRES_EVENTS_URI
 */
function init(connectionString) {
  pool = new Pool({
    connectionString,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  });

  pool.on("error", (err) => {
    logger.error({ err: err.message }, "Unexpected PostgreSQL pool error");
  });
}

/** Get the pool (for direct queries). */
function getPool() {
  if (!pool) throw new Error("Database not initialized — call db.init() first");
  return pool;
}

/**
 * Insert a batch of raw events into flow_raw_events.
 * Uses ON CONFLICT DO NOTHING for idempotent writes.
 *
 * @param {Array} events - Array of { blockHeight, blockTimestamp, transactionHash, logIndex, topics, data }
 * @returns {number} Number of rows actually inserted
 */
async function insertEvents(events) {
  if (!events.length) return 0;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    let inserted = 0;
    // Batch insert in chunks of 100 to avoid parameter limit
    const CHUNK = 100;
    for (let i = 0; i < events.length; i += CHUNK) {
      const chunk = events.slice(i, i + CHUNK);
      const values = [];
      const params = [];
      let paramIdx = 1;

      for (const ev of chunk) {
        values.push(
          `($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3}, $${paramIdx + 4}, $${paramIdx + 5})`
        );
        params.push(
          ev.blockHeight,
          ev.blockTimestamp,
          ev.transactionHash,
          ev.logIndex,
          ev.topics,
          JSON.stringify(ev.data)
        );
        paramIdx += 6;
      }

      const sql = `
        INSERT INTO flow_raw_events (block_height, block_timestamp, transaction_hash, log_index, topics, data)
        VALUES ${values.join(", ")}
        ON CONFLICT DO NOTHING
      `;

      const result = await client.query(sql, params);
      inserted += result.rowCount;
    }

    await client.query("COMMIT");
    return inserted;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Log processed blocks to block_sync_log for gap detection.
 * @param {number} blockHeight
 * @param {number} eventCount
 */
async function logBlockSync(blockHeight, eventCount) {
  await pool.query(
    `INSERT INTO block_sync_log (block_height, event_count)
     VALUES ($1, $2)
     ON CONFLICT (block_height) DO UPDATE SET event_count = $2, processed_at = NOW()`,
    [blockHeight, eventCount]
  );
}

/**
 * Batch log a range of blocks to block_sync_log.
 * @param {number} fromBlock
 * @param {number} toBlock
 * @param {number} totalEvents - total events across all blocks in range
 */
async function logBlockSyncRange(fromBlock, toBlock, totalEvents) {
  // For ranges, log start and end blocks
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    // Log each block in range with proportional event count
    // For efficiency, just log the range endpoints
    const evPerBlock = totalEvents / Math.max(toBlock - fromBlock + 1, 1);
    for (let h = fromBlock; h <= toBlock; h++) {
      await client.query(
        `INSERT INTO block_sync_log (block_height, event_count)
         VALUES ($1, $2)
         ON CONFLICT (block_height) DO NOTHING`,
        [h, Math.round(evPerBlock)]
      );
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Get or set indexer state (key-value store).
 */
async function getState(key) {
  const res = await pool.query(
    "SELECT value FROM indexer_state WHERE key = $1",
    [key]
  );
  return res.rows[0]?.value ?? null;
}

async function setState(key, value) {
  await pool.query(
    `INSERT INTO indexer_state (key, value, updated_at)
     VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`,
    [key, String(value)]
  );
}

/**
 * Get the last processed block height from indexer_state.
 * @returns {number}
 */
async function getLastProcessedBlock() {
  const val = await getState("last_processed_block");
  return val ? Number(val) : 0;
}

/**
 * Update the last processed block height.
 * @param {number} height
 */
async function setLastProcessedBlock(height) {
  await setState("last_processed_block", height);
}

/**
 * Check database connectivity.
 * @returns {boolean}
 */
async function isHealthy() {
  try {
    await pool.query("SELECT 1");
    return true;
  } catch {
    return false;
  }
}

/**
 * Close the pool gracefully.
 */
async function close() {
  if (pool) await pool.end();
}

module.exports = {
  init,
  getPool,
  insertEvents,
  logBlockSync,
  logBlockSyncRange,
  getState,
  setState,
  getLastProcessedBlock,
  setLastProcessedBlock,
  isHealthy,
  close,
};
