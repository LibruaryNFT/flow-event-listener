/* gap-detector.cjs — Block sync tracking + gap detection + backfill triggers */

const db = require("./db.cjs");
const logger = require("../logger.cjs");

/**
 * Find gaps in the block_sync_log.
 * Returns array of { gapStart, gapEnd } ranges where blocks are missing.
 * Only checks blocks between our first and last recorded block.
 *
 * @param {number} [limit=100] - Max gaps to return
 * @returns {Array<{gapStart: number, gapEnd: number}>}
 */
async function findGaps(limit = 100) {
  const pool = db.getPool();

  const result = await pool.query(`
    WITH bounds AS (
      SELECT MIN(block_height) AS lo, MAX(block_height) AS hi
      FROM block_sync_log
    ),
    all_blocks AS (
      SELECT generate_series(lo, hi) AS block_height
      FROM bounds
    )
    SELECT a.block_height AS gap_start,
           (SELECT MIN(b.block_height) - 1
            FROM block_sync_log b
            WHERE b.block_height > a.block_height) AS gap_end
    FROM all_blocks a
    LEFT JOIN block_sync_log s ON s.block_height = a.block_height
    WHERE s.block_height IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM block_sync_log prev
        WHERE prev.block_height = a.block_height - 1
      )
    ORDER BY a.block_height
    LIMIT $1
  `, [limit]);

  // Collapse consecutive missing blocks into ranges
  if (!result.rows.length) return [];

  const gaps = [];
  let current = null;

  for (const row of result.rows) {
    if (!current) {
      current = { gapStart: row.gap_start, gapEnd: row.gap_end || row.gap_start };
    } else if (row.gap_start <= current.gapEnd + 1) {
      current.gapEnd = Math.max(current.gapEnd, row.gap_end || row.gap_start);
    } else {
      gaps.push(current);
      current = { gapStart: row.gap_start, gapEnd: row.gap_end || row.gap_start };
    }
  }
  if (current) gaps.push(current);

  return gaps;
}

/**
 * Simple gap detection: compare last processed block vs current chain height.
 * Use this on startup to detect if we missed blocks while offline.
 *
 * @param {number} chainHeight - Current chain block height
 * @returns {{ lastBlock: number, chainHeight: number, gap: number, needsCatchUp: boolean }}
 */
async function checkStartupGap(chainHeight) {
  const lastBlock = await db.getLastProcessedBlock();
  const gap = chainHeight - lastBlock;

  if (gap > 1000) {
    logger.warn(
      { lastBlock, chainHeight, gap },
      "Large gap detected on startup — entering CATCH-UP mode"
    );
  } else if (gap > 0) {
    logger.info(
      { lastBlock, chainHeight, gap },
      "Small gap detected — will catch up normally"
    );
  } else {
    logger.info({ lastBlock, chainHeight }, "No gap — fully synced");
  }

  return {
    lastBlock,
    chainHeight,
    gap,
    needsCatchUp: gap > 0,
  };
}

/**
 * Record a known gap as a spork transition (not an error).
 * @param {number} fromBlock
 * @param {number} toBlock
 * @param {string} reason
 */
async function markSporkTransition(fromBlock, toBlock, reason) {
  await db.setState(
    `spork_gap_${fromBlock}_${toBlock}`,
    JSON.stringify({ fromBlock, toBlock, reason, recordedAt: new Date().toISOString() })
  );
  logger.info({ fromBlock, toBlock, reason }, "Spork transition gap recorded");
}

/**
 * Get sync statistics.
 */
async function getSyncStats() {
  const pool = db.getPool();

  const stats = await pool.query(`
    SELECT
      COUNT(*) AS total_blocks,
      MIN(block_height) AS earliest_block,
      MAX(block_height) AS latest_block,
      SUM(event_count) AS total_events,
      MAX(processed_at) AS last_processed_at
    FROM block_sync_log
  `);

  return stats.rows[0];
}

module.exports = {
  findGaps,
  checkStartupGap,
  markSporkTransition,
  getSyncStats,
};
