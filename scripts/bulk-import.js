#!/usr/bin/env node
/**
 * bulk-import.js — Stream BigQuery JSONL export into PostgreSQL flow_raw_events
 *
 * Usage: node scripts/bulk-import.js /path/to/export/dir
 *
 * Streams compressed JSONL files, builds multi-row VALUES inserts.
 * Handles ~20GB across 4000+ files. Supports resume via progress file.
 */

const fs = require("fs");
const path = require("path");
const zlib = require("zlib");
const readline = require("readline");
const { Pool } = require("pg");

const EXPORT_DIR = process.argv[2];
if (!EXPORT_DIR) {
  console.error("Usage: node scripts/bulk-import.js /path/to/export/dir");
  process.exit(1);
}

const DB_URL =
  process.env.POSTGRES_EVENTS_URI ||
  "postgresql://indexer:indexer_secure_2026@localhost:5432/flow_events";
const PROGRESS_FILE = path.join(EXPORT_DIR, ".import-progress");
const BATCH_SIZE = 2000;

async function main() {
  const pool = new Pool({ connectionString: DB_URL, max: 4 });

  // Test connection
  const client = await pool.connect();
  console.log("Connected to PostgreSQL");
  client.release();

  // List files
  const files = fs
    .readdirSync(EXPORT_DIR)
    .filter((f) => f.endsWith(".jsonl.gz"))
    .sort();

  console.log(`Found ${files.length} files in ${EXPORT_DIR}`);

  // Resume support
  let lastDone = "";
  if (fs.existsSync(PROGRESS_FILE)) {
    lastDone = fs.readFileSync(PROGRESS_FILE, "utf8").trim();
    console.log(`Resuming after: ${lastDone}`);
  }

  let started = !lastDone;
  let totalRows = 0;
  let fileNum = 0;
  let skipped = 0;
  const startTime = Date.now();

  for (const file of files) {
    if (!started) {
      if (file === lastDone) started = true;
      skipped++;
      continue;
    }

    fileNum++;
    const filePath = path.join(EXPORT_DIR, file);
    const fileStart = Date.now();

    let rows = 0;
    let batch = [];
    const client = await pool.connect();

    try {
      const fileStream = fs.createReadStream(filePath);
      const gunzip = zlib.createGunzip();
      const rl = readline.createInterface({
        input: fileStream.pipe(gunzip),
        crlfDelay: Infinity,
      });

      for await (const line of rl) {
        if (!line.trim()) continue;

        try {
          const row = JSON.parse(line);
          batch.push(row);

          if (batch.length >= BATCH_SIZE) {
            rows += await insertBatch(client, batch);
            batch = [];
          }
        } catch (e) {
          // Skip malformed lines
        }
      }

      if (batch.length > 0) {
        rows += await insertBatch(client, batch);
      }
    } finally {
      client.release();
    }

    totalRows += rows;
    const elapsed = ((Date.now() - fileStart) / 1000).toFixed(1);
    const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    const rate = Math.round(totalRows / ((Date.now() - startTime) / 1000));

    process.stdout.write(
      `[${fileNum + skipped}/${files.length}] ${file}: ${rows.toLocaleString()} rows (${elapsed}s) | Total: ${totalRows.toLocaleString()} | ${rate}/s | ${totalElapsed}min\n`
    );

    fs.writeFileSync(PROGRESS_FILE, file);
  }

  // Update indexer_state
  console.log("\nUpdating indexer_state...");
  const client2 = await pool.connect();
  try {
    await client2.query(`
      UPDATE indexer_state
      SET value = (SELECT MAX(block_height)::text FROM flow_raw_events),
          updated_at = NOW()
      WHERE key = 'last_processed_block'
      AND (SELECT MAX(block_height) FROM flow_raw_events) > value::bigint
    `);
  } finally {
    client2.release();
  }

  const totalMin = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\n=== Import Complete ===`);
  console.log(`Total rows: ${totalRows.toLocaleString()}`);
  console.log(`Time: ${totalMin} minutes`);
  console.log(
    `Rate: ${Math.round(totalRows / ((Date.now() - startTime) / 1000))}/s`
  );

  await pool.end();
}

async function insertBatch(client, batch) {
  if (batch.length === 0) return 0;

  // Build multi-row INSERT with parameterized VALUES
  const values = [];
  const params = [];
  let paramIdx = 1;

  for (const row of batch) {
    // BigQuery exports numbers as strings — cast them
    const blockHeight = parseInt(row.block_height, 10);
    const logIndex = parseInt(row.log_index, 10);
    // Format topics as PostgreSQL array literal: {val1,val2}
    const topicsLiteral =
      "{" + (row.topics || []).map((t) => '"' + t + '"').join(",") + "}";
    // data is a JSON string from BigQuery
    const dataStr =
      typeof row.data === "string"
        ? row.data
        : JSON.stringify(row.data || null);

    values.push(
      `($${paramIdx}, $${paramIdx + 1}, $${paramIdx + 2}, $${paramIdx + 3}, $${paramIdx + 4}::text[], $${paramIdx + 5}::jsonb)`
    );
    params.push(
      blockHeight,
      row.block_timestamp,
      row.transaction_hash,
      logIndex,
      topicsLiteral,
      dataStr
    );
    paramIdx += 6;
  }

  const sql = `
    INSERT INTO flow_raw_events (block_height, block_timestamp, transaction_hash, log_index, topics, data)
    VALUES ${values.join(",\n")}
    ON CONFLICT DO NOTHING
  `;

  const result = await client.query(sql, params);
  return result.rowCount;
}

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
