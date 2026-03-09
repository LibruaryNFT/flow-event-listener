#!/usr/bin/env bash
# import-bigquery-export.sh — Stream BigQuery JSONL export into PostgreSQL flow_raw_events
#
# Usage: bash scripts/import-bigquery-export.sh /path/to/export/dir
#
# Expects directory of *.jsonl.gz files exported from BigQuery.
# Each line is JSON: {block_height, block_timestamp, transaction_hash, log_index, topics, data}
#
# The script:
#   1. Decompresses each file
#   2. Transforms JSONL → PostgreSQL COPY format (tab-separated)
#   3. Streams into flow_raw_events via psql \copy
#   4. Tracks progress and can resume from last completed file

set -euo pipefail

EXPORT_DIR="${1:?Usage: $0 /path/to/export/dir}"
DB="${PGDATABASE:-flow_events}"
PROGRESS_FILE="${EXPORT_DIR}/.import-progress"
TOTAL_ROWS=0
SKIPPED_FILES=0

# Check dependencies
command -v jq >/dev/null 2>&1 || { echo "ERROR: jq required. Install with: apt-get install -y jq"; exit 1; }
command -v psql >/dev/null 2>&1 || { echo "ERROR: psql required."; exit 1; }

# Count files
FILES=($(ls -1 "${EXPORT_DIR}"/events-*.jsonl.gz 2>/dev/null | sort))
TOTAL_FILES=${#FILES[@]}

if [ "$TOTAL_FILES" -eq 0 ]; then
  echo "ERROR: No events-*.jsonl.gz files found in ${EXPORT_DIR}"
  exit 1
fi

echo "=== BigQuery → PostgreSQL Import ==="
echo "Files: ${TOTAL_FILES}"
echo "Database: ${DB}"
echo "Progress file: ${PROGRESS_FILE}"
echo ""

# Resume support: skip already imported files
LAST_DONE=""
if [ -f "$PROGRESS_FILE" ]; then
  LAST_DONE=$(cat "$PROGRESS_FILE")
  echo "Resuming from after: ${LAST_DONE}"
fi

STARTED=false
if [ -z "$LAST_DONE" ]; then
  STARTED=true
fi

for f in "${FILES[@]}"; do
  BASENAME=$(basename "$f")

  # Skip already imported files
  if [ "$STARTED" = false ]; then
    if [ "$BASENAME" = "$LAST_DONE" ]; then
      STARTED=true
      SKIPPED_FILES=$((SKIPPED_FILES + 1))
      continue
    fi
    SKIPPED_FILES=$((SKIPPED_FILES + 1))
    continue
  fi

  FILE_NUM=$((SKIPPED_FILES + 1))
  echo -n "[${FILE_NUM}/${TOTAL_FILES}] ${BASENAME} ... "

  # Decompress → transform JSON to tab-separated COPY format → pipe to psql
  # Format: block_height\tblock_timestamp\ttransaction_hash\tlog_index\t{topics_array}\tdata_json
  ROWS=$(zcat "$f" | jq -r '
    [
      (.block_height | tostring),
      .block_timestamp,
      .transaction_hash,
      (.log_index | tostring),
      ("{" + ([.topics[] | "\"" + . + "\""] | join(",")) + "}"),
      (.data // "null")
    ] | @tsv
  ' | psql -d "$DB" -c "
    COPY flow_raw_events (block_height, block_timestamp, transaction_hash, log_index, topics, data)
    FROM STDIN WITH (FORMAT text)
  " 2>&1 | grep -oP 'COPY \K\d+' || echo "0")

  TOTAL_ROWS=$((TOTAL_ROWS + ROWS))
  echo "${ROWS} rows (total: ${TOTAL_ROWS})"

  # Record progress
  echo "$BASENAME" > "$PROGRESS_FILE"
  SKIPPED_FILES=$((SKIPPED_FILES + 1))
done

echo ""
echo "=== Import Complete ==="
echo "Total rows imported: ${TOTAL_ROWS}"
echo "Files processed: ${TOTAL_FILES}"

# Update indexer_state to bridge the gap
echo "Updating indexer_state with max block from imported data..."
psql -d "$DB" -c "
  UPDATE indexer_state
  SET value = (SELECT MAX(block_height)::text FROM flow_raw_events),
      updated_at = NOW()
  WHERE key = 'last_processed_block'
  AND (SELECT MAX(block_height) FROM flow_raw_events) > value::bigint;
"
echo "Done. Indexer will continue from the latest block."
