#!/usr/bin/env bash
# post-import.sh — Run after bulk-import.js completes
# Sets up seamless data continuity and restarts the indexer
set -euo pipefail

DB="flow_events"
BRIDGE_BLOCK=144585503  # BigQuery's last block + 1

echo "=== Post-Import Setup ==="
echo "$(date): Starting post-import tasks"

# 1. Verify import completed
ROW_COUNT=$(sudo -u postgres psql -d $DB -t -c "SELECT COUNT(*) FROM flow_raw_events;")
echo "Total rows in flow_raw_events: $ROW_COUNT"

if [ "$(echo $ROW_COUNT | tr -d ' ')" -lt 100000000 ]; then
  echo "WARNING: Row count seems low (<100M). Import may not be complete."
  echo "Check /opt/flow-indexer/backfill/import.log"
  exit 1
fi

# 2. Set indexer start block to bridge the gap
echo ""
echo "=== Bridging the gap ==="
echo "Setting last_processed_block to $BRIDGE_BLOCK (BigQuery's last block + 1)"
sudo -u postgres psql -d $DB -c "
  UPDATE indexer_state
  SET value = '${BRIDGE_BLOCK}', updated_at = NOW()
  WHERE key = 'last_processed_block';
"

# 3. Re-create indexes (dropped before import for speed)
echo ""
echo "=== Rebuilding indexes ==="
echo "$(date): Creating idx_raw_events_block..."
sudo -u postgres psql -d $DB -c "CREATE INDEX IF NOT EXISTS idx_raw_events_block ON flow_raw_events (block_height);"
echo "$(date): Creating idx_raw_events_topics (GIN)..."
sudo -u postgres psql -d $DB -c "CREATE INDEX IF NOT EXISTS idx_raw_events_topics ON flow_raw_events USING GIN (topics);"
echo "$(date): Creating idx_raw_events_tx..."
sudo -u postgres psql -d $DB -c "CREATE INDEX IF NOT EXISTS idx_raw_events_tx ON flow_raw_events (transaction_hash);"
echo "$(date): Indexes rebuilt"

# 4. Restart the indexer — it'll catch up from bridge block to live
echo ""
echo "=== Restarting indexer ==="
systemctl start flow-indexer
sleep 10
echo "Indexer status:"
systemctl status flow-indexer --no-pager | head -5
echo ""
journalctl -u flow-indexer --no-pager -n 5

# 5. Wait for indexer to catch up before running refresh
echo ""
echo "=== Waiting for indexer to catch up ==="
for i in $(seq 1 60); do
  LAG=$(journalctl -u flow-indexer --no-pager -n 1 | grep -oP '"lag":\K\d+' || echo "unknown")
  if [ "$LAG" != "unknown" ] && [ "$LAG" -lt 100 ]; then
    echo "Indexer caught up (lag: $LAG blocks)"
    break
  fi
  echo "  Waiting... (lag: $LAG)"
  sleep 30
done

# 6. Run the full derived table refresh
echo ""
echo "=== Running derived table refresh ==="
echo "$(date): Starting refresh_all_derived_tables()..."
sudo -u postgres psql -d $DB -c "SELECT refresh_all_derived_tables();" 2>&1
echo "$(date): Refresh complete"

# 7. Final stats
echo ""
echo "=== Final Status ==="
sudo -u postgres psql -d $DB -c "
  SELECT
    (SELECT COUNT(*) FROM flow_raw_events) as raw_events,
    (SELECT value FROM indexer_state WHERE key = 'last_processed_block') as last_block,
    (SELECT MIN(block_height) FROM flow_raw_events) as first_block,
    (SELECT MAX(block_height) FROM flow_raw_events) as latest_block;
"

# 8. Cleanup
echo ""
echo "=== Cleanup ==="
rm -f /tmp/gcs-key.json
echo "Removed service account key"
echo "NOTE: Run these manually when ready:"
echo "  - Delete GCS export bucket: gcloud storage rm -r gs://flow-events-export/"
echo "  - Delete local export files: rm -rf /opt/flow-indexer/backfill/"
echo "  - Revoke SA key: gcloud iam service-accounts keys delete KEY_ID --iam-account=..."

echo ""
echo "$(date): Post-import complete. Pipeline is live!"
