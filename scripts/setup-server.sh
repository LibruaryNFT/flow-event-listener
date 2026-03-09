#!/usr/bin/env bash
# setup-server.sh — Provision a Hetzner server for the Flow event indexer
#
# Run this on the NEW dedicated server (not the existing Vaultopolis VPS).
# Assumes Ubuntu 24.04 LTS.
#
# Usage: ssh root@<new-server-ip> 'bash -s' < scripts/setup-server.sh

set -euo pipefail

echo "=== Flow Event Indexer — Server Setup ==="
echo ""

# ── System updates ──────────────────────────────────────────
echo "1/7 — Updating system packages..."
apt-get update -q && apt-get upgrade -y -q

# ── PostgreSQL 16 + TimescaleDB ────────────────────────────
echo "2/7 — Installing PostgreSQL 16..."
apt-get install -y -q postgresql-16 postgresql-client-16

echo "3/7 — Installing TimescaleDB..."
# Add TimescaleDB repo
apt-get install -y -q gnupg lsb-release
echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" \
  > /etc/apt/sources.list.d/timescaledb.list
curl -fsSL https://packagecloud.io/timescale/timescaledb/gpgkey | gpg --dearmor -o /etc/apt/trusted.gpg.d/timescaledb.gpg
apt-get update -q
apt-get install -y -q timescaledb-2-postgresql-16

# Configure TimescaleDB
timescaledb-tune --yes --quiet

# Restart PostgreSQL
systemctl restart postgresql

# ── Create database + schema ────────────────────────────────
echo "4/7 — Creating database and schema..."
sudo -u postgres psql -c "CREATE DATABASE vaultopolis_events;"
sudo -u postgres psql -d vaultopolis_events -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

# Schema will be applied separately via: psql vaultopolis_events < sql/00_schema.sql

# ── Create app user ─────────────────────────────────────────
echo "5/7 — Creating indexer user..."
# Generate random password
PGPASS=$(openssl rand -base64 24)
sudo -u postgres psql -c "CREATE ROLE indexer WITH LOGIN PASSWORD '${PGPASS}';"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE vaultopolis_events TO indexer;"
sudo -u postgres psql -d vaultopolis_events -c "GRANT ALL ON SCHEMA public TO indexer;"
sudo -u postgres psql -d vaultopolis_events -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO indexer;"

echo ""
echo "  ╔══════════════════════════════════════════════════════╗"
echo "  ║  SAVE THIS — PostgreSQL connection string:           ║"
echo "  ║                                                      ║"
echo "  ║  postgresql://indexer:${PGPASS}@localhost:5432/vaultopolis_events"
echo "  ║                                                      ║"
echo "  ╚══════════════════════════════════════════════════════╝"
echo ""

# ── Node.js 20 ─────────────────────────────────────────────
echo "6/7 — Installing Node.js 20..."
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y -q nodejs

# ── Systemd service ─────────────────────────────────────────
echo "7/7 — Creating systemd service..."

cat > /etc/systemd/system/flow-indexer.service << 'EOF'
[Unit]
Description=Flow Blockchain Event Indexer
After=network.target postgresql.service
Wants=postgresql.service

[Service]
Type=simple
User=nodeservices
WorkingDirectory=/opt/flow-indexer
ExecStart=/usr/bin/node indexer/index.cjs
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=flow-indexer

# Environment
EnvironmentFile=/opt/flow-indexer/.env

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/flow-indexer

[Install]
WantedBy=multi-user.target
EOF

# Create app user and directory
useradd -r -s /bin/false nodeservices 2>/dev/null || true
mkdir -p /opt/flow-indexer
chown nodeservices:nodeservices /opt/flow-indexer

systemctl daemon-reload
systemctl enable flow-indexer

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next steps:"
echo "  1. Clone the repo:   git clone <repo-url> /opt/flow-indexer"
echo "  2. Create .env:      nano /opt/flow-indexer/.env"
echo "  3. Apply schema:     sudo -u postgres psql vaultopolis_events < /opt/flow-indexer/sql/00_schema.sql"
echo "  4. Install deps:     cd /opt/flow-indexer && npm install --production"
echo "  5. Set ownership:    chown -R nodeservices:nodeservices /opt/flow-indexer"
echo "  6. Start service:    systemctl start flow-indexer"
echo "  7. Check health:     curl http://localhost:8093/health"
echo ""
echo "Required .env vars:"
echo "  FLOW_ACCESS_NODE=https://rest-mainnet.onflow.org"
echo "  POSTGRES_EVENTS_URI=postgresql://indexer:${PGPASS}@localhost:5432/vaultopolis_events"
echo "  INDEXER_HEALTH_PORT=8093"
echo "  NODE_ENV=production"
echo "  LOG_LEVEL=info"
