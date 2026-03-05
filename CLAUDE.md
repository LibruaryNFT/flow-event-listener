# Flow Event Listener — Claude Code Instructions

## Command Center

This project is part of the LibruaryNFT agent network, coordinated by the command center at `c:\Code\command-center`.

| Resource | Path |
|----------|------|
| Task list | `c:\Code\command-center\TODO.md` |
| Deployment log | `c:\Code\command-center\deployments\log.md` |
| Incident log | `c:\Code\command-center\incidents\log.md` |
| Cost tracker | `c:\Code\command-center\costs\tracker.md` |
| Agent registry | `c:\Code\command-center\agents\registry.md` |

**Session start:** Read this repo's CLAUDE.md (especially Current Status). Check TODO.md for active tasks.

**After completing work:**
1. Update this repo's **Current Status** section below (milestones, what's next)
2. Update command center files: mark TODOs done, log deployments, log incidents
3. Follow commit format and conventions in `c:\Code\command-center\CONVENTIONS.md`

## Current Status

| Milestone | Status | Notes |
|-----------|--------|-------|
| Event listener live on GCP VM | Done | systemd service |
| TSHOT/TSHOTExchange event tracking | Done | Mints, burns, swaps |
| MongoDB state persistence | Done | |

**Last updated:** 2026-03-05

## What This Is

Monitors Flow blockchain events and saves them to MongoDB. Tracks TSHOT/TSHOTExchange contract events (mints, burns, swaps) with support for additional contracts (TopShot, Pinnacle) if uncommented.

## Stack
- Node.js (v16+)
- Flow Access Node (mainnet REST API)
- MongoDB (raw_events + processed_blocks collections)

## Key Files
- `monitor.cjs` — Main event polling loop. Contracts/events to watch are defined here.
- `backfill_wallet_stats.cjs` — Backfill historical wallet statistics
- `package.json` — Dependencies

## Infrastructure
- Runs on GCP VM (us-central1-b) as a systemd service
- Shares VM with pinnacle-pin-bot
- See `c:\Code\command-center\infrastructure\gcp\vm.md` for VM details

## Configuration
- `.env` file with `FLOW_ACCESS_NODE`, `MONGODB_URI`, optional `PROJECT_ID`
- Secrets in GCP Secret Manager, never committed

## Related Repos
- `c:\Code\command-center` — Coordination hub
- `c:\Code\pinnacle-pin-bot` — Shares the same GCP VM
- `c:\Code\vaultopolis` — Consumes event data via shared MongoDB
