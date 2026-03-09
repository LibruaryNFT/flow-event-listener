/* config.cjs — Event types, contract addresses, polling config
 *
 * SINGLE SOURCE OF TRUTH for which contracts/events to monitor.
 * Adding a new contract = add entry here + restart indexer.
 * Filtering (which events matter to which product) happens in Layer 2 SQL,
 * NOT here. We capture EVERYTHING from prefix-matched contracts.
 */

/**
 * Contracts captured via prefix match — ALL events from these contracts
 * are stored in flow_raw_events. No JSON payload filtering at capture time.
 */
const PREFIX_CONTRACTS = [
  // ── TopShot ──────────────────────────────────────────────────
  "A.0b2a3299cc857e29.TopShot",
  "A.0b2a3299cc857e29.TopShotLocking",
  "A.0b2a3299cc857e29.PackNFT",
  "A.c1e4f4f4c4257510.Market",
  "A.c1e4f4f4c4257510.TopShotMarketV2",
  "A.c1e4f4f4c4257510.TopShotMarketV3",

  // ── AllDay ───────────────────────────────────────────────────
  "A.e4cf4bdc1751c65d.AllDay",

  // ── Pinnacle ─────────────────────────────────────────────────
  "A.edf9df96c92f4595.Pinnacle",

  // ── TSHOTExchange (currently in monitor.cjs → MongoDB) ──────
  "A.05b67ba314000b2d.TSHOTExchange",
];

/**
 * Contracts captured with JSON payload filtering.
 * We fetch ALL events from these contracts but only STORE events where
 * the specified JSON field matches one of our NFT type prefixes.
 */
const FILTERED_CONTRACTS = [
  {
    address: "A.b8ea91944fd51c43.Offers",
    filterField: "nftType",
  },
  {
    address: "A.b8ea91944fd51c43.OffersV2",
    filterField: "nftType",
  },
  {
    address: "A.4eb8a10cb9f87357.NFTStorefront",
    filterField: "nftType",
  },
  {
    address: "A.4eb8a10cb9f87357.NFTStorefrontV2",
    filterField: "nftType",
  },
  {
    address: "A.3cdbb3d569211ff3.NFTStorefrontV2",
    filterField: "nftType",
  },
  {
    address: "A.1d7e57aa55817448.NonFungibleToken",
    filterField: "type",
  },
];

/**
 * Exact event types to capture (no prefix matching needed).
 */
const EXACT_EVENTS = [
  "A.15f55a75d7843780.Swap.ProposalExecuted",
];

/**
 * NFT type prefixes for JSON payload filtering.
 * Events from FILTERED_CONTRACTS are only stored if the filter field
 * starts with one of these prefixes.
 */
const NFT_TYPE_PREFIXES = [
  "A.0b2a3299cc857e29.TopShot.NFT",
  "A.e4cf4bdc1751c65d.AllDay.NFT",
  "A.edf9df96c92f4595.Pinnacle.NFT",
];

/* ── Polling parameters ── */
const LIVE = { RANGE: 25, DELAY: 2000, IDLE: 10000 };
const CATCH = { RANGE: 250, DELAY: 100, IDLE: 2000 };
const BEHIND_THRESHOLD = 10_000; // blocks behind → switch to CATCH mode

/* ── Retry settings ── */
const RETRIES = 5;
const BACKOFF_MS = 500;
const JITTER_MS = 250;

/* ── Access Node failover ── */
const ACCESS_NODES = [
  "https://rest-mainnet.onflow.org",
  "https://access.mainnet.nodes.onflow.org",
];

module.exports = {
  PREFIX_CONTRACTS,
  FILTERED_CONTRACTS,
  EXACT_EVENTS,
  NFT_TYPE_PREFIXES,
  LIVE,
  CATCH,
  BEHIND_THRESHOLD,
  RETRIES,
  BACKOFF_MS,
  JITTER_MS,
  ACCESS_NODES,
};
