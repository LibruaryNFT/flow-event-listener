# Data Gaps & Known Differences

Last reviewed: 2026-03-10

## Summary

The EX44 PostgreSQL database was bootstrapped from a BigQuery export (raw Flow blockchain events),
then caught up to live via the flow-indexer. The data quality is **equivalent to BigQuery** — they
share the same source (on-chain events) and the same extraction logic.

## Gap 1: TopShot V1 Marketplace Sales (pre-July 2021)

**Status:** Not a gap — same in BigQuery and EX44.

The V1 `Market` contract (`A.c1e4f4f4c4257510.Market`) did NOT emit `MomentPurchased` events
on-chain. The Flow blockchain simply doesn't have these events. Both BigQuery and EX44 have zero
V1 purchase events. The earliest purchase events are from `TopShotMarketV3` starting July 27, 2021.

This means `total_sales_lifetime` and `asp_lifetime` for pre-Series 3 editions (3,545 of 13,886)
undercount true historical sales. **This was already the case in BigQuery/Heroku.**

## Gap 2: TopShot V2 Marketplace — Zero Events

**Status:** Not a gap — same in BigQuery and EX44.

The V2 `TopShotMarketV2` contract was extremely short-lived and emitted zero purchase events.
Both BigQuery and EX44 have zero V2 events.

## Gap 3: NFTStorefront V1 Sales — Not Extracted

**Status:** Potential gap — needs investigation.

We have 5.4M `NFTStorefront.ListingCompleted` raw events (Oct 2021+) from third-party marketplaces.
These are NOT currently extracted into `topshot_sales`. BigQuery's `topshot_sales` also does not
include NFTStorefront V1 — it only uses `MomentPurchasedV3`, `MomentPurchasedFlowty`, and
`OfferCompleted`. **So this is not a regression from BigQuery.**

If we wanted to add these, it would increase coverage but would also change numbers vs Heroku.

## Gap 4: Minor Lifetime Sales Count Differences

**Status:** Expected — timing differences.

Example: LeBron 4_133 shows 20 sales on Heroku vs 15 on EX44. Both use the same extraction logic
from `topshot_events`. The difference comes from:
- Different data sync timing (Heroku MongoDB was last synced from a BigQuery run at a different time)
- Possible slight differences in OfferCompleted event matching due to JSON structure

The 180d/30d/7d windows match exactly. Lifetime counts differ by small amounts on some editions.

## Gap 5: MongoDB Metadata Fields Not in Market Data Table

**Status:** Expected — by design.

Heroku serves from a MongoDB "combined" collection that merges market data with TopShot metadata
(plays, sets, tiers). The EX44 `topshot_edition_market_data` table contains only market analytics.

Fields in Heroku but not EX44:
- `momentCount`, `retired`, `locked`, `locked_count` — edition state metadata
- `tierSource`, `playOrder`, `edition_status` — classification metadata
- `subedition_name`, `subeditionID`, `subeditions` — parallel/subedition info
- `JerseyNumber`, `name`, `FullName`, `TeamAtMoment` — player metadata (we have `player_name`, `team`)
- `market_data_source`, `market_data_updatedAt`, `mergedAt` — ETL provenance
- `snapshot_rank`, `rank_change` — historical ranking

The frontend uses some of these in detail views. For the parallel run, detail views can continue
using Heroku while the listing/search endpoints use EX44.

## Gap 6: Field Naming Differences

**Status:** Expected — different source systems.

| Heroku (MongoDB) | EX44 (PostgreSQL) | Notes |
|---|---|---|
| `FullName` | `player_name` | Frontend checks both |
| `TeamAtMoment` | `team` | Frontend checks both |
| `PlayCategory` | `play_category` | Frontend checks both |
| `momentCount` | `existing_supply` | Different field name |

## Verified: Data That Matches

These fields were validated against Heroku and match within expected tolerance:
- `grail_score` — matches (after serial extraction bug fix)
- `asp_180d`, `asp_30d`, `asp_7d` — matches
- `estimated_value` — matches
- `floor_price`, `floor_price_dapper`, `floor_price_flowty` — matches
- `total_sales_180d`, `total_sales_30d`, `total_sales_7d` — matches
- `highest_edition_offer`, `edition_offer_count` — minor real-time drift (offers change constantly)
- `total_listings`, `listings_at_floor` — matches
- All normalized scores (liquidity, sales_activity, etc.) — matches

## Bugs Found and Fixed (2026-03-10)

1. **Serial number extraction path** — `raw_data->'metadata'->>'serialNumber'` was wrong;
   TopShot MomentMinted events store serialNumber at `raw_data->>'serialNumber'` (root level).
   Fixed in `03_topshot_edition_market_data.sql`.

2. **Unsafe bigint casts** — `expiry`, `serialNumber`, `jersey_number` fields could contain
   non-numeric values (`<invalid Value>`). Added regex guards (`~ '^[0-9]+$'`) to all casts.
   Fixed in `03_topshot_edition_market_data.sql`.
