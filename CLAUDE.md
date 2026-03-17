# PDP SP Dashboard

## Overview
Comprehensive monitoring dashboard for PDP Storage Providers on Filecoin mainnet. Combines data from Better Stack logs, PDP Scan subgraph, FilecoinPay subgraph, and FWSS subgraph to provide SPs with a unified view of their operations, proofs, storage, revenue, and health.

## Data Sources

### 1. Better Stack (Logs & Metrics)
Team: FOC WG (t468215). See memory file `reference_betterstack_sources.md` for full details.

**SP Logs** (pdp-warp-speed, source 1560290):
- Table: `t468215.pdp_spx`
- Recent: `remote(t468215_pdp_spx_logs)`
- Historical: `s3Cluster(primary, t468215_pdp_spx_s3)` (must use `_row_type = 1`)
- Auth: Basic HTTP auth to `eu-nbg-2-connect.betterstackdata.com`
- Fields: level, logger, msg, err, client_id, curio_version, epoch, gas, piece_cid, took

**Dealbot Logs** (Infra Prod K8s, source 1678395):
- Table: `t468215.infra_prod`
- Filter: `kubernetes.container_name = 'dealbot-worker'`, namespace `dealbot`
- Fields: providerAddress, providerId, dealId, pieceCid, ipfsRootCID, event, durationMs, TTFB

**Dealbot Metrics** (Prometheus, sources 1726029 + 1701980):
- Infra Staging: `remote(t468215_infra_staging_2_metrics)`
- Dealbot staging: `remote(t468215_dealbot_staging_2_metrics)`
- Key metrics: deals_created_total, deal_creation_duration_seconds, retrieval_latency_seconds, retrieval_ttfb_seconds, wallet_balance

### 2. PDP Scan Subgraph (On-Chain Proofs)
- Endpoint: `https://api.goldsky.com/api/public/project_cmdfaaxeuz6us01u359yjdctw/subgraphs/pdp-explorer/mainnet311b/gn`
- IMPORTANT: `mainnet311b` not `311a`
- Entities: Provider, DataSet (proof set), Root (piece), ProvingWindow, FaultRecord, Transaction, NetworkMetric
- Weekly/Monthly activity rollups per provider

### 3. FilecoinPay Subgraph (Revenue)
- Endpoint: `https://api.goldsky.com/api/public/project_cmb9tuo8r1xdw01ykb8uidk7h/subgraphs/filecoin-pay-mainnet/1.0.6/gn`
- Entities: Rail, Settlement, Token, DailyTokenMetric, WeeklyMetric, PaymentsMetric
- Per-SP revenue via payee address on rails

### 4. FWSS Subgraph (Storage/Datasets)
- Endpoint: `https://api.goldsky.com/api/public/project_cmb9tuo8r1xdw01ykb8uidk7h/subgraphs/fwss-mainnet-tim/1.1.0/gn`
- Entities: DataSet, Piece, Account, Fault, GlobalMetric, DailyMetric
- Join key: `DataSet.pdpRailId` -> FilecoinPay `Rail.railId`

## Tracked SPs (Mainnet)
| Provider ID | Name | Address | serviceURL | BS client_id |
|---|---|---|---|---|
| 1 | ezpdpz-main | 0x32c90c...D618 | https://main.ezpdpz.net | ezpdpz-main |
| 2 | beck-main | 0x86d026...B150 | https://pdp-main.660688.xyz:8443 | beck-main |
| 5 | Mongo2Stor Mainnet | 0x010ecc...252c | https://pdp.lotus.dedyn.io | Mongo2Stor Mainnet |
| 7 | infrafolio-mainnet-pdp | 0x89B589...F573 | https://mainnet-pdp.infrafolio.com | infrafolio-mainnet-pdp |
| 9 | ruka-main | 0xB8f10d...a5Cd | https://ruka.drongyl.com | ruka-main |
| 11 | laughstorage | 0x846e8C...89c2 | https://la-pdp.laughstorage.com | la-pdp.laughstorage.com |
| 14 | pdp-superusey | 0xbd0FfC...6C52 | https://pdp.superusey.com | pdp-superusey |

27 total active providers on mainnet (SP Registry: `0xf55dDbf63F1b55c3F1D4FA7e339a68AB7b64A5eB`).

## Cross-Source Join Keys
- SP Registry: providerId <-> wallet address <-> name <-> serviceURL
- FWSS `DataSet.pdpRailId` -> FilecoinPay `Rail.railId` (storage to revenue)
- FWSS `DataSet.providerId` -> PDP Scan `Provider.id` (storage to proofs)
- Better Stack `client_id` -> SP name (manual mapping)
- Dealbot `providerAddress`/`providerId` -> SP Registry

## SP Public Endpoints
- `/pdp/ping` - liveness (200 = alive) -- all SPs except laughstorage (down)
- `/health` - "Service is up and running"
- All other PDP API routes require auth (service key)

## Environment Variables (.env)
- `BETTERSTACK_HOST` - SQL API hostname
- `BETTERSTACK_USERNAME` - ClickHouse SQL username
- `BETTERSTACK_PASSWORD` - ClickHouse SQL password
- `PORT` - HTTP port

## Critical Rules

### Better Stack Query Rules
- Max 4 concurrent queries per user
- Use `?wait=true` to queue instead of reject
- Recent data: `remote()`, Historical: `s3Cluster()` with `_row_type = 1`
- Must use `SELECT dt, raw` (COMMON_COLS) in UNION ALL, NOT `SELECT *`
- s3Cluster and remote have different column sets
- Single quotes in messages must be escaped to `''` for ClickHouse

### Data First -- No Manual Calculations
- ALWAYS introspect ALL subgraph entities and fields BEFORE writing any calculation
- Check related entities (Account, UserToken, DailyMetric, GlobalMetric etc)
- The data you need almost certainly already exists -- display it, don't derive it
- FilecoinPay `UserToken` has funds/payout/fundsCollected -- no need to calculate from epochs
- FilecoinPay `paymentRate` is per Filecoin EPOCH (30s), NOT per second
- Token is USDFC, use `$` prefix, 18 decimals
- `payer`/`payee` on Rails are entity refs -- need `{ id }` sub-selection

### Subgraph Rules
- GraphQL POST to endpoint with `{"query": "..."}` body
- Pagination: `first: 1000, skip: N` (max 1000 per query)
- All BigInt fields returned as strings
- ALWAYS introspect schema with `__type(name: "EntityName") { fields { name } }` before querying
