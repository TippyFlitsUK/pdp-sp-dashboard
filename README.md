# PDP SP Dashboard

Comprehensive monitoring dashboard for PDP Storage Providers on Filecoin Onchain Cloud (FOC), mainnet and calibration.

**Live:** [spdash.ezpdpz.net](http://spdash.ezpdpz.net)

## Features

- **Homepage** – Network-wide stats with per-SP performance cards showing deal/retrieval success rates and SLA pass/fail status (72h). Two-tier sort: SPs with dealbot test traffic first, then everyone else, by id ascending. Cards greyed when no dealbot activity, green when both SLAs pass, red when either fails.
- **Performance** – Dealbot test results (deals, retrieval) with SLA thresholds, timing histograms (first byte, last byte, throughput, IPNI verify), success/failure timelines
- **Activity** – Per-dataset addPieces volume in window, client address via FWSS, revenue cross-reference, activity timeline
- **Proving & Storage** – Proof sets, gap-based fault detection, weekly activity chart, per-dataset detail modals with live proving status
- **Economics** – FilecoinPay rails, settlements, daily revenue chart, FIL + USDFC wallet balances, FilecoinPay account state
- **Logs** – Error/warning timeline, top issues, error patterns, raw log viewer with level filtering (SPs that ship to BetterStack pdp-warp-speed only)

## Data Sources

| Source | What | API |
|--------|------|-----|
| foc-observer (Rod's instance) | All FOC indexed events + live FilecoinPay/FWSS/PDP contract state | REST + SQL |
| BetterStack `pdp-warp-speed` | SP Curio operator logs | ClickHouse SQL |
| BetterStack dealbot Prometheus | Dealbot timing histograms (`_sum`/`_count` pairs) | ClickHouse SQL |
| viem direct to Glif | Native FIL balance + USDFC ERC-20 `balanceOf` | eth_call |
| SP `/pdp/ping` | Liveness probes every 60s | HTTP |

In May 2026 the data layer was migrated off Goldsky subgraphs (PDP Scan + FilecoinPay) and the FWSS StateView viem contract calls onto foc-observer. BetterStack is retained only for the two things observer does not expose: Curio operator logs and dealbot timing histograms.

## SP Discovery

The dashboard dynamically loads all active SPs from observer's `/providers/:network` endpoint. As of May 2026: 28 mainnet, 22 calibration. The set updates with a 5-minute cache.

`lib/sp-overrides.js` maps `providerId → { clientId, hasLogs }` for the small subset of SPs that ship Curio logs to BetterStack — add an entry there when an SP onboards to pdp-warp-speed.

## Stack

- Node.js + Express + vanilla HTML/CSS/JS (no frameworks, no build step)
- Dark theme with Outfit + JetBrains Mono fonts
- Canvas-based charts (no chart library)
- Server-side TTL cache (5 min)

## Setup

```bash
cp .env.example .env
# Fill in BetterStack credentials (see Environment Variables below)
npm install
npm start
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `PORT` | HTTP port (default: 3848) |
| `OBSERVER_URL` | foc-observer base URL (default: `https://foc-observer.va.gg`) |
| `BETTERSTACK_HOST` | SP logs SQL hostname (default: `eu-nbg-2-connect.betterstackdata.com`) |
| `BETTERSTACK_USERNAME` | SP logs SQL username |
| `BETTERSTACK_PASSWORD` | SP logs SQL password |
| `BETTERSTACK_DEALBOT_HOST` | Dealbot metrics SQL hostname (default: `us-east-9-connect.betterstackdata.com`) |
| `BETTERSTACK_DEALBOT_USERNAME` | Dealbot metrics SQL username |
| `BETTERSTACK_DEALBOT_PASSWORD` | Dealbot metrics SQL password |

## Endpoints

| Path | Returns |
|------|---------|
| `/api/health/observer` | Observer connectivity + per-network row counts |
| `/api/config?network=...` | SP list with liveness |
| `/api/network/global` | Network totals |
| `/api/network/overview` | Per-SP merged view (PDP + economics + log health + dealbot status + activity tier) |
| `/api/network/performance` | Per-provider dealbot success/failed counters |
| `/api/sp/:id/proving` | Provider rollup + dataset list + weekly activity |
| `/api/sp/:id/dataset/:setId` | Single dataset detail + live proving state |
| `/api/sp/:id/economics` | Rails + FilecoinPay account + wallet balances |
| `/api/sp/:id/revenue` | Daily settlement aggregation |
| `/api/sp/:id/rail/:railId` | Single rail detail + recent settlements + live state |
| `/api/sp/:id/activity?hours=N` | Per-dataset addPieces + timeline + FWSS client |
| `/api/sp/:id/performance` | Dealbot metrics for the performance tab (per-SP) |
| `/api/sp/:id/logs|log-summary|errors|error-detail|patterns|timeline` | BetterStack-backed log queries |

## Deployment

Deployed on 77.42.75.71 behind nginx, managed by PM2:

```bash
rsync -avz --exclude='node_modules' --exclude='.env' --exclude='.git' . 77.42.75.71:~/pdp-sp-dashboard/
ssh 77.42.75.71 'pm2 restart pdp-sp-dashboard'
```

**Always bump `?v=N` in `public/index.html`** before deploying frontend changes — browsers cache the JS files indefinitely otherwise.

`pm2 restart` does NOT reload `.env`. For env changes use `pm2 delete pdp-sp-dashboard && pm2 start ecosystem.config.cjs`.

## License

MIT
