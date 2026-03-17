# PDP SP Dashboard

Comprehensive monitoring dashboard for PDP Storage Providers on Filecoin Onchain Cloud (FOC) mainnet.

**Live:** [spdash.ezpdpz.net](http://spdash.ezpdpz.net)

## Features

- **Homepage** - Network-wide stats with per-SP performance cards showing deal/retrieval success rates and SLA pass/fail status (24h)
- **Performance** - Dealbot test results: deals, retrieval, data retention success rates with SLA thresholds, timing metrics, and timeline charts
- **Proving & Storage** - On-chain proof sets, proving rate, faulted periods, weekly activity charts, and per-dataset detail modals
- **Economics** - FilecoinPay rails, settlements, revenue tracking with cumulative revenue chart and per-rail detail modals
- **Logs** - Error/warning timeline, top issues, error patterns, and recent log viewer with level filtering

## Data Sources

| Source | What | API |
|--------|------|-----|
| Better Stack (pdp-warp-speed) | SP Curio logs | ClickHouse SQL |
| Better Stack (infra_prod) | Dealbot Prometheus metrics | ClickHouse SQL |
| PDP Scan subgraph | On-chain proofs, proof sets, faults | GraphQL |
| FilecoinPay subgraph | Payment rails, settlements, revenue | GraphQL |
| FWSS subgraph | Datasets, storage, pieces | GraphQL |
| SP Registry | Liveness pings via /pdp/ping | HTTP |

## Tracked SPs (Mainnet)

| ID | Name | Logs |
|----|------|------|
| 1 | ezpdpz-main | Yes |
| 2 | beck-main | Yes |
| 5 | Mongo2Stor Mainnet | Yes |
| 7 | infrafolio-mainnet-pdp | Yes |
| 9 | ruka-main | Yes |
| 11 | laughstorage | Yes |
| 14 | pdp-superusey | Yes |

## Stack

- Express + vanilla HTML/CSS/JS (no frameworks, no build step)
- Dark theme with Outfit + JetBrains Mono fonts
- Canvas-based charts (no chart library)
- Server-side caching with TTL

## Setup

```bash
cp .env.example .env
# Fill in Better Stack credentials
npm install
npm start
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `BETTERSTACK_HOST` | SP logs SQL API hostname |
| `BETTERSTACK_USERNAME` | SP logs SQL username |
| `BETTERSTACK_PASSWORD` | SP logs SQL password |
| `BETTERSTACK_DEALBOT_HOST` | Dealbot metrics SQL hostname |
| `BETTERSTACK_DEALBOT_USERNAME` | Dealbot metrics SQL username |
| `BETTERSTACK_DEALBOT_PASSWORD` | Dealbot metrics SQL password |
| `PORT` | HTTP port (default: 3848) |

## Deployment

Deployed on 77.42.75.71 behind nginx, managed by PM2:

```bash
# Deploy updates
rsync -avz --exclude='node_modules' --exclude='.env' . 77.42.75.71:~/pdp-sp-dashboard/
ssh 77.42.75.71 'pm2 restart pdp-sp-dashboard'
```
