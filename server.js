require("dotenv").config()
const express = require("express")
const path = require("path")
const { Cache } = require("./lib/cache")
const { getAllSPs, getTrackedSPs, getSP } = require("./lib/sp-config")
const { getEndpoints, gqlFetch, gqlPaginate } = require("./lib/subgraph")
const {
  queryBetterStack, queryDealbot, validateHours, timeBucket, getDealbotMetrics,
  RECENT_TABLE, HISTORICAL_TABLE, COMMON_COLS, SUPPRESS_FILTER, CID_CONTACT_FILTER,
} = require("./lib/betterstack")
const { startLivenessProbes, getLiveness } = require("./lib/liveness")
const { getDataSetInfoBatch, getWalletBalances } = require("./lib/fwss")

const app = express()
const PORT = parseInt(process.env.PORT, 10) || 3848
const cache = new Cache()

const SUBGRAPH_TTL = 5 * 60 * 1000  // 5 min
const BS_TTL = 5 * 60 * 1000          // 5 min (SP logs — historical queries over hours, no need for 1 min)
const DEALBOT_TTL = 5 * 60 * 1000    // 5 min (dealbot tests run every ~45 min)

function shouldRefresh(req) {
  return req.query.refresh === "1"
}

function getCached(req, key) {
  if (shouldRefresh(req)) return null
  return cache.get(key)
}

app.use(express.static(path.join(__dirname, "public")))

// --- Helpers ---

function parseNetwork(req) {
  return req.query.network === "calibration" ? "calibration" : "mainnet"
}

function getClientIds(network) {
  return getTrackedSPs(network).map(sp => `'${sp.clientId}'`).join(", ")
}

// --- API Routes ---

// GET /api/config — SP list with liveness
app.get("/api/config", (req, res) => {
  const network = parseNetwork(req)
  const liveness = getLiveness(network)
  const sps = getAllSPs(network).map(sp => ({
    ...sp,
    liveness: liveness[sp.id] || { alive: null, latencyMs: null, lastCheck: null },
  }))
  res.json(sps)
})

// GET /api/network/global — NetworkMetric + GlobalMetric totals
app.get("/api/network/global", async (req, res) => {
  const network = parseNetwork(req)
  const endpoints = getEndpoints(network)
  const cacheKey = `network:global:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const pdpData = await gqlFetch(endpoints.pdpScan, `{
      networkMetrics(first: 1) {
        totalProviders
        totalProofSets
        totalRoots
        totalDataSize
        totalProofs
        totalFaultedPeriods
      }
    }`)

    const pdp = pdpData.networkMetrics?.[0] || {}

    const result = {
      providers: Number(pdp.totalProviders || 0),
      proofSets: Number(pdp.totalProofSets || 0),
      roots: Number(pdp.totalRoots || 0),
      dataSize: pdp.totalDataSize || "0",
      storageSize: pdp.totalDataSize || "0",
      proofsSubmitted: Number(pdp.totalProofs || 0),
      faultedPeriods: Number(pdp.totalFaultedPeriods || 0),
    }

    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error("network/global error:", err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/network/overview — unified SP data from all sources
app.get("/api/network/overview", async (req, res) => {
  const network = parseNetwork(req)
  const endpoints = getEndpoints(network)
  const CLIENT_IDS = getClientIds(network)
  const cacheKey = `network:overview:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    // Parallel fetch all data sources
    const [pdpProviders, filpayRails, bsOverview, bsVersions] = await Promise.all([
      // PDP Scan providers
      gqlFetch(endpoints.pdpScan, `{
        providers(first: 100) {
          id
          address
          totalProofSets
          totalRoots
          totalDataSize
          totalFaultedPeriods
          totalProvingPeriods
        }
      }`).then(d => d.providers || []).catch(() => []),

      // FilecoinPay active rails
      gqlPaginate(endpoints.filecoinPay, "rails", `
        railId
        paymentRate
        state
        totalSettledAmount
        payer { id }
        payee { id }
      `, 'state: "ACTIVE"').catch(() => []),

      // Better Stack overview (tracked SPs)
      CLIENT_IDS ? queryBetterStack(`
        SELECT
          JSONExtract(raw, 'client_id', 'Nullable(String)') AS sp,
          CASE WHEN ${CID_CONTACT_FILTER} THEN 'cid.contact'
               ELSE JSONExtract(raw, 'level', 'Nullable(String)') END AS level,
          count(*) AS cnt,
          max(dt) AS last_seen
        FROM (
          SELECT ${COMMON_COLS} FROM ${RECENT_TABLE}
          WHERE dt > now() - INTERVAL 24 HOUR
            AND JSONExtract(raw, 'client_id', 'Nullable(String)') IN (${CLIENT_IDS})
            AND JSONExtract(raw, 'level', 'Nullable(String)') IS NOT NULL
            AND JSONExtract(raw, 'level', 'Nullable(String)') != ''
            ${SUPPRESS_FILTER}
          UNION ALL
          SELECT ${COMMON_COLS} FROM ${HISTORICAL_TABLE}
          WHERE _row_type = 1
            AND dt > now() - INTERVAL 24 HOUR
            AND JSONExtract(raw, 'client_id', 'Nullable(String)') IN (${CLIENT_IDS})
            AND JSONExtract(raw, 'level', 'Nullable(String)') IS NOT NULL
            AND JSONExtract(raw, 'level', 'Nullable(String)') != ''
            ${SUPPRESS_FILTER}
        )
        GROUP BY sp, level
        FORMAT JSONEachRow`).catch(() => []) : Promise.resolve([]),

      // Better Stack curio versions
      CLIENT_IDS ? queryBetterStack(`
        SELECT
          JSONExtract(raw, 'client_id', 'Nullable(String)') AS sp,
          argMax(JSONExtract(raw, 'curio_version', 'Nullable(String)'), dt) AS curio_version
        FROM (
          SELECT ${COMMON_COLS} FROM ${RECENT_TABLE}
          WHERE dt > now() - INTERVAL 24 HOUR
            AND JSONExtract(raw, 'client_id', 'Nullable(String)') IN (${CLIENT_IDS})
            AND JSONExtract(raw, 'curio_version', 'Nullable(String)') IS NOT NULL
            AND JSONExtract(raw, 'curio_version', 'Nullable(String)') NOT IN ('', 'Error parsing language')
          UNION ALL
          SELECT ${COMMON_COLS} FROM ${HISTORICAL_TABLE}
          WHERE _row_type = 1
            AND dt > now() - INTERVAL 24 HOUR
            AND JSONExtract(raw, 'client_id', 'Nullable(String)') IN (${CLIENT_IDS})
            AND JSONExtract(raw, 'curio_version', 'Nullable(String)') IS NOT NULL
            AND JSONExtract(raw, 'curio_version', 'Nullable(String)') NOT IN ('', 'Error parsing language')
        )
        GROUP BY sp
        FORMAT JSONEachRow`).catch(() => []) : Promise.resolve([]),
    ])

    // Build per-provider index from PDP Scan (keyed by lowercase address)
    const providerMap = {}
    for (const p of pdpProviders) {
      providerMap[p.address.toLowerCase()] = {
        pdp: {
          proofSets: Number(p.totalProofSets || 0),
          roots: Number(p.totalRoots || 0),
          dataSize: p.totalDataSize || "0",
          faultedPeriods: Number(p.totalFaultedPeriods || 0),
          provingPeriods: Number(p.totalProvingPeriods || 0),
        },
      }
    }

    // Map FilecoinPay rails by payee address (lowercase)
    const railsByPayee = {}
    for (const r of filpayRails) {
      const addr = (r.payee?.id || r.payee || "").toLowerCase()
      if (!railsByPayee[addr]) railsByPayee[addr] = { activeRails: 0, totalRate: BigInt(0), totalSettled: BigInt(0) }
      railsByPayee[addr].activeRails++
      railsByPayee[addr].totalRate += BigInt(r.paymentRate || 0)
      railsByPayee[addr].totalSettled += BigInt(r.totalSettledAmount || 0)
    }

    // Better Stack log counts by SP
    const bsByClient = {}
    for (const row of bsOverview) {
      if (!bsByClient[row.sp]) bsByClient[row.sp] = { errors: 0, warns: 0, info: 0, cid_contact: 0, last_seen: null }
      if (row.level === "error") bsByClient[row.sp].errors += row.cnt
      else if (row.level === "warn") bsByClient[row.sp].warns += row.cnt
      else if (row.level === "info") bsByClient[row.sp].info += row.cnt
      else if (row.level === "cid.contact") bsByClient[row.sp].cid_contact += row.cnt
      if (row.last_seen) {
        if (!bsByClient[row.sp].last_seen || row.last_seen > bsByClient[row.sp].last_seen) {
          bsByClient[row.sp].last_seen = row.last_seen
        }
      }
    }

    // Curio versions
    const versionByClient = {}
    for (const v of bsVersions) {
      versionByClient[v.sp] = v.curio_version
    }

    // Merge all into SP list
    const liveness = getLiveness(network)
    const result = getAllSPs(network).map(sp => {
      const addr = sp.address.toLowerCase()
      const pdp = providerMap[addr]?.pdp || null
      const rails = railsByPayee[addr]
      const bs = sp.hasLogs ? bsByClient[sp.clientId] || null : null
      const version = sp.hasLogs ? versionByClient[sp.clientId] || null : null

      return {
        id: sp.id,
        name: sp.name,
        address: sp.address,
        hasLogs: sp.hasLogs,
        endorsed: sp.endorsed || false,
        liveness: liveness[sp.id] || null,
        pdp,
        economics: rails ? {
          activeRails: rails.activeRails,
          totalRate: rails.totalRate.toString(),
          totalSettled: rails.totalSettled.toString(),
        } : null,
        logHealth: bs,
        curioVersion: version,
      }
    })

    cache.set(cacheKey, result, BS_TTL)
    res.json(result)
  } catch (err) {
    console.error("network/overview error:", err.message)
    res.status(500).json({ error: err.message })
  }
})

// --- SP Detail Routes ---

// GET /api/sp/:id/proving
app.get("/api/sp/:id/proving", async (req, res) => {
  const network = parseNetwork(req)
  const endpoints = getEndpoints(network)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const cacheKey = `sp:${sp.id}:proving:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const [providerData, dataSets, weeklyData] = await Promise.all([
      gqlFetch(endpoints.pdpScan, `{
        provider(id: "${addr}") {
          totalProofSets
          totalRoots
          totalDataSize
          totalFaultedPeriods
          totalProvingPeriods
          totalFaultedRoots
          createdAt
        }
      }`),

      gqlPaginate(endpoints.pdpScan, "dataSets", `
        id setId leafCount totalRoots totalDataSize isActive
        lastProvenEpoch nextChallengeEpoch nextDeadline
        totalFaultedRoots totalFaultedPeriods createdAt
      `, `owner: "${addr}"`, "setId"),

      gqlFetch(endpoints.pdpScan, `{
        weeklyProviderActivities(first: 20, where: {provider: "${addr}"}, orderBy: id, orderDirection: desc) {
          id
          totalProofs
          totalRootsProved
          totalFaultedRoots
          totalFaultedPeriods
          totalRootsAdded
          totalDataSizeAdded
          totalProofSetsCreated
        }
      }`),
    ])

    const data = {
      provider: providerData.provider,
      dataSets: dataSets.reverse(),
      weeklyProviderActivities: weeklyData.weeklyProviderActivities || [],
    }

    // Determine status: PDP Scan isActive is always true, so use totalRoots/totalDataSize as heuristic
    for (const ds of (data.dataSets || [])) {
      const hasData = Number(ds.totalRoots || 0) > 0 || ds.totalDataSize !== "0"
      ds.status = hasData ? "Active" : "Terminated"
    }

    // Fault data comes from DataSet.totalFaultedPeriods and WeeklyProviderActivity.totalFaultedPeriods
    // (FaultRecord entity is empty on both networks — PDP Scan website uses these embedded fields)
    const result = {
      provider: data.provider,
      dataSets: data.dataSets || [],
      weeklyActivity: data.weeklyProviderActivities || [],
    }

    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/proving error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/dataset/:setId — single dataset detail from PDP Scan
app.get("/api/sp/:id/dataset/:setId", async (req, res) => {
  const network = parseNetwork(req)
  const endpoints = getEndpoints(network)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  const setId = req.params.setId
  if (!/^\d+$/.test(setId)) return res.status(400).json({ error: "Invalid setId" })

  const cacheKey = `sp:${sp.id}:dataset:${setId}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const pdpData = await gqlFetch(endpoints.pdpScan, `{
      dataSets(first: 1, where: {setId: "${setId}", owner: "${addr}"}) {
        setId leafCount challengeRange isActive
        lastProvenEpoch nextChallengeEpoch nextDeadline firstDeadline
        maxProvingPeriod challengeWindowSize currentDeadlineCount provenThisPeriod
        totalRoots nextPieceId totalDataSize totalProofs totalProvedRoots
        totalFeePaid totalFaultedPeriods totalFaultedRoots
        totalTransactions totalEventLogs createdAt updatedAt blockNumber
      }
    }`).then(d => d.dataSets?.[0] || null).catch(() => null)

    if (!pdpData) return res.status(404).json({ error: "Dataset not found" })

    const result = { pdp: pdpData, fwss: null }
    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/dataset/${setId} error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/revenue — daily settlement history
app.get("/api/sp/:id/revenue", async (req, res) => {
  const network = parseNetwork(req)
  const endpoints = getEndpoints(network)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const cacheKey = `sp:${sp.id}:revenue:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const settlements = await gqlPaginate(endpoints.filecoinPay, "settlements", `
      totalNetPayeeAmount
      createdAt
    `, `rail_: {payee: "${addr}"}, totalNetPayeeAmount_gt: "0"`, "createdAt")

    // Aggregate by day
    const byDay = {}
    for (const s of settlements) {
      const d = new Date(Number(s.createdAt) * 1000)
      const key = d.toISOString().slice(0, 10)
      if (!byDay[key]) byDay[key] = 0
      byDay[key] += Number(s.totalNetPayeeAmount) / 1e18
    }

    const days = Object.keys(byDay).sort()
    const result = days.map(d => ({ date: d, revenue: byDay[d] }))

    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/revenue error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/economics
app.get("/api/sp/:id/economics", async (req, res) => {
  const network = parseNetwork(req)
  const endpoints = getEndpoints(network)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const cacheKey = `sp:${sp.id}:economics:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const [rails, accountData, walletBalances] = await Promise.all([
      gqlPaginate(endpoints.filecoinPay, "rails", `
        railId
        paymentRate
        state
        totalSettledAmount
        settledUpto
        totalSettlements
        payer { id }
        payee { id }
        createdAt
      `, `payee: "${addr}"`, "railId"),

      gqlFetch(endpoints.filecoinPay, `{
        userTokens(where: {account: "${addr}"}) {
          funds
          payout
          fundsCollected
          lockupCurrent
          lockupRate
          lockupLastSettledUntilTimestamp
          token { symbol decimals }
        }
      }`).catch(() => ({ userTokens: [] })),

      getWalletBalances(addr, network).catch(() => null),
    ])

    // Sum totalSettledAmount directly from rails
    let totalSettled = BigInt(0)
    for (const r of rails) {
      totalSettled += BigInt(r.totalSettledAmount || 0)
    }

    // Account balance from subgraph
    const ut = accountData.userTokens?.[0] || {}

    const result = {
      rails,
      account: {
        funds: ut.funds || "0",
        payout: ut.payout || "0",
        fundsCollected: ut.fundsCollected || "0",
        lockupCurrent: ut.lockupCurrent || "0",
        lockupRate: ut.lockupRate || "0",
        lastSettled: ut.lockupLastSettledUntilTimestamp || null,
        token: ut.token?.symbol || "USDFC",
      },
      summary: {
        activeRails: rails.filter(r => r.state === "ACTIVE").length,
        totalRails: rails.length,
        totalSettled: totalSettled.toString(),
      },
      wallet: walletBalances,
    }

    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/economics error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/rail/:railId — single rail detail
app.get("/api/sp/:id/rail/:railId", async (req, res) => {
  const network = parseNetwork(req)
  const endpoints = getEndpoints(network)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  const railId = req.params.railId
  if (!/^\d+$/.test(railId)) return res.status(400).json({ error: "Invalid railId" })

  const cacheKey = `sp:${sp.id}:rail:${railId}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const data = await gqlFetch(endpoints.filecoinPay, `{
      rails(first: 1, where: {railId: "${railId}", payee: "${addr}"}) {
        railId
        paymentRate
        lockupFixed
        lockupPeriod
        settledUpto
        state
        endEpoch
        commissionRateBps
        totalOneTimePaymentAmount
        totalSettledAmount
        totalOneTimePayments
        totalSettlements
        totalRateChanges
        createdAt
        payer { id }
        payee { id }
        operator { id }
        token { symbol decimals }
        settlements(first: 20, orderBy: createdAt, orderDirection: desc) {
          totalSettledAmount
          totalNetPayeeAmount
          networkFee
          operatorCommission
          settledUpto
          createdAt
          txHash
        }
      }
    }`)

    const rail = data.rails?.[0]
    if (!rail) return res.status(404).json({ error: "Rail not found" })

    const result = { rail, dataset: null }
    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/rail/${railId} error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})


// GET /api/sp/:id/activity?hours=N — per-dataset activity analysis (addPieces volume, revenue cross-ref)
app.get("/api/sp/:id/activity", async (req, res) => {
  const network = parseNetwork(req)
  const endpoints = getEndpoints(network)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const hours = validateHours(req.query.hours)
  const cacheKey = `sp:${sp.id}:activity:${hours}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const cutoff = Math.floor(Date.now() / 1000) - (hours * 3600)

    // Parallel: datasets, recent addPieces txs, recent roots (data size), weekly activity, FilecoinPay rails
    const [dataSets, recentTxs, recentRoots, weeklyActivity, rails] = await Promise.all([
      gqlPaginate(endpoints.pdpScan, "dataSets", `
        setId totalRoots totalDataSize totalTransactions totalFeePaid
        createdAt updatedAt isActive
      `, `owner: "${addr}"`, "setId"),

      // Recent transactions — paginate using createdAt cursor to avoid skip limit
      (async () => {
        let all = []
        let cursor = cutoff
        const pageSize = 1000
        for (let i = 0; i < 20; i++) {
          const data = await gqlFetch(endpoints.pdpScan, `{
            transactions(first: ${pageSize}, orderBy: createdAt, orderDirection: asc,
              where: {proofSet_: {owner: "${addr}"}, createdAt_gt: "${cursor}", method: "addPieces"}) {
              dataSetId createdAt
            }
          }`)
          const items = data.transactions || []
          all.push(...items)
          if (items.length < pageSize) break
          cursor = items[items.length - 1].createdAt
        }
        return all
      })(),

      // Recent roots — paginate to sum actual data size added per dataset
      (async () => {
        let all = []
        let cursor = cutoff
        const pageSize = 1000
        for (let i = 0; i < 20; i++) {
          const data = await gqlFetch(endpoints.pdpScan, `{
            roots(first: ${pageSize}, orderBy: createdAt, orderDirection: asc,
              where: {proofSet_: {owner: "${addr}"}, createdAt_gt: "${cursor}"}) {
              setId rawSize createdAt
            }
          }`)
          const items = data.roots || []
          all.push(...items)
          if (items.length < pageSize) break
          cursor = items[items.length - 1].createdAt
        }
        return all
      })(),

      // Weekly proof set activity for all SP datasets (last 12 weeks)
      gqlPaginate(endpoints.pdpScan, "weeklyProofSetActivities", `
        id dataSetId totalRootsAdded totalDataSizeAdded totalProofs totalFaultedPeriods
      `, `proofSet_: {owner: "${addr}"}`, "id"),

      // FilecoinPay rails for revenue cross-reference
      gqlPaginate(endpoints.filecoinPay, "rails", `
        railId paymentRate state payer { id } totalSettledAmount totalSettlements createdAt
      `, `payee: "${addr}"`, "railId"),
    ])

    // Sum recent data size per dataset from actual roots
    const recentSizeByDataset = {}
    for (const root of recentRoots) {
      const dsId = root.setId
      recentSizeByDataset[dsId] = (recentSizeByDataset[dsId] || 0n) + BigInt(root.rawSize || 0)
    }

    // Count addPieces per dataset and build timeline buckets
    const txByDataset = {}
    const timelineBuckets = {}
    // Bucket size: <=6h -> 15min, <=24h -> 1h, <=72h -> 3h, else 6h
    const bucketSec = hours <= 6 ? 900 : hours <= 24 ? 3600 : hours <= 72 ? 10800 : 21600
    for (const tx of recentTxs) {
      txByDataset[tx.dataSetId] = (txByDataset[tx.dataSetId] || 0) + 1
      const bucket = Math.floor(Number(tx.createdAt) / bucketSec) * bucketSec
      const key = `${bucket}:${tx.dataSetId}`
      timelineBuckets[key] = (timelineBuckets[key] || 0) + 1
    }
    // Convert to array: [{time, dataSetId, count}]
    const timeline = Object.entries(timelineBuckets).map(([key, count]) => {
      const [time, dataSetId] = key.split(":")
      return { time: Number(time), dataSetId, count }
    })

    // Parse weekly activity into per-dataset arrays
    const weeklyByDataset = {}
    for (const w of weeklyActivity) {
      const dsId = w.dataSetId
      if (!weeklyByDataset[dsId]) weeklyByDataset[dsId] = []
      weeklyByDataset[dsId].push(w)
    }

    // Index rails by railId for fast lookup
    const railById = {}
    for (const r of rails) {
      railById[r.railId] = r
    }

    // Only fetch FWSS data for datasets with activity (saves RPC calls)
    const activeSetIds = Object.keys(txByDataset)
    const fwssData = activeSetIds.length > 0
      ? await getDataSetInfoBatch(activeSetIds, network)
      : {}

    // Build per-dataset activity with FWSS cross-references
    let datasets = dataSets.map(ds => {
      const dsId = ds.setId
      const recentTxCount = txByDataset[dsId] || 0
      const weekly = (weeklyByDataset[dsId] || [])
        .sort((a, b) => a.id > b.id ? -1 : 1)
        .slice(0, 12)

      const fwss = fwssData[dsId] || null
      const client = fwss?.payer || null
      const pdpRailId = fwss?.pdpRailId || null
      const appMetadata = fwss?.appMetadata || null

      // Find linked rail via FWSS pdpRailId
      let dailyRevenue = 0
      let railState = null
      if (pdpRailId && railById[String(pdpRailId)]) {
        const rail = railById[String(pdpRailId)]
        railState = rail.state
        if (rail.state === "ACTIVE" && rail.paymentRate !== "0") {
          dailyRevenue = (Number(rail.paymentRate) / 1e18) * 2880
        }
      }

      const hasData = Number(ds.totalRoots || 0) > 0 || ds.totalDataSize !== "0"

      return {
        setId: dsId,
        status: hasData ? "Active" : "Terminated",
        totalRoots: ds.totalRoots,
        totalDataSize: ds.totalDataSize,
        totalTransactions: ds.totalTransactions,
        totalFeePaid: ds.totalFeePaid,
        createdAt: ds.createdAt,
        updatedAt: ds.updatedAt,
        recentAddPieces: recentTxCount,
        recentDataSize: (recentSizeByDataset[dsId] || 0n).toString(),
        client,
        appMetadata,
        pdpRailId,
        railState,
        dailyRevenue,
        weeklyActivity: weekly,
      }
    })

    // Filter to only datasets with activity in the period, sort by most active
    datasets = datasets.filter(ds => ds.recentAddPieces > 0)
    datasets.sort((a, b) => b.recentAddPieces - a.recentAddPieces)

    const totalRecentTx = recentTxs.length
    const truncated = recentTxs.length >= 20000

    const result = {
      datasets,
      totalRecentAddPieces: totalRecentTx,
      truncated,
      hours,
      timeline,
    }

    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/activity error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// Dealbot counter delta SQL — matches Better Stack dashboard pattern exactly:
// 1. avgMerge(value_avg) grouped by series_id to get per-pod values
// 2. sum() across series to get total per time bucket
// 3. lagInFrame to compute deltas between consecutive buckets
// 4. greatest(delta, 0) to handle counter resets
function dealbotDeltaSql(metricName, hours, providerFilter, checkTypeFilter, metricsTable) {
  const ctFilter = checkTypeFilter
    ? `AND JSONExtract(tags, 'checkType', 'Nullable(String)') = '${checkTypeFilter}'`
    : ""
  return `
    WITH raw AS (
      SELECT toStartOfFiveMinutes(dt) AS time,
        if(startsWith(JSONExtract(tags, 'value', 'Nullable(String)'), 'failure'), 'failure',
          JSONExtract(tags, 'value', 'Nullable(String)')) AS status,
        avgMerge(value_avg) AS inner_value
      FROM ${metricsTable}
      WHERE name = '${metricName}'
        AND dt > now() - INTERVAL ${hours} HOUR
        AND JSONExtract(tags, 'value', 'Nullable(String)') != 'pending'
        AND ${providerFilter}
        ${ctFilter}
      GROUP BY time, status, series_id
    ),
    series_values AS (
      SELECT time, status, sum(inner_value) AS value
      FROM raw GROUP BY time, status
    ),
    series_deltas AS (
      SELECT status,
        if(isNull(prev_value), 0, greatest(value - prev_value, 0)) AS delta
      FROM (
        SELECT time, status, value,
          lagInFrame(value) OVER (PARTITION BY status ORDER BY time) AS prev_value
        FROM series_values
      )
    )
    SELECT status AS value, toUInt64(round(sum(delta))) AS cnt
    FROM series_deltas
    GROUP BY status
    ORDER BY status
    FORMAT JSONEachRow`
}

// GET /api/network/performance — bulk dealbot performance for all providers (72h for 200-sample SLA)
app.get("/api/network/performance", async (req, res) => {
  const network = parseNetwork(req)
  const DEALBOT_METRICS = getDealbotMetrics(network)
  const cacheKey = `network:performance:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  const networkFilter = network === "mainnet"
    ? `AND JSONExtract(tags, 'network', 'Nullable(String)') = 'mainnet'`
    : ""

  try {
    function bulkDeltaSql(metricName, checkType, checkTypeFilter) {
      const ctFilter = checkTypeFilter
        ? `AND JSONExtract(tags, 'checkType', 'Nullable(String)') = '${checkTypeFilter}'`
        : ""
      return `
        WITH raw AS (
          SELECT toStartOfFiveMinutes(dt) AS time,
            JSONExtract(tags, 'providerId', 'Nullable(String)') AS providerId,
            if(startsWith(JSONExtract(tags, 'value', 'Nullable(String)'), 'failure'), 'failure',
              JSONExtract(tags, 'value', 'Nullable(String)')) AS status,
            avgMerge(value_avg) AS inner_value
          FROM ${DEALBOT_METRICS}
          WHERE name = '${metricName}'
            AND dt > now() - INTERVAL 72 HOUR
            AND JSONExtract(tags, 'value', 'Nullable(String)') != 'pending'
            ${networkFilter}
            ${ctFilter}
          GROUP BY time, providerId, status, series_id
        ),
        series_values AS (
          SELECT time, providerId, status, sum(inner_value) AS value
          FROM raw GROUP BY time, providerId, status
        ),
        series_deltas AS (
          SELECT providerId, status,
            if(isNull(prev_value), 0, greatest(value - prev_value, 0)) AS delta
          FROM (
            SELECT time, providerId, status, value,
              lagInFrame(value) OVER (PARTITION BY providerId, status ORDER BY time) AS prev_value
            FROM series_values
          )
        )
        SELECT providerId, '${checkType}' AS checkType, status AS value, toUInt64(round(sum(delta))) AS cnt
        FROM series_deltas
        GROUP BY providerId, status
        FORMAT JSONEachRow`
    }

    const [storageRows, retrievalRows] = await Promise.all([
      queryDealbot(bulkDeltaSql("dataStorageStatus", "dataStorage", null)),
      queryDealbot(bulkDeltaSql("retrievalStatus", "retrieval", "retrieval")),
    ])

    const rows = [...storageRows, ...retrievalRows]

    // Aggregate per provider
    const byProvider = {}
    for (const r of rows) {
      const pid = r.providerId
      if (!byProvider[pid]) byProvider[pid] = {}
      if (!byProvider[pid][r.checkType]) byProvider[pid][r.checkType] = { success: 0, failed: 0 }
      if (r.value === "success") byProvider[pid][r.checkType].success += r.cnt
      else if (r.value === "failure") byProvider[pid][r.checkType].failed += r.cnt
    }

    cache.set(cacheKey, byProvider, DEALBOT_TTL)
    res.json(byProvider)
  } catch (err) {
    console.error("network/performance error:", err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/performance — dealbot Prometheus metrics (Better Stack infra_prod)
app.get("/api/sp/:id/performance", async (req, res) => {
  const network = parseNetwork(req)
  const DEALBOT_METRICS = getDealbotMetrics(network)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const hours = validateHours(req.query.hours)
  const cacheKey = `sp:${sp.id}:performance:${hours}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  const networkFilterClause = network === "mainnet"
    ? `AND JSONExtract(tags, 'network', 'Nullable(String)') = 'mainnet'`
    : ""

  try {
    const provFilter = `JSONExtract(tags, 'providerId', 'Nullable(String)') = '${sp.id}'
      ${networkFilterClause}`

    // Data Storage (deals) and Retrieval - separate metrics, matching Better Stack dashboard
    const storageSql = dealbotDeltaSql("dataStorageStatus", hours, provFilter, null, DEALBOT_METRICS)
    const retrievalSql = dealbotDeltaSql("retrievalStatus", hours, provFilter, "retrieval", DEALBOT_METRICS)

    // Timing averages from _sum/_count gauge pairs
    const timingSql = `
      SELECT
        name,
        JSONExtract(tags, 'checkType', 'Nullable(String)') AS checkType,
        avgMerge(value_avg) AS avg_val
      FROM ${DEALBOT_METRICS}
      WHERE dt > now() - INTERVAL ${hours} HOUR
        AND name IN (
          'retrievalCheckMs_sum', 'retrievalCheckMs_count',
          'ipfsRetrievalFirstByteMs_sum', 'ipfsRetrievalFirstByteMs_count',
          'ipfsRetrievalLastByteMs_sum', 'ipfsRetrievalLastByteMs_count',
          'ipfsRetrievalThroughputBps_sum', 'ipfsRetrievalThroughputBps_count',
          'ipniVerifyMs_sum', 'ipniVerifyMs_count'
        )
        AND ${provFilter}
      GROUP BY name, checkType
      ORDER BY name
      FORMAT JSONEachRow`

    const bucket = timeBucket(hours)

    // Timeline SQL (success/fail over time)
    const timelineSql = `
      WITH storage_raw AS (
        SELECT toStartOfFiveMinutes(dt) AS time, ${bucket} AS time_bucket,
          if(startsWith(JSONExtract(tags, 'value', 'Nullable(String)'), 'failure'), 'failure',
            JSONExtract(tags, 'value', 'Nullable(String)')) AS status,
          avgMerge(value_avg) AS inner_value
        FROM ${DEALBOT_METRICS}
        WHERE name = 'dataStorageStatus'
          AND dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(tags, 'value', 'Nullable(String)') != 'pending'
          AND ${provFilter}
        GROUP BY time, time_bucket, status, series_id
      ),
      storage_values AS (
        SELECT time, time_bucket, status, sum(inner_value) AS value
        FROM storage_raw GROUP BY time, time_bucket, status
      ),
      storage_deltas AS (
        SELECT time_bucket, status,
          if(isNull(prev_value), 0, greatest(value - prev_value, 0)) AS delta
        FROM (
          SELECT time, time_bucket, status, value,
            lagInFrame(value) OVER (PARTITION BY status ORDER BY time) AS prev_value
          FROM storage_values
        )
      ),
      ret_raw AS (
        SELECT toStartOfFiveMinutes(dt) AS time, ${bucket} AS time_bucket,
          if(startsWith(JSONExtract(tags, 'value', 'Nullable(String)'), 'failure'), 'failure',
            JSONExtract(tags, 'value', 'Nullable(String)')) AS status,
          avgMerge(value_avg) AS inner_value
        FROM ${DEALBOT_METRICS}
        WHERE name = 'retrievalStatus'
          AND JSONExtract(tags, 'checkType', 'Nullable(String)') = 'retrieval'
          AND dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(tags, 'value', 'Nullable(String)') != 'pending'
          AND ${provFilter}
        GROUP BY time, time_bucket, status, series_id
      ),
      ret_values AS (
        SELECT time, time_bucket, status, sum(inner_value) AS value
        FROM ret_raw GROUP BY time, time_bucket, status
      ),
      ret_deltas AS (
        SELECT time_bucket, status,
          if(isNull(prev_value), 0, greatest(value - prev_value, 0)) AS delta
        FROM (
          SELECT time, time_bucket, status, value,
            lagInFrame(value) OVER (PARTITION BY status ORDER BY time) AS prev_value
          FROM ret_values
        )
      ),
      combined AS (
        SELECT time_bucket, 'dataStorage' AS checkType, status, delta FROM storage_deltas
        UNION ALL
        SELECT time_bucket, 'retrieval' AS checkType, status, delta FROM ret_deltas
      )
      SELECT time_bucket AS time, checkType,
        sumIf(delta, status = 'success') AS success,
        sumIf(delta, status = 'failure') AS failed
      FROM combined
      GROUP BY time_bucket, checkType
      ORDER BY time_bucket ASC
      FORMAT JSONEachRow`

    // Latency SQL (timing over time)
    const latencySql = `
      SELECT
        ${bucket} AS time,
        replace(name, '_sum', '') AS metric,
        avgMerge(value_avg) AS sum_val
      FROM ${DEALBOT_METRICS}
      WHERE dt > now() - INTERVAL ${hours} HOUR
        AND name IN ('retrievalCheckMs_sum', 'ipfsRetrievalFirstByteMs_sum', 'ipniVerifyMs_sum')
        AND ${provFilter}
        AND JSONExtract(tags, 'checkType', 'Nullable(String)') = 'retrieval'
      GROUP BY time, metric
      ORDER BY time ASC
      FORMAT JSONEachRow`

    // Run all 5 queries in parallel
    const [storageCounters, retrievalCounters, timingRaw, timeline, latency] = await Promise.all([
      queryDealbot(storageSql),
      queryDealbot(retrievalSql),
      queryDealbot(timingSql),
      queryDealbot(timelineSql).catch(() => []),
      queryDealbot(latencySql).catch(() => []),
    ])

    // Merge into unified counters array with checkType
    const counters = [
      ...storageCounters.map(c => ({ checkType: "dataStorage", ...c })),
      ...retrievalCounters.map(c => ({ checkType: "retrieval", ...c })),
    ]

    // Compute averages from _sum / _count pairs
    const sums = {}, counts = {}
    for (const t of timingRaw) {
      const base = t.name.replace(/_sum$/, "").replace(/_count$/, "")
      const key = base + ":" + (t.checkType || "")
      if (t.name.endsWith("_sum")) sums[key] = t.avg_val
      else if (t.name.endsWith("_count")) counts[key] = t.avg_val
    }
    const timing = []
    for (const key in sums) {
      if (counts[key] && counts[key] > 0) {
        const [base, checkType] = key.split(":")
        timing.push({ name: base, checkType, avgMs: sums[key] / counts[key] })
      }
    }

    const result = { available: true, counters, timing, timeline, latency }
    cache.set(cacheKey, result, DEALBOT_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/performance error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/logs?hours=N&level=X — raw logs
app.get("/api/sp/:id/logs", async (req, res) => {
  const network = parseNetwork(req)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false, logs: [] })

  const hours = validateHours(req.query.hours)
  const level = req.query.level
  const validLevels = new Set(["error", "warn", "info", "debug"])

  let levelFilter = ""
  if (level === "cid.contact") {
    levelFilter = `AND ${CID_CONTACT_FILTER}`
  } else if (level && validLevels.has(level)) {
    levelFilter = `AND JSONExtract(raw, 'level', 'Nullable(String)') = '${level}'`
  }

  const cacheKey = `sp:${sp.id}:logs:${hours}:${level || "all"}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const sql = `
      SELECT
        dt,
        CASE WHEN ${CID_CONTACT_FILTER} THEN 'cid.contact'
             ELSE JSONExtract(raw, 'level', 'Nullable(String)') END AS level,
        JSONExtract(raw, 'logger', 'Nullable(String)') AS logger,
        JSONExtract(raw, 'msg', 'Nullable(String)') AS msg,
        JSONExtract(raw, 'err', 'Nullable(String)') AS err,
        JSONExtract(raw, 'taskID', 'Nullable(Int64)') AS taskID
      FROM (
        SELECT ${COMMON_COLS} FROM ${RECENT_TABLE}
        WHERE dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IS NOT NULL
          AND JSONExtract(raw, 'level', 'Nullable(String)') != ''
          ${SUPPRESS_FILTER}
          ${levelFilter}
        UNION ALL
        SELECT ${COMMON_COLS} FROM ${HISTORICAL_TABLE}
        WHERE _row_type = 1
          AND dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IS NOT NULL
          AND JSONExtract(raw, 'level', 'Nullable(String)') != ''
          ${SUPPRESS_FILTER}
          ${levelFilter}
      )
      ORDER BY dt DESC
      LIMIT 100
      FORMAT JSONEachRow`

    const rows = await queryBetterStack(sql)
    const result = { available: true, logs: rows }
    cache.set(cacheKey, result, BS_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/logs error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/log-summary?hours=N — total counts by level
app.get("/api/sp/:id/log-summary", async (req, res) => {
  const network = parseNetwork(req)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false })

  const hours = validateHours(req.query.hours)
  const cacheKey = `sp:${sp.id}:log-summary:${hours}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const sql = `
      SELECT
        CASE WHEN ${CID_CONTACT_FILTER} THEN 'cid.contact'
             ELSE JSONExtract(raw, 'level', 'Nullable(String)') END AS level,
        count(*) AS cnt,
        max(dt) AS last_seen
      FROM (
        SELECT ${COMMON_COLS} FROM ${RECENT_TABLE}
        WHERE dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IS NOT NULL
          AND JSONExtract(raw, 'level', 'Nullable(String)') != ''
          ${SUPPRESS_FILTER}
        UNION ALL
        SELECT ${COMMON_COLS} FROM ${HISTORICAL_TABLE}
        WHERE _row_type = 1
          AND dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IS NOT NULL
          AND JSONExtract(raw, 'level', 'Nullable(String)') != ''
          ${SUPPRESS_FILTER}
      )
      GROUP BY level
      FORMAT JSONEachRow`

    const rows = await queryBetterStack(sql)
    var errors = 0, warns = 0, info = 0, lastSeen = null
    for (const r of rows) {
      if (r.level === "error") errors = r.cnt
      else if (r.level === "warn") warns = r.cnt
      else if (r.level === "info") info = r.cnt
      if (r.last_seen && (!lastSeen || r.last_seen > lastSeen)) lastSeen = r.last_seen
    }
    const result = { available: true, errors, warns, info, last_seen: lastSeen }
    cache.set(cacheKey, result, BS_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/log-summary error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/errors?hours=N — top errors
app.get("/api/sp/:id/errors", async (req, res) => {
  const network = parseNetwork(req)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false, errors: [] })

  const hours = validateHours(req.query.hours)
  const cacheKey = `sp:${sp.id}:errors:${hours}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const sql = `
      SELECT
        JSONExtract(raw, 'msg', 'Nullable(String)') AS msg,
        CASE WHEN ${CID_CONTACT_FILTER} THEN 'cid.contact'
             ELSE JSONExtract(raw, 'level', 'Nullable(String)') END AS level,
        JSONExtract(raw, 'logger', 'Nullable(String)') AS logger,
        JSONExtract(raw, 'err', 'Nullable(String)') AS err,
        count(*) AS cnt,
        max(dt) AS last_seen
      FROM (
        SELECT ${COMMON_COLS} FROM ${RECENT_TABLE}
        WHERE dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IN ('error', 'warn')
          ${SUPPRESS_FILTER}
        UNION ALL
        SELECT ${COMMON_COLS} FROM ${HISTORICAL_TABLE}
        WHERE _row_type = 1
          AND dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IN ('error', 'warn')
          ${SUPPRESS_FILTER}
      )
      GROUP BY msg, level, logger, err
      ORDER BY cnt DESC
      LIMIT 20
      FORMAT JSONEachRow`

    const rows = await queryBetterStack(sql)
    const result = { available: true, errors: rows }
    cache.set(cacheKey, result, BS_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/errors error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/error-detail?hours=N&msg=X — individual occurrences of a grouped error
app.get("/api/sp/:id/error-detail", async (req, res) => {
  const network = parseNetwork(req)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false, entries: [] })

  const hours = validateHours(req.query.hours)
  const msg = req.query.msg
  if (!msg) return res.status(400).json({ error: "msg parameter required" })

  // No caching — these are drill-down queries
  try {
    // Escape single quotes for ClickHouse
    const escapedMsg = msg.replace(/'/g, "''")
    const sql = `
      SELECT
        dt,
        JSONExtract(raw, 'level', 'Nullable(String)') AS level,
        JSONExtract(raw, 'logger', 'Nullable(String)') AS logger,
        JSONExtract(raw, 'msg', 'Nullable(String)') AS msg,
        JSONExtract(raw, 'err', 'Nullable(String)') AS err,
        JSONExtract(raw, 'errorVerbose', 'Nullable(String)') AS errorVerbose,
        JSONExtract(raw, 'taskID', 'Nullable(Int64)') AS taskID,
        JSONExtract(raw, 'type', 'Nullable(String)') AS taskType,
        JSONExtract(raw, 'id', 'Nullable(Int64)') AS taskIdField,
        JSONExtract(raw, 'caller', 'Nullable(String)') AS caller,
        JSONExtract(raw, 'piece_cid', 'Nullable(String)') AS piece_cid
      FROM (
        SELECT ${COMMON_COLS} FROM ${RECENT_TABLE}
        WHERE dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'msg', 'Nullable(String)') = '${escapedMsg}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IN ('error', 'warn')
          ${SUPPRESS_FILTER}
        UNION ALL
        SELECT ${COMMON_COLS} FROM ${HISTORICAL_TABLE}
        WHERE _row_type = 1
          AND dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'msg', 'Nullable(String)') = '${escapedMsg}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IN ('error', 'warn')
          ${SUPPRESS_FILTER}
      )
      ORDER BY dt DESC
      LIMIT 20
      FORMAT JSONEachRow`

    const rows = await queryBetterStack(sql)
    res.json({ available: true, entries: rows })
  } catch (err) {
    console.error(`sp/${sp.id}/error-detail error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/patterns?hours=N — error patterns (grouped by message, no err field)
app.get("/api/sp/:id/patterns", async (req, res) => {
  const network = parseNetwork(req)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false, patterns: [] })

  const hours = validateHours(req.query.hours)
  const cacheKey = `sp:${sp.id}:patterns:${hours}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const sql = `
      SELECT
        JSONExtract(raw, 'msg', 'Nullable(String)') AS pattern,
        CASE WHEN ${CID_CONTACT_FILTER} THEN 'cid.contact'
             ELSE JSONExtract(raw, 'level', 'Nullable(String)') END AS level,
        JSONExtract(raw, 'logger', 'Nullable(String)') AS logger,
        count(*) AS cnt,
        min(dt) AS first_seen,
        max(dt) AS last_seen
      FROM (
        SELECT ${COMMON_COLS} FROM ${RECENT_TABLE}
        WHERE dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IN ('error', 'warn')
          ${SUPPRESS_FILTER}
        UNION ALL
        SELECT ${COMMON_COLS} FROM ${HISTORICAL_TABLE}
        WHERE _row_type = 1
          AND dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IN ('error', 'warn')
          ${SUPPRESS_FILTER}
      )
      GROUP BY pattern, level, logger
      ORDER BY cnt DESC
      LIMIT 20
      FORMAT JSONEachRow`

    const rows = await queryBetterStack(sql)
    const result = { available: true, patterns: rows }
    cache.set(cacheKey, result, BS_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/patterns error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/timeline?hours=N — error timeline
app.get("/api/sp/:id/timeline", async (req, res) => {
  const network = parseNetwork(req)
  const sp = getSP(req.params.id, network)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false, timeline: [] })

  const hours = validateHours(req.query.hours)
  const bucket = timeBucket(hours)
  const cacheKey = `sp:${sp.id}:timeline:${hours}:${network}`
  const cached = getCached(req, cacheKey)
  if (cached) return res.json(cached)

  try {
    const sql = `
      SELECT
        ${bucket} AS time,
        CASE WHEN ${CID_CONTACT_FILTER} THEN 'cid.contact'
             ELSE JSONExtract(raw, 'level', 'Nullable(String)') END AS level,
        count(*) AS cnt
      FROM (
        SELECT ${COMMON_COLS} FROM ${RECENT_TABLE}
        WHERE dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IN ('error', 'warn')
          ${SUPPRESS_FILTER}
        UNION ALL
        SELECT ${COMMON_COLS} FROM ${HISTORICAL_TABLE}
        WHERE _row_type = 1
          AND dt > now() - INTERVAL ${hours} HOUR
          AND JSONExtract(raw, 'client_id', 'Nullable(String)') = '${sp.clientId}'
          AND JSONExtract(raw, 'level', 'Nullable(String)') IN ('error', 'warn')
          ${SUPPRESS_FILTER}
      )
      GROUP BY time, level
      ORDER BY time ASC
      FORMAT JSONEachRow`

    const rows = await queryBetterStack(sql)
    const result = { available: true, timeline: rows }
    cache.set(cacheKey, result, BS_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/timeline error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// Background pre-warming — hit local endpoints to populate cache
async function prewarm(network) {
  const base = `http://localhost:${PORT}`
  const endpoints = [
    `/api/network/global?network=${network}`,
    `/api/network/overview?network=${network}`,
    `/api/network/performance?network=${network}`,
  ]
  for (const ep of endpoints) {
    try {
      await fetch(`${base}${ep}`, { signal: AbortSignal.timeout(30000) })
    } catch {}
  }
  console.log(`Prewarm ${network}: done`)
}

// Prewarm FWSS cache — fetch all dataset IDs from PDP Scan and pre-populate client mappings
async function prewarmFWSS() {
  try {
    const endpoints = getEndpoints("mainnet")
    const dataSets = await gqlPaginate(endpoints.pdpScan, "dataSets", "setId", "", "setId")
    const ids = dataSets.map(ds => ds.setId)
    console.log(`FWSS prewarm: fetching ${ids.length} datasets...`)
    await getDataSetInfoBatch(ids, "mainnet")
    console.log(`FWSS prewarm: done (${ids.length} datasets cached)`)
  } catch (err) {
    console.error("FWSS prewarm error:", err.message)
  }
}

function startPrewarm() {
  // Initial prewarm after 2s (let server start first)
  setTimeout(() => {
    prewarm("mainnet")
    prewarm("calibration")
    prewarmFWSS()
  }, 2000)

  // Repeat every 5 min (network), 30 min (FWSS — matches FWSS cache TTL)
  const interval = setInterval(() => {
    prewarm("mainnet")
    prewarm("calibration")
  }, 5 * 60 * 1000)
  const fwssInterval = setInterval(() => {
    prewarmFWSS()
  }, 30 * 60 * 1000)
  fwssInterval.unref()
  interval.unref()
}

// Start server
startLivenessProbes()
startPrewarm()
app.listen(PORT, "0.0.0.0", () => {
  console.log(`PDP SP Dashboard running on http://0.0.0.0:${PORT}`)
})
