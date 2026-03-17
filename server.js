require("dotenv").config()
const express = require("express")
const path = require("path")
const { Cache } = require("./lib/cache")
const { getAllSPs, getTrackedSPs, getSP } = require("./lib/sp-config")
const { ENDPOINTS, gqlFetch, gqlPaginate } = require("./lib/subgraph")
const {
  queryBetterStack, queryDealbot, validateHours, timeBucket,
  RECENT_TABLE, HISTORICAL_TABLE, COMMON_COLS, SUPPRESS_FILTER, CID_CONTACT_FILTER,
} = require("./lib/betterstack")
const { startLivenessProbes, getLiveness } = require("./lib/liveness")

const app = express()
const PORT = parseInt(process.env.PORT, 10) || 3848
const cache = new Cache()

const SUBGRAPH_TTL = 5 * 60 * 1000  // 5 min
const BS_TTL = 60 * 1000             // 1 min

app.use(express.static(path.join(__dirname, "public")))

// --- Helpers ---

const CLIENT_IDS = getTrackedSPs().map(sp => `'${sp.clientId}'`).join(", ")

// --- API Routes ---

// GET /api/config — SP list with liveness
app.get("/api/config", (req, res) => {
  const liveness = getLiveness()
  const sps = getAllSPs().map(sp => ({
    ...sp,
    liveness: liveness[sp.id] || { alive: null, latencyMs: null, lastCheck: null },
  }))
  res.json(sps)
})

// GET /api/network/global — NetworkMetric + GlobalMetric totals
app.get("/api/network/global", async (req, res) => {
  const cacheKey = "network:global"
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    const [pdpData, fwssData] = await Promise.all([
      gqlFetch(ENDPOINTS.pdpScan, `{
        networkMetrics(first: 1) {
          totalProviders
          totalProofSets
          totalRoots
          totalDataSize
          totalProofs
          totalFaultedPeriods
        }
      }`),
      gqlFetch(ENDPOINTS.fwss, `{
        globalMetrics(first: 1) {
          totalDataSets
          totalPieces
          totalStorageBytes
        }
      }`),
    ])

    const pdp = pdpData.networkMetrics?.[0] || {}
    const fwss = fwssData.globalMetrics?.[0] || {}

    const result = {
      providers: Number(pdp.totalProviders || 0),
      proofSets: Number(pdp.totalProofSets || 0),
      roots: Number(pdp.totalRoots || 0),
      dataSize: pdp.totalDataSize || "0",
      proofsSubmitted: Number(pdp.totalProofs || 0),
      faultedPeriods: Number(pdp.totalFaultedPeriods || 0),
      datasets: Number(fwss.totalDataSets || 0),
      pieces: Number(fwss.totalPieces || 0),
      storageSize: fwss.totalStorageBytes || "0",
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
  const cacheKey = "network:overview"
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    // Parallel fetch all data sources
    const [pdpProviders, fwssDatasets, filpayRails, bsOverview, bsVersions] = await Promise.all([
      // PDP Scan providers
      gqlFetch(ENDPOINTS.pdpScan, `{
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

      // FWSS datasets aggregated per provider
      gqlPaginate(ENDPOINTS.fwss, "dataSets", `
        dataSetId
        providerId
        payer
        payee
        pdpRailId
        totalPieces
        totalSize
        status
      `).catch(() => []),

      // FilecoinPay active rails
      gqlPaginate(ENDPOINTS.filecoinPay, "rails", `
        railId
        paymentRate
        state
        totalSettledAmount
        payer { id }
        payee { id }
      `, 'state: "ACTIVE"').catch(() => []),

      // Better Stack overview (7 tracked SPs)
      queryBetterStack(`
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
        FORMAT JSONEachRow`).catch(() => []),

      // Better Stack curio versions
      queryBetterStack(`
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
        FORMAT JSONEachRow`).catch(() => []),
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

    // Aggregate FWSS datasets per provider
    const fwssByProvider = {}
    for (const ds of fwssDatasets) {
      const pid = ds.providerId
      if (!fwssByProvider[pid]) fwssByProvider[pid] = { datasets: 0, pieces: 0, totalSize: BigInt(0) }
      fwssByProvider[pid].datasets++
      fwssByProvider[pid].pieces += Number(ds.totalPieces || 0)
      fwssByProvider[pid].totalSize += BigInt(ds.totalSize || 0)
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
    const liveness = getLiveness()
    const result = getAllSPs().map(sp => {
      const addr = sp.address.toLowerCase()
      const pdp = providerMap[addr]?.pdp || null
      const fwss = fwssByProvider[String(sp.id)]
      const rails = railsByPayee[addr]
      const bs = sp.hasLogs ? bsByClient[sp.clientId] || null : null
      const version = sp.hasLogs ? versionByClient[sp.clientId] || null : null

      return {
        id: sp.id,
        name: sp.name,
        address: sp.address,
        hasLogs: sp.hasLogs,
        liveness: liveness[sp.id] || null,
        pdp,
        fwss: fwss ? {
          datasets: fwss.datasets,
          pieces: fwss.pieces,
          totalSize: fwss.totalSize.toString(),
        } : null,
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
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const cacheKey = `sp:${sp.id}:proving`
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const data = await gqlFetch(ENDPOINTS.pdpScan, `{
      provider(id: "${addr}") {
        totalProofSets
        totalRoots
        totalDataSize
        totalFaultedPeriods
        totalProvingPeriods
        totalFaultedRoots
        createdAt
      }
      dataSets(first: 100, where: {owner: "${addr}"}, orderBy: id, orderDirection: desc) {
        id
        setId
        leafCount
        totalRoots
        totalDataSize
        isActive
        lastProvenEpoch
        nextChallengeEpoch
        nextDeadline
        totalFaultedRoots
        totalFaultedPeriods
        createdAt
      }
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
    }`)

    // Fetch FWSS dataset status (has Terminated status that PDP Scan doesn't)
    const fwssDatasets = await gqlPaginate(ENDPOINTS.fwss, "dataSets", `
      dataSetId
      status
      pdpRailId
    `, `providerId: "${sp.id}"`).catch(() => [])

    // Merge FWSS status into PDP Scan datasets (dataSetId matches setId)
    const fwssStatusMap = {}
    for (const fd of fwssDatasets) {
      fwssStatusMap[fd.dataSetId] = { status: fd.status, railId: fd.pdpRailId }
    }
    for (const ds of (data.dataSets || [])) {
      const fwss = fwssStatusMap[ds.setId] || {}
      ds.status = fwss.status || (ds.isActive ? "Active" : "Inactive")
      ds.railId = fwss.railId || null
    }

    // Fetch faults from FWSS subgraph
    const faults = await gqlPaginate(ENDPOINTS.fwss, "faults", `
      id
      periodsFaulted
      deadline
      timestamp
      blockNumber
      txHash
      dataSet { dataSetId }
    `, `dataSet_: {providerId: "${sp.id}"}`, "timestamp").catch(() => [])

    const result = {
      provider: data.provider,
      dataSets: data.dataSets || [],
      weeklyActivity: data.weeklyProviderActivities || [],
      faults: faults.reverse(),
    }

    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/proving error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/dataset/:setId — single dataset detail from PDP Scan + FWSS
app.get("/api/sp/:id/dataset/:setId", async (req, res) => {
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  const setId = req.params.setId

  try {
    const addr = sp.address.toLowerCase()
    const [pdpData, fwssData, fwssFaults] = await Promise.all([
      gqlFetch(ENDPOINTS.pdpScan, `{
        dataSets(first: 1, where: {setId: "${setId}", owner: "${addr}"}) {
          setId leafCount challengeRange isActive
          lastProvenEpoch nextChallengeEpoch nextDeadline firstDeadline
          maxProvingPeriod challengeWindowSize currentDeadlineCount provenThisPeriod
          totalRoots nextPieceId totalDataSize totalProofs totalProvedRoots
          totalFeePaid totalFaultedPeriods totalFaultedRoots
          totalTransactions totalEventLogs createdAt updatedAt blockNumber
        }
      }`).then(d => d.dataSets?.[0] || null).catch(() => null),

      gqlFetch(ENDPOINTS.fwss, `{
        dataSets(first: 1, where: {dataSetId: "${setId}", providerId: "${sp.id}"}) {
          dataSetId providerId payer payee
          pdpRailId cacheMissRailId cdnRailId
          totalPieces totalSize withCDN withIPFSIndexing
          status createdAt createdAtBlock createdAtTxHash
        }
      }`).then(d => d.dataSets?.[0] || null).catch(() => null),

      gqlPaginate(ENDPOINTS.fwss, "faults", `
        id periodsFaulted deadline timestamp txHash
      `, `dataSet: "${setId}"`, "timestamp").catch(() => []),
    ])

    if (!pdpData && !fwssData) return res.status(404).json({ error: "Dataset not found" })

    res.json({ pdp: pdpData, fwss: fwssData, faults: fwssFaults })
  } catch (err) {
    console.error(`sp/${sp.id}/dataset/${setId} error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/revenue — daily settlement history
app.get("/api/sp/:id/revenue", async (req, res) => {
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const cacheKey = `sp:${sp.id}:revenue`
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const settlements = await gqlPaginate(ENDPOINTS.filecoinPay, "settlements", `
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
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const cacheKey = `sp:${sp.id}:economics`
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    const addr = sp.address.toLowerCase()
    const [rails, accountData] = await Promise.all([
      gqlPaginate(ENDPOINTS.filecoinPay, "rails", `
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

      gqlFetch(ENDPOINTS.filecoinPay, `{
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
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  const railId = req.params.railId

  try {
    const addr = sp.address.toLowerCase()
    const data = await gqlFetch(ENDPOINTS.filecoinPay, `{
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

    // Find linked FWSS dataset via pdpRailId
    const fwssDataset = await gqlFetch(ENDPOINTS.fwss, `{
      dataSets(first: 1, where: {pdpRailId: "${railId}", providerId: "${sp.id}"}) {
        dataSetId
        status
        totalPieces
        totalSize
      }
    }`).then(d => d.dataSets?.[0] || null).catch(() => null)

    res.json({ rail, dataset: fwssDataset })
  } catch (err) {
    console.error(`sp/${sp.id}/rail/${railId} error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/performance/timeline?hours=N — success % over time
app.get("/api/sp/:id/performance/timeline", async (req, res) => {
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const hours = validateHours(req.query.hours)
  const bucket = timeBucket(hours)
  const cacheKey = `sp:${sp.id}:perf-timeline:${hours}`
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    const sql = `
      SELECT time, checkType,
        sumIf(increase, value = 'success') AS success,
        sumIf(increase, value LIKE 'failure%') AS failed
      FROM (
        SELECT checkType, value, ${bucket} AS time,
          if(max_val < lagInFrame(max_val, 1, 0) OVER (PARTITION BY checkType, value ORDER BY minute_bucket),
            max_val,
            max_val - lagInFrame(max_val, 1, 0) OVER (PARTITION BY checkType, value ORDER BY minute_bucket)
          ) AS increase, minute_bucket
        FROM (
          SELECT toStartOfMinute(dt) AS minute_bucket, ${bucket} AS time_bucket,
            JSONExtract(tags, 'checkType', 'Nullable(String)') AS checkType,
            JSONExtract(tags, 'value', 'Nullable(String)') AS value,
            maxMerge(value_max) AS max_val
          FROM ${DEALBOT_METRICS}
          WHERE dt > now() - INTERVAL ${hours} HOUR
            AND name = 'retrievalStatus'
            AND JSONExtract(tags, 'providerId', 'Nullable(String)') = '${sp.id}'
            AND JSONExtract(tags, 'network', 'Nullable(String)') = 'mainnet'
            AND JSONExtract(tags, 'value', 'Nullable(String)') != 'pending'
          GROUP BY minute_bucket, time_bucket, checkType, value
          ORDER BY checkType, value, minute_bucket
        )
      )
      GROUP BY time, checkType
      ORDER BY time ASC
      FORMAT JSONEachRow`

    const rows = await queryDealbot(sql)
    cache.set(cacheKey, rows, BS_TTL)
    res.json(rows)
  } catch (err) {
    console.error(`sp/${sp.id}/performance/timeline error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/performance/latency?hours=N — latency metrics over time
app.get("/api/sp/:id/performance/latency", async (req, res) => {
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const hours = validateHours(req.query.hours)
  const bucket = timeBucket(hours)
  const cacheKey = `sp:${sp.id}:perf-latency:${hours}`
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    const sql = `
      SELECT
        ${bucket} AS time,
        replace(name, '_sum', '') AS metric,
        avgMerge(value_avg) AS sum_val
      FROM ${DEALBOT_METRICS}
      WHERE dt > now() - INTERVAL ${hours} HOUR
        AND name IN ('retrievalCheckMs_sum', 'ipfsRetrievalFirstByteMs_sum', 'ipniVerifyMs_sum')
        AND JSONExtract(tags, 'providerId', 'Nullable(String)') = '${sp.id}'
        AND JSONExtract(tags, 'network', 'Nullable(String)') = 'mainnet'
        AND JSONExtract(tags, 'checkType', 'Nullable(String)') = 'retrieval'
      GROUP BY time, metric
      ORDER BY time ASC
      FORMAT JSONEachRow`

    const rows = await queryDealbot(sql)
    cache.set(cacheKey, rows, BS_TTL)
    res.json(rows)
  } catch (err) {
    console.error(`sp/${sp.id}/performance/latency error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// Dealbot metrics table (Prometheus, infra_prod)
const DEALBOT_METRICS = "remote(t468215_infra_prod_metrics)"

// Counter increase SQL: detects Prometheus counter resets and sums increments
// Uses window function lagInFrame to compare consecutive 1-min buckets
function counterIncreaseSql(hours, providerFilter) {
  return `
    SELECT checkType, value, sum(increase) AS cnt
    FROM (
      SELECT checkType, value,
        if(max_val < lagInFrame(max_val, 1, 0) OVER (PARTITION BY checkType, value ORDER BY bucket),
          max_val,
          max_val - lagInFrame(max_val, 1, 0) OVER (PARTITION BY checkType, value ORDER BY bucket)
        ) AS increase, bucket
      FROM (
        SELECT toStartOfMinute(dt) AS bucket,
          JSONExtract(tags, 'checkType', 'Nullable(String)') AS checkType,
          JSONExtract(tags, 'value', 'Nullable(String)') AS value,
          maxMerge(value_max) AS max_val
        FROM ${DEALBOT_METRICS}
        WHERE dt > now() - INTERVAL ${hours} HOUR
          AND name = 'retrievalStatus'
          AND ${providerFilter}
          AND JSONExtract(tags, 'value', 'Nullable(String)') != 'pending'
        GROUP BY bucket, checkType, value
        ORDER BY checkType, value, bucket
      )
    )
    GROUP BY checkType, value
    ORDER BY checkType, value
    FORMAT JSONEachRow`
}

// GET /api/network/performance — bulk dealbot performance for all providers (24h)
app.get("/api/network/performance", async (req, res) => {
  const cacheKey = "network:performance"
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    const sql = `
      SELECT providerId, checkType, value, sum(increase) AS cnt
      FROM (
        SELECT providerId, checkType, value,
          if(max_val < lagInFrame(max_val, 1, 0) OVER (PARTITION BY providerId, checkType, value ORDER BY bucket),
            max_val,
            max_val - lagInFrame(max_val, 1, 0) OVER (PARTITION BY providerId, checkType, value ORDER BY bucket)
          ) AS increase, bucket
        FROM (
          SELECT toStartOfMinute(dt) AS bucket,
            JSONExtract(tags, 'providerId', 'Nullable(String)') AS providerId,
            JSONExtract(tags, 'checkType', 'Nullable(String)') AS checkType,
            JSONExtract(tags, 'value', 'Nullable(String)') AS value,
            maxMerge(value_max) AS max_val
          FROM ${DEALBOT_METRICS}
          WHERE dt > now() - INTERVAL 24 HOUR
            AND name = 'retrievalStatus'
            AND JSONExtract(tags, 'network', 'Nullable(String)') = 'mainnet'
            AND JSONExtract(tags, 'value', 'Nullable(String)') != 'pending'
          GROUP BY bucket, providerId, checkType, value
          ORDER BY providerId, checkType, value, bucket
        )
      )
      GROUP BY providerId, checkType, value
      ORDER BY providerId, checkType, value
      FORMAT JSONEachRow`

    const rows = await queryDealbot(sql)

    // Aggregate per provider
    const byProvider = {}
    for (const r of rows) {
      const pid = r.providerId
      if (!byProvider[pid]) byProvider[pid] = {}
      if (!byProvider[pid][r.checkType]) byProvider[pid][r.checkType] = { success: 0, failed: 0 }
      if (r.value === "success") byProvider[pid][r.checkType].success += r.cnt
      else if (r.value && r.value.startsWith("failure")) byProvider[pid][r.checkType].failed += r.cnt
    }

    cache.set(cacheKey, byProvider, BS_TTL)
    res.json(byProvider)
  } catch (err) {
    console.error("network/performance error:", err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/performance — dealbot Prometheus metrics (Better Stack infra_prod)
app.get("/api/sp/:id/performance", async (req, res) => {
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })

  const hours = validateHours(req.query.hours)
  const cacheKey = `sp:${sp.id}:performance:${hours}`
  const cached = cache.get(cacheKey)
  if (cached) return res.json(cached)

  try {
    const provFilter = `JSONExtract(tags, 'providerId', 'Nullable(String)') = '${sp.id}'
      AND JSONExtract(tags, 'network', 'Nullable(String)') = 'mainnet'`

    // Status counters using counter increase detection
    const counterSql = counterIncreaseSql(hours, provFilter)

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

    const [counters, timingRaw] = await Promise.all([
      queryDealbot(counterSql),
      queryDealbot(timingSql),
    ])

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

    const result = { available: true, counters, timing }
    cache.set(cacheKey, result, SUBGRAPH_TTL)
    res.json(result)
  } catch (err) {
    console.error(`sp/${sp.id}/performance error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/logs?hours=N&level=X — raw logs
app.get("/api/sp/:id/logs", async (req, res) => {
  const sp = getSP(req.params.id)
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
    res.json({ available: true, logs: rows })
  } catch (err) {
    console.error(`sp/${sp.id}/logs error:`, err.message)
    res.status(500).json({ error: err.message })
  }
})

// GET /api/sp/:id/errors?hours=N — top errors
app.get("/api/sp/:id/errors", async (req, res) => {
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false, errors: [] })

  const hours = validateHours(req.query.hours)
  const cacheKey = `sp:${sp.id}:errors:${hours}`
  const cached = cache.get(cacheKey)
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

// GET /api/sp/:id/patterns?hours=N — error patterns (grouped by message, no err field)
app.get("/api/sp/:id/patterns", async (req, res) => {
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false, patterns: [] })

  const hours = validateHours(req.query.hours)
  const cacheKey = `sp:${sp.id}:patterns:${hours}`
  const cached = cache.get(cacheKey)
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
  const sp = getSP(req.params.id)
  if (!sp) return res.status(404).json({ error: "Unknown SP" })
  if (!sp.hasLogs) return res.json({ available: false, timeline: [] })

  const hours = validateHours(req.query.hours)
  const bucket = timeBucket(hours)
  const cacheKey = `sp:${sp.id}:timeline:${hours}`
  const cached = cache.get(cacheKey)
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

// Start server
startLivenessProbes()
app.listen(PORT, "0.0.0.0", () => {
  console.log(`PDP SP Dashboard running on http://0.0.0.0:${PORT}`)
})
