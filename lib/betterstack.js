// Better Stack ClickHouse SQL API — extracted from sp-health patterns
// Query queue keeps us under 4 concurrent query limit

const BS_HOST = process.env.BETTERSTACK_HOST || "eu-nbg-2-connect.betterstackdata.com"
const BS_USERNAME = process.env.BETTERSTACK_USERNAME
const BS_PASSWORD = process.env.BETTERSTACK_PASSWORD

// Dealbot (infra_prod) — separate cloud connection, different host
const BS_DEALBOT_HOST = process.env.BETTERSTACK_DEALBOT_HOST || "us-east-9-connect.betterstackdata.com"
const BS_DEALBOT_USERNAME = process.env.BETTERSTACK_DEALBOT_USERNAME
const BS_DEALBOT_PASSWORD = process.env.BETTERSTACK_DEALBOT_PASSWORD

// SP log tables
const RECENT_TABLE = "remote(t468215_pdp_spx_logs)"
const HISTORICAL_TABLE = "s3Cluster(primary, t468215_pdp_spx_s3)"

// Common columns for UNION ALL (different schemas between remote/s3Cluster)
const COMMON_COLS = "dt, raw"

// PDP logger whitelist
const PDP_LOGGERS = [
  "pdp", "pdp/add", "pdp/create", "pdp-contract",
  "harmonytask", "harmony-res",
  "curio/message", "curio/chainsched",
  "cached-reader", "dealdata",
  "indexing", "ipni", "ipni-provider", "indexstore",
  "retrievals", "remote-blockstore",
  "proof", "chunker",
  "filecoin-pay", "filecoin-pay-settle",
]
const LOGGER_FILTER = `AND JSONExtract(raw, 'logger', 'Nullable(String)') IN (${PDP_LOGGERS.map(l => `'${l}'`).join(", ")})`

// cid.contact reclassification
const CID_CONTACT_FILTER = `(JSONExtract(raw, 'logger', 'Nullable(String)') = 'ipni-provider' AND JSONExtract(raw, 'msg', 'Nullable(String)') = 'failed to publish head for provide')`

// Suppressed messages
const SUPPRESSED_MSGS = [
  "No alert plugins enabled, not sending an alert",
  "curio's defaults are for running alone. Use task maximums or CGroups.",
  "expected to use cached proof for large sub-piece but failed, will attempt to rehydrate cache and fall back to full memtree",
  "cached pdp proof not used, may require fallback to full memtree",
  "Request limit reached",
]
const SUPPRESS_FILTER = [
  LOGGER_FILTER,
  "AND dt <= now()",
  ...SUPPRESSED_MSGS.map(m => `AND JSONExtract(raw, 'msg', 'Nullable(String)') != '${m.replace(/'/g, "''")}'`),
].join("\n          ")

// Query queue
const MAX_CONCURRENT = 3
let activeQueries = 0
const queryQueue = []

function enqueue(fn) {
  return new Promise((resolve, reject) => {
    queryQueue.push({ fn, resolve, reject })
    drainQueue()
  })
}

function drainQueue() {
  while (activeQueries < MAX_CONCURRENT && queryQueue.length > 0) {
    const { fn, resolve, reject } = queryQueue.shift()
    activeQueries++
    fn().then(resolve, reject).finally(() => {
      activeQueries--
      drainQueue()
    })
  }
}

async function queryBetterStack(sql) {
  return enqueue(() => _queryBetterStack(sql))
}

async function _queryBetterStack(sql) {
  if (!BS_USERNAME || !BS_PASSWORD) {
    throw new Error("BETTERSTACK_USERNAME and BETTERSTACK_PASSWORD must be set")
  }
  const auth = Buffer.from(`${BS_USERNAME}:${BS_PASSWORD}`).toString("base64")
  const url = `https://${BS_HOST}?output_format_pretty_row_numbers=0&wait=true`

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "text/plain",
    },
    body: sql,
    signal: AbortSignal.timeout(30000),
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Better Stack query failed (${res.status}): ${text}`)
  }

  const text = await res.text()
  if (!text.trim()) return []
  return text.trim().split("\n").map(line => JSON.parse(line))
}

async function queryDealbot(sql) {
  return enqueue(() => _queryDealbot(sql))
}

async function _queryDealbot(sql) {
  if (!BS_DEALBOT_USERNAME || !BS_DEALBOT_PASSWORD) {
    throw new Error("BETTERSTACK_DEALBOT credentials not set")
  }
  const auth = Buffer.from(`${BS_DEALBOT_USERNAME}:${BS_DEALBOT_PASSWORD}`).toString("base64")
  const url = `https://${BS_DEALBOT_HOST}?output_format_pretty_row_numbers=0&wait=true`

  const res = await fetch(url, {
    method: "POST",
    headers: {
      Authorization: `Basic ${auth}`,
      "Content-Type": "text/plain",
    },
    body: sql,
    signal: AbortSignal.timeout(60000),
  })

  if (!res.ok) {
    const text = await res.text()
    throw new Error(`Dealbot query failed (${res.status}): ${text}`)
  }

  const text = await res.text()
  if (!text.trim()) return []
  return text.trim().split("\n").map(line => JSON.parse(line))
}

function validateHours(val) {
  const h = parseInt(val, 10)
  if (isNaN(h) || h < 1 || h > 168) return 24
  return h
}

function timeBucket(hours) {
  if (hours <= 1) return "toStartOfMinute(dt)"
  if (hours <= 6) return "toStartOfFiveMinutes(dt)"
  if (hours <= 24) return "toStartOfFifteenMinutes(dt)"
  return "toStartOfHour(dt)"
}

module.exports = {
  queryBetterStack,
  queryDealbot,
  validateHours,
  timeBucket,
  RECENT_TABLE,
  HISTORICAL_TABLE,
  COMMON_COLS,
  SUPPRESS_FILTER,
  CID_CONTACT_FILTER,
}
