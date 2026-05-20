// foc-observer client — Rod's indexed FOC events + live contract state.
// Endpoint: $OBSERVER_URL (default https://foc-observer.va.gg).
//
// Two helpers:
//   observerSql(network, sql)   POST /sql  → { rows, columns, rowCount }
//   observerGet(network, path)  GET  /:path/:network[/...]
//
// Retry policy mirrors foc-metrics/weekly.py: exponential backoff on
// 502/503/504 (Ponder occasionally restarts on Glif null-round errors).

const OBSERVER_URL = (process.env.OBSERVER_URL || "https://foc-observer.va.gg").replace(/\/$/, "")
const DEFAULT_TIMEOUT_MS = 30000
const STATUS_TIMEOUT_MS = 15000 // /status counts rows across all tables; cap shorter than data calls
const MAX_RETRIES = 4 // 5 total attempts: 1s, 2s, 4s, 8s

// spdash uses "calibration" everywhere; observer uses "calibnet".
function toObserverNetwork(network) {
  return network === "calibration" ? "calibnet" : "mainnet"
}

// Defense-in-depth for SQL interpolation. Observer SQL has no parameter binding,
// so every value that ends up inside an `'${x}'` or `${x}` SQL fragment must
// match a known-safe format. Throw — not return null — so a misuse surfaces
// immediately rather than producing an empty result.
function assertEthAddress(addr) {
  if (typeof addr !== "string" || !/^0x[0-9a-f]{40}$/.test(addr)) {
    throw new Error(`Invalid ETH address: ${addr}`)
  }
  return addr
}

function assertNumericId(id) {
  const s = String(id)
  if (!/^\d+$/.test(s)) {
    throw new Error(`Invalid numeric id: ${id}`)
  }
  return s
}

function isTransient(status) {
  return status === 502 || status === 503 || status === 504
}

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms))
}

async function observerFetch(url, init, opts = {}) {
  const timeoutMs = opts.timeoutMs || DEFAULT_TIMEOUT_MS
  const maxRetries = opts.maxRetries ?? MAX_RETRIES
  let lastErr
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(url, {
        ...init,
        signal: AbortSignal.timeout(timeoutMs),
      })
      if (res.ok) return res
      if (!isTransient(res.status) || attempt === maxRetries) {
        throw new Error(`Observer ${res.status}: ${await res.text()}`)
      }
      lastErr = new Error(`Observer ${res.status}`)
    } catch (err) {
      if (attempt === maxRetries) throw err
      lastErr = err
    }
    await sleep(1000 * 2 ** attempt)
  }
  throw lastErr
}

async function observerSql(network, sql) {
  const res = await observerFetch(`${OBSERVER_URL}/sql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ network: toObserverNetwork(network), sql }),
  })
  const json = await res.json()
  if (json.error) throw new Error(`Observer SQL error: ${json.error}`)
  return json.rows || []
}

async function observerGet(path) {
  const res = await observerFetch(`${OBSERVER_URL}${path.startsWith("/") ? path : "/" + path}`)
  return res.json()
}

async function observerStatus() {
  // Short timeout + only 1 retry — health endpoint must fail fast.
  const res = await observerFetch(`${OBSERVER_URL}/status`, undefined, {
    timeoutMs: STATUS_TIMEOUT_MS,
    maxRetries: 1,
  })
  return res.json()
}

module.exports = {
  OBSERVER_URL,
  toObserverNetwork,
  observerSql,
  observerGet,
  observerStatus,
  assertEthAddress,
  assertNumericId,
}
