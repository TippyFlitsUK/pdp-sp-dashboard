const DEALBOT_URLS = {
  mainnet: "https://dealbot.filoz.org",
  calibration: "https://staging.dealbot.filoz.org",
}

const CACHE_TTL_MS = 5 * 60 * 1000
const PAGE_SIZE = 200
const cache = new Map()

async function fetchScheduledIds(network) {
  const base = DEALBOT_URLS[network]
  const ids = new Set()
  let offset = 0
  for (;;) {
    const url = `${base}/api/v1/providers?network=${network}&limit=${PAGE_SIZE}&offset=${offset}`
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) })
    if (!res.ok) throw new Error(`HTTP ${res.status} from ${url}`)
    const data = await res.json()
    const providers = data.providers || []
    for (const p of providers) {
      if (p.providerId != null) ids.add(String(p.providerId))
    }
    offset += providers.length
    if (providers.length === 0 || offset >= Number(data.total || 0)) break
  }
  return ids
}

async function getDealbotScheduledIds(network) {
  const cached = cache.get(network)
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) return cached.data

  try {
    const ids = await fetchScheduledIds(network)
    if (ids.size > 0) cache.set(network, { data: ids, ts: Date.now() })
    return ids.size > 0 ? ids : (cached ? cached.data : null)
  } catch (err) {
    console.error(`dealbot-providers: failed to load ${network} list:`, err.message)
    return cached ? cached.data : null
  }
}

module.exports = { getDealbotScheduledIds }
