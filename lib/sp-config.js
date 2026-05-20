// SP list — dynamically loaded from foc-observer's /providers endpoint.
// Merges on-chain registry data (name, address, serviceURL, isEndorsed)
// with per-network overrides from lib/sp-overrides.js (BetterStack
// clientId mapping, hasLogs flag).
//
// All exported functions are async. Result is cached for 5 minutes —
// SP registration changes are infrequent and the dashboard already has
// per-endpoint caches downstream.

const { observerGet, toObserverNetwork } = require("./observer")
const OVERRIDES = require("./sp-overrides")

const CACHE_TTL_MS = 5 * 60 * 1000
const cache = new Map()

async function loadSPs(network) {
  const cached = cache.get(network)
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) return cached.data

  let providers
  try {
    const data = await observerGet(`/providers/${toObserverNetwork(network)}`)
    providers = data.providers || []
  } catch (err) {
    console.error(`sp-config: failed to load ${network} providers from observer:`, err.message)
    // Serve stale on failure; never cache empty so the next request retries the observer.
    return cached ? cached.data : []
  }

  const overrides = OVERRIDES[network] || {}
  const sps = providers
    .filter(p => p.isActive)
    .map(p => {
      const id = Number(p.providerId)
      const ov = overrides[id] || {}
      return {
        id,
        name: p.name,
        address: p.serviceProvider,
        serviceURL: (p.capabilities && p.capabilities.serviceURL) || null,
        clientId: ov.clientId || p.name,
        hasLogs: !!ov.hasLogs,
        endorsed: !!p.isEndorsed,
        approved: !!p.isApproved,
        description: p.description || "",
        network,
      }
    })
    .sort((a, b) => a.id - b.id)

  // Only cache when the observer actually returned something (avoid persisting
  // a transient zero-providers response and starving the dashboard for 5 min).
  if (sps.length > 0) {
    cache.set(network, { data: sps, ts: Date.now() })
  }
  return sps
}

async function getSP(id, network) {
  network = network || "mainnet"
  const n = Number(id)
  const sps = await loadSPs(network)
  return sps.find(sp => sp.id === n) || null
}

async function getAllSPs(network) {
  return loadSPs(network || "mainnet")
}

async function getTrackedSPs(network) {
  const sps = await loadSPs(network || "mainnet")
  return sps.filter(sp => sp.hasLogs)
}

module.exports = { getSP, getAllSPs, getTrackedSPs }
