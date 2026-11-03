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
const COUNTRY = new Intl.DisplayNames(["en"], { type: "region" })

// Registry location is SP-typed: usually "C=FR;ST=Grand Est;L=Strasbourg", sometimes
// comma-separated or free text ("earth", "EU-NO"). Structured values read "City, Country";
// anything else is shown as written.
function regionFromLocation(location) {
  if (!location) return null
  const parts = {}
  for (const kv of location.split(/[;,]/)) {
    const m = kv.trim().match(/^(C|ST|L)=(.*)$/)
    if (m) parts[m[1]] = m[2].trim()
  }
  if (!parts.C) return location.trim()
  let country = parts.C
  try { country = COUNTRY.of(parts.C.toUpperCase()) || parts.C } catch (e) {}
  const city = parts.L || parts.ST
  return city && !country.startsWith(city) ? `${city}, ${country}` : country
}

// capacityTiB (curio#1344) is a big-endian unsigned integer. The observer returns the bytes
// as text when they are valid UTF-8 ("\x14" is 20) and as 0x-hex otherwise ("0x01f4" is 500).
// A value of two or more ASCII digits was written as decimal text by a non-Curio tool, and is
// read as that number. Null when the provider has not set it.
function decodeCapacityTiB(value) {
  if (value == null || value === "") return null
  if (/^0x[0-9a-f]+$/i.test(value)) return Number(BigInt(value))
  if (/^[0-9]{2,}$/.test(value)) return Number(value)
  let n = 0n
  for (const b of Buffer.from(value, "utf8")) n = (n << 8n) | BigInt(b)
  return Number(n)
}

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
      const caps = p.capabilities || {}
      return {
        id,
        name: p.name,
        address: p.serviceProvider,
        serviceURL: caps.serviceURL || null,
        location: caps.location || null,
        region: regionFromLocation(caps.location),
        capacityTiB: decodeCapacityTiB(caps.capacityTiB),
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

module.exports = { getSP, getAllSPs, getTrackedSPs, regionFromLocation, decodeCapacityTiB }
