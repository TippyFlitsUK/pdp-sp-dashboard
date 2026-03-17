// Background /pdp/ping probes — 60s interval
const { getAllSPs } = require("./sp-config")

// Key by network:id to avoid collisions between mainnet and calibnet SP IDs
const livenessState = new Map()

async function probeSP(sp) {
  const url = `${sp.serviceURL}/pdp/ping`
  const key = `${sp.network}:${sp.id}`
  const start = Date.now()
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) })
    const latencyMs = Date.now() - start
    livenessState.set(key, {
      alive: res.ok,
      latencyMs,
      lastCheck: new Date().toISOString(),
    })
  } catch {
    livenessState.set(key, {
      alive: false,
      latencyMs: null,
      lastCheck: new Date().toISOString(),
    })
  }
}

async function probeAll() {
  const sps = [...getAllSPs("mainnet"), ...getAllSPs("calibration")]
  // Probe in parallel, 5 at a time
  for (let i = 0; i < sps.length; i += 5) {
    const batch = sps.slice(i, i + 5)
    await Promise.all(batch.map(probeSP))
  }
}

function startLivenessProbes() {
  probeAll()
  const interval = setInterval(probeAll, 60000)
  interval.unref()
}

function getLiveness(network) {
  network = network || "mainnet"
  const prefix = network + ":"
  const result = {}
  for (const [key, state] of livenessState) {
    if (key.startsWith(prefix)) {
      const id = Number(key.slice(prefix.length))
      result[id] = state
    }
  }
  return result
}

module.exports = { startLivenessProbes, getLiveness }
