// Background /pdp/ping probes — 60s interval
const { getAllSPs } = require("./sp-config")

const livenessState = new Map() // id -> { alive: bool, latencyMs: number, lastCheck: Date }

async function probeSP(sp) {
  const url = `${sp.serviceURL}/pdp/ping`
  const start = Date.now()
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(10000) })
    const latencyMs = Date.now() - start
    livenessState.set(sp.id, {
      alive: res.ok,
      latencyMs,
      lastCheck: new Date().toISOString(),
    })
  } catch {
    livenessState.set(sp.id, {
      alive: false,
      latencyMs: null,
      lastCheck: new Date().toISOString(),
    })
  }
}

async function probeAll() {
  const sps = getAllSPs()
  // Probe in parallel, 5 at a time
  for (let i = 0; i < sps.length; i += 5) {
    const batch = sps.slice(i, i + 5)
    await Promise.all(batch.map(probeSP))
  }
}

function startLivenessProbes() {
  probeAll()
  setInterval(probeAll, 60000)
}

function getLiveness() {
  const result = {}
  for (const [id, state] of livenessState) {
    result[id] = state
  }
  return result
}

module.exports = { startLivenessProbes, getLiveness }
