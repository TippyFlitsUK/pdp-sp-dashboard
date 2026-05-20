// Background /pdp/ping probes — 60s interval
const { getAllSPs } = require("./sp-config")

// Key by network:id to avoid collisions between mainnet and calibnet SP IDs
const livenessState = new Map()

async function probeSP(sp) {
  const key = `${sp.network}:${sp.id}`
  const start = Date.now()
  try {
    const res = await fetch(`${sp.serviceURL}/pdp/ping`, { signal: AbortSignal.timeout(10000) })
    if (res.ok) {
      livenessState.set(key, {
        alive: true,
        latencyMs: Date.now() - start,
        lastCheck: new Date().toISOString(),
      })
      return
    }
  } catch {}
  const fallbackStart = Date.now()
  try {
    const res = await fetch(`${sp.serviceURL}/health`, { signal: AbortSignal.timeout(10000) })
    livenessState.set(key, {
      alive: res.ok,
      latencyMs: res.ok ? Date.now() - fallbackStart : null,
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
  try {
    const [mainnetSPs, calibrationSPs] = await Promise.all([
      getAllSPs("mainnet"),
      getAllSPs("calibration"),
    ])
    const sps = [...mainnetSPs, ...calibrationSPs].filter(sp => sp.serviceURL)
    // Probe in parallel, 5 at a time
    for (let i = 0; i < sps.length; i += 5) {
      const batch = sps.slice(i, i + 5)
      await Promise.all(batch.map(probeSP))
    }
  } catch (err) {
    console.error("liveness probeAll error:", err.message)
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
