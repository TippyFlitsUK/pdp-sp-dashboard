// Background /pdp/ping probes — 60s interval; Curio /version probes — 10 min interval
const { getAllSPs } = require("./sp-config")

// Key by network:id to avoid collisions between mainnet and calibnet SP IDs
const livenessState = new Map()
const versionState = new Map()
const VERSION_INTERVAL_MS = 10 * 60 * 1000
// Curio answers GET /version with its build string as plain text, e.g.
// "1.29.0+mainnet+git_409a2ab3_2026-10-12T16:13:22-06:00"; anything else is not a version.
const CURIO_VERSION = /^\d+\.\d+\.\d+[0-9A-Za-z.+_:-]*$/

async function probeVersion(sp) {
  const key = `${sp.network}:${sp.id}`
  try {
    const res = await fetch(`${sp.serviceURL.replace(/\/+$/, "")}/version`, { signal: AbortSignal.timeout(10000) })
    const text = res.ok ? (await res.text()).trim() : ""
    if (CURIO_VERSION.test(text) && text.length <= 200) {
      versionState.set(key, { version: text, lastCheck: new Date().toISOString() })
    }
  } catch {}
}

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
    const checkVersions = Date.now() - lastVersionRun >= VERSION_INTERVAL_MS
    if (checkVersions) lastVersionRun = Date.now()
    // Probe in parallel, 5 at a time
    for (let i = 0; i < sps.length; i += 5) {
      const batch = sps.slice(i, i + 5)
      await Promise.all(batch.map(sp => checkVersions ? Promise.all([probeSP(sp), probeVersion(sp)]) : probeSP(sp)))
    }
  } catch (err) {
    console.error("liveness probeAll error:", err.message)
  }
}

let lastVersionRun = 0

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

// Last version each SP reported on its /version endpoint; kept through a failed probe.
function getVersions(network) {
  network = network || "mainnet"
  const prefix = network + ":"
  const result = {}
  for (const [key, state] of versionState) {
    if (key.startsWith(prefix)) result[Number(key.slice(prefix.length))] = state.version
  }
  return result
}

module.exports = { startLivenessProbes, getLiveness, getVersions }
