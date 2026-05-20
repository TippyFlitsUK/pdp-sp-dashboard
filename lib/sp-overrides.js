// Per-network overrides applied on top of observer's /providers data.
// Only entries where the BetterStack pdp-warp-speed log `client_id` differs
// from the SP's on-chain name, or where we need to flag log availability.
//
// Defaults if no entry:
//   clientId = name (from observer)
//   hasLogs  = false  (no BS log queries)
//
// Add an entry the moment an SP starts shipping Curio logs to pdp-warp-speed.

module.exports = {
  mainnet: {
    1:  { clientId: "ezpdpz-main",            hasLogs: true },
    2:  { clientId: "beck-main",              hasLogs: true },
    5:  { clientId: "Mongo2Stor Mainnet",     hasLogs: true },
    7:  { clientId: "infrafolio-mainnet-pdp", hasLogs: true },
    9:  { clientId: "ruka-main",              hasLogs: true },
    11: { clientId: "la-pdp.laughstorage.com", hasLogs: true },
    14: { clientId: "pdp-superusey",          hasLogs: true },
  },
  calibration: {
    2:  { clientId: "ezpdpz-calib2",   hasLogs: true },
    4:  { clientId: "infrafolio-calib", hasLogs: true },
    5:  { clientId: "Mongo2Stor",      hasLogs: true },
    6:  { clientId: "beck-calib",      hasLogs: true },
    9:  { clientId: "ezpdpz-calib",    hasLogs: true },
    16: { clientId: "ruka",            hasLogs: true },
    17: { clientId: "superusey-calib", hasLogs: true },
    19: { clientId: "nd-calib",        hasLogs: true },
  },
}
