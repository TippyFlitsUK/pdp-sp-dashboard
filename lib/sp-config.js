// All 27 mainnet providers from SP Registry
// hasLogs = true for the 7 tracked in Better Stack pdp-warp-speed
const SP_LIST = [
  { id: 1, name: "ezpdpz-main", address: "0x32c90c26bCA6eD3945De9b29BA4e19D38314D618", serviceURL: "https://main.ezpdpz.net", clientId: "ezpdpz-main", hasLogs: true },
  { id: 2, name: "beck-main", address: "0x86d026029052c6582d277d9b28700Edc9670B150", serviceURL: "https://pdp-main.660688.xyz:8443", clientId: "beck-main", hasLogs: true },
  { id: 5, name: "Mongo2Stor Mainnet", address: "0x010ecc2436E0c5eA4741CD25A27A5476fE7A252c", serviceURL: "https://pdp.lotus.dedyn.io", clientId: "Mongo2Stor Mainnet", hasLogs: true },
  { id: 7, name: "infrafolio-mainnet-pdp", address: "0x89B5899619d93A180d38011B8aeC849DEEa3F573", serviceURL: "https://mainnet-pdp.infrafolio.com", clientId: "infrafolio-mainnet-pdp", hasLogs: true },
  { id: 9, name: "ruka-main", address: "0xB8f10dA7A39AA54D696246C8E68A1a4aA123a5Cd", serviceURL: "https://ruka.drongyl.com", clientId: "ruka-main", hasLogs: true },
  { id: 11, name: "laughstorage", address: "0x846e8CAd00fAD118604623A283fc472d902B89c2", serviceURL: "https://la-pdp.laughstorage.com", clientId: "la-pdp.laughstorage.com", hasLogs: true },
  { id: 14, name: "pdp-superusey", address: "0xbd0FfC89500920349140b8e6016416cc7eD96C52", serviceURL: "https://pdp.superusey.com", clientId: "pdp-superusey", hasLogs: true },
]

// Map by provider ID for quick lookup
const SP_BY_ID = new Map()
for (const sp of SP_LIST) {
  SP_BY_ID.set(sp.id, sp)
}

function getSP(id) {
  return SP_BY_ID.get(Number(id)) || null
}

function getAllSPs() {
  return SP_LIST
}

function getTrackedSPs() {
  return SP_LIST.filter(sp => sp.hasLogs)
}

module.exports = { getSP, getAllSPs, getTrackedSPs }
