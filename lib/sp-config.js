// All mainnet and calibnet providers
// hasLogs = true for SPs tracked in Better Stack pdp-warp-speed
const SP_LIST = [
  // Mainnet
  { id: 1, name: "ezpdpz-main", address: "0x32c90c26bCA6eD3945De9b29BA4e19D38314D618", serviceURL: "https://main.ezpdpz.net", clientId: "ezpdpz-main", hasLogs: true, network: "mainnet" },
  { id: 2, name: "beck-main", address: "0x86d026029052c6582d277d9b28700Edc9670B150", serviceURL: "https://pdp-main.660688.xyz:8443", clientId: "beck-main", hasLogs: true, network: "mainnet" },
  { id: 5, name: "Mongo2Stor Mainnet", address: "0x010ecc2436E0c5eA4741CD25A27A5476fE7A252c", serviceURL: "https://pdp.lotus.dedyn.io", clientId: "Mongo2Stor Mainnet", hasLogs: true, network: "mainnet" },
  { id: 7, name: "infrafolio-mainnet-pdp", address: "0x89B5899619d93A180d38011B8aeC849DEEa3F573", serviceURL: "https://mainnet-pdp.infrafolio.com", clientId: "infrafolio-mainnet-pdp", hasLogs: true, network: "mainnet" },
  { id: 9, name: "ruka-main", address: "0xB8f10dA7A39AA54D696246C8E68A1a4aA123a5Cd", serviceURL: "https://ruka.drongyl.com", clientId: "ruka-main", hasLogs: true, network: "mainnet" },
  { id: 11, name: "laughstorage", address: "0x846e8CAd00fAD118604623A283fc472d902B89c2", serviceURL: "https://la-pdp.laughstorage.com", clientId: "la-pdp.laughstorage.com", hasLogs: true, network: "mainnet" },
  { id: 14, name: "pdp-superusey", address: "0xbd0FfC89500920349140b8e6016416cc7eD96C52", serviceURL: "https://pdp.superusey.com", clientId: "pdp-superusey", hasLogs: true, network: "mainnet" },

  // Calibration
  { id: 2, name: "ezpdpz-calib2", address: "0xbCdf1bdc1a97D071a5a8EF03F1F05225b6E2a1Ba", serviceURL: "https://calib2.ezpdpz.net", clientId: "ezpdpz-calib2", hasLogs: true, network: "calibration" },
  { id: 4, name: "infrafolio-calib", address: "0xCb9e86945cA31E6C3120725BF0385CBAD684040c", serviceURL: "https://caliberation-pdp.infrafolio.com", clientId: "infrafolio-calib", hasLogs: true, network: "calibration" },
  { id: 5, name: "Mongo2Stor", address: "0xB709A785c765d7d3F7d94dbA367DA6a611D7972b", serviceURL: "https://warp.lotus.dedyn.io", clientId: "Mongo2Stor", hasLogs: true, network: "calibration" },
  { id: 6, name: "beck-calib", address: "0x86d026029052c6582d277d9b28700Edc9670B150", serviceURL: "https://pdp-main.660688.xyz:8443", clientId: "beck-calib", hasLogs: true, network: "calibration" },
  { id: 9, name: "ezpdpz-calib", address: "0xa3971A7234a3379A1813d9867B531e7EeB20ae07", serviceURL: "https://calib.ezpdpz.net", clientId: "ezpdpz-calib", hasLogs: true, network: "calibration" },
  { id: 16, name: "ruka", address: "0x483F1CD029EFCFE5ebe382a8D63E73b0E53c7778", serviceURL: "https://ruka.drongyl.com", clientId: "ruka", hasLogs: true, network: "calibration" },
  { id: 17, name: "superusey-calib", address: "0x994b21F3Ae6960eBCfA0926EA0aCF1Bb9321B2f4", serviceURL: "https://pdp.superusey.com", clientId: "superusey-calib", hasLogs: true, network: "calibration" },
  { id: 19, name: "nd-calib", address: "0x9790c7BFCfe6264f78Da6D3402566766aB6fDA5C", serviceURL: "https://laughstorage.com", clientId: "nd-calib", hasLogs: true, network: "calibration" },
]

function getSP(id, network) {
  network = network || "mainnet"
  var n = Number(id)
  return SP_LIST.find(function(sp) { return sp.id === n && sp.network === network }) || null
}

function getAllSPs(network) {
  network = network || "mainnet"
  return SP_LIST.filter(function(sp) { return sp.network === network })
}

function getTrackedSPs(network) {
  network = network || "mainnet"
  return SP_LIST.filter(function(sp) { return sp.network === network && sp.hasLogs })
}

module.exports = { getSP, getAllSPs, getTrackedSPs }
