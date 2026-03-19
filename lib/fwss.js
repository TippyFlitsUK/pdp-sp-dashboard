const { createPublicClient, http, getAddress } = require("viem")
const { filecoin } = require("viem/chains")

const FWSS_STATEVIEW = {
  mainnet: "0x638a4986332bF9B889E5D7435B966C5ecdE077Fa",
  calibration: "0x53d235D474585EC102ccaB7e0cdcE951dD00f716",
}

const getDataSetAbi = [
  {
    type: "function",
    inputs: [{ name: "dataSetId", internalType: "uint256", type: "uint256" }],
    name: "getDataSet",
    outputs: [
      {
        name: "info",
        internalType: "struct FilecoinWarmStorageService.DataSetInfoView",
        type: "tuple",
        components: [
          { name: "pdpRailId", internalType: "uint256", type: "uint256" },
          { name: "cacheMissRailId", internalType: "uint256", type: "uint256" },
          { name: "cdnRailId", internalType: "uint256", type: "uint256" },
          { name: "payer", internalType: "address", type: "address" },
          { name: "payee", internalType: "address", type: "address" },
          { name: "serviceProvider", internalType: "address", type: "address" },
          { name: "commissionBps", internalType: "uint256", type: "uint256" },
          { name: "clientDataSetId", internalType: "uint256", type: "uint256" },
          { name: "pdpEndEpoch", internalType: "uint256", type: "uint256" },
          { name: "providerId", internalType: "uint256", type: "uint256" },
          { name: "dataSetId", internalType: "uint256", type: "uint256" },
        ],
      },
    ],
    stateMutability: "view",
  },
  {
    type: "function",
    inputs: [
      { name: "dataSetId", internalType: "uint256", type: "uint256" },
      { name: "key", internalType: "string", type: "string" },
    ],
    name: "getDataSetMetadata",
    outputs: [
      { name: "exists", internalType: "bool", type: "bool" },
      { name: "value", internalType: "string", type: "string" },
    ],
    stateMutability: "view",
  },
]

const CHAIN_CONFIG = {
  mainnet: {
    chain: filecoin,
    rpc: "https://api.node.glif.io/rpc/v1",
  },
  calibration: {
    chain: {
      ...filecoin,
      id: 314159,
      name: "Filecoin Calibration",
      network: "filecoin-calibration",
      rpcUrls: {
        default: { http: ["https://api.calibration.node.glif.io/rpc/v1"] },
      },
    },
    rpc: "https://api.calibration.node.glif.io/rpc/v1",
  },
}

const clients = {}
function getClient(network = "mainnet") {
  if (!clients[network]) {
    const cfg = CHAIN_CONFIG[network] || CHAIN_CONFIG.mainnet
    clients[network] = createPublicClient({
      chain: cfg.chain,
      transport: http(cfg.rpc),
    })
  }
  return clients[network]
}

// Cache FWSS data per dataset (rarely changes)
const fwssCache = new Map()
const FWSS_CACHE_TTL = 30 * 60 * 1000 // 30 min

async function getDataSetInfo(dataSetId, network = "mainnet") {
  const key = `${network}:${dataSetId}`
  const cached = fwssCache.get(key)
  if (cached && Date.now() - cached.ts < FWSS_CACHE_TTL) return cached.data

  const address = FWSS_STATEVIEW[network]
  if (!address) return null

  try {
    const rpcClient = getClient(network)
    const info = await rpcClient.readContract({
      address,
      abi: getDataSetAbi,
      functionName: "getDataSet",
      args: [BigInt(dataSetId)],
    })

    // pdpRailId === 0n means dataset not managed by FWSS
    if (info.pdpRailId === 0n) {
      fwssCache.set(key, { data: null, ts: Date.now() })
      return null
    }

    const data = {
      payer: info.payer.toLowerCase(),
      payee: info.payee.toLowerCase(),
      serviceProvider: info.serviceProvider.toLowerCase(),
      pdpRailId: Number(info.pdpRailId),
      providerId: Number(info.providerId),
      dataSetId: Number(info.dataSetId),
    }

    // Try to get app metadata
    try {
      const [exists, value] = await rpcClient.readContract({
        address,
        abi: getDataSetAbi,
        functionName: "getDataSetMetadata",
        args: [BigInt(dataSetId), "app"],
      })
      if (exists) data.appMetadata = value
    } catch {}

    fwssCache.set(key, { data, ts: Date.now() })
    return data
  } catch (err) {
    console.error(`FWSS getDataSet(${dataSetId}) error:`, err.message)
    fwssCache.set(key, { data: null, ts: Date.now() })
    return null
  }
}

// Batch fetch for multiple datasets (with concurrency limit)
async function getDataSetInfoBatch(dataSetIds, network = "mainnet") {
  const results = {}
  const BATCH_SIZE = 10
  for (let i = 0; i < dataSetIds.length; i += BATCH_SIZE) {
    const batch = dataSetIds.slice(i, i + BATCH_SIZE)
    const batchResults = await Promise.all(
      batch.map(id => getDataSetInfo(id, network).catch(() => null))
    )
    for (let j = 0; j < batch.length; j++) {
      if (batchResults[j]) results[batch[j]] = batchResults[j]
    }
  }
  return results
}

// USDFC token contract on Filecoin mainnet
const USDFC_ADDRESS = "0x80b98d3AA09FFfF255C3Ba4a241111Ff1262F045"
const erc20BalanceAbi = [{
  type: "function",
  inputs: [{ name: "account", type: "address" }],
  name: "balanceOf",
  outputs: [{ name: "", type: "uint256" }],
  stateMutability: "view",
}]

async function getWalletBalances(address, network = "mainnet") {
  try {
    const rpcClient = getClient(network)
    const addr = getAddress(address)
    const usdfcAddr = getAddress(USDFC_ADDRESS)
    const [filBalance, usdfcBalance] = await Promise.all([
      rpcClient.getBalance({ address: addr }),
      rpcClient.readContract({
        address: usdfcAddr,
        abi: erc20BalanceAbi,
        functionName: "balanceOf",
        args: [addr],
      }),
    ])
    // Format with string math to avoid BigInt precision loss
    const filStr = filBalance.toString()
    const usdfcStr = usdfcBalance.toString()
    return {
      fil: Number(filStr) / 1e18,
      usdfc: Number(usdfcStr) / 1e18,
    }
  } catch (err) {
    console.error(`getWalletBalances(${address}) error:`, err.message)
    return null
  }
}

module.exports = { getDataSetInfoBatch, getWalletBalances }
