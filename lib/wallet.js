// Wallet balance helpers — direct Filecoin RPC via viem.
// Native FIL + USDFC ERC-20 balance. Observer does NOT expose these
// (its /account endpoint is FilecoinPay account state, not wallet balance).

const { createPublicClient, http, getAddress } = require("viem")
const { filecoin } = require("viem/chains")

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
    clients[network] = createPublicClient({ chain: cfg.chain, transport: http(cfg.rpc) })
  }
  return clients[network]
}

const USDFC_ADDRESS = {
  mainnet: "0x80b98d3AA09FFfF255C3Ba4a241111Ff1262F045",
  calibration: "0xb3042734b608a1B16e9e86B374A3f3e389B4cDf0",
}
const erc20BalanceAbi = [{
  type: "function",
  inputs: [{ name: "account", type: "address" }],
  name: "balanceOf",
  outputs: [{ name: "", type: "uint256" }],
  stateMutability: "view",
}]

async function getWalletBalances(address, network = "mainnet") {
  try {
    const usdfcRaw = USDFC_ADDRESS[network]
    if (!usdfcRaw) return null
    const client = getClient(network)
    const addr = getAddress(address)
    const usdfcAddr = getAddress(usdfcRaw)
    const [filBalance, usdfcBalance] = await Promise.all([
      client.getBalance({ address: addr }),
      client.readContract({
        address: usdfcAddr,
        abi: erc20BalanceAbi,
        functionName: "balanceOf",
        args: [addr],
      }),
    ])
    return {
      fil: Number(filBalance.toString()) / 1e18,
      usdfc: Number(usdfcBalance.toString()) / 1e18,
    }
  } catch (err) {
    console.error(`getWalletBalances(${address}) error:`, err.message)
    return null
  }
}

module.exports = { getWalletBalances }
