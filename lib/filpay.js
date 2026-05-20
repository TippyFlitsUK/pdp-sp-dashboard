// FilecoinPay data layer — reads indexed `fp_*` events from foc-observer
// and live state via observer's /rail and /account endpoints. Replaces
// the FilecoinPay (Goldsky) subgraph.
//
// State derivation:
//   ACTIVE     = in fp_rail_created and NOT in fp_rail_terminated/finalized
//   TERMINATED = in fp_rail_terminated and NOT in fp_rail_finalized
//   FINALIZED  = in fp_rail_finalized
//
// Current rate / lockup come from latest fp_rail_rate_modified /
// fp_rail_lockup_modified per rail_id; 0 if no modification events.
// Total settled per rail = SUM(fp_rail_settled.total_settled_amount).

const { observerSql, observerGet, toObserverNetwork, assertEthAddress, assertNumericId } = require("./observer")

const USDFC_TOKEN = {
  mainnet: "0x80b98d3aa09ffff255c3ba4a241111ff1262f045",
  calibration: "0xb3042734b608a1b16e9e86b374a3f3e389b4cdf0",
}

function lc(addr) {
  return (addr || "").toLowerCase()
}

// Latest rate / lockup state per rail (subquery used by getSpRails + getAllActiveRailsByPayee)
const RAIL_LATEST_CTE = `
  WITH latest_rate AS (
    SELECT DISTINCT ON (rail_id) rail_id, new_rate
    FROM fp_rail_rate_modified
    ORDER BY rail_id, block_number DESC
  ),
  latest_lockup AS (
    SELECT DISTINCT ON (rail_id) rail_id, new_lockup_period, new_lockup_fixed
    FROM fp_rail_lockup_modified
    ORDER BY rail_id, block_number DESC
  ),
  settlement_totals AS (
    SELECT rail_id, SUM(total_settled_amount::numeric) AS total_settled,
           MAX(settled_up_to) AS settled_up_to, COUNT(*) AS settlement_count
    FROM fp_rail_settled
    GROUP BY rail_id
  ),
  termination AS (
    SELECT rail_id, MAX(end_epoch) AS end_epoch FROM fp_rail_terminated GROUP BY rail_id
  ),
  finalization AS (
    SELECT DISTINCT rail_id FROM fp_rail_finalized
  )
`

// ---------- per-payee aggregate for network/overview ----------

async function getAllActiveRailsByPayee(network) {
  const rows = await observerSql(network, `
    ${RAIL_LATEST_CTE}
    SELECT
      c.payee,
      COUNT(*) FILTER (WHERE f.rail_id IS NULL AND t.rail_id IS NULL) AS active_rails,
      COALESCE(SUM(CASE WHEN f.rail_id IS NULL AND t.rail_id IS NULL THEN COALESCE(lr.new_rate::numeric, 0) ELSE 0 END), 0) AS total_rate,
      COALESCE(SUM(st.total_settled), 0) AS total_settled
    FROM fp_rail_created c
    LEFT JOIN latest_rate lr ON lr.rail_id = c.rail_id
    LEFT JOIN settlement_totals st ON st.rail_id = c.rail_id
    LEFT JOIN termination t ON t.rail_id = c.rail_id
    LEFT JOIN finalization f ON f.rail_id = c.rail_id
    GROUP BY c.payee
  `)

  const out = {}
  for (const r of rows) {
    out[r.payee] = {
      activeRails: Number(r.active_rails || 0),
      totalRate: String(r.total_rate || "0"),
      totalSettled: String(r.total_settled || "0"),
    }
  }
  return out
}

// ---------- all rails for an SP (sp/economics, sp/activity) ----------

async function getSpRails(network, address) {
  const addr = assertEthAddress(lc(address))
  const rows = await observerSql(network, `
    ${RAIL_LATEST_CTE}
    SELECT
      c.rail_id,
      c.payer, c.payee, c.operator, c.validator, c.token,
      c.commission_rate_bps,
      c.timestamp AS created_at,
      COALESCE(lr.new_rate, 0)               AS payment_rate,
      COALESCE(ll.new_lockup_period, 0)      AS lockup_period,
      COALESCE(ll.new_lockup_fixed, 0)       AS lockup_fixed,
      COALESCE(st.total_settled, 0)          AS total_settled,
      COALESCE(st.settled_up_to, 0)          AS settled_up_to,
      COALESCE(st.settlement_count, 0)       AS settlement_count,
      COALESCE(t.end_epoch, 0)               AS end_epoch,
      (f.rail_id IS NOT NULL)                AS is_finalized,
      (t.rail_id IS NOT NULL)                AS is_terminated
    FROM fp_rail_created c
    LEFT JOIN latest_rate lr ON lr.rail_id = c.rail_id
    LEFT JOIN latest_lockup ll ON ll.rail_id = c.rail_id
    LEFT JOIN settlement_totals st ON st.rail_id = c.rail_id
    LEFT JOIN termination t ON t.rail_id = c.rail_id
    LEFT JOIN finalization f ON f.rail_id = c.rail_id
    WHERE c.payee = '${addr}'
    ORDER BY c.rail_id ASC
  `)

  return rows.map(r => ({
    railId: String(r.rail_id),
    paymentRate: String(r.payment_rate || "0"),
    state: r.is_finalized ? "FINALIZED" : r.is_terminated ? "TERMINATED" : "ACTIVE",
    totalSettledAmount: String(r.total_settled || "0"),
    settledUpto: String(r.settled_up_to || "0"),
    totalSettlements: Number(r.settlement_count || 0),
    totalRateChanges: 0,
    lockupFixed: String(r.lockup_fixed || "0"),
    lockupPeriod: String(r.lockup_period || "0"),
    endEpoch: String(r.end_epoch || "0"),
    commissionRateBps: String(r.commission_rate_bps || "0"),
    payer: { id: r.payer },
    payee: { id: r.payee },
    operator: { id: r.operator },
    token: { symbol: "USDFC", decimals: "18" },
    createdAt: String(r.created_at),
  }))
}

// ---------- single rail detail with recent settlements (sp/rail/:railId) ----------

async function getRailDetail(network, railId) {
  let id
  try { id = assertNumericId(railId) } catch { return null }

  // Per-rail focused query — NOT the network-wide RAIL_LATEST_CTE.
  // The CTE version aggregates across ALL 1500+ rails (~915k rate-modified
  // rows, ~8k settled rows) before filtering and was the 17s hot spot.
  // Scalar subqueries scoped to this rail_id finish in <100ms.
  const railFocusedSql = `
    SELECT
      c.rail_id, c.payer, c.payee, c.operator, c.validator, c.token,
      c.commission_rate_bps, c.timestamp AS created_at,
      COALESCE((
        SELECT new_rate FROM fp_rail_rate_modified
        WHERE rail_id = ${id} ORDER BY block_number DESC LIMIT 1
      ), 0) AS payment_rate,
      COALESCE((
        SELECT new_lockup_period FROM fp_rail_lockup_modified
        WHERE rail_id = ${id} ORDER BY block_number DESC LIMIT 1
      ), 0) AS lockup_period,
      COALESCE((
        SELECT new_lockup_fixed FROM fp_rail_lockup_modified
        WHERE rail_id = ${id} ORDER BY block_number DESC LIMIT 1
      ), 0) AS lockup_fixed,
      COALESCE((
        SELECT SUM(total_settled_amount::numeric) FROM fp_rail_settled WHERE rail_id = ${id}
      ), 0) AS total_settled,
      COALESCE((
        SELECT MAX(settled_up_to) FROM fp_rail_settled WHERE rail_id = ${id}
      ), 0) AS settled_up_to,
      COALESCE((
        SELECT COUNT(*) FROM fp_rail_settled WHERE rail_id = ${id}
      ), 0) AS settlement_count,
      COALESCE((
        SELECT MAX(end_epoch) FROM fp_rail_terminated WHERE rail_id = ${id}
      ), 0) AS end_epoch,
      EXISTS(SELECT 1 FROM fp_rail_finalized WHERE rail_id = ${id}) AS is_finalized,
      EXISTS(SELECT 1 FROM fp_rail_terminated WHERE rail_id = ${id}) AS is_terminated
    FROM fp_rail_created c
    WHERE c.rail_id = ${id}
    LIMIT 1
  `

  const [createdRows, settlements, liveProm] = await Promise.all([
    observerSql(network, railFocusedSql),
    observerSql(network, `
      SELECT total_settled_amount, total_net_payee_amount, operator_commission,
             network_fee, settled_up_to, timestamp AS created_at, tx_hash
      FROM fp_rail_settled
      WHERE rail_id = ${id}
      ORDER BY block_number DESC
      LIMIT 20
    `),
    observerGet(`/rail/${toObserverNetwork(network)}/${id}`).catch(() => null),
  ])

  if (createdRows.length === 0) return null
  const r = createdRows[0]
  // Total one-time payments via separate query (cheap). Amount = net + operator + fee.
  const otpRows = await observerSql(network, `
    SELECT COALESCE(SUM((net_payee_amount + operator_commission + network_fee)::numeric), 0) AS total,
           COUNT(*) AS n
    FROM fp_one_time_payment WHERE rail_id = ${id}
  `).catch(() => [])

  return {
    railId: String(r.rail_id),
    paymentRate: String(r.payment_rate || "0"),
    lockupFixed: String(r.lockup_fixed || "0"),
    lockupPeriod: String(r.lockup_period || "0"),
    settledUpto: String(r.settled_up_to || "0"),
    state: r.is_finalized ? "FINALIZED" : r.is_terminated ? "TERMINATED" : "ACTIVE",
    endEpoch: String(r.end_epoch || "0"),
    commissionRateBps: String(r.commission_rate_bps || "0"),
    totalOneTimePaymentAmount: String((otpRows[0] || {}).total || "0"),
    totalSettledAmount: String(r.total_settled || "0"),
    totalOneTimePayments: Number((otpRows[0] || {}).n || 0),
    totalSettlements: Number(r.settlement_count || 0),
    totalRateChanges: 0,
    createdAt: String(r.created_at),
    payer: { id: r.payer },
    payee: { id: r.payee },
    operator: { id: r.operator },
    token: { symbol: "USDFC", decimals: "18" },
    settlements: settlements.map(s => ({
      totalSettledAmount: String(s.total_settled_amount || "0"),
      totalNetPayeeAmount: String(s.total_net_payee_amount || "0"),
      networkFee: String(s.network_fee || "0"),
      operatorCommission: String(s.operator_commission || "0"),
      settledUpto: String(s.settled_up_to || "0"),
      createdAt: String(s.created_at),
      txHash: s.tx_hash,
    })),
    live: liveProm || null,
  }
}

// ---------- daily revenue (sp/revenue) ----------

async function getDailyRevenue(network, address) {
  const addr = assertEthAddress(lc(address))
  const rows = await observerSql(network, `
    SELECT
      date_trunc('day', to_timestamp(s.timestamp))::date AS day,
      SUM(s.total_net_payee_amount::numeric) AS revenue
    FROM fp_rail_settled s
    JOIN fp_rail_created c ON s.rail_id = c.rail_id
    WHERE c.payee = '${addr}'
      AND s.total_net_payee_amount::numeric > 0
    GROUP BY day
    ORDER BY day ASC
  `)
  // Observer serializes Postgres `date` as ISO timestamp (`2026-05-20T00:00:00.000Z`).
  // Trim to YYYY-MM-DD for the frontend.
  // BigInt math for wei→USDFC conversion; Number(>1e21) silently loses precision.
  return rows.map(r => ({
    date: String(r.day).slice(0, 10),
    revenue: weiToUsdfcNumber(r.revenue),
  }))
}

function weiToUsdfcNumber(wei) {
  if (wei === null || wei === undefined) return 0
  const s = String(wei).split(".")[0]
  if (!/^-?\d+$/.test(s)) return 0
  const negative = s.startsWith("-")
  const digits = negative ? s.slice(1) : s
  const padded = digits.padStart(19, "0")
  const whole = padded.slice(0, -18)
  const frac = padded.slice(-18, -12) // 6 decimal places of precision
  const out = Number(`${whole}.${frac}`)
  return negative ? -out : out
}

// ---------- FilecoinPay USDFC account state (sp/economics) ----------

async function getAccount(network, address) {
  const addr = lc(address)
  const token = USDFC_TOKEN[network]
  if (!token) return null
  try {
    const data = await observerGet(`/account/${toObserverNetwork(network)}/${token}/${addr}`)
    return {
      funds: String(data.funds || "0"),
      payout: "0", // not exposed by observer's account endpoint; rarely used in UI
      fundsCollected: "0",
      lockupCurrent: String(data.lockupCurrent || "0"),
      lockupRate: String(data.lockupRate || "0"),
      lastSettled: data.lockupLastSettledAt ? String(data.lockupLastSettledAt) : null,
      availableFunds: String(data.availableFunds || "0"),
      fundedUntilEpoch: String(data.fundedUntilEpoch || "0"),
      token: { symbol: "USDFC", decimals: 18 },
    }
  } catch (err) {
    console.error(`getAccount(${addr}) error:`, err.message)
    return null
  }
}

module.exports = {
  getAllActiveRailsByPayee,
  getSpRails,
  getRailDetail,
  getDailyRevenue,
  getAccount,
}
