// PDP data layer — reads indexed events from foc-observer and (where
// needed) live contract state via observer's REST endpoints. Replaces
// PDP Scan (Goldsky) subgraph queries.
//
// All return shapes match the subgraph shapes the frontend already
// consumes, so server.js handlers stay the same externally.

const { observerSql, observerGet, toObserverNetwork, assertEthAddress, assertNumericId } = require("./observer")

// ---------- helpers ----------

function lc(addr) {
  return (addr || "").toLowerCase()
}

// Observer's /proving response returns `week` as a truncated Date.toString().
// Extract the "MMM dd yyyy" prefix and return unix ms; 0 on parse failure.
function parseObserverWeek(week) {
  if (!week) return 0
  const m = String(week).match(/^\w+\s+(\w+)\s+(\d+)\s+(\d+)/)
  if (!m) return 0
  const ms = Date.parse(`${m[1]} ${m[2]} ${m[3]} 00:00:00 UTC`)
  return Number.isFinite(ms) ? ms : 0
}

// ---------- network totals (replaces networkMetrics) ----------

async function getNetworkTotals(network) {
  const rows = await observerSql(network, `
    SELECT
      (SELECT COUNT(*) FROM spr_provider_registered) AS providers,
      (SELECT COUNT(*) FROM pdp_data_set_created)    AS proof_sets,
      (SELECT COALESCE(SUM(piece_count), 0) FROM pdp_pieces_added)    AS adds,
      (SELECT COALESCE(SUM(piece_count), 0) FROM pdp_pieces_removed)  AS removes,
      (SELECT COUNT(*) FROM pdp_possession_proven)   AS proofs,
      (SELECT COALESCE(SUM(periods_faulted::numeric), 0) FROM fwss_fault_record)          AS fault_fwss,
      (SELECT COALESCE(SUM(periods_faulted::numeric), 0) FROM storacha_fwss_fault_record) AS fault_storacha,
      (SELECT COALESCE(SUM(raw_size::numeric), 0) FROM fwss_piece_added)                  AS size_fwss,
      (SELECT COALESCE(SUM(raw_size::numeric), 0) FROM storacha_fwss_piece_added)         AS size_storacha
  `)
  const r = rows[0] || {}
  const totalRoots = Number(r.adds || 0) - Number(r.removes || 0)
  const totalSize = BigInt(r.size_fwss || 0) + BigInt(r.size_storacha || 0)
  const totalFaulted = Number(r.fault_fwss || 0) + Number(r.fault_storacha || 0)
  return {
    providers: Number(r.providers || 0),
    proofSets: Number(r.proof_sets || 0),
    roots: totalRoots,
    dataSize: totalSize.toString(),
    storageSize: totalSize.toString(),
    proofsSubmitted: Number(r.proofs || 0),
    faultedPeriods: totalFaulted,
  }
}

// ---------- per-provider rollups (replaces providers query) ----------

async function getAllProvidersRollup(network) {
  // Two parallel queries:
  // (a) per-SP rollup of proof sets, roots, proofs, periods, faults, data size,
  //     and max(timestamp) across pdp_pieces_added + pdp_possession_proven
  // (b) max settlement timestamp per payee from fp_rail_settled JOIN fp_rail_created
  // Merged in JS into a single per-SP record. activityStatus is computed by
  // the consumer (server.js) since the "dormant" threshold belongs to that layer.
  const [pdpRows, settleRows] = await Promise.all([
    observerSql(network, `
      WITH set_stats AS (
        SELECT
          d.set_id,
          d.storage_provider,
          COALESCE(pa.added, 0) - COALESCE(pr.removed, 0) AS roots,
          COALESCE(fpa.size, 0) + COALESCE(spa.size, 0) AS data_size,
          COALESCE(pp.proofs, 0) AS proofs,
          COALESCE(npp.periods, 0) AS proving_periods,
          COALESCE(ff.fault_periods, 0) + COALESCE(sff.fault_periods, 0) AS fault_periods,
          GREATEST(COALESCE(pa.last_ts, 0), COALESCE(pp.last_ts, 0)) AS last_pdp_ts
        FROM pdp_data_set_created d
        LEFT JOIN (SELECT set_id, SUM(piece_count) AS added, MAX(timestamp) AS last_ts FROM pdp_pieces_added GROUP BY set_id) pa ON pa.set_id = d.set_id
        LEFT JOIN (SELECT set_id, SUM(piece_count) AS removed FROM pdp_pieces_removed GROUP BY set_id) pr ON pr.set_id = d.set_id
        LEFT JOIN (SELECT data_set_id, SUM(raw_size::numeric) AS size FROM fwss_piece_added GROUP BY data_set_id) fpa ON fpa.data_set_id = d.set_id
        LEFT JOIN (SELECT data_set_id, SUM(raw_size::numeric) AS size FROM storacha_fwss_piece_added GROUP BY data_set_id) spa ON spa.data_set_id = d.set_id
        LEFT JOIN (SELECT set_id, COUNT(*) AS proofs, MAX(timestamp) AS last_ts FROM pdp_possession_proven GROUP BY set_id) pp ON pp.set_id = d.set_id
        LEFT JOIN (SELECT set_id, COUNT(*) AS periods FROM pdp_next_proving_period GROUP BY set_id) npp ON npp.set_id = d.set_id
        LEFT JOIN (SELECT data_set_id, SUM(periods_faulted::numeric) AS fault_periods FROM fwss_fault_record GROUP BY data_set_id) ff ON ff.data_set_id = d.set_id
        LEFT JOIN (SELECT data_set_id, SUM(periods_faulted::numeric) AS fault_periods FROM storacha_fwss_fault_record GROUP BY data_set_id) sff ON sff.data_set_id = d.set_id
      )
      SELECT
        storage_provider AS address,
        COUNT(*) AS proof_sets,
        SUM(roots) AS roots,
        SUM(data_size) AS data_size,
        SUM(proofs) AS proofs,
        SUM(proving_periods) AS proving_periods,
        SUM(fault_periods) AS fault_periods,
        MAX(last_pdp_ts) AS last_pdp_ts
      FROM set_stats
      GROUP BY storage_provider
    `),
    observerSql(network, `
      SELECT c.payee AS address, MAX(s.timestamp) AS last_settle_ts
      FROM fp_rail_settled s
      JOIN fp_rail_created c ON s.rail_id = c.rail_id
      GROUP BY c.payee
    `),
  ])

  // Index settlement timestamps by lowercase payee address
  const settleByAddr = {}
  for (const r of settleRows) {
    settleByAddr[r.address] = Number(r.last_settle_ts || 0)
  }

  return pdpRows.map(r => {
    const lastPdp = Number(r.last_pdp_ts || 0)
    const lastSettle = settleByAddr[r.address] || 0
    const lastActivity = Math.max(lastPdp, lastSettle)
    return {
      id: r.address,
      address: r.address,
      totalProofSets: Number(r.proof_sets || 0),
      totalRoots: Number(r.roots || 0),
      totalDataSize: String(r.data_size || "0"),
      totalFaultedPeriods: Number(r.fault_periods || 0),
      totalProvingPeriods: Number(r.proving_periods || 0),
      lastActivity: lastActivity || null,
    }
  })
}

// ---------- per-SP proving detail (replaces sp/proving) ----------

async function getProvingDetail(network, address) {
  const addr = assertEthAddress(lc(address))

  // Provider summary + per-dataset proving stats via observer (gap-based fault
  // detection, operator-agnostic). Weekly activity also bundled.
  const obsNet = toObserverNetwork(network)
  // Bounded timeout: observer's gap-detection statement-times-out for SPs with
  // thousands of datasets. Cap well under nginx's 30s proxy_read_timeout so the
  // call fails fast and we fall back to SQL-derived weekly/fault data below,
  // rather than blocking the whole request into a 504.
  const provingPromise = observerGet(`/proving/provider/${obsNet}/${addr}?weeks=12`, { timeoutMs: 15000 }).catch(() => null)

  // Per-dataset stats: size, createdAt, leafCount, totalProofs, lastProofTs, roots.
  // Correlated subqueries-per-row collapse to single grouped scans here — the old
  // form ran 7 correlated subqueries × N datasets over multi-million-row tables and
  // hit observer's statement timeout for large SPs. The CTE narrows every aggregate
  // to this SP's set_ids first.
  const setStatsPromise = observerSql(network, `
    WITH my_sets AS (
      SELECT set_id, timestamp AS created_at
      FROM pdp_data_set_created
      WHERE storage_provider = '${addr}'
    )
    SELECT
      ms.set_id,
      ms.created_at,
      COALESCE(pa.s, 0) AS pieces_added,
      COALESCE(pr.s, 0) AS pieces_removed,
      COALESCE(pp.c, 0) AS total_proofs,
      pp.last_ts AS last_proof_ts,
      npp.leaf_count AS leaf_count,
      (COALESCE(fa.s, 0) + COALESCE(sfa.s, 0)) AS total_data_size
    FROM my_sets ms
    LEFT JOIN (
      SELECT set_id, SUM(piece_count) AS s FROM pdp_pieces_added
      WHERE set_id IN (SELECT set_id FROM my_sets) GROUP BY set_id
    ) pa ON pa.set_id = ms.set_id
    LEFT JOIN (
      SELECT set_id, SUM(piece_count) AS s FROM pdp_pieces_removed
      WHERE set_id IN (SELECT set_id FROM my_sets) GROUP BY set_id
    ) pr ON pr.set_id = ms.set_id
    LEFT JOIN (
      SELECT set_id, COUNT(*) AS c, MAX(timestamp) AS last_ts FROM pdp_possession_proven
      WHERE set_id IN (SELECT set_id FROM my_sets) GROUP BY set_id
    ) pp ON pp.set_id = ms.set_id
    LEFT JOIN LATERAL (
      SELECT leaf_count FROM pdp_next_proving_period npp
      WHERE npp.set_id = ms.set_id ORDER BY block_number DESC LIMIT 1
    ) npp ON true
    LEFT JOIN (
      SELECT data_set_id, SUM(raw_size::numeric) AS s FROM fwss_piece_added
      WHERE data_set_id IN (SELECT set_id FROM my_sets) GROUP BY data_set_id
    ) fa ON fa.data_set_id = ms.set_id
    LEFT JOIN (
      SELECT data_set_id, SUM(raw_size::numeric) AS s FROM storacha_fwss_piece_added
      WHERE data_set_id IN (SELECT set_id FROM my_sets) GROUP BY data_set_id
    ) sfa ON sfa.data_set_id = ms.set_id
    ORDER BY ms.set_id ASC
  `)

  // Provider-level fault count from indexed records (sum across both FWSS variants).
  const faultPromise = observerSql(network, `
    SELECT
      (
        COALESCE((SELECT SUM(f.periods_faulted::numeric) FROM fwss_fault_record f JOIN pdp_data_set_created d ON f.data_set_id = d.set_id WHERE d.storage_provider = '${addr}'), 0)
        + COALESCE((SELECT SUM(f.periods_faulted::numeric) FROM storacha_fwss_fault_record f JOIN pdp_data_set_created d ON f.data_set_id = d.set_id WHERE d.storage_provider = '${addr}'), 0)
      ) AS fault_periods
  `)

  const [proving, setStats, faultRow] = await Promise.all([provingPromise, setStatsPromise, faultPromise])

  // Observer's gap-detection endpoint is the preferred source for per-dataset
  // proving/fault stats and weekly activity. It statement-times-out for SPs with
  // thousands of datasets, so when it's unavailable we reconstruct the two series
  // the proving tab actually renders — weekly proofs+faults and per-set faulted
  // periods — directly from indexed events.
  const obsBySet = {}
  let weeklyActivity
  if (proving) {
    if (proving.datasets) {
      for (const ds of proving.datasets) obsBySet[String(ds.setId)] = ds
    }
    // Observer returns `week` as a truncated Date.toString() like
    // "Mon Apr 27 2026 00:00:00 GM"; parseObserverWeek turns it into unix-ms.
    weeklyActivity = (proving.weeklyActivity || []).map((w) => ({
      id: w.week,
      weekStart: parseObserverWeek(w.week),
      totalProofs: Number(w.proofs || 0),
      totalRootsProved: 0,
      totalFaultedPeriods: Number(w.faults || 0),
      totalRootsAdded: Number(w.piecesAdded || 0),
      totalDataSizeAdded: "0",
      totalProofSetsCreated: Number(w.datasetsCreated || 0),
    }))
  } else {
    const [weeklyRows, setFaultRows] = await Promise.all([
      getWeeklyActivityFromEvents(network, addr),
      getPerSetFaultsFromEvents(network, addr),
    ])
    for (const f of setFaultRows) {
      obsBySet[String(f.set_id)] = { totalFaultedPeriods: Number(f.faulted || 0) }
    }
    // SQL returns `week` as an ISO timestamp (e.g. "2026-05-25T00:00:00.000Z").
    weeklyActivity = weeklyRows.map((w) => ({
      id: w.week,
      weekStart: Date.parse(w.week),
      totalProofs: Number(w.proofs || 0),
      totalRootsProved: 0,
      totalFaultedPeriods: Number(w.faults || 0),
      totalRootsAdded: 0,
      totalDataSizeAdded: "0",
      totalProofSetsCreated: 0,
    }))
  }

  // Build datasets list — subgraph shape
  const dataSets = []
  for (const r of setStats) {
    const obs = obsBySet[String(r.set_id)] || {}
    const totalRoots = Number(r.pieces_added || 0) - Number(r.pieces_removed || 0)
    const totalDataSize = String(r.total_data_size || "0")
    const hasData = totalRoots > 0 || totalDataSize !== "0"
    dataSets.push({
      setId: String(r.set_id),
      leafCount: r.leaf_count ? Number(r.leaf_count) : 0,
      totalRoots,
      totalDataSize,
      isActive: hasData,
      status: hasData ? "Active" : "Terminated",
      lastProvenEpoch: 0, // not directly available; we have lastProofTs
      lastProofTs: obs.lastProofTs || (r.last_proof_ts ? String(r.last_proof_ts) : null),
      nextChallengeEpoch: 0,
      nextDeadline: 0,
      totalFaultedRoots: 0,
      totalFaultedPeriods: Number(obs.totalFaultedPeriods || 0),
      totalProofs: Number(r.total_proofs || 0),
      totalProvingPeriods: Number(obs.totalProvingPeriods || 0),
      createdAt: String(r.created_at || ""),
    })
  }

  // Provider summary
  const provSummary = (proving && proving.provider) || {}
  const totalPiecesAddedAll = setStats.reduce((a, r) => a + Number(r.pieces_added || 0), 0)
  const totalPiecesRemovedAll = setStats.reduce((a, r) => a + Number(r.pieces_removed || 0), 0)
  const totalDataSizeAll = setStats.reduce((a, r) => a + BigInt(r.total_data_size || 0), BigInt(0))
  const totalProofsAll = setStats.reduce((a, r) => a + Number(r.total_proofs || 0), 0)
  const provider = {
    address: addr,
    totalProofSets: setStats.length,
    totalRoots: totalPiecesAddedAll - totalPiecesRemovedAll,
    totalDataSize: totalDataSizeAll.toString(),
    totalFaultedPeriods: Number((faultRow[0] || {}).fault_periods || 0),
    totalProvingPeriods: Number(provSummary.totalProvingPeriods || 0),
    totalFaultedRoots: 0,
    totalProofs: totalProofsAll,
    createdAt: setStats.length > 0 ? String(setStats[0].created_at) : null,
  }

  return { provider, dataSets, weeklyActivity }
}

// Weekly proofs + faulted periods for a provider, reconstructed from indexed
// events (newest week first, matching observer's /proving ordering). Fallback for
// when observer's gap-detection endpoint is unavailable for large SPs.
async function getWeeklyActivityFromEvents(network, address) {
  const addr = assertEthAddress(lc(address))
  return observerSql(network, `
    WITH my_sets AS (
      SELECT set_id FROM pdp_data_set_created WHERE storage_provider = '${addr}'
    ),
    proofs AS (
      SELECT date_trunc('week', to_timestamp(timestamp)) AS week, COUNT(*) AS proofs
      FROM pdp_possession_proven
      WHERE set_id IN (SELECT set_id FROM my_sets)
        AND timestamp >= extract(epoch FROM now() - interval '12 weeks')
      GROUP BY 1
    ),
    faults AS (
      SELECT date_trunc('week', to_timestamp(timestamp)) AS week, SUM(periods_faulted::numeric) AS faults
      FROM (
        SELECT timestamp, periods_faulted FROM fwss_fault_record
        WHERE data_set_id IN (SELECT set_id FROM my_sets)
          AND timestamp >= extract(epoch FROM now() - interval '12 weeks')
        UNION ALL
        SELECT timestamp, periods_faulted FROM storacha_fwss_fault_record
        WHERE data_set_id IN (SELECT set_id FROM my_sets)
          AND timestamp >= extract(epoch FROM now() - interval '12 weeks')
      ) f
      GROUP BY 1
    )
    SELECT COALESCE(p.week, f.week) AS week,
           COALESCE(p.proofs, 0) AS proofs,
           COALESCE(f.faults, 0) AS faults
    FROM proofs p
    FULL OUTER JOIN faults f ON p.week = f.week
    ORDER BY week DESC
  `)
}

// Per-dataset faulted-period totals from indexed fault records (both FWSS variants).
// Keyed by set_id so the proving tab's faults modal stays consistent with the
// provider-level fault count when observer's per-set data is unavailable.
async function getPerSetFaultsFromEvents(network, address) {
  const addr = assertEthAddress(lc(address))
  return observerSql(network, `
    WITH my_sets AS (
      SELECT set_id FROM pdp_data_set_created WHERE storage_provider = '${addr}'
    )
    SELECT set_id, SUM(periods_faulted::numeric) AS faulted
    FROM (
      SELECT data_set_id AS set_id, periods_faulted FROM fwss_fault_record
      WHERE data_set_id IN (SELECT set_id FROM my_sets)
      UNION ALL
      SELECT data_set_id AS set_id, periods_faulted FROM storacha_fwss_fault_record
      WHERE data_set_id IN (SELECT set_id FROM my_sets)
    ) x
    GROUP BY set_id
  `)
}

// ---------- single dataset detail (replaces sp/dataset/:setId) ----------

async function getDatasetDetail(network, address, setId) {
  let addr, id
  try {
    addr = assertEthAddress(lc(address))
    id = assertNumericId(setId)
  } catch {
    return null
  }

  const [createdRows, totalsRows, livePromise] = await Promise.all([
    observerSql(network, `
      SELECT timestamp AS created_at, block_number, tx_hash
      FROM pdp_data_set_created
      WHERE set_id = ${id} AND storage_provider = '${addr}'
      LIMIT 1
    `),
    observerSql(network, `
      SELECT
        COALESCE((SELECT SUM(piece_count) FROM pdp_pieces_added WHERE set_id = ${id}), 0) AS pieces_added,
        COALESCE((SELECT SUM(piece_count) FROM pdp_pieces_removed WHERE set_id = ${id}), 0) AS pieces_removed,
        COALESCE((SELECT COUNT(*) FROM pdp_pieces_added WHERE set_id = ${id}), 0) AS tx_added,
        COALESCE((SELECT COUNT(*) FROM pdp_pieces_removed WHERE set_id = ${id}), 0) AS tx_removed,
        COALESCE((SELECT COUNT(*) FROM pdp_possession_proven WHERE set_id = ${id}), 0) AS total_proofs,
        COALESCE((SELECT MAX(timestamp) FROM pdp_possession_proven WHERE set_id = ${id}), 0) AS last_proof_ts,
        (SELECT MAX(timestamp) FROM (
          SELECT timestamp FROM pdp_pieces_added WHERE set_id = ${id}
          UNION ALL
          SELECT timestamp FROM pdp_pieces_removed WHERE set_id = ${id}
          UNION ALL
          SELECT timestamp FROM pdp_possession_proven WHERE set_id = ${id}
        ) t) AS updated_at,
        (SELECT leaf_count FROM pdp_next_proving_period WHERE set_id = ${id} ORDER BY block_number DESC LIMIT 1) AS leaf_count,
        (
          COALESCE((SELECT SUM(raw_size::numeric) FROM fwss_piece_added WHERE data_set_id = ${id}), 0)
          + COALESCE((SELECT SUM(raw_size::numeric) FROM storacha_fwss_piece_added WHERE data_set_id = ${id}), 0)
        ) AS total_data_size,
        COALESCE((SELECT SUM(fee::numeric) FROM pdp_proof_fee_paid WHERE set_id = ${id}), 0) AS total_fee_paid,
        COALESCE((SELECT SUM(periods_faulted::numeric) FROM fwss_fault_record WHERE data_set_id = ${id}), 0)
        + COALESCE((SELECT SUM(periods_faulted::numeric) FROM storacha_fwss_fault_record WHERE data_set_id = ${id}), 0) AS faulted_periods
    `),
    // Live contract state for next-challenge / proving-window / current period
    observerGet(`/dataset/${toObserverNetwork(network)}/${id}/proving`).catch(() => null),
  ])

  if (createdRows.length === 0) return null
  const created = createdRows[0]
  const totals = totalsRows[0] || {}
  const live = livePromise || {}

  const totalRoots = Number(totals.pieces_added || 0) - Number(totals.pieces_removed || 0)
  const totalDataSize = String(totals.total_data_size || "0")

  return {
    setId: id,
    leafCount: live.leafCount ? Number(live.leafCount) : Number(totals.leaf_count || 0),
    challengeRange: 0,
    isActive: totalRoots > 0,
    lastProvenEpoch: 0,
    nextChallengeEpoch: live.deadline ? Number(live.deadline) : 0,
    nextDeadline: live.deadline ? Number(live.deadline) : 0,
    firstDeadline: 0,
    maxProvingPeriod: 0,
    challengeWindowSize: 0,
    currentDeadlineCount: 0,
    provenThisPeriod: !!live.provenThisPeriod,
    totalRoots,
    nextPieceId: 0,
    totalDataSize,
    totalProofs: Number(totals.total_proofs || 0),
    totalProvedRoots: 0,
    totalFeePaid: String(totals.total_fee_paid || "0"),
    totalFaultedPeriods: Number(totals.faulted_periods || 0),
    totalFaultedRoots: 0,
    totalTransactions: Number(totals.tx_added || 0) + Number(totals.tx_removed || 0) + Number(totals.total_proofs || 0),
    totalEventLogs: 0,
    createdAt: String(created.created_at),
    updatedAt: String(totals.updated_at || created.created_at),
    blockNumber: String(created.block_number),
  }
}

// ---------- SP activity (replaces transactions + roots + dataSets queries in sp/activity) ----------

async function getSpActivity(network, address, hours) {
  const addr = assertEthAddress(lc(address))
  const cutoff = Math.floor(Date.now() / 1000) - hours * 3600

  // Three queries in parallel: full dataset list, recent addPieces by dataset,
  // recent total per-dataset size added in window.
  const [dataSets, recentByDataset, recentSizeBySet] = await Promise.all([
    observerSql(network, `
      WITH my_sets AS (
        SELECT set_id, timestamp AS created_at
        FROM pdp_data_set_created
        WHERE storage_provider = '${addr}'
      )
      SELECT
        ms.set_id,
        ms.created_at,
        COALESCE(pa.s, 0) AS pieces_added,
        COALESCE(pr.s, 0) AS pieces_removed,
        COALESCE(pa.last_ts, ms.created_at) AS updated_at,
        COALESCE(pf.s, 0) AS total_fee_paid,
        (COALESCE(fa.s, 0) + COALESCE(sfa.s, 0)) AS total_data_size
      FROM my_sets ms
      LEFT JOIN (
        SELECT set_id, SUM(piece_count) AS s, MAX(timestamp) AS last_ts FROM pdp_pieces_added
        WHERE set_id IN (SELECT set_id FROM my_sets) GROUP BY set_id
      ) pa ON pa.set_id = ms.set_id
      LEFT JOIN (
        SELECT set_id, SUM(piece_count) AS s FROM pdp_pieces_removed
        WHERE set_id IN (SELECT set_id FROM my_sets) GROUP BY set_id
      ) pr ON pr.set_id = ms.set_id
      LEFT JOIN (
        SELECT set_id, SUM(fee::numeric) AS s FROM pdp_proof_fee_paid
        WHERE set_id IN (SELECT set_id FROM my_sets) GROUP BY set_id
      ) pf ON pf.set_id = ms.set_id
      LEFT JOIN (
        SELECT data_set_id, SUM(raw_size::numeric) AS s FROM fwss_piece_added
        WHERE data_set_id IN (SELECT set_id FROM my_sets) GROUP BY data_set_id
      ) fa ON fa.data_set_id = ms.set_id
      LEFT JOIN (
        SELECT data_set_id, SUM(raw_size::numeric) AS s FROM storacha_fwss_piece_added
        WHERE data_set_id IN (SELECT set_id FROM my_sets) GROUP BY data_set_id
      ) sfa ON sfa.data_set_id = ms.set_id
      ORDER BY ms.set_id DESC
    `),
    observerSql(network, `
      SELECT set_id, COUNT(*) AS tx_count, MIN(timestamp) AS first_ts, MAX(timestamp) AS last_ts
      FROM pdp_pieces_added pa
      WHERE pa.timestamp > ${cutoff}
        AND pa.set_id IN (SELECT set_id FROM pdp_data_set_created WHERE storage_provider = '${addr}')
      GROUP BY set_id
    `),
    observerSql(network, `
      SELECT
        fp.data_set_id AS set_id,
        SUM(fp.raw_size::numeric) AS bytes_added
      FROM fwss_piece_added fp
      WHERE fp.timestamp > ${cutoff}
        AND fp.data_set_id IN (SELECT set_id FROM pdp_data_set_created WHERE storage_provider = '${addr}')
      GROUP BY fp.data_set_id
      UNION ALL
      SELECT
        fp.data_set_id AS set_id,
        SUM(fp.raw_size::numeric) AS bytes_added
      FROM storacha_fwss_piece_added fp
      WHERE fp.timestamp > ${cutoff}
        AND fp.data_set_id IN (SELECT set_id FROM pdp_data_set_created WHERE storage_provider = '${addr}')
      GROUP BY fp.data_set_id
    `),
  ])

  // Aggregate recent size per dataset
  const sizeBySet = {}
  for (const r of recentSizeBySet) {
    const id = String(r.set_id)
    sizeBySet[id] = (sizeBySet[id] || 0n) + BigInt(r.bytes_added || 0)
  }

  // Fetch addPieces transactions in window for timeline. Limit to 20k to mirror old cap.
  const txRows = await observerSql(network, `
    SELECT set_id, timestamp
    FROM pdp_pieces_added
    WHERE timestamp > ${cutoff}
      AND set_id IN (SELECT set_id FROM pdp_data_set_created WHERE storage_provider = '${addr}')
    ORDER BY timestamp ASC
    LIMIT 20000
  `)

  return {
    dataSets: dataSets.map(d => ({
      setId: String(d.set_id),
      totalRoots: Number(d.pieces_added || 0) - Number(d.pieces_removed || 0),
      totalDataSize: String(d.total_data_size || "0"),
      totalTransactions: 0,
      totalFeePaid: String(d.total_fee_paid || "0"),
      createdAt: String(d.created_at),
      updatedAt: String(d.updated_at || d.created_at),
      isActive: (Number(d.pieces_added || 0) - Number(d.pieces_removed || 0)) > 0,
    })),
    recentByDataset: recentByDataset.map(r => ({
      setId: String(r.set_id),
      txCount: Number(r.tx_count || 0),
      firstTs: Number(r.first_ts || 0),
      lastTs: Number(r.last_ts || 0),
    })),
    recentSizeBySet: sizeBySet,
    transactions: txRows.map(t => ({
      setId: String(t.set_id),
      createdAt: Number(t.timestamp),
    })),
  }
}

module.exports = {
  getNetworkTotals,
  getAllProvidersRollup,
  getProvingDetail,
  getDatasetDetail,
  getSpActivity,
}
