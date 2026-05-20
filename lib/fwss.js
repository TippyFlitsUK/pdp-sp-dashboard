// FWSS dataset lookup — reads indexed `fwss_data_set_created` events from
// foc-observer instead of FWSS StateView contract via viem.
//
// Pre-migration this file used a 30-min batched viem prewarm of all dataset
// IDs to populate a Map<setId, {payer, payee, pdpRailId, providerId, ...}>.
// SQL on the indexed table is instant — no prewarm, no batching, no rate use.

const { observerSql } = require("./observer")

// Cap a single IN-list at 1000 ids to stay well below any parser limit.
// One SP's activity tab tops out around 250 active datasets in 72h today,
// but a network-wide call from a future feature could exceed that.
const IN_LIST_CHUNK = 1000

// Return shape mirrors the old viem-backed function:
//   { [setId]: { payer, payee, serviceProvider, pdpRailId, providerId, dataSetId, appMetadata? } }
// pdpRailId === 0 → caller treated this as "not managed by FWSS"; SQL won't
// return rows for those, so the absent key has the same meaning.
async function getDataSetInfoBatch(dataSetIds, network = "mainnet") {
  if (!dataSetIds || dataSetIds.length === 0) return {}
  const ids = dataSetIds.map(id => String(id)).filter(id => /^\d+$/.test(id))
  if (ids.length === 0) return {}

  // Split into chunks; query in parallel; merge.
  const chunks = []
  for (let i = 0; i < ids.length; i += IN_LIST_CHUNK) {
    chunks.push(ids.slice(i, i + IN_LIST_CHUNK))
  }
  const chunkResults = await Promise.all(chunks.map(chunk => observerSql(network, `
    SELECT
      data_set_id,
      payer,
      payee,
      service_provider,
      pdp_rail_id,
      provider_id,
      source,
      metadata
    FROM fwss_data_set_created
    WHERE data_set_id IN (${chunk.join(",")})
      AND pdp_rail_id != 0
  `)))

  const result = {}
  for (const rows of chunkResults) {
    for (const r of rows) {
      let appMetadata
      if (r.metadata) {
        try {
          const parsed = JSON.parse(r.metadata)
          if (parsed && parsed.app) appMetadata = parsed.app
        } catch {}
      }
      result[r.data_set_id] = {
        payer: r.payer,
        payee: r.payee,
        serviceProvider: r.service_provider,
        pdpRailId: Number(r.pdp_rail_id),
        providerId: Number(r.provider_id),
        dataSetId: Number(r.data_set_id),
        source: r.source || null,
        appMetadata,
      }
    }
  }
  return result
}

module.exports = { getDataSetInfoBatch }
