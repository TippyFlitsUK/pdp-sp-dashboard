// GraphQL fetch helper with pagination for Goldsky subgraphs

const ENDPOINTS = {
  pdpScan: "https://api.goldsky.com/api/public/project_cmdfaaxeuz6us01u359yjdctw/subgraphs/pdp-explorer/mainnet311b/gn",
  filecoinPay: "https://api.goldsky.com/api/public/project_cmb9tuo8r1xdw01ykb8uidk7h/subgraphs/filecoin-pay-mainnet/1.0.6/gn",
  fwss: "https://api.goldsky.com/api/public/project_cmb9tuo8r1xdw01ykb8uidk7h/subgraphs/fwss-mainnet-tim/1.1.0/gn",
}

async function gqlFetch(endpoint, query, variables = {}) {
  const res = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(15000),
  })
  if (!res.ok) {
    throw new Error(`Subgraph ${res.status}: ${await res.text()}`)
  }
  const json = await res.json()
  if (json.errors) {
    throw new Error(`Subgraph error: ${json.errors[0].message}`)
  }
  return json.data
}

// Paginate through a collection (max 1000 per query, use skip)
async function gqlPaginate(endpoint, entityName, fields, where = "", orderBy = "id", pageSize = 1000) {
  const MAX_PAGES = 50
  const results = []
  let skip = 0
  while (true) {
    if (skip / pageSize >= MAX_PAGES) break
    const whereClause = where ? `where: {${where}},` : ""
    const query = `{
      ${entityName}(first: ${pageSize}, skip: ${skip}, ${whereClause} orderBy: ${orderBy}, orderDirection: asc) {
        ${fields}
      }
    }`
    const data = await gqlFetch(endpoint, query)
    const items = data[entityName]
    if (!items || items.length === 0) break
    results.push(...items)
    if (items.length < pageSize) break
    skip += pageSize
  }
  return results
}

module.exports = { ENDPOINTS, gqlFetch, gqlPaginate }
