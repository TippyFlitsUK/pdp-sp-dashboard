// Network Overview -- performance-focused SP cards

async function loadOverview() {
  var statsBar = document.getElementById("global-stats")
  var grid = document.getElementById("sp-grid")

  statsBar.innerHTML = '<div class="loading">Loading network stats</div>'
  grid.innerHTML = '<div class="loading">Loading providers</div>'

  try {
    var [global, providers, perfData] = await Promise.all([
      fetchJSON("/api/network/global"),
      fetchJSON("/api/network/overview"),
      fetchJSON("/api/network/performance").catch(function() { return {} }),
    ])

    renderStatsBar(statsBar, global)
    spConfig = providers
    renderSPGrid(grid, providers, perfData)

    document.getElementById("last-updated").textContent = "Updated " + new Date().toLocaleTimeString()
  } catch (err) {
    grid.innerHTML = '<div class="error-banner">Failed to load: ' + escapeHtml(err.message) + '</div>'
  }
}

function renderStatsBar(container, g) {
  container.innerHTML = '<div class="panel" style="padding:20px 24px">' +
    summaryGrid([
      { label: "Providers", value: formatNum(g.providers) },
      { label: "Proof Sets", value: formatNum(g.proofSets) },
      { label: "Pieces", value: formatNum(g.roots) },
      { label: "Storage", value: formatBytes(g.storageSize), cls: "blue" },
      { label: "Proofs", value: formatNum(g.proofsSubmitted) },
    ]) +
  '</div>'
}

function renderSPGrid(container, providers, perfData) {
  var html = '<div class="panel" style="padding:20px 24px">'
  html += '<div class="sp-home-grid">'

  for (var i = 0; i < providers.length; i++) {
    var sp = providers[i]
    var pdp = sp.pdp || {}
    var fwss = sp.fwss || {}
    var econ = sp.economics || {}
    var logs = sp.logHealth || {}
    var hbClass = heartbeatClass(sp.liveness)

    // Performance data
    var perf = perfData[String(sp.id)] || {}
    var ds = perf["dataStorage"] || { success: 0, failed: 0 }
    var rt = perf["retrieval"] || { success: 0, failed: 0 }
    var dsTotal = ds.success + ds.failed
    var rtTotal = rt.success + rt.failed
    var dsPct = dsTotal > 0 ? ((ds.success / dsTotal) * 100).toFixed(1) : "N/A"
    var rtPct = rtTotal > 0 ? ((rt.success / rtTotal) * 100).toFixed(1) : "N/A"
    var dsSLA = dsPct !== "N/A" && parseFloat(dsPct) >= 97
    var rtSLA = rtPct !== "N/A" && parseFloat(rtPct) >= 97
    var allPass = dsSLA && rtSLA

    // Card border color based on SLA
    var statusCls = "status-healthy"
    if (sp.liveness && sp.liveness.alive === false) statusCls = "status-offline"
    else if (dsTotal === 0 && rtTotal === 0) statusCls = "status-nologs"
    else if (!allPass) statusCls = "status-error"

    // Proving
    var faults = pdp.faultedPeriods || 0
    var proofs = pdp.provingPeriods || 0
    var provingTotal = proofs + faults
    var provingPct = provingTotal > 0 ? ((proofs / provingTotal) * 100).toFixed(1) : "N/A"

    // Ping
    var pingText = sp.liveness && sp.liveness.alive ? sp.liveness.latencyMs + 'ms'
      : sp.liveness && sp.liveness.alive === false ? 'Offline' : '...'

    // Bullet color matches SLA
    var bulletCls = allPass ? "alive" : (dsTotal === 0 && rtTotal === 0) ? "unknown" : "dead"

    html += '<div class="sp-home-card ' + statusCls + '" data-spid="' + sp.id + '">'

    // Header
    html += '<div class="sp-home-header">' +
      '<div style="display:flex;align-items:center;gap:6px;min-width:0">' +
        '<span class="heartbeat-dot ' + bulletCls + '"></span>' +
        '<span class="sp-home-name">' + escapeHtml(sp.name) + '</span>' +
        '<span class="sp-period">24h</span>' +
      '</div>' +
      '<span class="sp-id">ID ' + sp.id + '</span>' +
    '</div>'

    // Performance only
    html += '<div class="sp-home-metrics">' +
      '<div class="sp-home-row sp-home-row-4">' +
        spStat("Deals", dsPct + "%", dsSLA ? "rate-good" : dsTotal > 0 ? "rate-bad" : "muted") +
        spStat("Tests", formatNum(dsTotal), "") +
        spStat("Failed", formatNum(ds.failed), ds.failed > 0 ? "errors" : "zero") +
        spStat("SLA", dsSLA ? "PASS" : dsTotal === 0 ? "--" : "FAIL", dsSLA ? "sla-pass" : dsTotal > 0 ? "sla-fail" : "muted") +
      '</div>' +
      '<div class="sp-home-row sp-home-row-4">' +
        spStat("Retrievals", rtPct + "%", rtSLA ? "rate-good" : rtTotal > 0 ? "rate-bad" : "muted") +
        spStat("Tests", formatNum(rtTotal), "") +
        spStat("Failed", formatNum(rt.failed), rt.failed > 0 ? "errors" : "zero") +
        spStat("SLA", rtSLA ? "PASS" : rtTotal === 0 ? "--" : "FAIL", rtSLA ? "sla-pass" : rtTotal > 0 ? "sla-fail" : "muted") +
      '</div>' +
    '</div>'

    // Footer - full curio version
    html += '<div class="sp-home-footer">' +
      '<span>' + escapeHtml(sp.curioVersion || pingText) + '</span>' +
    '</div>'

    html += '</div>'
  }

  html += '</div></div>'
  container.innerHTML = html

  container.querySelectorAll(".sp-home-card").forEach(function(card) {
    card.addEventListener("click", function() {
      navigate("#sp/" + card.dataset.spid)
    })
  })
}

function spStat(label, value, cls) {
  return '<div class="sp-home-stat">' +
    '<span class="stat-label">' + label + '</span>' +
    '<span class="stat-value ' + (cls || '') + '">' + value + '</span>' +
  '</div>'
}

