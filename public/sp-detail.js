// SP Detail -- rich overview tab + tabbed sections with proper layouts

var spDataCache = {}

async function loadSPDetail(sp) {
  var titleEl = document.getElementById("detail-title")
  titleEl.innerHTML = sp.name + ' (ID ' + sp.id + ')' +
    (sp.curioVersion ? '<span class="detail-version">' + escapeHtml(sp.curioVersion) + '</span>' : '')
  spDataCache = {}

  // Show/hide logs tab based on hasLogs
  var logTab = document.querySelector('[data-tab="logs"]')
  if (logTab) logTab.style.display = sp.hasLogs ? "" : "none"

  // Set dealbot dashboard link with SP provider ID
  var dealbotLink = document.getElementById("perf-dealbot-link")
  if (dealbotLink) dealbotLink.href = "https://telemetry.betterstack.com/dashboards/Zz8k7L?rf=now-72h&rt=now&vs%5Bprovider_id%5D=" + sp.id + "&top=0"

  var logsLink = document.getElementById("logs-betterstack-link")
  if (logsLink) logsLink.href = "https://telemetry.betterstack.com/dashboards/DcQ0In?rf=now-72h&rt=now&vs%5BproviderId%5D=" + sp.id

  // Reset panels
  setLoading("proving-content", "Loading proving data")
  setLoading("economics-content", "Loading economics data")
  setLoading("performance-content", "Loading performance data")
  if (sp.hasLogs) {
    document.getElementById("errors-content").innerHTML = ""
    document.getElementById("logs-content").innerHTML = ""
  }

  // Load performance first (default tab), then proving+economics in parallel, then revenue async
  await loadPerformance(sp)
  await Promise.all([loadProving(sp), loadEconomics(sp)])
  loadRevenue(sp)

  if (sp.hasLogs) {
    loadLogsSummary(sp)
    await loadSPTimeline(sp)
    await loadSPErrors(sp)
    await loadSPPatterns(sp)
    await loadSPLogs(sp)
  }

  document.getElementById("last-updated").textContent = "Updated " + new Date().toLocaleTimeString()
}

// ============================================================
// HELPERS
// ============================================================
function setLoading(id, text) {
  var el = document.getElementById(id)
  if (!el) return
  if (text) {
    el.innerHTML = '<div class="loading">' + text + '</div>'
    el.classList.add("is-loading")
  } else {
    el.innerHTML = ''
    el.classList.remove("is-loading")
  }
}

function setError(id, msg) {
  var el = document.getElementById(id)
  if (el) el.innerHTML = '<div class="error-banner">' + escapeHtml(msg) + '</div>'
}

function setNoData(id, msg) {
  var el = document.getElementById(id)
  if (el) el.innerHTML = '<div class="no-data">' + msg + '</div>'
}

function summaryGrid(items) {
  return '<div class="summary-grid">' + items.map(function(item) {
    var clickable = item.tab ? ' clickable" onclick="switchTab(\'' + item.tab + '\')"' : '"'
    return '<div class="sg-card' + clickable + '>' +
      '<div class="stat-label">' + item.label + '</div>' +
      '<div class="stat-value ' + (item.cls || '') + '">' + item.value + '</div>' +
    '</div>'
  }).join("") + '</div>'
}

// Show faults modal - filter 'all' or '7d'
function showFaultsModal(period) {
  var data = spDataCache.proving
  if (!data || !data.faults) return

  var faults = data.faults
  var title = "Faulted Periods (All Time)"

  if (period === "7d") {
    title = "Faulted Periods (Last 7 Days)"
    var sevenDaysAgo = Math.floor(Date.now() / 1000) - 7 * 86400
    faults = faults.filter(function(f) { return Number(f.timestamp) >= sevenDaysAgo })
  }

  var body = document.getElementById("faults-modal-body")
  document.getElementById("faults-modal-title").textContent = title

  if (!faults.length) {
    body.innerHTML = '<div class="no-data">No faults recorded</div>'
  } else {
    var html = '<table style="width:100%"><thead><tr><th>Data Set</th><th>Periods</th><th>Deadline</th><th>Time</th><th>Tx</th></tr></thead><tbody>'
    for (var i = 0; i < faults.length; i++) {
      var f = faults[i]
      var time = f.timestamp ? formatTime(new Date(Number(f.timestamp) * 1000).toISOString()) : '-'
      var txLink = f.txHash ? '<a href="https://filfox.info/en/tx/' + f.txHash + '" target="_blank" style="color:inherit;text-decoration:none">' + f.txHash + '</a>' : '-'
      html += '<tr>' +
        '<td>' + (f.dataSet ? f.dataSet.dataSetId : '-') + '</td>' +
        '<td class="level-error">' + f.periodsFaulted + '</td>' +
        '<td>' + Number(f.deadline || 0).toLocaleString() + '</td>' +
        '<td>' + time + '</td>' +
        '<td>' + txLink + '</td>' +
      '</tr>'
    }
    html += '</tbody></table>'
    body.innerHTML = html
  }

  document.getElementById("faults-modal").style.display = "flex"
}

async function showDatasetModal(spId, setId) {
  var modal = document.getElementById("dataset-modal")
  var body = document.getElementById("dataset-modal-body")
  document.getElementById("dataset-modal-title").textContent = "Dataset " + setId
  body.innerHTML = '<div class="loading">Loading dataset details</div>'
  modal.style.display = "flex"

  try {
    var data = await fetchJSON(apiUrl("/api/sp/" + spId + "/dataset/" + setId))
    var pdp = data.pdp || {}
    var fwss = data.fwss || {}
    var faults = data.faults || []

    var status = fwss.status || (pdp.isActive ? "Active" : "Inactive")
    var statusClass = status === "Active" ? "active" : "terminated"

    var created = pdp.createdAt ? formatTime(new Date(Number(pdp.createdAt) * 1000).toISOString()) : '-'
    var updated = pdp.updatedAt ? formatTime(new Date(Number(pdp.updatedAt) * 1000).toISOString()) : '-'

    var html = '<div class="dataset-detail-grid">'

    // Identity & Status
    html += '<div class="dd-section">' +
      '<h4>Identity & Status</h4>' +
      '<div class="dd-row"><span>Status</span><span class="badge ' + statusClass + '">' + status + '</span></div>' +
      '<div class="dd-row"><span>Data Set ID</span><span>' + setId + '</span></div>' +
      (fwss.pdpRailId ? '<div class="dd-row"><span>Pay Rail ID</span><span>' + fwss.pdpRailId + '</span></div>' : '') +
      (fwss.cacheMissRailId ? '<div class="dd-row"><span>Cache Miss Rail ID</span><span>' + fwss.cacheMissRailId + '</span></div>' : '') +
      (fwss.cdnRailId ? '<div class="dd-row"><span>CDN Rail ID</span><span>' + fwss.cdnRailId + '</span></div>' : '') +
      '<div class="dd-row"><span>CDN</span><span>' + (fwss.withCDN ? 'Yes' : 'No') + '</span></div>' +
      '<div class="dd-row"><span>IPFS Indexing</span><span>' + (fwss.withIPFSIndexing ? 'Yes' : 'No') + '</span></div>' +
      '<div class="dd-row"><span>Created</span><span>' + created + '</span></div>' +
      '<div class="dd-row"><span>Last Updated</span><span>' + updated + '</span></div>' +
      (fwss.createdAtTxHash ? '<div class="dd-row"><span>Creation Tx</span><span><a href="https://filfox.info/en/tx/' + fwss.createdAtTxHash + '" target="_blank" style="color:var(--accent);font-size:11px">' + fwss.createdAtTxHash.slice(0, 14) + '...</a></span></div>' : '') +
    '</div>'

    // Proving
    html += '<div class="dd-section">' +
      '<h4>Proving</h4>' +
      '<div class="dd-row"><span>Total Proofs</span><span>' + Number(pdp.totalProofs || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Proved Roots</span><span>' + Number(pdp.totalProvedRoots || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Proven This Period</span><span>' + (pdp.provenThisPeriod ? 'Yes' : 'No') + '</span></div>' +
      '<div class="dd-row"><span>Last Proven Epoch</span><span>' + Number(pdp.lastProvenEpoch || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Next Challenge</span><span>' + Number(pdp.nextChallengeEpoch || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Next Deadline</span><span>' + Number(pdp.nextDeadline || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Deadline Count</span><span>' + Number(pdp.currentDeadlineCount || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Max Proving Period</span><span>' + Number(pdp.maxProvingPeriod || 0).toLocaleString() + ' epochs</span></div>' +
      '<div class="dd-row"><span>Challenge Window</span><span>' + Number(pdp.challengeWindowSize || 0).toLocaleString() + ' epochs</span></div>' +
    '</div>'

    // Faults
    html += '<div class="dd-section">' +
      '<h4>Faults</h4>' +
      '<div class="dd-row"><span>Faulted Periods</span><span class="' + (Number(pdp.totalFaultedPeriods) > 0 ? 'level-error' : '') + '">' + Number(pdp.totalFaultedPeriods || 0) + '</span></div>'
    if (faults.length > 0) {
      html += '<table style="margin-top:8px"><thead><tr><th>Periods</th><th>Deadline</th><th>Time</th><th>Tx</th></tr></thead><tbody>'
      for (var i = 0; i < faults.length; i++) {
        var f = faults[i]
        var time = f.timestamp ? formatTime(new Date(Number(f.timestamp) * 1000).toISOString()) : '-'
        var txShort = f.txHash ? f.txHash.slice(0, 10) + '...' : '-'
        var txLink = f.txHash ? '<a href="https://filfox.info/en/tx/' + f.txHash + '" target="_blank" style="color:var(--accent)">' + txShort + '</a>' : '-'
        html += '<tr>' +
          '<td class="level-error">' + f.periodsFaulted + '</td>' +
          '<td>' + Number(f.deadline || 0).toLocaleString() + '</td>' +
          '<td>' + time + '</td>' +
          '<td>' + txLink + '</td>' +
        '</tr>'
      }
      html += '</tbody></table>'
    } else {
      html += '<div style="color:var(--text-muted);font-size:12px;margin-top:4px">No fault records</div>'
    }
    html += '</div>'

    // Storage & Economics
    html += '<div class="dd-section">' +
      '<h4>Storage & Economics</h4>' +
      '<div class="dd-row"><span>Total Pieces</span><span>' + Number(pdp.totalRoots || fwss.totalPieces || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Data Size</span><span>' + formatBytes(pdp.totalDataSize || fwss.totalSize) + '</span></div>' +
      '<div class="dd-row"><span>Leaf Count</span><span>' + Number(pdp.leafCount || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Challenge Range</span><span>' + Number(pdp.challengeRange || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Total Fee Paid</span><span>' + formatWei(pdp.totalFeePaid) + '</span></div>' +
      '<div class="dd-row"><span>Transactions</span><span>' + Number(pdp.totalTransactions || 0).toLocaleString() + '</span></div>' +
      (fwss.payer ? '<div class="dd-row"><span>Payer</span><span style="font-size:11px">' + escapeHtml(fwss.payer) + '</span></div>' : '') +
      (fwss.payee ? '<div class="dd-row"><span>Payee</span><span style="font-size:11px">' + escapeHtml(fwss.payee) + '</span></div>' : '') +
    '</div>'

    html += '</div>'
    body.innerHTML = html
  } catch (err) {
    body.innerHTML = '<div class="error-banner">' + escapeHtml(err.message) + '</div>'
  }
}

// ============================================================
// PROVING TAB
// ============================================================
async function loadProving(sp) {
  try {
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/proving"))
    spDataCache.proving = data

    var prov = data.provider || {}

    // Count faults from actual records (same source as modal)
    var allFaults = data.faults || []
    var sevenDaysAgo = Math.floor(Date.now() / 1000) - 7 * 86400
    var faults7d = allFaults.filter(function(f) { return Number(f.timestamp) >= sevenDaysAgo })
    var totalFaultPeriods = allFaults.reduce(function(sum, f) { return sum + (f.periodsFaulted || 1) }, 0)
    var faultPeriods7d = faults7d.reduce(function(sum, f) { return sum + (f.periodsFaulted || 1) }, 0)

    // Last Success = latest week's success rate (matching PDP Scan)
    var latestWeek = data.weeklyActivity && data.weeklyActivity[0] ? data.weeklyActivity[0] : null
    var lastSuccess = "N/A"
    if (latestWeek) {
      var wp = Number(latestWeek.totalProofs || 0)
      var wf = Number(latestWeek.totalFaultedPeriods || 0)
      lastSuccess = wp > 0 ? (((wp - wf) / wp) * 100).toFixed(2) + "%" : "100.00%"
    }

    var html = summaryGrid([
      { label: "Total Data Sets", value: formatNum(prov.totalProofSets) },
      { label: "Data Stored", value: formatBytes(prov.totalDataSize) },
      { label: "Total Pieces", value: Number(prov.totalRoots || 0).toLocaleString() },
    ]) + '<div class="summary-grid">' +
      '<div class="sg-card clickable" onclick="showFaultsModal(\'all\')">' +
        '<div class="stat-label">Faulted Periods (All Time)</div>' +
        '<div class="stat-value ' + (totalFaultPeriods > 0 ? 'errors' : '') + '">' + totalFaultPeriods + '</div>' +
      '</div>' +
      '<div class="sg-card clickable" onclick="showFaultsModal(\'7d\')">' +
        '<div class="stat-label">Faulted Periods (7d)</div>' +
        '<div class="stat-value ' + (faultPeriods7d > 0 ? 'errors' : 'green') + '">' + faultPeriods7d + '</div>' +
      '</div>' +
      '<div class="sg-card">' +
        '<div class="stat-label">Last Success (%)</div>' +
        '<div class="stat-value green">' + lastSuccess + '</div>' +
      '</div>' +
    '</div>'

    // Weekly Provider Activity chart
    if (data.weeklyActivity && data.weeklyActivity.length > 0) {
      html += '<h4 style="font-size:11px;color:var(--text-muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.06em;font-weight:500">Weekly Provider Activity</h4>'
      html += '<div class="chart-container"><canvas id="proving-chart"></canvas></div>'
    }

    // Data Sets table (matching PDP Scan layout)
    if (data.dataSets && data.dataSets.length > 0) {
      html += '<h4 style="font-size:11px;color:var(--text-muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.06em;font-weight:500">Data Sets (' + data.dataSets.length + ')</h4>'
      html += '<div class="table-scroll"><table id="datasets-table"><thead><tr>' +
        '<th class="sortable" data-col="setId">Data Set ID</th>' +
        '<th class="sortable" data-col="status">Status</th>' +
        '<th class="sortable" data-col="dataSize">Data Size</th>' +
        '<th class="sortable" data-col="pieces">Pieces</th>' +
        '<th class="sortable" data-col="lastProven">Last Proven Epoch</th>' +
        '<th class="sortable" data-col="createdAt">Created At</th>' +
        '<th class="sortable" data-col="railId">Rail ID</th>' +
      '</tr></thead><tbody id="datasets-tbody">'
      for (var j = 0; j < data.dataSets.length; j++) {
        var ds = data.dataSets[j]
        var created = ds.createdAt ? formatTime(new Date(Number(ds.createdAt) * 1000).toISOString()) : '-'
        html += '<tr' +
          ' data-setid="' + Number(ds.setId || ds.id) + '"' +
          ' data-status="' + (ds.status === "Active" ? '1' : '0') + '"' +
          ' data-datasize="' + Number(ds.totalDataSize || 0) + '"' +
          ' data-pieces="' + Number(ds.totalRoots || ds.leafCount || 0) + '"' +
          ' data-lastproven="' + Number(ds.lastProvenEpoch || 0) + '"' +
          ' data-createdat="' + Number(ds.createdAt || 0) + '"' +
          ' data-railid="' + Number(ds.railId || 0) + '"' +
        '>' +
          '<td>' + (ds.setId || ds.id) + '</td>' +
          '<td><span class="badge ' + (ds.status === "Active" ? "active" : "terminated") + '">' + escapeHtml(ds.status || "Unknown") + '</span></td>' +
          '<td>' + formatBytes(ds.totalDataSize) + '</td>' +
          '<td>' + Number(ds.totalRoots || ds.leafCount || 0).toLocaleString() + '</td>' +
          '<td>' + Number(ds.lastProvenEpoch || 0).toLocaleString() + '</td>' +
          '<td>' + created + '</td>' +
          '<td>' + (ds.railId || '-') + '</td>' +
        '</tr>'
      }
      html += '</tbody></table></div>'
    }

    document.getElementById("proving-content").innerHTML = html || '<div class="no-data">No proving data available</div>'

    // Wire up dataset row clicks
    document.querySelectorAll('#datasets-tbody tr').forEach(function(tr) {
      tr.style.cursor = 'pointer'
      tr.addEventListener('click', function() {
        showDatasetModal(sp.id, tr.dataset.setid)
      })
    })

    // Wire up sortable columns
    var colMap = { setId: 'setid', status: 'status', dataSize: 'datasize', pieces: 'pieces', lastProven: 'lastproven', createdAt: 'createdat', railId: 'railid' }
    document.querySelectorAll('#datasets-table th.sortable').forEach(function(th) {
      th.addEventListener('click', function() {
        var col = colMap[th.dataset.col]
        var tbody = document.getElementById('datasets-tbody')
        var rows = Array.from(tbody.querySelectorAll('tr'))
        var asc = th.classList.contains('sort-asc')
        // Clear all sort indicators
        document.querySelectorAll('#datasets-table th.sortable').forEach(function(h) { h.classList.remove('sort-asc', 'sort-desc') })
        th.classList.add(asc ? 'sort-desc' : 'sort-asc')
        rows.sort(function(a, b) {
          var va = Number(a.dataset[col]) || 0
          var vb = Number(b.dataset[col]) || 0
          return asc ? vb - va : va - vb
        })
        rows.forEach(function(r) { tbody.appendChild(r) })
      })
    })

    // Draw weekly activity line chart
    if (data.weeklyActivity && data.weeklyActivity.length > 0) {
      var canvas = document.getElementById("proving-chart")
      if (canvas) {
        var weeks = data.weeklyActivity.slice().reverse()
        var latestWeekNum = parseInt(weeks[weeks.length - 1].id.slice(2, 4), 16)
        var nowMs = Date.now()
        var currentWeekStart = nowMs - (nowMs % (7 * 86400000))
        var labels = weeks.map(function(w) {
          var weekNum = parseInt(w.id.slice(2, 4), 16)
          var offset = (latestWeekNum - weekNum) * 7 * 86400000
          var d = new Date(currentWeekStart - offset)
          return d.toLocaleDateString([], { month: "short", day: "numeric" })
        })
        drawLineChart(canvas, [
          { name: "Proofs", color: "#00d68f", data: weeks.map(function(w) { return Number(w.totalProofs) }) },
          { name: "Faults", color: "#ff4d6a", data: weeks.map(function(w) { return Number(w.totalFaultedPeriods) }) },
        ], { labels: labels, emptyText: "No proving activity" })
      }
    }
  } catch (err) {
    setError("proving-content", err.message)
  }
}

// ============================================================
// ECONOMICS TAB
// ============================================================
async function loadEconomics(sp) {
  try {
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/economics"))
    spDataCache.economics = data
    var s = data.summary || {}

    var acct = data.account || {}

    // Expected settlement: accrued on active rails since last settlement
    // paymentRate is per Filecoin epoch (30s), settledUpto is epoch number
    var currentEpoch = Math.floor((Date.now() / 1000 - 1598306400) / 30)
    var expectedSettlement = 0
    if (data.rails) {
      for (var a = 0; a < data.rails.length; a++) {
        var rl = data.rails[a]
        if (rl.state === "ACTIVE" && rl.settledUpto && rl.paymentRate !== "0") {
          var diff = currentEpoch - Number(rl.settledUpto)
          if (diff > 0) expectedSettlement += diff * Number(rl.paymentRate) / 1e18
        }
      }
    }
    var totalSettled = Number(acct.fundsCollected || 0) / 1e18
    var totalRevenue = totalSettled + expectedSettlement

    var html = summaryGrid([
      { label: "Active Rails", value: formatNum(s.activeRails) },
      { label: "Total Rails", value: formatNum(s.totalRails) },
      { label: "Expected Settlement", value: "$" + expectedSettlement.toFixed(2), cls: "green" },
    ]) + summaryGrid([
      { label: "Total Settled", value: "$" + totalSettled.toFixed(2), cls: "amber" },
      { label: "Total Revenue", value: "$" + totalRevenue.toFixed(2) },
      { label: "Last Settlement", value: acct.lastSettled ? formatTime(new Date(Number(acct.lastSettled) * 1000).toISOString()) : '-' },
    ])

    if (data.rails && data.rails.length > 0) {
      // Sort descending by rail ID
      var sortedRails = data.rails.slice().sort(function(a, b) { return Number(b.railId) - Number(a.railId) })

      // Controls: state filter + settle all button
      var states = {}
      for (var s2 = 0; s2 < sortedRails.length; s2++) states[sortedRails[s2].state] = true
      html += '<div style="margin-bottom:12px;display:flex;justify-content:space-between;align-items:center"><select id="econ-state-filter" style="font-family:Outfit,sans-serif">' +
        '<option value="">All States</option>'
      for (var st in states) {
        html += '<option value="' + st + '">' + st + ' (' + sortedRails.filter(function(r){return r.state===st}).length + ')</option>'
      }
      html += '</select></div>'

      html += '<div class="table-scroll" id="econ-table-scroll"><table id="econ-table"><thead><tr>' +
        '<th class="sortable" data-col="railid">Rail ID</th>' +
        '<th class="sortable" data-col="state">State</th>' +
        '<th class="sortable" data-col="settled">Settled</th>' +
        '<th class="sortable" data-col="settlements">Settlements</th>' +
        '<th>Payer</th>' +
      '</tr></thead><tbody id="econ-tbody">'
      for (var i = 0; i < sortedRails.length; i++) {
        var r = sortedRails[i]
        var stateClass = r.state === "ACTIVE" ? "active" : r.state === "TERMINATED" ? "terminated" : r.state === "FINALIZED" ? "finalized" : "zerorate"

        html += '<tr data-state="' + r.state + '"' +
          ' data-railid="' + Number(r.railId) + '"' +
          ' data-settled="' + Number(r.totalSettledAmount || 0) + '"' +
          ' data-settlements="' + Number(r.totalSettlements || 0) + '"' +
        '>' +
          '<td>' + r.railId + '</td>' +
          '<td><span class="badge ' + stateClass + '">' + r.state + '</span></td>' +
          '<td>' + formatWei(r.totalSettledAmount) + '</td>' +
          '<td>' + (r.totalSettlements || '0') + '</td>' +
          '<td>' + escapeHtml(r.payer ? r.payer.id : '-') + '</td>' +
        '</tr>'
      }
      html += '</tbody></table></div>'
    }

    document.getElementById("economics-content").innerHTML = html || '<div class="no-data">No economics data</div>'

    // Wire up state filter
    var stateFilter = document.getElementById("econ-state-filter")
    if (stateFilter) {
      stateFilter.addEventListener("change", function() {
        var val = stateFilter.value
        var rows = document.querySelectorAll("#econ-tbody tr")
        rows.forEach(function(row) {
          row.style.display = (!val || row.dataset.state === val) ? "" : "none"
        })
      })
    }

    // Wire up sortable columns
    document.querySelectorAll('#econ-table th.sortable').forEach(function(th) {
      th.addEventListener('click', function() {
        var col = th.dataset.col
        var tbody = document.getElementById('econ-tbody')
        var rows = Array.from(tbody.querySelectorAll('tr'))
        var asc = th.classList.contains('sort-asc')
        document.querySelectorAll('#econ-table th.sortable').forEach(function(h) { h.classList.remove('sort-asc', 'sort-desc') })
        th.classList.add(asc ? 'sort-desc' : 'sort-asc')
        rows.sort(function(a, b) {
          if (col === 'state') {
            var va = a.dataset.state || ''
            var vb = b.dataset.state || ''
            return asc ? vb.localeCompare(va) : va.localeCompare(vb)
          }
          var va2 = Number(a.dataset[col]) || 0
          var vb2 = Number(b.dataset[col]) || 0
          return asc ? vb2 - va2 : va2 - vb2
        })
        rows.forEach(function(r) { tbody.appendChild(r) })
      })
    })
    // Wire up rail row clicks
    document.querySelectorAll('#econ-tbody tr').forEach(function(tr) {
      tr.style.cursor = 'pointer'
      tr.addEventListener('click', function() {
        showRailModal(sp.id, tr.dataset.railid)
      })
    })
  } catch (err) {
    setError("economics-content", err.message)
  }
}

async function showRailModal(spId, railId) {
  var modal = document.getElementById("rail-modal")
  var body = document.getElementById("rail-modal-body")
  document.getElementById("rail-modal-title").textContent = "Rail " + railId
  body.innerHTML = '<div class="loading">Loading rail details</div>'
  modal.style.display = "flex"

  try {
    var data = await fetchJSON(apiUrl("/api/sp/" + spId + "/rail/" + railId))
    var r = data.rail || {}
    var ds = data.dataset
    var stateClass = r.state === "ACTIVE" ? "active" : r.state === "TERMINATED" ? "terminated" : r.state === "FINALIZED" ? "finalized" : "zerorate"
    var created = r.createdAt ? formatTime(new Date(Number(r.createdAt) * 1000).toISOString()) : '-'

    // Calculate accrued since last settlement
    var currentEpoch = Math.floor((Date.now() / 1000 - 1598306400) / 30)
    var accrued = 0
    if (r.state === "ACTIVE" && r.settledUpto && r.paymentRate !== "0") {
      var diff = currentEpoch - Number(r.settledUpto)
      if (diff > 0) accrued = diff * Number(r.paymentRate) / 1e18
    }

    var html = '<div class="dataset-detail-grid">'

    // Status & Identity
    html += '<div class="dd-section">' +
      '<h4>Status & Identity</h4>' +
      '<div class="dd-row"><span>State</span><span class="badge ' + stateClass + '">' + r.state + '</span></div>' +
      '<div class="dd-row"><span>Rail ID</span><span>' + r.railId + '</span></div>' +
      '<div class="dd-row"><span>Token</span><span>' + (r.token ? r.token.symbol : 'USDFC') + '</span></div>' +
      '<div class="dd-row"><span>Created</span><span>' + created + '</span></div>' +
      (r.endEpoch && r.endEpoch !== "0" ? '<div class="dd-row"><span>End Epoch</span><span>' + Number(r.endEpoch).toLocaleString() + '</span></div>' : '') +
    '</div>'

    // Payment
    html += '<div class="dd-section">' +
      '<h4>Payment</h4>' +
      '<div class="dd-row"><span>Rate</span><span>' + formatRate(r.paymentRate) + '</span></div>' +
      '<div class="dd-row"><span>Settled Upto</span><span>Epoch ' + Number(r.settledUpto || 0).toLocaleString() + '</span></div>' +
      '<div class="dd-row"><span>Total Settled</span><span>' + formatWei(r.totalSettledAmount) + '</span></div>' +
      '<div class="dd-row"><span>Accrued (Unsettled)</span><span style="color:var(--green)">$' + accrued.toFixed(4) + '</span></div>' +
      '<div class="dd-row"><span>Lockup Period</span><span>' + Number(r.lockupPeriod || 0).toLocaleString() + 's</span></div>' +
      '<div class="dd-row"><span>Lockup Fixed</span><span>' + formatWei(r.lockupFixed) + '</span></div>' +
      '<div class="dd-row"><span>Total Settlements</span><span>' + (r.totalSettlements || '0') + '</span></div>' +
    '</div>'

    // Parties
    html += '<div class="dd-section">' +
      '<h4>Parties</h4>' +
      '<div class="dd-row"><span>Payer</span><span style="font-size:11px">' + escapeHtml(r.payer ? r.payer.id : '-') + '</span></div>' +
      '<div class="dd-row"><span>Payee</span><span style="font-size:11px">' + escapeHtml(r.payee ? r.payee.id : '-') + '</span></div>' +
      '<div class="dd-row"><span>Operator</span><span style="font-size:11px">' + escapeHtml(r.operator ? r.operator.id : '-') + '</span></div>' +
      (ds ? '<div class="dd-row"><span>Linked Dataset</span><span>' + ds.dataSetId + ' (' + ds.status + ')</span></div>' +
        '<div class="dd-row"><span>Dataset Pieces</span><span>' + Number(ds.totalPieces || 0).toLocaleString() + '</span></div>' +
        '<div class="dd-row"><span>Dataset Size</span><span>' + formatBytes(ds.totalSize) + '</span></div>'
      : '') +
    '</div>'

    // Settlement History
    var settlements = r.settlements || []
    html += '<div class="dd-section">' +
      '<h4>Settlement History</h4>'
    if (settlements.length > 0) {
      html += '<table><thead><tr><th>Net Amount</th><th>Fee</th><th>Epoch</th><th>Time</th><th>Tx</th></tr></thead><tbody>'
      for (var i = 0; i < settlements.length; i++) {
        var s = settlements[i]
        var time = s.createdAt ? formatTime(new Date(Number(s.createdAt) * 1000).toISOString()) : '-'
        var txShort = s.txHash ? s.txHash.slice(0, 10) + '...' : '-'
        var txLink = s.txHash ? '<a href="https://filfox.info/en/tx/' + s.txHash + '" target="_blank" style="color:var(--accent)">' + txShort + '</a>' : '-'
        html += '<tr>' +
          '<td>' + formatWei(s.totalNetPayeeAmount) + '</td>' +
          '<td>' + formatWei(s.networkFee) + '</td>' +
          '<td>' + Number(s.settledUpto || 0).toLocaleString() + '</td>' +
          '<td>' + time + '</td>' +
          '<td>' + txLink + '</td>' +
        '</tr>'
      }
      html += '</tbody></table>'
    } else {
      html += '<div style="color:var(--text-muted);font-size:12px">No settlements yet</div>'
    }
    html += '</div>'

    html += '</div>'
    body.innerHTML = html
  } catch (err) {
    body.innerHTML = '<div class="error-banner">' + escapeHtml(err.message) + '</div>'
  }
}

// Revenue chart (loaded async, non-blocking)
async function loadRevenue(sp) {
  try {
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/revenue"))
    if (!data || !data.length) return

    // Build cumulative series
    var cumulative = []
    var running = 0
    for (var i = 0; i < data.length; i++) {
      running += data[i].revenue
      cumulative.push(running)
    }

    // Inject chart after summary grids, before the table (remove existing first)
    var econContent = document.getElementById("economics-content")
    if (!econContent) return
    var existing = document.getElementById("revenue-chart-section")
    if (existing) existing.remove()
    var chartSection = document.createElement("div")
    chartSection.id = "revenue-chart-section"
    chartSection.innerHTML = '<h4 style="font-size:11px;color:var(--text-muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.06em;font-weight:500">Total Revenue Over Time</h4>' +
      '<div class="chart-container"><canvas id="revenue-canvas"></canvas></div>'

    // Insert before the first table-scroll (the rails table)
    var table = econContent.querySelector(".table-scroll")
    if (table) {
      econContent.insertBefore(chartSection, table.previousElementSibling || table)
    } else {
      econContent.appendChild(chartSection)
    }

    var canvas = document.getElementById("revenue-canvas")
    var labels = data.map(function(d) {
      var parts = d.date.split("-")
      return parts[1] + "/" + parts[2]
    })
    drawLineChart(canvas, [
      { name: "Total Revenue (USDFC)", color: "#00d68f", data: cumulative },
    ], { labels: labels, emptyText: "No settlement data" })
  } catch (err) {
    // silently fail — chart is supplementary
  }
}

// ============================================================
// PERFORMANCE TAB
// ============================================================
async function loadPerformance(sp) {
  try {
    setLoading("performance-content", "Loading performance data")
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/performance?hours=" + selectedPerfHours))
    spDataCache.performance = data
    if (!data.available) { setNoData("performance-content", "No performance data for this SP"); return }
    var counters = data.counters || []
    var timing = data.timing || []
    if (!counters.length && !timing.length) { setNoData("performance-content", "No dealbot metrics for this provider"); return }

    // Aggregate counters by checkType and value
    var byCheck = {}
    for (var i = 0; i < counters.length; i++) {
      var c = counters[i]
      if (!byCheck[c.checkType]) byCheck[c.checkType] = { success: 0, failed: 0, pending: 0 }
      if (c.value === "success") byCheck[c.checkType].success += c.cnt
      else if (c.value === "pending") byCheck[c.checkType].pending += c.cnt
      else if (c.value && c.value.startsWith("failure")) byCheck[c.checkType].failed += c.cnt
    }

    // Build timing map: name:checkType -> avgMs
    var timingMap = {}
    for (var t = 0; t < timing.length; t++) {
      timingMap[timing[t].name + ":" + (timing[t].checkType || "")] = timing[t].avgMs
    }

    var html = ''

    // Render each check type
    var checkTypes = [
      { key: "dataStorage", label: "Deals", slaPct: 97 },
      { key: "retrieval", label: "Retrievals", slaPct: 97 },
      { key: "dataRetention", label: "Data Retention", slaFaultPct: 0.2 },
    ]

    for (var ci = 0; ci < checkTypes.length; ci++) {
      var ct = checkTypes[ci]
      var stats = byCheck[ct.key]
      if (!stats) continue

      var total = stats.success + stats.failed
      var pct, pctCls, meetsSLA

      if (ct.key === "dataRetention") {
        // Fault rate (lower is better)
        var faultPct = total > 0 ? ((stats.failed / total) * 100).toFixed(2) : "0"
        meetsSLA = parseFloat(faultPct) <= ct.slaFaultPct
        html += sectionHeading(ct.label)
        html += summaryGrid([
          { label: "Fault Rate", value: faultPct + "%", cls: faultRateClass(faultPct) },
          { label: "Total Checks", value: formatNum(total) },
          { label: "Faults", value: formatNum(stats.failed), cls: stats.failed > 0 ? "errors" : "zero" },
          { label: "SLA (\u2264" + ct.slaFaultPct + "%)", value: meetsSLA ? "PASS" : "FAIL", cls: slaClass(meetsSLA) },
        ])
      } else {
        // Success rate (higher is better)
        pct = total > 0 ? ((stats.success / total) * 100).toFixed(1) : "N/A"
        meetsSLA = pct !== "N/A" && parseFloat(pct) >= ct.slaPct
        html += sectionHeading(ct.label)
        html += summaryGrid([
          { label: "Success Rate", value: pct + "%", cls: rateClass(pct) },
          { label: "Total Tests", value: formatNum(total) },
          { label: "Successful", value: formatNum(stats.success), cls: "green", tab: "logs" },
          { label: "Failed", value: formatNum(stats.failed), cls: stats.failed > 0 ? "errors" : "zero", tab: "logs" },
          { label: "SLA (\u2265" + ct.slaPct + "%)", value: meetsSLA ? "PASS" : "FAIL", cls: slaClass(meetsSLA) },
        ])
      }
    }

    // Timing section
    var retCheckMs = timingMap["retrievalCheckMs:retrieval"]
    var ipfsFirstByte = timingMap["ipfsRetrievalFirstByteMs:retrieval"] || timingMap["ipfsRetrievalFirstByteMs:dataStorage"]
    var ipfsLastByte = timingMap["ipfsRetrievalLastByteMs:retrieval"] || timingMap["ipfsRetrievalLastByteMs:dataStorage"]
    var ipfsThroughput = timingMap["ipfsRetrievalThroughputBps:retrieval"] || timingMap["ipfsRetrievalThroughputBps:dataStorage"]
    var ipniVerify = timingMap["ipniVerifyMs:dataStorage"] || timingMap["ipniVerifyMs:retrieval"]

    // Chart placeholder
    html += '<div class="chart-container" id="perf-chart-container"><canvas id="perf-chart-canvas"></canvas></div>'

    html += sectionHeading("Timing (Avg)")
    html += summaryGrid([
      { label: "SP Retrieval", value: retCheckMs ? fmtDuration(retCheckMs / 1000) : "--" },
      { label: "IPFS First Byte", value: ipfsFirstByte ? fmtDuration(ipfsFirstByte / 1000) : "--" },
      { label: "IPFS Last Byte", value: ipfsLastByte ? fmtDuration(ipfsLastByte / 1000) : "--" },
      { label: "IPFS Throughput", value: ipfsThroughput ? formatBytes(ipfsThroughput) + "/s" : "--" },
      { label: "IPNI Verification", value: ipniVerify ? fmtDuration(ipniVerify / 1000) : "--" },
    ])

    // Latency chart placeholder
    html += sectionHeading("Latency Over Time")
    html += '<div class="chart-container" id="latency-chart-container"><canvas id="latency-chart-canvas"></canvas></div>'

    document.getElementById("performance-content").innerHTML = html

    // Load charts async
    loadPerfTimeline(sp)
    loadPerfLatency(sp)
  } catch (err) {
    setError("performance-content", err.message)
  }
}

async function loadPerfTimeline(sp) {
  try {
    var container = document.getElementById("perf-chart-container")
    if (container) container.innerHTML = '<div class="loading">Loading chart</div>'
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/performance/timeline?hours=" + selectedPerfHours))
    if (!data || !data.length) {
      if (container) container.innerHTML = '<canvas id="perf-chart-canvas"></canvas>'
      return
    }
    if (container) container.innerHTML = '<canvas id="perf-chart-canvas"></canvas>'

    // Group by time, compute success % per checkType
    var times = {}
    for (var i = 0; i < data.length; i++) {
      var row = data[i]
      if (!times[row.time]) times[row.time] = {}
      var total = row.success + row.failed
      times[row.time][row.checkType] = total > 0 ? (row.success / total) * 100 : 100
    }

    var sortedTimes = Object.keys(times).sort()
    var hours = selectedPerfHours
    var labels = sortedTimes.map(function(t) {
      var d = new Date(t + (t.includes("Z") ? "" : "Z"))
      return hours > 24
        ? d.toLocaleDateString([], { month: "short", day: "numeric" })
        : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    })

    var dealData = sortedTimes.map(function(t) { return times[t]["dataStorage"] != null ? times[t]["dataStorage"] : null })
    var retData = sortedTimes.map(function(t) { return times[t]["retrieval"] != null ? times[t]["retrieval"] : null })

    var series = []
    if (retData.some(function(v) { return v !== null })) {
      series.push({ name: "Retrievals %", color: "#0090ff", data: retData.map(function(v) { return v || 0 }) })
    }
    if (dealData.some(function(v) { return v !== null })) {
      series.push({ name: "Deals %", color: "#00d68f", data: dealData.map(function(v) { return v || 0 }) })
    }

    var canvas = document.getElementById("perf-chart-canvas")
    if (canvas && series.length) {
      drawLineChart(canvas, series, { labels: labels, emptyText: "No timeline data", maxVal: 110 })
    }
  } catch (err) {
    // silently fail — chart is supplementary
  }
}

async function loadPerfLatency(sp) {
  try {
    var container = document.getElementById("latency-chart-container")
    if (container) container.innerHTML = '<div class="loading">Loading chart</div>'
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/performance/latency?hours=" + selectedPerfHours))
    if (!data || !data.length) {
      if (container) container.innerHTML = '<canvas id="latency-chart-canvas"></canvas>'
      return
    }
    if (container) container.innerHTML = '<canvas id="latency-chart-canvas"></canvas>'

    // Group by time, build series per metric
    var times = {}
    var metricSet = {}
    for (var i = 0; i < data.length; i++) {
      var row = data[i]
      if (!times[row.time]) times[row.time] = {}
      times[row.time][row.metric] = row.sum_val
      metricSet[row.metric] = true
    }

    var sortedTimes = Object.keys(times).sort()
    var hours = selectedPerfHours
    var labels = sortedTimes.map(function(t) {
      var d = new Date(t + (t.includes("Z") ? "" : "Z"))
      return hours > 24
        ? d.toLocaleDateString([], { month: "short", day: "numeric" })
        : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    })

    var metricConfig = {
      "retrievalCheckMs": { name: "SP Retrieval", color: "#0090ff" },
      "ipfsRetrievalFirstByteMs": { name: "IPFS First Byte", color: "#00d68f" },
      "ipniVerifyMs": { name: "IPNI Verify", color: "#a78bfa" },
    }

    var series = []
    for (var metric in metricConfig) {
      if (!metricSet[metric]) continue
      var cfg = metricConfig[metric]
      series.push({
        name: cfg.name + " (ms)",
        color: cfg.color,
        data: sortedTimes.map(function(t) { return times[t][metric] || 0 }),
      })
    }

    var canvas = document.getElementById("latency-chart-canvas")
    if (canvas && series.length) {
      drawLineChart(canvas, series, { labels: labels, emptyText: "No latency data" })
    }
  } catch (err) {
    // silently fail
  }
}

function rateClass(pct) {
  if (pct === "N/A") return "rate-warn"
  var v = parseFloat(pct)
  if (v >= 97) return "rate-good"
  if (v >= 90) return "rate-warn"
  return "rate-bad"
}

function faultRateClass(pct) {
  var v = parseFloat(pct)
  if (v <= 0.2) return "rate-good"
  if (v <= 1) return "rate-warn"
  return "rate-bad"
}

function slaClass(pass) {
  return pass ? "sla-pass" : "sla-fail"
}

function sectionHeading(text) {
  return '<h4 style="font-size:11px;color:var(--text-muted);margin-bottom:8px;text-transform:uppercase;letter-spacing:0.06em;font-weight:500">' + text + '</h4>'
}

function fmtDuration(seconds) {
  if (!seconds) return '--'
  var s = Number(seconds)
  if (s < 1) return (s * 1000).toFixed(0) + 'ms'
  if (s < 60) return s.toFixed(1) + 's'
  return (s / 60).toFixed(1) + 'min'
}

// ============================================================
// LOGS TAB
// ============================================================

// Cached log data for modal drill-down
var logDataCache = { errors: [], patterns: [], logs: [] }

function showLogModal(type, index) {
  var modal = document.getElementById("log-modal")
  var body = document.getElementById("log-modal-body")
  var title = document.getElementById("log-modal-title")
  var item, html

  if (type === "issue") {
    item = logDataCache.errors[index]
    if (!item) return
    title.textContent = "Issue Detail"
    html = '<div class="dataset-detail-grid">' +
      '<div class="dd-section">' +
        '<h4>Details</h4>' +
        '<div class="dd-row"><span>Level</span><span class="level-' + (item.level || "") + '">' + (item.level || "-") + '</span></div>' +
        '<div class="dd-row"><span>Logger</span><span>' + escapeHtml(item.logger || "-") + '</span></div>' +
        '<div class="dd-row"><span>Count</span><span>' + formatNum(item.cnt) + '</span></div>' +
        '<div class="dd-row"><span>Last Seen</span><span>' + formatTime(item.last_seen) + '</span></div>' +
      '</div>' +
      '<div class="dd-section">' +
        '<h4>Message</h4>' +
        '<div style="font-family:JetBrains Mono,monospace;font-size:12px;color:var(--text-secondary);word-break:break-all;white-space:pre-wrap">' + escapeHtml(item.msg || "-") + '</div>' +
      '</div>' +
      (item.err ? '<div class="dd-section" style="grid-column:1/-1">' +
        '<h4>Error</h4>' +
        '<div style="font-family:JetBrains Mono,monospace;font-size:12px;color:var(--red);word-break:break-all;white-space:pre-wrap">' + escapeHtml(item.err) + '</div>' +
      '</div>' : '') +
    '</div>'
  } else if (type === "pattern") {
    item = logDataCache.patterns[index]
    if (!item) return
    title.textContent = "Error Pattern"
    html = '<div class="dataset-detail-grid">' +
      '<div class="dd-section">' +
        '<h4>Details</h4>' +
        '<div class="dd-row"><span>Level</span><span class="level-' + (item.level || "") + '">' + (item.level || "-") + '</span></div>' +
        '<div class="dd-row"><span>Logger</span><span>' + escapeHtml(item.logger || "-") + '</span></div>' +
        '<div class="dd-row"><span>Count</span><span>' + formatNum(item.cnt) + '</span></div>' +
        '<div class="dd-row"><span>First Seen</span><span>' + formatTime(item.first_seen) + '</span></div>' +
        '<div class="dd-row"><span>Last Seen</span><span>' + formatTime(item.last_seen) + '</span></div>' +
      '</div>' +
      '<div class="dd-section">' +
        '<h4>Pattern</h4>' +
        '<div style="font-family:JetBrains Mono,monospace;font-size:12px;color:var(--text-secondary);word-break:break-all;white-space:pre-wrap">' + escapeHtml(item.pattern || "-") + '</div>' +
      '</div>' +
    '</div>'
  } else if (type === "log") {
    item = logDataCache.logs[index]
    if (!item) return
    var d = new Date(item.dt + (item.dt.includes("Z") ? "" : "Z"))
    title.textContent = "Log Entry"
    html = '<div class="dataset-detail-grid">' +
      '<div class="dd-section">' +
        '<h4>Details</h4>' +
        '<div class="dd-row"><span>Time</span><span>' + d.toLocaleString() + '</span></div>' +
        '<div class="dd-row"><span>Level</span><span class="level-' + (item.level || "") + '">' + (item.level || "-") + '</span></div>' +
        '<div class="dd-row"><span>Logger</span><span>' + escapeHtml(item.logger || "-") + '</span></div>' +
        (item.taskID ? '<div class="dd-row"><span>Task ID</span><span>' + item.taskID + '</span></div>' : '') +
      '</div>' +
      '<div class="dd-section">' +
        '<h4>Message</h4>' +
        '<div style="font-family:JetBrains Mono,monospace;font-size:12px;color:var(--text-secondary);word-break:break-all;white-space:pre-wrap">' + escapeHtml(item.msg || "-") + '</div>' +
      '</div>' +
      (item.err ? '<div class="dd-section" style="grid-column:1/-1">' +
        '<h4>Error</h4>' +
        '<div style="font-family:JetBrains Mono,monospace;font-size:12px;color:var(--red);word-break:break-all;white-space:pre-wrap">' + escapeHtml(item.err) + '</div>' +
      '</div>' : '') +
    '</div>'
  }

  body.innerHTML = html
  modal.style.display = "flex"
}

// Summary cards + load all log sections
async function loadLogsSummary(sp) {
  var cardsEl = document.getElementById("log-summary-cards")
  if (!cardsEl) return
  var logs = sp.logHealth || {}
  var errCls = (logs.errors || 0) > 100 ? "rate-bad" : (logs.errors || 0) > 0 ? "rate-warn" : "rate-good"
  var warnCls = (logs.warns || 0) > 0 ? "rate-warn" : "rate-good"
  cardsEl.innerHTML = summaryGrid([
    { label: "Errors", value: formatNum(logs.errors || 0), cls: "errors" },
    { label: "Warnings", value: formatNum(logs.warns || 0), cls: "warnings" },
    { label: "Info Logs", value: formatNum(logs.info || 0), cls: "info" },
    { label: "Last Seen", value: logs.last_seen ? formatTime(logs.last_seen) : "--" },
  ])
}

async function loadSPTimeline(sp) {
  try {
    var container = document.getElementById("timeline-container")
    if (container) container.innerHTML = '<div class="loading">Loading chart</div>'
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/timeline?hours=" + getHours()))
    if (container) container.innerHTML = '<canvas id="timeline-canvas"></canvas>'
    if (!data.available) return
    var timeline = data.timeline || []
    var canvas = document.getElementById("timeline-canvas")

    var buckets = {}
    for (var i = 0; i < timeline.length; i++) {
      var row = timeline[i]
      if (!buckets[row.time]) buckets[row.time] = { errors: 0, warns: 0 }
      if (row.level === "error") buckets[row.time].errors += row.cnt
      else buckets[row.time].warns += row.cnt
    }

    var times = Object.keys(buckets).sort()
    var hours = getHours()
    var labels = times.map(function(t) {
      var d = new Date(t + (t.includes("Z") ? "" : "Z"))
      return hours > 24
        ? d.toLocaleDateString([], { month: "short", day: "numeric" })
        : d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    })

    var series = [
      { name: "Errors", color: "#ff4d6a", data: times.map(function(t) { return buckets[t].errors }) },
      { name: "Warnings", color: "#ffb020", data: times.map(function(t) { return buckets[t].warns }) },
    ]

    drawLineChart(canvas, series, { labels: labels, emptyText: "No errors or warnings in this time range" })
  } catch (err) {
    // silently fail
  }
}

async function loadSPErrors(sp) {
  try {
    setLoading("errors-content", "Loading top issues")
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/errors?hours=" + getHours()))
    if (!data.available) { setNoData("errors-content", "Logs not available"); return }
    var errors = data.errors || []
    var html = sectionHeading("Top Issues")
    if (!errors.length) {
      html += '<div class="no-data">No errors or warnings in this time range</div>'
      document.getElementById("errors-content").innerHTML = html
      return
    }

    var maxCnt = errors[0].cnt || 1
    html += '<div class="table-scroll" style="max-height:400px"><table id="issues-table"><thead><tr>' +
      '<th class="sortable" data-col="cnt">Count</th>' +
      '<th class="sortable" data-col="level">Level</th>' +
      '<th class="sortable" data-col="logger">Logger</th>' +
      '<th class="sortable" data-col="msg">Message</th>' +
      '<th class="sortable" data-col="err">Error</th>' +
      '<th class="sortable" data-col="lastseen">Last Seen</th>' +
    '</tr></thead><tbody>'
    for (var i = 0; i < errors.length; i++) {
      var e = errors[i]
      var pct = Math.round((e.cnt / maxCnt) * 100)
      var barClass = e.level === "warn" ? "warn" : ""
      var lsTs = e.last_seen ? new Date(e.last_seen + (e.last_seen.includes("Z") ? "" : "Z")).getTime() : 0
      html += '<tr data-idx="' + i + '" data-cnt="' + e.cnt + '" data-level="' + (e.level || "") + '" data-logger="' + escapeHtml(e.logger || "") + '" data-msg="' + escapeHtml(e.msg || "") + '" data-err="' + escapeHtml(e.err || "") + '" data-lastseen="' + lsTs + '">' +
        '<td><div class="pattern-bar"><span>' + formatNum(e.cnt) + '</span><div class="pattern-bar-fill ' + barClass + '" style="width:' + pct + 'px"></div></div></td>' +
        '<td class="level-' + (e.level || "") + '">' + (e.level || "-") + '</td>' +
        '<td>' + escapeHtml(e.logger || "-") + '</td>' +
        '<td title="' + escapeHtml(e.msg || "") + '">' + escapeHtml(truncate(e.msg || "-", 80)) + '</td>' +
        '<td title="' + escapeHtml(e.err || "") + '">' + escapeHtml(truncate(e.err || "-", 60)) + '</td>' +
        '<td>' + timeAgo(e.last_seen) + '</td>' +
      '</tr>'
    }
    html += '</tbody></table></div>'
    logDataCache.errors = errors
    document.getElementById("errors-content").innerHTML = html
    wireSortable("issues-table")
    document.querySelectorAll('#issues-table tbody tr').forEach(function(tr, idx) {
      tr.style.cursor = 'pointer'
      tr.addEventListener('click', function() { showLogModal("issue", Number(tr.dataset.idx)) })
    })
  } catch (err) {
    setError("errors-content", err.message)
  }
}

async function loadSPPatterns(sp) {
  try {
    setLoading("patterns-content", "Loading error patterns")
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/patterns?hours=" + getHours()))
    if (!data.available) { setNoData("patterns-content", "Logs not available"); return }
    var patterns = data.patterns || []
    var html = sectionHeading("Error Patterns")
    if (!patterns.length) {
      html += '<div class="no-data">No error patterns in this time range</div>'
      document.getElementById("patterns-content").innerHTML = html
      return
    }

    var maxCnt = patterns[0].cnt || 1
    html += '<div class="table-scroll" style="max-height:400px"><table id="patterns-table"><thead><tr>' +
      '<th class="sortable" data-col="cnt">Count</th>' +
      '<th class="sortable" data-col="level">Level</th>' +
      '<th class="sortable" data-col="logger">Logger</th>' +
      '<th class="sortable" data-col="pattern">Pattern</th>' +
      '<th class="sortable" data-col="firstseen">First Seen</th>' +
      '<th class="sortable" data-col="lastseen">Last Seen</th>' +
    '</tr></thead><tbody>'
    for (var i = 0; i < patterns.length; i++) {
      var p = patterns[i]
      var pct = Math.round((p.cnt / maxCnt) * 100)
      var barClass = p.level === "warn" ? "warn" : ""
      var fsTs = p.first_seen ? new Date(p.first_seen + (p.first_seen.includes("Z") ? "" : "Z")).getTime() : 0
      var lsTs = p.last_seen ? new Date(p.last_seen + (p.last_seen.includes("Z") ? "" : "Z")).getTime() : 0
      html += '<tr data-idx="' + i + '" data-cnt="' + p.cnt + '" data-level="' + (p.level || "") + '" data-logger="' + escapeHtml(p.logger || "") + '" data-pattern="' + escapeHtml(p.pattern || "") + '" data-firstseen="' + fsTs + '" data-lastseen="' + lsTs + '">' +
        '<td><div class="pattern-bar"><span>' + formatNum(p.cnt) + '</span><div class="pattern-bar-fill ' + barClass + '" style="width:' + pct + 'px"></div></div></td>' +
        '<td class="level-' + (p.level || "") + '">' + (p.level || "-") + '</td>' +
        '<td>' + escapeHtml(p.logger || "-") + '</td>' +
        '<td title="' + escapeHtml(p.pattern || "") + '">' + escapeHtml(truncate(p.pattern || "-", 80)) + '</td>' +
        '<td>' + timeAgo(p.first_seen) + '</td>' +
        '<td>' + timeAgo(p.last_seen) + '</td>' +
      '</tr>'
    }
    html += '</tbody></table></div>'
    logDataCache.patterns = patterns
    document.getElementById("patterns-content").innerHTML = html
    wireSortable("patterns-table")
    document.querySelectorAll('#patterns-table tbody tr').forEach(function(tr) {
      tr.style.cursor = 'pointer'
      tr.addEventListener('click', function() { showLogModal("pattern", Number(tr.dataset.idx)) })
    })
  } catch (err) {
    setError("patterns-content", err.message)
  }
}

async function loadSPLogs(sp) {
  var filterEl = document.getElementById("log-level-filter")
  var level = filterEl ? filterEl.value : ""
  var levelParam = level ? "&level=" + level : ""
  try {
    setLoading("logs-content", "Loading recent logs")
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/logs?hours=" + getHours() + levelParam))
    if (!data.available) { setNoData("logs-content", "Logs not available"); return }
    var logs = data.logs || []
    var currentLevel = document.getElementById("log-level-filter") ? document.getElementById("log-level-filter").value : ""
    var html = '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">' +
      '<h4 style="font-size:11px;color:var(--text-muted);text-transform:uppercase;letter-spacing:0.06em;font-weight:500;margin:0">Recent Logs</h4>' +
      '<select id="log-level-filter" style="font-family:Outfit,sans-serif">' +
        '<option value=""' + (currentLevel === "" ? " selected" : "") + '>All levels</option>' +
        '<option value="error"' + (currentLevel === "error" ? " selected" : "") + '>Errors</option>' +
        '<option value="warn"' + (currentLevel === "warn" ? " selected" : "") + '>Warnings</option>' +
        '<option value="info"' + (currentLevel === "info" ? " selected" : "") + '>Info</option>' +
      '</select>' +
    '</div>'
    if (!logs.length) {
      html += '<div class="no-data">No logs found</div>'
      document.getElementById("logs-content").innerHTML = html
      return
    }
    html += '<div class="table-scroll" style="max-height:500px"><table id="recentlogs-table"><thead><tr>' +
      '<th class="sortable" data-col="dt">Time</th>' +
      '<th class="sortable" data-col="level">Level</th>' +
      '<th class="sortable" data-col="logger">Logger</th>' +
      '<th class="sortable" data-col="msg">Message</th>' +
      '<th class="sortable" data-col="err">Error</th>' +
      '<th class="sortable" data-col="taskid">Task</th>' +
    '</tr></thead><tbody>'
    for (var i = 0; i < logs.length; i++) {
      var l = logs[i]
      var d = new Date(l.dt + (l.dt.includes("Z") ? "" : "Z"))
      html += '<tr data-idx="' + i + '" data-dt="' + d.getTime() + '" data-level="' + (l.level || "") + '" data-logger="' + escapeHtml(l.logger || "") + '" data-msg="' + escapeHtml(l.msg || "") + '" data-err="' + escapeHtml(l.err || "") + '" data-taskid="' + (l.taskID || 0) + '">' +
        '<td>' + d.toLocaleTimeString() + '</td>' +
        '<td class="level-' + (l.level || "") + '">' + (l.level || "-") + '</td>' +
        '<td>' + escapeHtml(l.logger || "-") + '</td>' +
        '<td title="' + escapeHtml(l.msg || "") + '">' + escapeHtml(truncate(l.msg || "-", 60)) + '</td>' +
        '<td title="' + escapeHtml(l.err || "") + '">' + escapeHtml(truncate(l.err || "-", 60)) + '</td>' +
        '<td>' + (l.taskID || "-") + '</td>' +
      '</tr>'
    }
    html += '</tbody></table></div>'
    logDataCache.logs = logs
    document.getElementById("logs-content").innerHTML = html
    wireSortable("recentlogs-table")
    document.querySelectorAll('#recentlogs-table tbody tr').forEach(function(tr) {
      tr.style.cursor = 'pointer'
      tr.addEventListener('click', function() { showLogModal("log", Number(tr.dataset.idx)) })
    })
  } catch (err) {
    setError("logs-content", err.message)
  }
}
