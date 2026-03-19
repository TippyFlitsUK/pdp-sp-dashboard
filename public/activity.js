// Activity tab — per-dataset upload activity, client identification

async function loadActivity(sp) {
  try {
    setLoading("activity-content", "Loading activity data")
    var data = await fetchJSON(apiUrl("/api/sp/" + sp.id + "/activity?hours=" + selectedActivityHours))
    spDataCache.activity = data

    var datasets = data.datasets || []
    if (!datasets.length) { setNoData("activity-content", "No datasets found"); return }

    var totalRecentTx = data.totalRecentAddPieces || 0
    var activeDatasets = datasets.filter(function(d) { return d.recentAddPieces > 0 })
    var topDataset = datasets[0]

    var html = ""

    // Summary cards
    html += summaryGrid([
      { label: "Total Datasets", value: formatNum(datasets.length) },
      { label: "Active (" + selectedActivityHours + "h)", value: formatNum(activeDatasets.length) },
      { label: "addPieces Txs (" + selectedActivityHours + "h)", value: formatNum(totalRecentTx) + (data.truncated ? "+" : "") },
      { label: "Most Active Dataset", value: topDataset.recentAddPieces > 0 ? "#" + topDataset.setId : "None" },
    ])

    // Activity chart from transaction timeline — right after summary cards
    var timeline = data.timeline || []
    if (timeline.length > 0) {
      html += sectionHeading("Activity Timeline (" + selectedActivityHours + "h)")
      html += '<div class="chart-container"><canvas id="activity-chart"></canvas></div>'
    }

    // Dataset activity table
    html += sectionHeading("Dataset Activity (" + selectedActivityHours + "h)")
    html += '<div class="table-scroll"><table id="activity-table"><thead><tr>' +
      '<th class="sortable" data-col="setid">Dataset</th>' +
      '<th class="sortable" data-col="status">Status</th>' +
      '<th class="sortable" data-col="recent">addPieces (' + selectedActivityHours + 'h)</th>' +
      '<th class="sortable" data-col="roots">Total Pieces</th>' +
      '<th class="sortable" data-col="size">Total Data Size</th>' +
      '<th class="sortable" data-col="recentsize">Added (' + selectedActivityHours + 'h)</th>' +
      '<th>Client</th>' +
      '<th class="sortable" data-col="rail">Rail</th>' +
    '</tr></thead><tbody>'

    for (var j = 0; j < datasets.length; j++) {
      var d = datasets[j]
      var statusClass = d.status === "Active" ? "active" : "terminated"
      var shortClient = d.client ? d.client.slice(0, 8) + "..." : "-"
      var recentCls = d.recentAddPieces > 100 ? "level-error" : d.recentAddPieces > 10 ? "level-warn" : ""
      var railInfo = d.pdpRailId ? "#" + d.pdpRailId : "-"

      html += '<tr' +
        ' data-setid="' + Number(d.setId) + '"' +
        ' data-status="' + (d.status === "Active" ? 1 : 0) + '"' +
        ' data-recent="' + d.recentAddPieces + '"' +
        ' data-roots="' + Number(d.totalRoots || 0) + '"' +
        ' data-size="' + Number(d.totalDataSize || 0) + '"' +
        ' data-recentsize="' + Number(d.recentDataSize || 0) + '"' +
        ' data-rail="' + (d.pdpRailId || 0) + '"' +
      '>' +
        '<td>' + d.setId + '</td>' +
        '<td><span class="badge ' + statusClass + '">' + d.status + '</span></td>' +
        '<td class="' + recentCls + '">' + formatNum(d.recentAddPieces) + '</td>' +
        '<td>' + Number(d.totalRoots || 0).toLocaleString() + '</td>' +
        '<td>' + formatBytes(d.totalDataSize) + '</td>' +
        '<td>' + formatBytes(d.recentDataSize) + '</td>' +
        '<td title="' + escapeHtml(d.client || "") + '">' + escapeHtml(shortClient) + '</td>' +
        '<td>' + escapeHtml(railInfo) + '</td>' +
      '</tr>'
    }
    html += '</tbody></table></div>'

    document.getElementById("activity-content").innerHTML = html

    // Wire sortable table
    wireSortable("activity-table")

    // Wire dataset row clicks to dataset modal
    document.querySelectorAll('#activity-table tbody tr').forEach(function(tr) {
      tr.style.cursor = "pointer"
      tr.addEventListener("click", function() {
        showDatasetModal(sp.id, tr.dataset.setid)
      })
    })

    // Draw activity timeline chart from transaction data
    if (timeline.length > 0) {
      var canvas = document.getElementById("activity-chart")
      if (canvas) {
        // Find top 5 datasets by total count in timeline
        var dsCounts = {}
        for (var ti = 0; ti < timeline.length; ti++) {
          var t = timeline[ti]
          dsCounts[t.dataSetId] = (dsCounts[t.dataSetId] || 0) + t.count
        }
        var topDs = Object.keys(dsCounts).sort(function(a, b) { return dsCounts[b] - dsCounts[a] }).slice(0, 5)

        // Collect all time buckets, sorted
        var allTimes = {}
        for (var tj = 0; tj < timeline.length; tj++) {
          allTimes[timeline[tj].time] = true
        }
        var sortedTimes = Object.keys(allTimes).map(Number).sort(function(a, b) { return a - b })

        // Build per-dataset lookup
        var byDsTime = {}
        for (var tk = 0; tk < timeline.length; tk++) {
          var row = timeline[tk]
          byDsTime[row.dataSetId + ":" + row.time] = row.count
        }

        var hours = selectedActivityHours
        var labels = sortedTimes.map(function(ts) {
          var d = new Date(ts * 1000)
          return hours <= 24
            ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
            : d.toLocaleDateString([], { month: "short", day: "numeric" })
        })

        var colors = ["#0090ff", "#00d68f", "#ff4d6a", "#a78bfa", "#ffb020"]
        var series = topDs.map(function(dsId, idx) {
          return {
            name: "#" + dsId,
            color: colors[idx % colors.length],
            data: sortedTimes.map(function(ts) { return byDsTime[dsId + ":" + ts] || 0 }),
          }
        })

        drawLineChart(canvas, series, { labels: labels, emptyText: "No activity data" })
      }
    }
  } catch (err) {
    setError("activity-content", err.message)
  }
}
