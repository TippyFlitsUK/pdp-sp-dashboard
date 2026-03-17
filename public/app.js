// Shared state and helpers
var selectedHours = 24
var selectedPerfHours = 24
var currentSP = null
var currentTab = "overview"
var spConfig = []
var selectedNetwork = "mainnet"
var forceRefresh = false

function getHours() {
  return selectedHours
}

function apiUrl(path) {
  var sep = path.indexOf("?") === -1 ? "?" : "&"
  var url = path + sep + "network=" + selectedNetwork
  if (forceRefresh) url += "&refresh=1"
  return url
}

async function fetchJSON(url) {
  var res = await fetch(url)
  if (!res.ok) {
    var body = await res.json().catch(function() { return {} })
    throw new Error(body.error || "HTTP " + res.status)
  }
  return res.json()
}

function escapeHtml(str) {
  if (!str) return ""
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;")
}

function truncate(str, len) {
  if (!str) return ""
  return str.length > len ? str.slice(0, len) + "..." : str
}

function formatNum(n) {
  if (n == null) return "-"
  n = Number(n)
  if (isNaN(n)) return "-"
  return n.toLocaleString()
}

function formatBytes(bytes) {
  if (!bytes) return "0 B"
  var b = Number(bytes)
  if (isNaN(b) || b === 0) return "0 B"
  if (b >= 1099511627776) return (b / 1099511627776).toFixed(2) + " TB"
  if (b >= 1073741824) return (b / 1073741824).toFixed(2) + " GB"
  if (b >= 1048576) return (b / 1048576).toFixed(2) + " MB"
  if (b >= 1024) return (b / 1024).toFixed(1) + " KB"
  return b + " B"
}

function formatRate(weiPerEpoch) {
  if (!weiPerEpoch || weiPerEpoch === "0") return "$0/mo"
  // paymentRate is per Filecoin epoch (30 sec), 18 decimals
  var epochsPerDay = 86400 / 30  // 2880
  var perDay = (Number(weiPerEpoch) * epochsPerDay) / 1e18
  var perMonth = perDay * 30
  if (perMonth >= 1) return "$" + perMonth.toFixed(2) + "/mo"
  if (perMonth >= 0.01) return "$" + perMonth.toFixed(4) + "/mo"
  return "$" + perDay.toFixed(4) + "/day"
}

function formatWei(wei) {
  if (!wei) return "$0"
  var s = String(wei).padStart(19, "0")
  var whole = s.slice(0, s.length - 18) || "0"
  var dec = s.slice(s.length - 18, s.length - 16)
  if (dec === "00") return "$" + whole
  return "$" + whole + "." + dec.replace(/0+$/, "")
}

function timeAgo(dt) {
  if (!dt) return "N/A"
  var now = Date.now()
  var then = new Date(dt + (dt.includes("Z") ? "" : "Z")).getTime()
  var mins = Math.floor((now - then) / 60000)
  if (mins < 1) return "just now"
  if (mins < 60) return mins + "m ago"
  var hrs = Math.floor(mins / 60)
  if (hrs < 24) return hrs + "h ago"
  return Math.floor(hrs / 24) + "d ago"
}

function formatTime(dt) {
  if (!dt) return "-"
  var d = new Date(dt + (dt.includes("Z") ? "" : "Z"))
  return d.toLocaleString([], { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" })
}

function heartbeatClass(liveness) {
  if (!liveness || liveness.alive == null) return "unknown"
  if (liveness.alive) return "alive"
  return "dead"
}

// Hash router
function getRoute() {
  var hash = window.location.hash || "#overview"
  if (hash.startsWith("#sp/")) {
    var parts = hash.slice(4).split("/")
    var id = parseInt(parts[0], 10)
    var tab = parts[1] || "performance"
    return { page: "detail", spId: id, tab: tab }
  }
  return { page: "overview" }
}

function navigate(hash) {
  window.location.hash = hash
}

function switchTab(tab) {
  if (!currentSP) return
  navigate("#sp/" + currentSP.id + "/" + tab)
}

function initRouter() {
  async function onRoute() {
    var route = getRoute()
    var overviewSection = document.getElementById("section-overview")
    var detailSection = document.getElementById("section-detail")

    if (route.page === "detail" && route.spId) {
      overviewSection.style.display = "none"
      detailSection.style.display = "block"

      // Ensure spConfig is loaded (needed when landing directly on SP detail URL)
      if (!spConfig.length) {
        try {
          spConfig = await fetchJSON(apiUrl("/api/network/overview"))
        } catch (e) {
          spConfig = []
        }
      }

      var sp = spConfig.find(function(s) { return s.id === route.spId })
      if (sp) {
        var spChanged = !currentSP || currentSP.id !== sp.id
        currentSP = sp
        currentTab = route.tab
        if (spChanged) {
          loadSPDetail(sp)
        }
        showTab(route.tab)
      }
    } else {
      overviewSection.style.display = "block"
      detailSection.style.display = "none"
      currentSP = null
      currentTab = "overview"
      loadOverview()
    }
  }

  window.addEventListener("hashchange", onRoute)

  // Time range buttons (logs)
  document.getElementById("time-range-btns").addEventListener("click", function(e) {
    if (e.target.tagName !== "BUTTON") return
    var hours = parseInt(e.target.dataset.hours, 10)
    if (!hours) return
    selectedHours = hours
    var btns = document.querySelectorAll("#time-range-btns button")
    btns.forEach(function(b) { b.classList.remove("active") })
    e.target.classList.add("active")
    if (currentSP && currentSP.hasLogs) {
      spDataCache.logs = false
      Promise.all([loadSPTimeline(currentSP), loadSPErrors(currentSP), loadSPPatterns(currentSP), loadSPLogs(currentSP)]).catch(function() {})
    }
  })

  // Performance time range buttons
  document.getElementById("perf-time-range-btns").addEventListener("click", function(e) {
    if (e.target.tagName !== "BUTTON") return
    var hours = parseInt(e.target.dataset.hours, 10)
    if (!hours) return
    selectedPerfHours = hours
    var btns = document.querySelectorAll("#perf-time-range-btns button")
    btns.forEach(function(b) { b.classList.remove("active") })
    e.target.classList.add("active")
    if (currentSP) { spDataCache.performance = null; loadPerformance(currentSP) }
  })

  // Network switcher
  document.querySelector(".network-switcher").addEventListener("click", function(e) {
    if (e.target.tagName !== "BUTTON") return
    var net = e.target.dataset.network
    if (!net || net === selectedNetwork) return
    selectedNetwork = net
    var btns = document.querySelectorAll(".net-btn")
    btns.forEach(function(b) { b.classList.toggle("active", b.dataset.network === net) })
    spConfig = []
    currentSP = null
    history.replaceState(null, "", "/")
    loadOverview()
  })

  // Refresh button
  document.getElementById("refresh-btn").addEventListener("click", refresh)

  // Escape key closes modals
  document.addEventListener("keydown", function(e) {
    if (e.key === "Escape") {
      document.querySelectorAll(".modal-overlay").forEach(function(m) { m.style.display = "none" })
    }
  })

  // Back button
  document.getElementById("back-btn").addEventListener("click", function() {
    history.replaceState(null, "", "/")
    document.getElementById("section-overview").style.display = "block"
    document.getElementById("section-detail").style.display = "none"
    currentSP = null
    currentTab = "overview"
    loadOverview()
  })

  // Tab clicks
  document.getElementById("detail-tabs").addEventListener("click", function(e) {
    var tab = e.target.closest(".detail-tab")
    if (!tab) return
    var tabName = tab.dataset.tab
    if (tabName) switchTab(tabName)
  })

  // Log level filter (delegated, element is dynamically created)
  document.getElementById("panel-logs").addEventListener("change", function(e) {
    if (e.target.id === "log-level-filter" && currentSP && currentSP.hasLogs) {
      loadSPLogs(currentSP)
    }
  })

  // Initial route
  onRoute()
}

function showTab(tabName) {
  // Update tab buttons
  var tabs = document.querySelectorAll(".detail-tab")
  tabs.forEach(function(t) {
    t.classList.toggle("active", t.dataset.tab === tabName)
  })

  // Show/hide tab content
  var contents = document.querySelectorAll(".tab-content")
  contents.forEach(function(c) {
    c.style.display = c.id === "tab-" + tabName ? "block" : "none"
  })

  currentTab = tabName

  // Lazy-load tab data on first click, then redraw charts
  if (currentSP) {
    loadTabData(tabName, currentSP)
    if (tabName === "performance") {
      loadPerfTimeline(currentSP)
      loadPerfLatency(currentSP)
    } else if (tabName === "proving" && document.getElementById("proving-chart")) {
      var provData = spDataCache.proving
      if (provData && provData.weeklyActivity && provData.weeklyActivity.length > 0) {
        var canvas = document.getElementById("proving-chart")
        if (canvas) {
          var weeks = provData.weeklyActivity.slice().reverse()
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
    } else if (tabName === "logs") {
      loadSPTimeline(currentSP)
    }
  }
}

async function refresh() {
  forceRefresh = true
  spDataCache = {}
  var route = getRoute()
  if (route.page === "detail" && currentSP) {
    await loadSPDetail(currentSP)
  } else {
    await loadOverview()
  }
  forceRefresh = false
}

// Canvas helpers
function drawBarChart(canvas, data, options) {
  var dpr = window.devicePixelRatio || 1
  var rect = canvas.parentElement.getBoundingClientRect()
  canvas.width = rect.width * dpr
  canvas.height = rect.height * dpr
  var ctx = canvas.getContext("2d")
  ctx.scale(dpr, dpr)
  var W = rect.width
  var H = rect.height

  ctx.clearRect(0, 0, W, H)

  if (!data || !data.length) {
    ctx.fillStyle = "#5a6e8a"
    ctx.font = "13px Outfit, sans-serif"
    ctx.textAlign = "center"
    ctx.fillText(options.emptyText || "No data", W / 2, H / 2)
    return
  }

  var padL = 50, padR = 20, padT = 10, padB = 30
  var chartW = W - padL - padR
  var chartH = H - padT - padB

  var maxVal = 0
  for (var i = 0; i < data.length; i++) {
    var total = 0
    for (var key in data[i].values) total += data[i].values[key]
    if (total > maxVal) maxVal = total
  }
  if (maxVal === 0) maxVal = 1

  // Y axis gridlines
  ctx.strokeStyle = "#1c2a45"
  ctx.lineWidth = 1
  var yTicks = 4
  for (var j = 0; j <= yTicks; j++) {
    var y = padT + chartH - (j / yTicks) * chartH
    ctx.beginPath()
    ctx.moveTo(padL, y)
    ctx.lineTo(W - padR, y)
    ctx.stroke()
    ctx.fillStyle = "#5a6e8a"
    ctx.font = "10px 'JetBrains Mono', monospace"
    ctx.textAlign = "right"
    ctx.fillText(formatNum(Math.round((maxVal * j) / yTicks)), padL - 6, y + 3)
  }

  // X axis labels
  var labelCount = Math.min(data.length, 8)
  var labelStep = Math.max(1, Math.floor(data.length / labelCount))
  ctx.fillStyle = "#5a6e8a"
  ctx.font = "10px 'JetBrains Mono', monospace"
  ctx.textAlign = "center"
  for (var k = 0; k < data.length; k += labelStep) {
    var x = padL + (k / (data.length - 1 || 1)) * chartW
    ctx.fillText(data[k].label, x, H - 6)
  }

  // Stacked bars
  var barW = Math.max(3, chartW / data.length - 2)
  var colors = options.colors || {}
  var stackOrder = options.stackOrder || Object.keys(colors)

  for (var m = 0; m < data.length; m++) {
    var bx = padL + (m / (data.length - 1 || 1)) * chartW - barW / 2
    var accumulated = 0
    for (var s = stackOrder.length - 1; s >= 0; s--) {
      var sKey = stackOrder[s]
      var val = data[m].values[sKey] || 0
      if (val <= 0) continue
      var barH = (val / maxVal) * chartH
      ctx.fillStyle = colors[sKey] || "#0090ff"
      ctx.beginPath()
      ctx.roundRect(bx, padT + chartH - accumulated - barH, barW, barH, 2)
      ctx.fill()
      accumulated += barH
    }
  }

  // Legend
  ctx.font = "11px Outfit, sans-serif"
  ctx.textAlign = "left"
  var lx = W - 20
  for (var li = stackOrder.length - 1; li >= 0; li--) {
    var lKey = stackOrder[li]
    var lLabel = options.labels ? options.labels[lKey] || lKey : lKey
    var tw = ctx.measureText(lLabel).width
    lx -= tw + 18
    ctx.fillStyle = colors[lKey]
    ctx.beginPath()
    ctx.roundRect(lx, 8, 10, 10, 2)
    ctx.fill()
    ctx.fillStyle = "#8899b4"
    ctx.fillText(lLabel, lx + 14, 17)
  }
}

// Line chart helper
function drawLineChart(canvas, series, options) {
  var dpr = window.devicePixelRatio || 1
  var rect = canvas.parentElement.getBoundingClientRect()
  canvas.width = rect.width * dpr
  canvas.height = rect.height * dpr
  var ctx = canvas.getContext("2d")
  ctx.scale(dpr, dpr)
  var W = rect.width
  var H = rect.height

  ctx.clearRect(0, 0, W, H)

  if (!series || !series.length || !series[0].data.length) {
    ctx.fillStyle = "#5a6e8a"
    ctx.font = "13px Outfit, sans-serif"
    ctx.textAlign = "center"
    ctx.fillText(options.emptyText || "No data", W / 2, H / 2)
    return
  }

  var padL = 50, padR = 20, padT = 36, padB = 30
  var chartW = W - padL - padR
  var chartH = H - padT - padB
  var labels = options.labels || []
  var len = series[0].data.length

  // Find max across all series, add padding (or use options.maxVal override)
  var maxVal = options.maxVal || 0
  if (!maxVal) {
    for (var s = 0; s < series.length; s++) {
      for (var d = 0; d < series[s].data.length; d++) {
        if (series[s].data[d] > maxVal) maxVal = series[s].data[d]
      }
    }
    maxVal = maxVal + 50
    if (maxVal === 50) maxVal = 100
  }

  // Y axis gridlines
  ctx.strokeStyle = "#1c2a45"
  ctx.lineWidth = 1
  var yTicks = 4
  for (var j = 0; j <= yTicks; j++) {
    var y = padT + chartH - (j / yTicks) * chartH
    ctx.beginPath()
    ctx.moveTo(padL, y)
    ctx.lineTo(W - padR, y)
    ctx.stroke()
    ctx.fillStyle = "#5a6e8a"
    ctx.font = "10px 'JetBrains Mono', monospace"
    ctx.textAlign = "right"
    ctx.fillText(formatNum(Math.round((maxVal * j) / yTicks)), padL - 6, y + 3)
  }

  // X axis labels
  ctx.fillStyle = "#5a6e8a"
  ctx.font = "10px 'JetBrains Mono', monospace"
  ctx.textAlign = "center"
  for (var k = 0; k < len; k++) {
    var x = padL + (k / (len - 1 || 1)) * chartW
    if (labels[k] && (len <= 12 || k % Math.ceil(len / 10) === 0)) {
      ctx.fillText(labels[k], x, H - 6)
    }
  }

  // Draw each series as a line
  for (var si = 0; si < series.length; si++) {
    var sr = series[si]
    ctx.strokeStyle = sr.color || "#0090ff"
    ctx.lineWidth = 2
    ctx.beginPath()
    for (var di = 0; di < sr.data.length; di++) {
      var px = padL + (di / (len - 1 || 1)) * chartW
      var py = padT + chartH - (sr.data[di] / maxVal) * chartH
      if (di === 0) ctx.moveTo(px, py)
      else ctx.lineTo(px, py)
    }
    ctx.stroke()

    // Draw dots
    for (var di2 = 0; di2 < sr.data.length; di2++) {
      var px2 = padL + (di2 / (len - 1 || 1)) * chartW
      var py2 = padT + chartH - (sr.data[di2] / maxVal) * chartH
      ctx.fillStyle = sr.color || "#0090ff"
      ctx.beginPath()
      ctx.arc(px2, py2, 3, 0, Math.PI * 2)
      ctx.fill()
    }
  }

  // Legend
  ctx.font = "11px Outfit, sans-serif"
  ctx.textAlign = "left"
  var lx = W - 20
  for (var li = series.length - 1; li >= 0; li--) {
    var lbl = series[li].name || ""
    var tw = ctx.measureText(lbl).width
    lx -= tw + 18
    ctx.fillStyle = series[li].color
    ctx.beginPath()
    ctx.roundRect(lx, 6, 10, 10, 2)
    ctx.fill()
    ctx.fillStyle = "#8899b4"
    ctx.fillText(lbl, lx + 14, 15)
  }
}

// Generic sortable table wiring
function wireSortable(tableId) {
  var table = document.getElementById(tableId)
  if (!table) return
  table.querySelectorAll('th.sortable').forEach(function(th) {
    th.addEventListener('click', function() {
      var col = th.dataset.col
      var tbody = table.querySelector('tbody')
      var rows = Array.from(tbody.querySelectorAll('tr'))
      var asc = th.classList.contains('sort-asc')
      table.querySelectorAll('th.sortable').forEach(function(h) { h.classList.remove('sort-asc', 'sort-desc') })
      th.classList.add(asc ? 'sort-desc' : 'sort-asc')
      rows.sort(function(a, b) {
        var va = a.dataset[col] || ''
        var vb = b.dataset[col] || ''
        var na = Number(va), nb = Number(vb)
        if (!isNaN(na) && !isNaN(nb) && va !== '' && vb !== '') {
          return asc ? nb - na : na - nb
        }
        return asc ? vb.localeCompare(va) : va.localeCompare(vb)
      })
      rows.forEach(function(r) { tbody.appendChild(r) })
    })
  })
}

// Boot
document.addEventListener("DOMContentLoaded", initRouter)
