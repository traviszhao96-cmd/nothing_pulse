const startDateInput = document.getElementById("startDate");
const endDateInput = document.getElementById("endDate");
const scopeSelect = document.getElementById("scopeSelect");
const refreshButton = document.getElementById("refreshButton");
const metricTemplate = document.getElementById("metricTemplate");
const metricsNode = document.getElementById("metrics");
const trendChartNode = document.getElementById("trendChart");
const categoryBarsNode = document.getElementById("categoryBars");
const sentimentBarsNode = document.getElementById("sentimentBars");
const originBarsNode = document.getElementById("originBars");
const mediaBarsNode = document.getElementById("mediaBars");
const keywordCloudNode = document.getElementById("keywordCloud");
const subTagCloudNode = document.getElementById("subTagCloud");
const positiveInsightsNode = document.getElementById("positiveInsights");
const neutralInsightsNode = document.getElementById("neutralInsights");
const negativeInsightsNode = document.getElementById("negativeInsights");
const runtimeStatusNode = document.getElementById("runtimeStatus");
const refreshStatusButton = document.getElementById("refreshStatusButton");
const syncWorkspaceButton = document.getElementById("syncLarkButton");
const exportVideoTasksButton = document.getElementById("exportVideoTasksButton");
const runVideoProcessButton = document.getElementById("runVideoProcessButton");
const runtimeActionResultNode = document.getElementById("runtimeActionResult");
const taskTabsNode = document.getElementById("taskTabs");
const taskPanelNode = document.getElementById("taskPanel");
const competitorBrandBarsNode = document.getElementById("competitorBrandBars");
const competitorVideoTypeBarsNode = document.getElementById("competitorVideoTypeBars");
const competitorFocusCloudNode = document.getElementById("competitorFocusCloud");
const competitorVideoListNode = document.getElementById("competitorVideoList");
const detailListNode = document.getElementById("detailList");
const topTabsNode = document.getElementById("topTabs");
const runtimeConfig = window.MEDIA_PULSE_CONFIG || window.NT_CAM_PULSE_CONFIG || {};
const apiBaseUrl = String(runtimeConfig.apiBaseUrl || "").replace(/\/+$/, "");

let selectedTaskId = "pipeline";
let latestStatusPayload = null;

init();

async function init() {
  bindTabs();
  refreshButton.addEventListener("click", refreshAll);
  startDateInput.addEventListener("change", refreshAll);
  endDateInput.addEventListener("change", refreshAll);
  scopeSelect.addEventListener("change", refreshAll);
  refreshStatusButton.addEventListener("click", refreshStatusOnly);
  syncWorkspaceButton.addEventListener("click", runManualWorkspaceSync);
  exportVideoTasksButton.addEventListener("click", exportVideoTasks);
  runVideoProcessButton.addEventListener("click", runManualVideoProcess);

  try {
    await loadDateOptions();
    await refreshAll();
  } catch (error) {
    console.error(error);
    renderGlobalError("数据接口暂时不可用，请确认服务已启动。");
  }
}

function bindTabs() {
  const buttons = Array.from(topTabsNode.querySelectorAll(".tab-btn"));
  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const tabId = button.dataset.tab || "overview";
      buttons.forEach((item) => item.classList.toggle("active", item === button));
      document.querySelectorAll(".tab-panel").forEach((panel) => {
        panel.classList.toggle("active", panel.id === `${tabId}Tab`);
      });
    });
  });
}

async function loadDateOptions() {
  const payload = await fetchJson("/api/dates");
  const publishedDates = payload.published_dates || [];
  const minDate = publishedDates.length ? publishedDates[publishedDates.length - 1] : "";
  const maxDate = publishedDates.length ? publishedDates[0] : "";

  if (minDate) {
    startDateInput.min = minDate;
    endDateInput.min = minDate;
  }
  if (maxDate) {
    startDateInput.max = maxDate;
    endDateInput.max = maxDate;
  }

  startDateInput.value = payload.default_start_date || minDate || todayString();
  endDateInput.value = payload.default_end_date || maxDate || startDateInput.value || todayString();
  normalizeDateRangeInputs();
}

async function refreshAll() {
  normalizeDateRangeInputs();
  const params = currentQuery();
  try {
    const [summary, status, competitor] = await Promise.all([
      fetchJson("/api/summary", params),
      fetchJson("/api/status", { start_date: params.start_date, end_date: params.end_date }),
      fetchJson("/api/competitor/videos", { start_date: params.start_date, end_date: params.end_date, limit: 20 }),
    ]);
    renderSummary(summary);
    renderStatus(status);
    renderCompetitorVideos(competitor);
  } catch (error) {
    console.error(error);
    renderGlobalError("加载失败，请刷新页面或检查接口。");
  }
}

async function refreshStatusOnly() {
  try {
    const params = currentQuery();
    const status = await fetchJson("/api/status", { start_date: params.start_date, end_date: params.end_date });
    renderStatus(status);
    setRuntimeResult("状态已刷新");
  } catch (error) {
    console.error(error);
    setRuntimeResult("刷新状态失败", true);
  }
}

function currentQuery() {
  return {
    start_date: startDateInput.value || todayString(),
    end_date: endDateInput.value || startDateInput.value || todayString(),
    scope: scopeSelect.value || "camera",
  };
}

function normalizeDateRangeInputs() {
  if (startDateInput.value && endDateInput.value && startDateInput.value > endDateInput.value) {
    endDateInput.value = startDateInput.value;
  }
}

function renderSummary(summary) {
  renderMetrics(summary);
  renderTrend(summary.trend || []);
  renderBars(categoryBarsNode, summary.categories || []);
  renderBars(sentimentBarsNode, summary.sentiments || []);
  renderBars(originBarsNode, summary.source_roles || []);
  renderBars(mediaBarsNode, summary.media_types || []);
  renderKeywords(keywordCloudNode, summary.top_keywords || []);
  renderKeywords(subTagCloudNode, summary.top_sub_tags || []);
  renderInsightList(positiveInsightsNode, (summary.sentiment_insights || {}).positive || []);
  renderInsightList(neutralInsightsNode, (summary.sentiment_insights || {}).neutral || []);
  renderInsightList(negativeInsightsNode, (summary.sentiment_insights || {}).negative || []);
  renderDetails(summary.latest_items || []);
}

function renderStatus(payload) {
  latestStatusPayload = payload;
  renderRuntimeCards(payload);
  renderTaskTabs((payload.tasks || {}).items || []);
}

function renderMetrics(summary) {
  const rangeText =
    summary.start_date && summary.end_date ? `${summary.start_date} ~ ${summary.end_date}` : summary.report_date || "-";
  const items = [
    ["时间范围", rangeText],
    ["当前数量", summary.total ?? 0],
    ["相机关联", summary.camera_related_total ?? 0],
    ["视频总数", summary.video_total ?? 0],
    ["视频待处理", summary.video_pending_total ?? 0],
    ["正面观点", summary.positive_eval_total ?? 0],
    ["负面观点", summary.negative_eval_total ?? 0],
    ["待跟进", summary.open_followups ?? 0],
  ];
  metricsNode.innerHTML = "";
  items.forEach(([label, value]) => {
    const fragment = metricTemplate.content.cloneNode(true);
    fragment.querySelector(".metric-label").textContent = label;
    fragment.querySelector(".metric-value").textContent = String(value);
    metricsNode.appendChild(fragment);
  });
}

function renderGlobalError(message) {
  metricsNode.innerHTML = "";
  const fragment = metricTemplate.content.cloneNode(true);
  fragment.querySelector(".metric-label").textContent = "状态";
  fragment.querySelector(".metric-value").textContent = message;
  metricsNode.appendChild(fragment);
}

function renderBars(container, data) {
  container.innerHTML = "";
  if (!data.length) {
    container.innerHTML = `<p class="empty">暂无数据</p>`;
    return;
  }
  const max = Math.max(...data.map((item) => Number(item.count) || 0), 1);
  data.slice(0, 8).forEach((item) => {
    const row = document.createElement("div");
    row.className = "bar-row";
    row.innerHTML = `
      <div class="bar-label">${escapeHtml(item.name || "-")}</div>
      <div class="bar-track"><div class="bar-fill" style="width:${((Number(item.count) || 0) / max) * 100}%"></div></div>
      <div class="bar-value">${Number(item.count) || 0}</div>
    `;
    container.appendChild(row);
  });
}

function renderTrend(items) {
  trendChartNode.innerHTML = "";
  if (!items.length) {
    trendChartNode.innerHTML = `<p class="empty">暂无趋势数据</p>`;
    return;
  }

  const width = 920;
  const height = 280;
  const padding = 28;
  const counts = items.map((item) => Number(item.total) || 0);
  const negatives = items.map((item) => Number(item.negative_total) || 0);
  const durations = items.map((item) => Number(item.duration_total_seconds) || 0);
  const maxValue = Math.max(...counts, ...negatives, 1);
  const maxDuration = Math.max(...durations, 1);
  const stepX = items.length > 1 ? (width - padding * 2) / (items.length - 1) : 0;

  const makeLine = (values, scaleMax, yBase) =>
    values
      .map((value, index) => {
        const x = padding + stepX * index;
        const ratio = scaleMax > 0 ? value / scaleMax : 0;
        const y = yBase - ratio * (height - padding * 2);
        return `${x},${y}`;
      })
      .join(" ");

  const totalPoints = makeLine(counts, maxValue, height - padding);
  const negativePoints = makeLine(negatives, maxValue, height - padding);
  const durationPoints = makeLine(durations, maxDuration, height - padding);

  const labels = items
    .map((item, index) => {
      const x = padding + stepX * index;
      return `<text x="${x}" y="${height - 6}" text-anchor="middle">${escapeHtml(String(item.report_date || "").slice(5))}</text>`;
    })
    .join("");

  trendChartNode.innerHTML = `
    <div class="chart-legend">
      <span><i class="legend-dot solid"></i>总反馈</span>
      <span><i class="legend-dot dashed"></i>负面观点</span>
      <span><i class="legend-dot thin"></i>总时长</span>
    </div>
    <svg viewBox="0 0 ${width} ${height}" class="chart-svg" role="img" aria-label="趋势图">
      <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" class="axis-line" />
      <polyline points="${totalPoints}" class="chart-line chart-line-main"></polyline>
      <polyline points="${negativePoints}" class="chart-line chart-line-dashed"></polyline>
      <polyline points="${durationPoints}" class="chart-line chart-line-thin"></polyline>
      ${labels}
    </svg>
    <div class="trend-table">
      ${items
        .map(
          (item) => `
        <div class="trend-row">
          <span>${escapeHtml(String(item.report_date || "").slice(5))}</span>
          <span>${Number(item.total) || 0} 条</span>
          <span>${formatDuration(Number(item.duration_total_seconds) || 0)}</span>
        </div>`,
        )
        .join("")}
    </div>
  `;
}

function renderKeywords(node, items) {
  node.innerHTML = "";
  if (!items.length) {
    node.innerHTML = `<p class="empty">暂无数据</p>`;
    return;
  }
  items.forEach((item) => {
    const chip = document.createElement("span");
    chip.className = "keyword-chip";
    chip.textContent = `${item.name} · ${item.count}`;
    node.appendChild(chip);
  });
}

function renderInsightList(node, items) {
  node.innerHTML = "";
  if (!items.length) {
    node.innerHTML = `<li class="empty">暂无观点</li>`;
    return;
  }
  items.forEach((item) => {
    const li = document.createElement("li");
    li.textContent = `${item.name} (${item.count})`;
    node.appendChild(li);
  });
}

function renderRuntimeCards(payload) {
  runtimeStatusNode.innerHTML = "";
  const cards = [
    {
      title: "AI 状态",
      text: payload.local_ai?.message || "未知",
      meta: [payload.local_ai?.model || "", payload.local_ai?.base_url || ""].filter(Boolean).join(" · "),
      state: payload.local_ai?.reachable ? "ok" : "warning",
    },
    {
      title: "视频处理",
      text: `已完成 ${payload.video_processing?.done || 0} / 待处理 ${payload.video_processing?.pending || 0}`,
      meta: `失败 ${payload.video_processing?.failed || 0} · 夜间任务 ${payload.video_processing?.nightly_enabled ? "开启" : "关闭"}`,
      state: Number(payload.video_processing?.pending || 0) > 0 ? "warning" : "ok",
    },
    {
      title: "工作台同步",
      text: payload.workspace_sync?.message || "未知",
      meta: `待同步 ${payload.workspace_sync?.pending_total || 0} · 当前范围 ${payload.workspace_sync?.pending_in_view || 0}`,
      state: Number(payload.workspace_sync?.pending_total || 0) > 0 ? "warning" : "ok",
    },
  ];

  cards.forEach((card) => {
    const article = document.createElement("article");
    article.className = `runtime-card state-${card.state}`;
    article.innerHTML = `
      <p class="runtime-card-title">${escapeHtml(card.title)}</p>
      <p class="runtime-card-text">${escapeHtml(card.text)}</p>
      <p class="runtime-card-meta">${escapeHtml(card.meta)}</p>
    `;
    runtimeStatusNode.appendChild(article);
  });
}

function renderTaskTabs(items) {
  taskTabsNode.innerHTML = "";
  if (!items.length) {
    taskPanelNode.innerHTML = `<p class="empty">暂无任务状态</p>`;
    return;
  }
  if (!items.some((item) => item.id === selectedTaskId)) {
    selectedTaskId = items[0].id;
  }

  items.forEach((item) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `subtab-btn ${item.id === selectedTaskId ? "active" : ""}`;
    button.textContent = item.title;
    button.addEventListener("click", () => {
      selectedTaskId = item.id;
      renderTaskTabs(items);
    });
    taskTabsNode.appendChild(button);
  });

  const current = items.find((item) => item.id === selectedTaskId) || items[0];
  taskPanelNode.innerHTML = `
    <article class="task-card state-${escapeHtml(current.state || "idle")}">
      <div class="task-card-head">
        <div>
          <p class="panel-kicker">Task</p>
          <h3>${escapeHtml(current.title || "-")}</h3>
        </div>
        <span class="state-pill state-${escapeHtml(current.state || "idle")}">${stateText(current.state)}</span>
      </div>
      <p class="task-summary">${escapeHtml(current.summary || "-")}</p>
      <ul class="task-meta-list">
        ${(current.meta || []).map((line) => `<li>${escapeHtml(line)}</li>`).join("")}
      </ul>
    </article>
  `;
}

function renderCompetitorVideos(payload) {
  renderBars(competitorBrandBarsNode, payload.brands || []);
  renderBars(competitorVideoTypeBarsNode, payload.video_types || []);
  renderKeywords(competitorFocusCloudNode, payload.focus_tags || []);

  competitorVideoListNode.innerHTML = "";
  const items = payload.items || [];
  if (!items.length) {
    competitorVideoListNode.innerHTML = `<p class="empty">当前时间范围内暂无竞品视频</p>`;
    return;
  }
  items.forEach((item) => {
    const article = document.createElement("article");
    article.className = "case-item";
    article.innerHTML = `
      <div class="case-topline">
        <span class="case-badge">${escapeHtml(item.brand || "竞品")}</span>
        <span class="case-badge subtle">${escapeHtml(item.video_type || "general")}</span>
      </div>
      <h4><a href="${escapeAttr(item.url || "#")}" target="_blank" rel="noreferrer">${escapeHtml(item.title || "-")}</a></h4>
      <p class="case-meta">${escapeHtml(
        [item.target, item.compare_to ? `vs ${item.compare_to}` : "", item.platform, formatDateTime(item.published_at)]
          .filter(Boolean)
          .join(" · "),
      )}</p>
      <p class="detail-summary">${escapeHtml(cleanDisplayText(item.summary || "") || "暂无摘要")}</p>
    `;
    competitorVideoListNode.appendChild(article);
  });
}

function renderDetails(items) {
  detailListNode.innerHTML = "";
  if (!items.length) {
    detailListNode.innerHTML = `<p class="empty">暂无反馈明细</p>`;
    return;
  }
  items.forEach((item) => {
    const article = document.createElement("article");
    article.className = "case-item";
    const detailLink =
      item.is_video || item.video_candidate
        ? `<a class="detail-link" href="/video.html?id=${Number(item.id) || 0}" target="_blank" rel="noreferrer">查看详情</a>`
        : "";
    article.innerHTML = `
      <div class="case-topline">
        <span class="case-badge">${escapeHtml(item.category || "未分类")}</span>
        <span class="case-badge subtle">${escapeHtml(sentimentText(item.sentiment))}</span>
        ${detailLink}
      </div>
      <h4><a href="${escapeAttr(item.url || "#")}" target="_blank" rel="noreferrer">${escapeHtml(item.title || "-")}</a></h4>
      <p class="case-meta">${escapeHtml(
        [item.source, item.source_actor_type, item.media_type, formatDateTime(item.published_at)].filter(Boolean).join(" · "),
      )}</p>
      <p class="detail-summary">${escapeHtml(cleanDisplayText(item.summary || "") || "暂无摘要")}</p>
    `;
    detailListNode.appendChild(article);
  });
}

async function runManualWorkspaceSync() {
  try {
    setRuntimeResult("正在同步工作台...");
    const payload = await postJson("/api/lark/sync", { date: currentQuery().end_date, limit: 200 });
    setRuntimeResult(`同步完成：成功 ${payload.synced || 0}，待同步 ${payload.pending_before || 0} -> ${payload.pending_after || 0}`);
    await refreshStatusOnly();
  } catch (error) {
    console.error(error);
    setRuntimeResult("同步工作台失败", true);
  }
}

async function exportVideoTasks() {
  try {
    setRuntimeResult("正在导出视频任务...");
    const payload = await postJson("/api/video/tasks/export", { date: currentQuery().end_date });
    setRuntimeResult(`任务已导出：${payload.path || "-"}`);
  } catch (error) {
    console.error(error);
    setRuntimeResult("导出视频任务失败", true);
  }
}

async function runManualVideoProcess() {
  try {
    setRuntimeResult("正在执行视频分析...");
    const payload = await postJson("/api/video/process", { date: currentQuery().end_date, limit: 5 });
    if (!payload.ok) {
      throw new Error(payload.error || "video_process_failed");
    }
    setRuntimeResult(
      `视频分析完成：处理 ${payload.processed || 0}，成功 ${payload.succeeded || 0}，失败 ${payload.failed || 0}，跳过重复 ${payload.skipped_duplicates || 0}`,
    );
    await refreshAll();
  } catch (error) {
    console.error(error);
    setRuntimeResult("视频分析失败", true);
  }
}

function setRuntimeResult(message, isError = false) {
  runtimeActionResultNode.textContent = message;
  runtimeActionResultNode.classList.toggle("error", Boolean(isError));
}

async function fetchJson(path, params = {}) {
  const response = await fetch(buildApiUrl(path, params));
  if (!response.ok) {
    throw new Error(`${path} failed: ${response.status}`);
  }
  return response.json();
}

async function postJson(path, payload = {}) {
  const response = await fetch(buildApiUrl(path), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || `${path} failed`);
  }
  return data;
}

function buildApiUrl(path, params = {}) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      query.set(key, String(value));
    }
  });
  const queryString = query.toString();
  return `${apiBaseUrl}${path}${queryString ? `?${queryString}` : ""}`;
}

function todayString() {
  return new Date().toISOString().slice(0, 10);
}

function cleanDisplayText(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function sentimentText(value) {
  const key = String(value || "").toLowerCase();
  if (key === "positive") return "正向";
  if (key === "negative") return "负向";
  return "中性";
}

function stateText(value) {
  const key = String(value || "").toLowerCase();
  if (key === "ok") return "正常";
  if (key === "warning") return "关注";
  if (key === "danger") return "异常";
  return "未运行";
}

function formatDateTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}-${String(date.getDate()).padStart(2, "0")} ${String(date.getHours()).padStart(2, "0")}:${String(date.getMinutes()).padStart(2, "0")}`;
}

function formatDuration(seconds) {
  const value = Math.max(0, Number(seconds) || 0);
  if (value >= 3600) {
    const hours = Math.floor(value / 3600);
    const minutes = Math.floor((value % 3600) / 60);
    return `${hours}小时${minutes}分`;
  }
  if (value >= 60) {
    const minutes = Math.floor(value / 60);
    const remain = value % 60;
    return `${minutes}分${remain}秒`;
  }
  return `${value}秒`;
}

function escapeHtml(value) {
  return String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttr(value) {
  return escapeHtml(value);
}
