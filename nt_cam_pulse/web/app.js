const startDateInput = document.getElementById("startDate");
const endDateInput = document.getElementById("endDate");
const scopeSelect = document.getElementById("scopeSelect");
const refreshButton = document.getElementById("refreshButton");
const metricsNode = document.getElementById("metrics");
const categoryBarsNode = document.getElementById("categoryBars");
const sentimentBarsNode = document.getElementById("sentimentBars");
const originBarsNode = document.getElementById("originBars");
const mediaBarsNode = document.getElementById("mediaBars");
const domainBarsNode = document.getElementById("domainBars");
const trendChartNode = document.getElementById("trendChart");
const keywordCloudNode = document.getElementById("keywordCloud");
const subTagCloudNode = document.getElementById("subTagCloud");
const competitorBrandBarsNode = document.getElementById("competitorBrandBars");
const competitorVideoTypeBarsNode = document.getElementById("competitorVideoTypeBars");
const competitorFocusCloudNode = document.getElementById("competitorFocusCloud");
const competitorVideoListNode = document.getElementById("competitorVideoList");
const caseListNode = document.getElementById("caseList");
const detailListNode = document.getElementById("detailList");
const positiveInsightsNode = document.getElementById("positiveInsights");
const neutralInsightsNode = document.getElementById("neutralInsights");
const negativeInsightsNode = document.getElementById("negativeInsights");
const metricTemplate = document.getElementById("metricTemplate");
const runtimeStatusNode = document.getElementById("runtimeStatus");
const refreshStatusButton = document.getElementById("refreshStatusButton");
const syncLarkButton = document.getElementById("syncLarkButton");
const exportVideoTasksButton = document.getElementById("exportVideoTasksButton");
const runVideoProcessButton = document.getElementById("runVideoProcessButton");
const runtimeActionResultNode = document.getElementById("runtimeActionResult");
const runtimeConfig = window.NT_CAM_PULSE_CONFIG || {};
const apiBaseUrl = String(runtimeConfig.apiBaseUrl || "").replace(/\/+$/, "");
let runtimeReportDate = "";
const PRODUCT_TAG_LABELS = {
  "4a pro": "4a pro",
  "4a": "4a",
  "3a pro": "3a pro",
  "3a": "3a",
  "phone3": "phone3",
  "2a": "2a",
  "phone2": "phone2",
  "cmf phone1": "CMF Phone 1",
  "nothing_os": "Nothing OS",
  "phone_3a_pro": "3a pro",
  "phone_3a": "3a",
  "phone_3": "phone3",
  "phone_2a": "2a",
  "phone_2": "phone2",
  "cmf_phone_1": "CMF Phone 1",
};

init();

async function init() {
  document.body.classList.add("js-animate");
  revealSections();
  refreshButton.addEventListener("click", refreshSummary);
  startDateInput.addEventListener("change", refreshSummary);
  endDateInput.addEventListener("change", refreshSummary);
  scopeSelect.addEventListener("change", refreshSummary);
  if (refreshStatusButton) {
    refreshStatusButton.addEventListener("click", async () => {
      await refreshRuntimeStatus(true);
    });
  }
  if (syncLarkButton) {
    syncLarkButton.addEventListener("click", runManualLarkSync);
  }
  if (exportVideoTasksButton) {
    exportVideoTasksButton.addEventListener("click", exportVideoTasks);
  }
  if (runVideoProcessButton) {
    runVideoProcessButton.addEventListener("click", runManualVideoProcess);
  }

  try {
    await loadDateOptions();
    await refreshSummary();
  } catch (error) {
    console.error(error);
    renderGlobalError("数据接口暂时不可用，请确认 dashboard 服务仍在运行。");
  }
}

async function loadDateOptions() {
  const response = await fetch(buildApiUrl("/api/dates"));
  if (!response.ok) {
    throw new Error(`load dates failed: ${response.status}`);
  }
  const payload = await response.json();
  const publishedDates = payload.published_dates || [];
  const minDate = publishedDates.length ? publishedDates[publishedDates.length - 1] : "";
  const maxDate = publishedDates.length ? publishedDates[0] : "";

  runtimeReportDate = payload.default_date || "";

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

async function refreshSummary() {
  try {
    normalizeDateRangeInputs();
    const selectedStartDate = startDateInput.value || todayString();
    const selectedEndDate = endDateInput.value || selectedStartDate;
    const scope = scopeSelect.value || "camera";
    const summaryResponse = await fetch(
      buildApiUrl("/api/summary", { start_date: selectedStartDate, end_date: selectedEndDate, scope }),
    );
    if (!summaryResponse.ok) {
      throw new Error(`load summary failed: ${summaryResponse.status}`);
    }
    const summary = await summaryResponse.json();

    renderMetrics(summary);
    renderBars(categoryBarsNode, summary.categories || [], ["#0b8f8a", "#31b6af"]);
    renderBars(sentimentBarsNode, summary.sentiments || [], ["#1f4b9a", "#4a7fd8"]);
    renderBars(originBarsNode, summary.source_roles || [], ["#8a4e16", "#c97a2b"]);
    renderBars(mediaBarsNode, summary.media_types || [], ["#176268", "#1ea4b0"]);
    renderBars(domainBarsNode, summary.domains || [], ["#325f29", "#4ba43a"]);
    renderKeywords(summary.top_keywords || []);
    renderKeywords(summary.top_sub_tags || [], subTagCloudNode);
    if (caseListNode) {
      renderCases(summary.cases || []);
    }
    renderDetails(summary.latest_items || []);
    renderInsights(summary.sentiment_insights || {});
    renderTrend(summary.trend || []);
    await refreshCompetitorVideos(selectedStartDate, selectedEndDate);
    await refreshRuntimeStatus(false);
  } catch (error) {
    console.error(error);
    renderGlobalError("加载失败，请检查接口返回或刷新页面。");
    renderBars(categoryBarsNode, [], ["#0b8f8a", "#31b6af"]);
    renderBars(sentimentBarsNode, [], ["#1f4b9a", "#4a7fd8"]);
    renderBars(originBarsNode, [], ["#8a4e16", "#c97a2b"]);
    renderBars(mediaBarsNode, [], ["#176268", "#1ea4b0"]);
    renderBars(domainBarsNode, [], ["#325f29", "#4ba43a"]);
    renderKeywords([]);
    renderKeywords([], subTagCloudNode);
    if (caseListNode) {
      renderCases([]);
    }
    renderDetails([]);
    renderInsights({});
    renderTrend([]);
    renderCompetitorVideos({ items: [], brands: [], video_types: [], focus_tags: [] });
    renderRuntimeStatusFallback("状态接口不可用");
  }
}

function renderMetrics(summary) {
  const rangeText =
    summary.start_date && summary.end_date ? `${summary.start_date} ~ ${summary.end_date}` : summary.report_date || "-";
  const items = [
    ["时间范围", rangeText],
    ["当前范围", summary.scope === "all" ? "全部" : "仅相机"],
    ["当前数量", summary.total ?? 0],
    ["总反馈", summary.total_all ?? 0],
    ["相机关联", summary.camera_related_total ?? 0],
    ["非相机", summary.non_camera_total ?? 0],
    ["视频条目", summary.video_total ?? 0],
    ["视频已分析", summary.video_done_total ?? 0],
    ["视频待处理", summary.video_pending_total ?? 0],
    ["正面评价量", summary.positive_eval_total ?? 0],
    ["中性评价量", summary.neutral_eval_total ?? 0],
    ["负面评价量", summary.negative_eval_total ?? 0],
    ["高风险", summary.high_risk ?? 0],
    ["中风险", summary.medium_risk ?? 0],
    ["待跟进", summary.open_followups ?? 0]
  ];

  metricsNode.innerHTML = "";
  items.forEach(([label, value]) => {
    const fragment = metricTemplate.content.cloneNode(true);
    const card = fragment.querySelector(".metric-card");
    const labelNode = fragment.querySelector(".metric-label");
    const valueNode = fragment.querySelector(".metric-value");
    labelNode.textContent = label;
    valueNode.textContent = value;
    if (label === "时间范围") {
      card.classList.add("metric-card-range");
      valueNode.classList.add("metric-value-range");
    }
    metricsNode.appendChild(fragment);
  });
}

function renderGlobalError(message) {
  metricsNode.innerHTML = `
    <article class="metric-card">
      <p class="metric-label">状态</p>
      <p class="metric-value" style="font-size:16px;line-height:1.5;">${message}</p>
    </article>
  `;
}

function renderBars(container, data, colors) {
  if (!container) return;
  container.innerHTML = "";
  if (!data.length) {
    container.innerHTML = `<p class="empty">暂无数据</p>`;
    return;
  }

  const max = Math.max(...data.map((item) => item.count), 1);
  data.slice(0, 8).forEach((item) => {
    const row = document.createElement("div");
    row.className = "bar-row";

    const label = document.createElement("div");
    label.textContent = item.name;

    const track = document.createElement("div");
    track.className = "bar-track";

    const fill = document.createElement("div");
    fill.className = "bar-fill";
    fill.style.width = `${(item.count / max) * 100}%`;
    fill.style.background = `linear-gradient(90deg, ${colors[0]}, ${colors[1]})`;

    track.appendChild(fill);

    const value = document.createElement("div");
    value.textContent = item.count;

    row.appendChild(label);
    row.appendChild(track);
    row.appendChild(value);
    container.appendChild(row);
  });
}

function renderKeywords(keywords, node = keywordCloudNode) {
  if (!node) return;
  node.innerHTML = "";
  if (!keywords.length) {
    node.innerHTML = `<p class="empty">暂无关键词</p>`;
    return;
  }

  keywords.forEach((item) => {
    const chip = document.createElement("span");
    chip.className = "keyword-chip";
    chip.textContent = `${item.name} · ${item.count}`;
    node.appendChild(chip);
  });
}

async function refreshCompetitorVideos(startDate, endDate) {
  if (!competitorVideoListNode) return;
  const response = await fetch(buildApiUrl("/api/competitor/videos", { start_date: startDate, end_date: endDate, limit: 20 }));
  if (!response.ok) {
    throw new Error(`load competitor videos failed: ${response.status}`);
  }
  const payload = await response.json();
  renderCompetitorVideos(payload);
}

function renderCompetitorVideos(payload) {
  if (!competitorVideoListNode) return;
  renderBars(competitorBrandBarsNode, payload.brands || [], ["#a34a1c", "#e39a3f"]);
  renderBars(competitorVideoTypeBarsNode, payload.video_types || [], ["#175d8d", "#3ab1d8"]);
  renderKeywords(payload.focus_tags || [], competitorFocusCloudNode);

  competitorVideoListNode.innerHTML = "";
  const items = payload.items || [];
  if (!items.length) {
    competitorVideoListNode.innerHTML = `<p class="empty">当前时间范围内暂无竞品视频</p>`;
    return;
  }

  items.forEach((item) => {
    const card = document.createElement("article");
    card.className = "case-item";

    const title = document.createElement("h4");
    const anchor = document.createElement("a");
    anchor.href = item.url;
    anchor.target = "_blank";
    anchor.rel = "noreferrer";
    anchor.textContent = item.title;
    title.appendChild(anchor);

    const meta = document.createElement("p");
    meta.className = "case-meta";

    [
      item.brand || "竞品",
      item.target || "",
      competitorVideoTypeText(item.video_type),
      item.platform || "",
      item.compare_to ? `vs ${item.compare_to}` : "",
      formatDateTime(item.published_at),
    ]
      .filter(Boolean)
      .forEach((part, index) => {
        if (index > 0) {
          meta.append(" · ");
        }
        meta.append(part);
      });

    const summary = document.createElement("p");
    summary.className = "detail-summary";
    summary.textContent = cleanDisplayText(item.summary || "") || "暂无摘要";

    const tags = document.createElement("p");
    tags.className = "detail-analysis-meta";
    const focusTags = (item.focus_tags || []).map((tag) => cleanDisplayText(tag)).filter(Boolean);
    const domainTags = (item.domain_subtags || []).map((tag) => cleanDisplayText(tag)).filter(Boolean);
    tags.textContent = [
      focusTags.length ? `焦点：${focusTags.join(" / ")}` : "",
      domainTags.length ? `归类：${domainTags.join(" / ")}` : "",
      item.author ? `作者：${cleanDisplayText(item.author)}` : "",
    ]
      .filter(Boolean)
      .join(" | ");

    card.appendChild(title);
    card.appendChild(meta);
    card.appendChild(summary);
    if (tags.textContent) {
      card.appendChild(tags);
    }
    competitorVideoListNode.appendChild(card);
  });
}

function renderCases(cases) {
  if (!caseListNode) {
    return;
  }
  caseListNode.innerHTML = "";
  if (!cases.length) {
    caseListNode.innerHTML = `<p class="empty">暂无重点案例</p>`;
    return;
  }

  cases.forEach((item) => {
    const card = document.createElement("article");
    card.className = "case-item";

    const title = document.createElement("h4");
    const anchor = document.createElement("a");
    anchor.href = item.url;
    anchor.target = "_blank";
    anchor.rel = "noreferrer";
    anchor.textContent = item.title;
    title.appendChild(anchor);

    const meta = document.createElement("p");
    meta.className = "case-meta";

    const tag = document.createElement("span");
    tag.className = `severity-tag severity-${item.severity || "low"}`;
    tag.textContent = severityText(item.severity);

    meta.appendChild(tag);
    meta.append(`${item.category} · ${item.source_actor_type || "-"} · ${item.source} · ${formatDateTime(item.published_at)}`);

    const summary = document.createElement("p");
    summary.textContent = cleanDisplayText(item.summary || "");

    card.appendChild(title);
    card.appendChild(meta);
    card.appendChild(summary);
    caseListNode.appendChild(card);
  });
}

function renderDetails(items) {
  detailListNode.innerHTML = "";
  if (!items.length) {
    detailListNode.innerHTML = `<p class="empty">暂无明细</p>`;
    return;
  }

  items.forEach((item) => {
    const card = document.createElement("article");
    card.className = "case-item";

    const title = document.createElement("h4");
    const anchor = document.createElement("a");
    anchor.href = item.url;
    anchor.target = "_blank";
    anchor.rel = "noreferrer";
    anchor.textContent = item.title;
    title.appendChild(anchor);

    const meta = document.createElement("p");
    meta.className = "case-meta";

    const tag = document.createElement("span");
    tag.className = `severity-tag severity-${item.severity || "low"}`;
    tag.textContent = severityText(item.severity);

    const related = document.createElement("span");
    related.className = `severity-tag ${item.camera_related ? "severity-low" : "severity-medium"}`;
    related.textContent = item.camera_related ? "相机关联" : "非相机";

    const mediaType = document.createElement("span");
    mediaType.className = "severity-tag severity-low";
    mediaType.textContent = item.media_type || "文章";

    const videoTag = document.createElement("span");
    const videoStatus = String(item.video_analysis_status || "").toLowerCase();
    const isTrackedVideo = Boolean(item.is_video || item.video_candidate);
    if (isTrackedVideo && videoStatus === "ok") {
      videoTag.className = "severity-tag severity-low";
      videoTag.textContent = "视频已分析";
    } else if (isTrackedVideo && videoStatus === "failed") {
      videoTag.className = "severity-tag severity-high";
      videoTag.textContent = "视频分析失败";
    } else if (isTrackedVideo && item.video_candidate) {
      videoTag.className = "severity-tag severity-medium";
      videoTag.textContent = "视频待补转写";
    } else if (isTrackedVideo) {
      videoTag.className = "severity-tag severity-low";
      videoTag.textContent = "视频候选";
    } else {
      videoTag.className = "severity-tag severity-low";
      videoTag.textContent = "非视频";
    }

    meta.appendChild(tag);
    meta.appendChild(related);
    meta.appendChild(mediaType);
    meta.appendChild(videoTag);
    const productTags = (item.product_tags || []).map((tag) => productTagText(tag)).filter(Boolean);
    productTags.slice(0, 3).forEach((tagText) => {
      const tagNode = document.createElement("span");
      tagNode.className = "severity-tag severity-low";
      tagNode.textContent = tagText;
      meta.appendChild(tagNode);
    });
    if (item.video_pinned) {
      const pinnedTag = document.createElement("span");
      pinnedTag.className = "severity-tag severity-medium";
      pinnedTag.textContent = "视频优先显示";
      meta.appendChild(pinnedTag);
    }
    meta.append(`${item.category} · ${item.source_actor_type || "-"} · ${item.source} · ${formatDateTime(item.published_at)}`);

    const summary = document.createElement("p");
    summary.className = "detail-summary";
    const summaryText = cleanDisplayText(item.summary || "");
    summary.textContent = summaryText || "暂无内容摘要（可先运行 backfill 重新抓取正文）";

    const analysisMeta = document.createElement("p");
    analysisMeta.className = "detail-analysis-meta";
    const subTags = (item.domain_subtags || []).map((tag) => cleanDisplayText(tag)).filter(Boolean).join(" / ");
    const analysisParts = [
      `情绪：${sentimentText(item.sentiment)}`,
      item.sentiment_reason ? `情绪依据：${cleanDisplayText(item.sentiment_reason)}` : "",
      subTags ? `标签：${subTags}` : "",
      productTags.length ? `机型：${productTags.join(" / ")}` : "",
      item.source_actor_reason ? `来源依据：${cleanDisplayText(item.source_actor_reason)}` : "",
    ].filter(Boolean);
    analysisMeta.textContent = analysisParts.join(" | ");

    const pointsWrap = document.createElement("div");
    pointsWrap.className = "detail-points-wrap";
    appendPointGroup(pointsWrap, "好评点", item.ai_positive_points || [], item.url || "");
    appendPointGroup(pointsWrap, "中性点", item.ai_neutral_points || [], item.url || "");
    appendPointGroup(pointsWrap, "差评点", item.ai_negative_points || [], item.url || "");

    if (!pointsWrap.childNodes.length) {
      const empty = document.createElement("p");
      empty.className = "detail-ai-empty";
      empty.textContent = "暂无结构化观点（本地 AI 未返回可解析结果时会出现）";
      pointsWrap.appendChild(empty);
    }

    card.appendChild(title);
    card.appendChild(meta);
    if (isTrackedVideo) {
      card.appendChild(buildVideoActions(item));
    }
    card.appendChild(summary);
    card.appendChild(analysisMeta);
    card.appendChild(pointsWrap);
    detailListNode.appendChild(card);
  });
}

function buildVideoActions(item) {
  const line = document.createElement("p");
  line.className = "detail-video-actions";

  const origin = document.createElement("a");
  origin.href = item.url;
  origin.target = "_blank";
  origin.rel = "noreferrer";
  origin.textContent = "打开原视频";
  line.appendChild(origin);

  if (item.id) {
    const divider = document.createElement("span");
    divider.textContent = " · ";
    line.appendChild(divider);

    const detail = document.createElement("a");
    detail.href = `/video.html?id=${encodeURIComponent(String(item.id))}`;
    detail.textContent = "进入视频详情页";
    line.appendChild(detail);

    const divider2 = document.createElement("span");
    divider2.textContent = " · ";
    line.appendChild(divider2);

    const runBtn = document.createElement("button");
    runBtn.type = "button";
    runBtn.className = "inline-action-btn";
    runBtn.textContent = "立即分析";
    runBtn.addEventListener("click", async () => {
      await runManualVideoProcess(item.id);
    });
    line.appendChild(runBtn);
  }

  return line;
}

function competitorVideoTypeText(value) {
  const key = String(value || "").toLowerCase();
  if (key === "comparison") return "对比";
  if (key === "camera_test") return "相机测试";
  if (key === "review") return "评测";
  if (key === "tips") return "教程";
  if (key === "general") return "泛内容";
  return key || "未分类";
}

function renderTrend(trend) {
  trendChartNode.innerHTML = "";
  if (!trend.length) {
    trendChartNode.innerHTML = `<p class="empty">暂无趋势数据</p>`;
    return;
  }

  const width = 960;
  const height = 280;
  const margin = { top: 16, right: 16, bottom: 48, left: 54 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const max = Math.max(...trend.map((item) => Math.max(Number(item.total || 0), Number(item.negative_total || 0))), 1);

  const xAt = (index) => margin.left + (index / Math.max(1, trend.length - 1)) * plotWidth;
  const yAt = (value) => margin.top + (1 - Number(value || 0) / max) * plotHeight;

  const totalPoints = trend.map((item, index) => ({
    x: xAt(index),
    y: yAt(item.total || 0),
    total: Number(item.total || 0),
    positive: Number(item.positive_total || 0),
    neutral: Number(item.neutral_total || 0),
    negative: Number(item.negative_total || 0),
    date: String(item.report_date || ""),
  }));
  const negativePoints = trend.map((item, index) => ({
    x: xAt(index),
    y: yAt(item.negative_total || 0),
    total: Number(item.total || 0),
    positive: Number(item.positive_total || 0),
    neutral: Number(item.neutral_total || 0),
    negative: Number(item.negative_total || 0),
    date: String(item.report_date || ""),
  }));

  const totalPathData = totalPoints
    .map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(2)},${point.y.toFixed(2)}`)
    .join(" ");
  const negativePathData = negativePoints
    .map((point, index) => `${index === 0 ? "M" : "L"}${point.x.toFixed(2)},${point.y.toFixed(2)}`)
    .join(" ");

  const areaPath = `${totalPathData} L ${totalPoints[totalPoints.length - 1].x},${height - margin.bottom} L ${
    totalPoints[0].x
  },${height - margin.bottom} Z`;

  const legend = document.createElement("div");
  legend.className = "trend-legend";
  legend.innerHTML = `
    <span class="trend-legend-item"><i class="trend-legend-dot" style="background:#0b8f8a"></i>总反馈量</span>
    <span class="trend-legend-item"><i class="trend-legend-dot" style="background:#b62d14"></i>负面评价量</span>
  `;
  trendChartNode.appendChild(legend);

  const tooltip = document.createElement("div");
  tooltip.className = "trend-tooltip";
  trendChartNode.appendChild(tooltip);

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);

  const yAxis = document.createElementNS("http://www.w3.org/2000/svg", "line");
  yAxis.setAttribute("x1", String(margin.left));
  yAxis.setAttribute("x2", String(margin.left));
  yAxis.setAttribute("y1", String(margin.top));
  yAxis.setAttribute("y2", String(height - margin.bottom));
  yAxis.setAttribute("stroke", "rgba(23,33,47,0.28)");
  yAxis.setAttribute("stroke-width", "1");

  const xAxis = document.createElementNS("http://www.w3.org/2000/svg", "line");
  xAxis.setAttribute("x1", String(margin.left));
  xAxis.setAttribute("x2", String(width - margin.right));
  xAxis.setAttribute("y1", String(height - margin.bottom));
  xAxis.setAttribute("y2", String(height - margin.bottom));
  xAxis.setAttribute("stroke", "rgba(23,33,47,0.28)");
  xAxis.setAttribute("stroke-width", "1");

  const yTickCount = Math.min(5, Math.max(2, max + 1));
  for (let i = 0; i < yTickCount; i += 1) {
    const ratio = i / Math.max(1, yTickCount - 1);
    const value = Math.round(max * (1 - ratio));
    const y = margin.top + ratio * plotHeight;

    const gridLine = document.createElementNS("http://www.w3.org/2000/svg", "line");
    gridLine.setAttribute("x1", String(margin.left));
    gridLine.setAttribute("x2", String(width - margin.right));
    gridLine.setAttribute("y1", String(y));
    gridLine.setAttribute("y2", String(y));
    gridLine.setAttribute("stroke", "rgba(23,33,47,0.1)");
    gridLine.setAttribute("stroke-width", "1");
    svg.appendChild(gridLine);

    const yLabel = document.createElementNS("http://www.w3.org/2000/svg", "text");
    yLabel.setAttribute("x", String(margin.left - 8));
    yLabel.setAttribute("y", String(y + 4));
    yLabel.setAttribute("text-anchor", "end");
    yLabel.setAttribute("fill", "#5f6b78");
    yLabel.setAttribute("font-size", "11");
    yLabel.textContent = String(value);
    svg.appendChild(yLabel);
  }

  const xTickIndices = buildTrendTickIndices(trend.length, 7);
  xTickIndices.forEach((index) => {
    const x = xAt(index);
    const tick = document.createElementNS("http://www.w3.org/2000/svg", "line");
    tick.setAttribute("x1", String(x));
    tick.setAttribute("x2", String(x));
    tick.setAttribute("y1", String(height - margin.bottom));
    tick.setAttribute("y2", String(height - margin.bottom + 5));
    tick.setAttribute("stroke", "rgba(23,33,47,0.28)");
    tick.setAttribute("stroke-width", "1");
    svg.appendChild(tick);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("x", String(x));
    label.setAttribute("y", String(height - margin.bottom + 18));
    label.setAttribute("text-anchor", "middle");
    label.setAttribute("fill", "#5f6b78");
    label.setAttribute("font-size", "11");
    label.textContent = formatTrendDate(trend[index].report_date);
    svg.appendChild(label);
  });

  const yTitle = document.createElementNS("http://www.w3.org/2000/svg", "text");
  yTitle.setAttribute("x", String(margin.left));
  yTitle.setAttribute("y", String(margin.top - 4));
  yTitle.setAttribute("fill", "#5f6b78");
  yTitle.setAttribute("font-size", "11");
  yTitle.textContent = "反馈数量";
  svg.appendChild(yTitle);

  const xTitle = document.createElementNS("http://www.w3.org/2000/svg", "text");
  xTitle.setAttribute("x", String(width - margin.right));
  xTitle.setAttribute("y", String(height - 8));
  xTitle.setAttribute("text-anchor", "end");
  xTitle.setAttribute("fill", "#5f6b78");
  xTitle.setAttribute("font-size", "11");
  xTitle.textContent = "日期";
  svg.appendChild(xTitle);

  const area = document.createElementNS("http://www.w3.org/2000/svg", "path");
  area.setAttribute("d", areaPath);
  area.setAttribute("fill", "rgba(11,143,138,0.14)");

  const totalLine = document.createElementNS("http://www.w3.org/2000/svg", "path");
  totalLine.setAttribute("d", totalPathData);
  totalLine.setAttribute("fill", "none");
  totalLine.setAttribute("stroke", "#0b8f8a");
  totalLine.setAttribute("stroke-width", "3");
  totalLine.setAttribute("stroke-linecap", "round");

  const negativeLine = document.createElementNS("http://www.w3.org/2000/svg", "path");
  negativeLine.setAttribute("d", negativePathData);
  negativeLine.setAttribute("fill", "none");
  negativeLine.setAttribute("stroke", "#b62d14");
  negativeLine.setAttribute("stroke-width", "2.5");
  negativeLine.setAttribute("stroke-linecap", "round");
  negativeLine.setAttribute("stroke-dasharray", "6 4");

  svg.appendChild(yAxis);
  svg.appendChild(xAxis);
  svg.appendChild(area);
  svg.appendChild(totalLine);
  svg.appendChild(negativeLine);

  totalPoints.forEach((point, index) => {
    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", String(point.x));
    circle.setAttribute("cy", String(point.y));
    circle.setAttribute("r", "4");
    circle.setAttribute("fill", "#0b8f8a");
    circle.setAttribute("opacity", "0");

    setTimeout(() => {
      circle.setAttribute("opacity", "1");
    }, 120 + index * 40);

    bindTrendTooltip(circle, point, tooltip);
    svg.appendChild(circle);
  });
  negativePoints.forEach((point, index) => {
    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", String(point.x));
    circle.setAttribute("cy", String(point.y));
    circle.setAttribute("r", "3.5");
    circle.setAttribute("fill", "#b62d14");
    circle.setAttribute("opacity", "0");

    setTimeout(() => {
      circle.setAttribute("opacity", "1");
    }, 120 + index * 40);

    bindTrendTooltip(circle, point, tooltip);
    svg.appendChild(circle);
  });

  trendChartNode.appendChild(svg);
}

function bindTrendTooltip(node, point, tooltip) {
  node.addEventListener("mousemove", (event) => {
    const rect = trendChartNode.getBoundingClientRect();
    const x = event.clientX - rect.left + 12;
    const y = event.clientY - rect.top - 12;
    tooltip.style.left = `${x}px`;
    tooltip.style.top = `${y}px`;
    tooltip.style.opacity = "1";
    tooltip.innerHTML = `
      <p class="trend-tooltip-title">${point.date}</p>
      <p>总反馈：${point.total}</p>
      <p>正面评价：${point.positive}</p>
      <p>中性评价：${point.neutral}</p>
      <p>负面评价：${point.negative}</p>
    `;
  });
  node.addEventListener("mouseleave", () => {
    tooltip.style.opacity = "0";
  });
}

function buildTrendTickIndices(length, maxTicks = 7) {
  if (length <= 1) return [0];
  const ticks = new Set([0, length - 1]);
  const middleCount = Math.max(0, maxTicks - 2);
  for (let i = 1; i <= middleCount; i += 1) {
    const index = Math.round((i / (middleCount + 1)) * (length - 1));
    ticks.add(index);
  }
  return Array.from(ticks).sort((a, b) => a - b);
}

function formatTrendDate(raw) {
  const value = String(raw || "");
  if (value.length < 10) return value;
  const month = value.slice(5, 7);
  const day = value.slice(8, 10);
  return `${month}/${day}`;
}

function revealSections() {
  document.querySelectorAll(".reveal").forEach((node, index) => {
    setTimeout(() => {
      node.classList.add("visible");
    }, 80 + index * 70);
  });
}

function severityText(value) {
  if (value === "high") return "高风险";
  if (value === "medium") return "中风险";
  return "低风险";
}

function sentimentText(value) {
  if (value === "positive") return "正向";
  if (value === "negative") return "负向";
  return "中性";
}

function productTagText(raw) {
  const value = String(raw || "").trim();
  if (!value) return "";
  if (PRODUCT_TAG_LABELS[value]) return PRODUCT_TAG_LABELS[value];
  return cleanDisplayText(value.replace(/_/g, " "));
}

function formatDateTime(raw) {
  if (!raw) return "-";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return raw;
  return `${date.getMonth() + 1}/${date.getDate()} ${String(date.getHours()).padStart(2, "0")}:${String(
    date.getMinutes(),
  ).padStart(2, "0")}`;
}

function todayString() {
  return new Date().toISOString().slice(0, 10);
}

function normalizeDateRangeInputs() {
  if (!startDateInput || !endDateInput) return;
  const startValue = startDateInput.value || todayString();
  const endValue = endDateInput.value || startValue;
  if (startValue > endValue) {
    startDateInput.value = endValue;
    endDateInput.value = startValue;
  } else {
    startDateInput.value = startValue;
    endDateInput.value = endValue;
  }
}

function renderInsights(insights) {
  renderInsightList(positiveInsightsNode, insights.positive || []);
  renderInsightList(neutralInsightsNode, insights.neutral || []);
  renderInsightList(negativeInsightsNode, insights.negative || []);
}

function renderInsightList(node, values) {
  node.innerHTML = "";
  if (!values.length) {
    const li = document.createElement("li");
    li.textContent = "暂无";
    node.appendChild(li);
    return;
  }
  values.forEach((item) => {
    const li = document.createElement("li");
    li.textContent = `${cleanDisplayText(item.name)} (${item.count})`;
    node.appendChild(li);
  });
}

async function refreshRuntimeStatus(showMessage = false) {
  if (!runtimeStatusNode) return;
  try {
    const statusDate = runtimeReportDate || endDateInput.value || todayString();
    const response = await fetch(buildApiUrl("/api/status", { date: statusDate }));
    if (!response.ok) {
      throw new Error(`load status failed: ${response.status}`);
    }
    const payload = await response.json();
    renderRuntimeStatus(payload);
    if (showMessage) {
      setRuntimeResult("状态已刷新", false);
    }
  } catch (error) {
    console.error(error);
    renderRuntimeStatusFallback("状态获取失败");
    if (showMessage) {
      setRuntimeResult("状态刷新失败", true);
    }
  }
}

function renderRuntimeStatus(payload) {
  if (!runtimeStatusNode) return;
  const localAi = payload.local_ai || {};
  const video = payload.video_processing || {};
  const lark = payload.lark_sync || {};

  runtimeStatusNode.innerHTML = "";

  const localAiCard = document.createElement("div");
  localAiCard.className = "runtime-card";
  const localAiTitle = document.createElement("h4");
  localAiTitle.textContent = "AI 接口";
  const localAiText = document.createElement("p");
  localAiText.textContent = `${localAi.message || "未知"}${localAi.model ? ` · 模型: ${localAi.model}` : ""}`;
  localAiCard.appendChild(localAiTitle);
  localAiCard.appendChild(localAiText);
  if (localAi.last_error) {
    const err = document.createElement("p");
    err.className = "runtime-error";
    err.textContent = cleanDisplayText(localAi.last_error);
    localAiCard.appendChild(err);
  }

  const videoCard = document.createElement("div");
  videoCard.className = "runtime-card";
  const videoTitle = document.createElement("h4");
  videoTitle.textContent = "视频处理";
  const videoText = document.createElement("p");
  const scheduleText = video.nightly_enabled
    ? `夜间自动: ${video.nightly_time || "--:--"} ${video.nightly_timezone || ""}`
    : "夜间自动: 未开启";
  videoText.textContent = `待处理 ${video.pending ?? 0} / 已完成 ${video.done ?? 0} / 失败 ${video.failed ?? 0} · ${scheduleText}`;
  videoCard.appendChild(videoTitle);
  videoCard.appendChild(videoText);

  const larkCard = document.createElement("div");
  larkCard.className = "runtime-card";
  const larkTitle = document.createElement("h4");
  larkTitle.textContent = "Lark 同步";
  const larkText = document.createElement("p");
  larkText.textContent = `${lark.message || "未知"} · 待同步 ${lark.pending_total ?? 0} · 当前范围待同步 ${
    lark.pending_in_view ?? 0
  }`;
  larkCard.appendChild(larkTitle);
  larkCard.appendChild(larkText);
  if (lark.last_error) {
    const err = document.createElement("p");
    err.className = "runtime-error";
    err.textContent = cleanDisplayText(lark.last_error);
    larkCard.appendChild(err);
  }

  runtimeStatusNode.appendChild(localAiCard);
  runtimeStatusNode.appendChild(videoCard);
  runtimeStatusNode.appendChild(larkCard);
}

function renderRuntimeStatusFallback(message) {
  if (!runtimeStatusNode) return;
  runtimeStatusNode.innerHTML = `<p class="empty">${message}</p>`;
}

async function exportVideoTasks() {
  if (!exportVideoTasksButton) return;
  try {
    exportVideoTasksButton.disabled = true;
    setRuntimeResult("正在导出视频任务...", false);
    const selectedDate = runtimeReportDate || endDateInput.value || todayString();
    const response = await fetch(buildApiUrl("/api/video/tasks/export"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ date: selectedDate }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || "export_failed");
    }
    setRuntimeResult(`已导出任务文件：${payload.path}`, false);
    await refreshRuntimeStatus(false);
  } catch (error) {
    console.error(error);
    setRuntimeResult("导出失败，请查看后端日志", true);
  } finally {
    exportVideoTasksButton.disabled = false;
  }
}

async function runManualVideoProcess(rowId = null) {
  if (!runVideoProcessButton && !rowId) return;
  const globalButton = runVideoProcessButton;
  try {
    if (globalButton) globalButton.disabled = true;
    setRuntimeResult(rowId ? `正在分析视频 ID=${rowId} ...` : "正在分析待处理视频...", false);
    const selectedDate = runtimeReportDate || endDateInput.value || todayString();
    const body = rowId
      ? { id: Number(rowId), limit: 1, only_unprocessed: false }
      : { date: selectedDate, limit: 5, only_unprocessed: true };
    const response = await fetch(buildApiUrl("/api/video/process"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || "video_process_failed");
    }
    setRuntimeResult(
      `视频处理完成：处理 ${payload.processed || 0} 条，成功 ${payload.succeeded || 0}，失败 ${payload.failed || 0}，跳过重复 ${
        payload.skipped_duplicates || 0
      } 条`,
      false,
    );
    await refreshSummary();
  } catch (error) {
    console.error(error);
    setRuntimeResult("视频处理失败，请检查 videosummary 环境", true);
  } finally {
    if (globalButton) globalButton.disabled = false;
  }
}

async function runManualLarkSync() {
  if (!syncLarkButton) return;
  try {
    syncLarkButton.disabled = true;
    setRuntimeResult("正在同步 Lark...", false);
    const selectedDate = runtimeReportDate || endDateInput.value || todayString();
    const response = await fetch(buildApiUrl("/api/lark/sync"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ date: selectedDate, limit: 200 }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || "lark_sync_failed");
    }
    setRuntimeResult(
      `Lark 同步完成：成功 ${payload.synced || 0}，待同步 ${payload.pending_before || 0} -> ${payload.pending_after || 0}`,
      false,
    );
    await refreshRuntimeStatus(false);
  } catch (error) {
    console.error(error);
    setRuntimeResult("Lark 同步失败，请检查飞书配置或网络", true);
  } finally {
    syncLarkButton.disabled = false;
  }
}

function setRuntimeResult(message, isError) {
  if (!runtimeActionResultNode) return;
  runtimeActionResultNode.textContent = message;
  runtimeActionResultNode.className = isError ? "runtime-result error" : "runtime-result";
}

function appendPointGroup(container, label, points, videoUrl = "") {
  const cleanPoints = (points || []).map((point) => cleanDisplayText(point)).filter(Boolean);
  if (!cleanPoints.length) return;

  const section = document.createElement("section");
  section.className = "detail-point-group";

  const title = document.createElement("h5");
  title.textContent = label;
  section.appendChild(title);

  const list = document.createElement("ul");
  list.className = "detail-point-list";
  cleanPoints.forEach((point) => {
    const li = document.createElement("li");
    const parsed = parsePointWithTimestamp(point);
    if (parsed.seconds !== null) {
      const tsLink = document.createElement("a");
      tsLink.className = "point-timestamp-link";
      tsLink.href = buildTimestampedUrl(videoUrl, parsed.seconds);
      tsLink.target = "_blank";
      tsLink.rel = "noreferrer";
      tsLink.textContent = `[${parsed.label}]`;
      li.appendChild(tsLink);
      if (parsed.text) {
        li.append(` ${parsed.text}`);
      }
    } else {
      li.textContent = point;
    }
    list.appendChild(li);
  });
  section.appendChild(list);
  container.appendChild(section);
}

function parsePointWithTimestamp(raw) {
  const text = cleanDisplayText(raw || "");
  if (!text) return { seconds: null, label: "", text: "" };
  const match = text.match(/^\s*(?:\[|\()?\s*((?:\d{1,2}:)?\d{1,2}:\d{2})\s*(?:\]|\))?\s*[-:：]?\s*(.*)$/);
  if (!match) return { seconds: null, label: "", text };
  const seconds = parseTimestampToSeconds(match[1]);
  if (seconds === null) return { seconds: null, label: "", text };
  const pointText = cleanDisplayText(match[2] || "");
  return { seconds, label: formatSecondsLabel(seconds), text: pointText };
}

function parseTimestampToSeconds(raw) {
  const token = String(raw || "").trim();
  if (!token) return null;
  const parts = token.split(":").map((item) => Number(item));
  if (parts.some((item) => !Number.isFinite(item))) return null;
  if (parts.length === 2) {
    return parts[0] * 60 + parts[1];
  }
  if (parts.length === 3) {
    return parts[0] * 3600 + parts[1] * 60 + parts[2];
  }
  return null;
}

function formatSecondsLabel(totalSeconds) {
  const value = Math.max(0, Number(totalSeconds) || 0);
  const hour = Math.floor(value / 3600);
  const minute = Math.floor((value % 3600) / 60);
  const second = Math.floor(value % 60);
  if (hour > 0) {
    return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}`;
  }
  return `${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}`;
}

function buildTimestampedUrl(videoUrl, seconds) {
  const fallback = videoUrl || "#";
  try {
    const absolute = new URL(fallback, window.location.origin);
    const host = absolute.hostname.toLowerCase();
    if (host.includes("youtube.com") || host.includes("youtu.be")) {
      absolute.searchParams.set("t", String(Math.max(0, Number(seconds) || 0)));
      return absolute.toString();
    }
    if (host.includes("bilibili.com") || host.includes("b23.tv")) {
      absolute.searchParams.set("t", String(Math.max(0, Number(seconds) || 0)));
      return absolute.toString();
    }
    absolute.hash = `t=${Math.max(0, Number(seconds) || 0)}`;
    return absolute.toString();
  } catch (_error) {
    return fallback;
  }
}

function cleanDisplayText(raw) {
  if (!raw) return "";
  let text = String(raw);
  text = text.replace(/<\s*\/?\s*a\b[^>]*?/gi, " ");
  text = text.replace(/<a\b[^>]*>/gi, " ");
  text = text.replace(/<\/a>/gi, " ");
  text = text.replace(/\b(?:href|ref)\s*=\s*"[^"]*"/gi, " ");
  text = text.replace(/\b(?:href|ref)\s*=\s*'[^']*'/gi, " ");
  text = text.replace(/\b(?:href|ref)\s*=\s*\S+/gi, " ");
  text = text.replace(/https?:\/\/\S+/gi, " ");
  text = text.replace(/[<>]/g, " ");
  text = text.replace(/\s+/g, " ").trim();
  return text;
}

function buildApiUrl(path, query = null) {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  const url = `${apiBaseUrl}${normalizedPath}`;
  if (!query) return url;
  const params = new URLSearchParams();
  Object.entries(query).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") {
      params.set(key, String(value));
    }
  });
  const qs = params.toString();
  return qs ? `${url}?${qs}` : url;
}
