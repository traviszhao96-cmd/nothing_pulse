const runtimeConfig = window.MEDIA_PULSE_CONFIG || window.NT_CAM_PULSE_CONFIG || {};
const apiBaseUrl = String(runtimeConfig.apiBaseUrl || "").replace(/\/+$/, "");

const headerNode = document.getElementById("videoDetailHeader");
const summaryNode = document.getElementById("videoDetailSummary");
const statusNode = document.getElementById("videoDetailStatus");
const pointsNode = document.getElementById("videoDetailPoints");
const contentNode = document.getElementById("videoDetailContent");

init();

async function init() {
  const id = getRowId();
  if (!id) {
    renderError("缺少参数 id");
    return;
  }
  await loadDetail(id);
}

function getRowId() {
  const params = new URLSearchParams(window.location.search);
  const raw = params.get("id") || "";
  const id = Number(raw);
  if (!Number.isFinite(id) || id <= 0) return null;
  return id;
}

async function loadDetail(id) {
  try {
    const response = await fetch(buildApiUrl("/api/video/item", { id }));
    if (!response.ok) {
      throw new Error(`load detail failed: ${response.status}`);
    }
    const payload = await response.json();
    renderDetail(payload);
  } catch (error) {
    console.error(error);
    renderError("加载视频详情失败，请稍后重试");
  }
}

function renderDetail(item) {
  headerNode.innerHTML = "";
  summaryNode.innerHTML = "";
  pointsNode.innerHTML = "";

  const title = document.createElement("h2");
  title.className = "video-title";
  title.textContent = item.title || "-";
  headerNode.appendChild(title);

  const meta = document.createElement("p");
  meta.className = "case-meta";
  meta.textContent = `${item.camera_category || "未分类"} · ${item.source_actor_type || "-"} · ${item.source || "-"} · ${formatDateTime(item.published_at)}`;
  headerNode.appendChild(meta);

  const links = document.createElement("p");
  links.className = "detail-video-actions";
  const origin = document.createElement("a");
  origin.href = item.url;
  origin.target = "_blank";
  origin.rel = "noreferrer";
  origin.textContent = "打开原视频链接";
  links.appendChild(origin);

  const sep = document.createElement("span");
  sep.textContent = " · ";
  links.appendChild(sep);

  const runBtn = document.createElement("button");
  runBtn.type = "button";
  runBtn.className = "inline-action-btn";
  runBtn.textContent = "立即重新分析";
  runBtn.addEventListener("click", async () => {
    await processSingleVideo(item.id);
  });
  links.appendChild(runBtn);
  headerNode.appendChild(links);

  const summary = document.createElement("p");
  summary.className = "detail-summary";
  summary.textContent = cleanDisplayText(item.summary || "") || "暂无摘要";
  summaryNode.appendChild(summary);

  const analysisMeta = document.createElement("p");
  analysisMeta.className = "detail-analysis-meta";
  const tags = (item.domain_subtags || []).map((tag) => cleanDisplayText(tag)).filter(Boolean).join(" / ");
  analysisMeta.textContent = [
    `情绪：${sentimentText(item.sentiment)}`,
    item.sentiment_reason ? `情绪依据：${cleanDisplayText(item.sentiment_reason)}` : "",
    tags ? `标签：${tags}` : "",
    item.source_actor_reason ? `来源依据：${cleanDisplayText(item.source_actor_reason)}` : "",
  ]
    .filter(Boolean)
    .join(" | ");
  summaryNode.appendChild(analysisMeta);

  appendPointGroup(pointsNode, "好评点", item.ai_positive_points || [], item.url || "");
  appendPointGroup(pointsNode, "中性点", item.ai_neutral_points || [], item.url || "");
  appendPointGroup(pointsNode, "差评点", item.ai_negative_points || [], item.url || "");
  if (!pointsNode.childNodes.length) {
    const empty = document.createElement("p");
    empty.className = "detail-ai-empty";
    empty.textContent = "暂无结构化观点";
    pointsNode.appendChild(empty);
  }

  const videoAnalysis = item.video_analysis || {};
  if (videoAnalysis.status === "ok") {
    statusNode.textContent = `视频分析完成${videoAnalysis.output_file ? ` · 输出文件: ${videoAnalysis.output_file}` : ""}`;
    statusNode.className = "runtime-result";
  } else if (videoAnalysis.status === "failed") {
    statusNode.textContent = `最近一次视频分析失败：${cleanDisplayText(videoAnalysis.error || "unknown_error")}`;
    statusNode.className = "runtime-result error";
  } else {
    statusNode.textContent = "尚未执行视频分析";
    statusNode.className = "runtime-result";
  }

  contentNode.textContent = cleanDisplayText(item.content || "").slice(0, 2200) || "暂无正文";
}

async function processSingleVideo(id) {
  try {
    statusNode.textContent = "正在触发视频分析...";
    statusNode.className = "runtime-result";
    const response = await fetch(buildApiUrl("/api/video/process"), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ id: Number(id), limit: 1, only_unprocessed: false }),
    });
    const payload = await response.json();
    if (!response.ok || !payload.ok) {
      throw new Error(payload.error || "video_process_failed");
    }
    statusNode.textContent = `分析完成：成功 ${payload.succeeded || 0}，失败 ${payload.failed || 0}，跳过重复 ${
      payload.skipped_duplicates || 0
    }`;
    statusNode.className = "runtime-result";
    await loadDetail(id);
  } catch (error) {
    console.error(error);
    statusNode.textContent = "分析失败，请检查 videosummary 运行环境";
    statusNode.className = "runtime-result error";
  }
}

function renderError(message) {
  headerNode.innerHTML = `<p class="empty">${message}</p>`;
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

function sentimentText(value) {
  if (value === "positive") return "正向";
  if (value === "negative") return "负向";
  return "中性";
}

function formatDateTime(raw) {
  if (!raw) return "-";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return raw;
  return `${date.getMonth() + 1}/${date.getDate()} ${String(date.getHours()).padStart(2, "0")}:${String(
    date.getMinutes(),
  ).padStart(2, "0")}`;
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
