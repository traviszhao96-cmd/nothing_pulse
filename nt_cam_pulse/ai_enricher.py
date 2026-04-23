from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

from .config import LocalAIConfig
from .models import FeedbackItem
from .source_profile import SOURCE_LABELS
from .utils import detect_language, normalize_text, truncate

DEFAULT_PROMPT = """你是手机相机用户反馈分析助手。请基于输入内容输出严格 JSON（不要 Markdown，不要额外解释）。

要求：
1) 从文本里提取真实评价点，不要杜撰。
2) 情感 sentiment 只能是 positive / neutral / negative。
3) source_role 只能是 real_user / official_kol / core_koc / unknown。
4) primary_tag 一级标签只能从以下枚举里选：
   ID / OS / Camera / Charge / Signal / Screen / Battery / PurchaseExperience / Others
5) secondary_tags 为二级标签数组，最多 8 个，优先使用固定标签，不要自造同义词。
6) Camera 领域二级标签只能优先从以下标签中选择：
   整体相机, 后置主摄拍照, 后置广角拍照, 后置长焦拍照, 后置人像模式,
   后置主摄视频, 后置广角视频, 后置长焦视频,
   前置拍照, 前置视频, 前置人像模式,
   专业模式, 50M/高清模式, 拍照预览, 动态照片, 菜单模式,
   三方app-效果, 相册, 软件, 不明确。
   如果信息不足但能确认是相机体验，优先用“整体相机”；只有完全无法判断时才用“不明确”。
7) severity 只能是 high / medium / low：
   - high: 严重体验/功能 bug（无法使用、严重效果异常、崩溃、关键能力失效）
   - medium: 功能缺失、明确痛点、频繁影响体验
   - low: 一般建议、轻微优化项
   若为负面观点且措辞强烈，可上调一级；若偏建议型，可下调。
8) positives / neutrals / negatives 分别提取 0-6 条关键观点，优先保留讨论度高、信息量高的内容。
9) 如果是视频链接或文本不足以判断细节，请把 needs_video_transcript 设为 true。
10) 必须输出 points 数组：每个观点都要给出完整标签（sentiment / primary_tag / secondary_tags / severity）。
11) points[].text 不要只写抽象标签，必须写成 1-2 句完整观点，尽量包含具体场景、对象、问题表现或体验影响；若是好评，要写清楚在什么使用/拍摄场景下表现好；若是差评，要写清楚用户在什么场景遇到了什么问题、造成了什么影响。
12) points[].original_text 必须是原文摘录且保持原语言，禁止翻译。拿不到就返回空字符串。
13) 若输入内容主要是英文，points[].original_text 应优先给英文原句，尽量直接引用输入文本中的片段。
14) positives / neutrals / negatives 里的每一条，也要复用这种“完整观点”写法，不要只返回“续航不错”“视频一般”这类短语。
15) 如果输入里包含 video_analysis_summary / video_analysis_positives / video_analysis_negatives，说明这些是视频内容里已经提炼出的观察点；请综合这些信息再输出观点，不要忽略。

返回 JSON schema:
{
  "summary": "string",
  "sentiment": "positive|neutral|negative",
  "sentiment_reason": "string",
  "primary_tag": "ID|OS|Camera|Charge|Signal|Screen|Battery|PurchaseExperience|Others",
  "secondary_tags": ["string"],
  "severity": "high|medium|low",
  "severity_reason": "string",
  "points": [
    {
      "text": "观点内容（建议中文）",
      "original_text": "原文片段（可选）",
      "sentiment": "positive|neutral|negative",
      "primary_tag": "ID|OS|Camera|Charge|Signal|Screen|Battery|PurchaseExperience|Others",
      "secondary_tags": ["string"],
      "severity": "high|medium|low",
      "severity_reason": "string",
      "timestamp_label": "mm:ss（可选）",
      "timestamp_seconds": 0
    }
  ],
  "positives": ["string"],
  "neutrals": ["string"],
  "negatives": ["string"],
  "source_role": "real_user|official_kol|core_koc|unknown",
  "source_role_reason": "string",
  "needs_video_transcript": true
}

输入内容：
{feedback_text}
"""


@dataclass(slots=True)
class EnrichResult:
    ok: bool
    error: str | None = None


def extract_json_object(raw_text: str) -> dict[str, Any]:
    text = raw_text.strip()
    if text.startswith("```"):
        text = text.strip("`")
        text = text.replace("json", "", 1).strip()

    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError(f"json object not found in response: {raw_text[:120]}")
    obj = text[start : end + 1]
    data = json.loads(obj)
    if not isinstance(data, dict):
        raise ValueError("json root is not object")
    return data


def apply_structured_analysis(item: FeedbackItem, data: dict[str, Any]) -> None:
    summary = normalize_text(str(data.get("summary", "")).strip())
    sentiment = str(data.get("sentiment", "")).strip().lower()
    primary_tag = _normalize_primary_tag(data.get("primary_tag", data.get("domain", "")))
    source_role = str(data.get("source_role", "")).strip().lower()
    severity = _normalize_severity(data.get("severity", ""))
    severity_reason = normalize_text(str(data.get("severity_reason", "")).strip())

    if summary:
        item.summary = truncate(summary, 280)
    if sentiment in {"positive", "neutral", "negative"}:
        item.sentiment = sentiment
    if primary_tag:
        item.camera_category = primary_tag[:40]
        item.domain_tag = primary_tag[:40]
    if severity:
        item.severity = severity

    item.sentiment_reason = truncate(normalize_text(str(data.get("sentiment_reason", ""))), 240)
    if severity_reason:
        item.extra["severity_reason"] = truncate(severity_reason, 240)

    structured_points = _normalize_structured_points(
        data.get("points"),
        source_language=item.language,
        source_content_language=detect_language(item.content or ""),
    )
    if structured_points:
        item.extra["ai_structured_points"] = structured_points
        if not primary_tag:
            item.camera_category = _majority_primary_tag(structured_points)
            item.domain_tag = item.camera_category
        if not severity:
            item.severity = _max_severity(structured_points)
        derived_tags = _collect_secondary_tags(structured_points, limit=8)
        if derived_tags:
            item.domain_subtags = derived_tags
        item.ai_positive_points = _extract_point_texts(structured_points, "positive", limit=6)
        item.ai_neutral_points = _extract_point_texts(structured_points, "neutral", limit=6)
        item.ai_negative_points = _extract_point_texts(structured_points, "negative", limit=6)
    else:
        # Avoid carrying stale structured points from previous runs.
        item.extra.pop("ai_structured_points", None)
        sub_tags = normalize_secondary_tags_for_primary(
            primary_tag=primary_tag,
            raw_tags=data.get("secondary_tags", data.get("sub_tags")),
            text=item.summary or item.content or item.title,
            limit=8,
        )
        if sub_tags:
            item.domain_subtags = sub_tags

        item.ai_positive_points = LocalAIEnricher._string_list(data.get("positives"), limit=6)
        item.ai_neutral_points = LocalAIEnricher._string_list(data.get("neutrals"), limit=6)
        item.ai_negative_points = LocalAIEnricher._string_list(data.get("negatives"), limit=6)

    if source_role in SOURCE_LABELS:
        item.source_actor_type = source_role
        item.source_actor_reason = truncate(
            normalize_text(str(data.get("source_role_reason", "")).strip()),
            200,
        )

    needs_video_transcript = bool(data.get("needs_video_transcript"))
    if needs_video_transcript:
        item.video_candidate = True


PRIMARY_TAG_ALIASES = {
    "id": "ID",
    "identity": "ID",
    "unlock": "ID",
    "biometric": "ID",
    "os": "OS",
    "system": "OS",
    "camera": "Camera",
    "imaging": "Camera",
    "charge": "Charge",
    "charging": "Charge",
    "signal": "Signal",
    "network": "Signal",
    "screen": "Screen",
    "display": "Screen",
    "battery": "Battery",
    "purchaseexperience": "PurchaseExperience",
    "purchase": "PurchaseExperience",
    "shopping": "PurchaseExperience",
    "buying": "PurchaseExperience",
    "other": "Others",
    "others": "Others",
    "其它": "Others",
    "其他": "Others",
    "身份": "ID",
    "解锁": "ID",
    "系统": "OS",
    "相机": "Camera",
    "拍照": "Camera",
    "充电": "Charge",
    "信号": "Signal",
    "网络": "Signal",
    "屏幕": "Screen",
    "电池": "Battery",
    "购买体验": "PurchaseExperience",
}

CAMERA_CANONICAL_SECONDARY_TAGS = {
    "整体相机",
    "后置主摄拍照",
    "后置广角拍照",
    "后置长焦拍照",
    "后置人像模式",
    "后置主摄视频",
    "后置广角视频",
    "后置长焦视频",
    "前置拍照",
    "前置视频",
    "前置人像模式",
    "专业模式",
    "50M/高清模式",
    "拍照预览",
    "动态照片",
    "菜单模式",
    "三方app-效果",
    "相册",
    "软件",
    "不明确",
}

CAMERA_SECONDARY_TAG_ALIASES = {
    "telephotosharpness": "后置长焦拍照",
    "telephoto": "后置长焦拍照",
    "periscope": "后置长焦拍照",
    "zoom": "后置长焦拍照",
    "photoexposure": "后置主摄拍照",
    "photocolor": "后置主摄拍照",
    "photohdr": "后置主摄拍照",
    "nightphotography": "后置主摄拍照",
    "photosharpness": "后置主摄拍照",
    "photoquality": "后置主摄拍照",
    "photodetail": "后置主摄拍照",
    "photoprocessing": "软件",
    "videoclarity": "后置主摄视频",
    "videospecs": "后置主摄视频",
    "videocolor": "后置主摄视频",
    "videoexposure": "后置主摄视频",
    "videostabilization": "后置主摄视频",
    "usability": "软件",
    "preset": "菜单模式",
    "hdr处理": "后置主摄拍照",
    "色彩表现": "后置主摄拍照",
    "对焦速度": "后置主摄拍照",
    "快门延迟": "后置主摄拍照",
    "噪点": "后置主摄拍照",
    "防抖": "后置主摄视频",
    "低光表现": "后置主摄拍照",
    "人像虚化": "后置人像模式",
    "视频稳定": "后置主摄视频",
    "算法调校": "软件",
}


def _normalize_primary_tag(raw: Any) -> str:
    text = normalize_text(str(raw or ""))
    if not text:
        return ""
    key = text.replace(" ", "").replace("-", "").replace("_", "").lower()
    return PRIMARY_TAG_ALIASES.get(key, text if text in {"ID", "OS", "Camera", "Charge", "Signal", "Screen", "Battery", "PurchaseExperience", "Others"} else "Others")


def normalize_secondary_tags_for_primary(primary_tag: Any, raw_tags: Any, text: str = "", limit: int = 8) -> list[str]:
    values = raw_tags if isinstance(raw_tags, list) else []
    normalized = [normalize_text(str(value)) for value in values if normalize_text(str(value))]
    primary = _normalize_primary_tag(primary_tag)
    if primary != "Camera":
        seen: set[str] = set()
        result: list[str] = []
        primary_key = normalize_text(str(primary_tag or "")).lower()
        for tag in normalized:
            key = tag.lower()
            if key == primary_key or key in seen:
                continue
            seen.add(key)
            result.append(tag)
            if len(result) >= limit:
                break
        return result

    combined_text = normalize_text(" ".join(normalized + [text])).lower()
    result: list[str] = []

    def add(tag: str) -> None:
        if tag not in CAMERA_CANONICAL_SECONDARY_TAGS:
            return
        if tag in result:
            return
        result.append(tag)

    for raw in normalized:
        alias_key = raw.replace(" ", "").replace("-", "").replace("_", "").lower()
        alias = CAMERA_SECONDARY_TAG_ALIASES.get(alias_key)
        if alias:
            add(alias)

    if any(token in combined_text for token in ("professional mode", "pro mode", "专业模式", "手动模式", "manual mode", "expert mode")):
        add("专业模式")
    if any(token in combined_text for token in ("50mp", "50 mp", "50m", "高清模式", "high-res", "highres", "full resolution")):
        add("50M/高清模式")
    if any(token in combined_text for token in ("preview", "viewfinder", "预览", "取景")):
        add("拍照预览")
    if any(token in combined_text for token in ("motion photo", "live photo", "动态照片", "实况照片")):
        add("动态照片")
    if any(token in combined_text for token in ("menu", "菜单", "mode switch", "模式切换", "preset", "滤镜预设")):
        add("菜单模式")
    if any(
        token in combined_text
        for token in ("third-party", "third party", "三方app", "第三方app", "instagram", "tiktok", "whatsapp", "snapchat", "gcam")
    ):
        add("三方app-效果")
    if any(token in combined_text for token in ("gallery", "album", "相册")):
        add("相册")
    if any(
        token in combined_text
        for token in ("software", "app", "ui", "algorithm", "processing", "bug", "crash", "闪退", "卡顿", "设置", "功能入口")
    ):
        add("软件")

    is_video = any(token in combined_text for token in ("video", "录像", "录影", "拍视频", "录制"))
    is_front = any(token in combined_text for token in ("front", "selfie", "前置", "自拍"))
    is_portrait = any(token in combined_text for token in ("portrait", "人像", "虚化", "bokeh"))
    is_ultrawide = any(token in combined_text for token in ("ultrawide", "ultra wide", "wide angle", "广角", "超广角"))
    is_tele = any(token in combined_text for token in ("telephoto", "periscope", "zoom", "长焦", "潜望"))

    if is_portrait:
        add("前置人像模式" if is_front else "后置人像模式")
    elif is_video:
        if is_front:
            add("前置视频")
        elif is_ultrawide:
            add("后置广角视频")
        elif is_tele:
            add("后置长焦视频")
        else:
            add("后置主摄视频")
    else:
        if is_front:
            add("前置拍照")
        elif is_ultrawide:
            add("后置广角拍照")
        elif is_tele:
            add("后置长焦拍照")
        elif any(token in combined_text for token in ("photo", "拍照", "照片", "exposure", "hdr", "color", "night", "对焦", "focus", "sharp", "noise", "噪点")):
            add("后置主摄拍照")

    if not result and any(token in combined_text for token in ("camera", "相机", "拍照", "录像", "video", "photo", "镜头")):
        add("整体相机")
    if not result:
        add("不明确")
    return result[:limit]


def _normalize_severity(raw: Any) -> str:
    text = normalize_text(str(raw or "")).lower()
    if not text:
        return ""
    if text in {"high", "严重", "高"}:
        return "high"
    if text in {"medium", "中", "中等"}:
        return "medium"
    if text in {"low", "低", "轻微"}:
        return "low"
    return ""


def _normalize_sentiment(raw: Any) -> str:
    text = normalize_text(str(raw or "")).lower()
    if text in {"positive", "neutral", "negative"}:
        return text
    return "neutral"


def _normalize_structured_points(
    raw: Any,
    source_language: str | None = None,
    source_content_language: str | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []
    result: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        text = normalize_text(str(item.get("text", "")))
        if not text:
            continue
        primary_tag = _normalize_primary_tag(item.get("primary_tag", ""))
        point: dict[str, Any] = {
            "text": truncate(text, 220),
            "original_text": truncate(_clean_optional_text(item.get("original_text", "")), 500),
            "sentiment": _normalize_sentiment(item.get("sentiment", "")),
            "primary_tag": primary_tag,
            "secondary_tags": normalize_secondary_tags_for_primary(
                primary_tag=primary_tag,
                raw_tags=item.get("secondary_tags"),
                text=text,
                limit=8,
            ),
            "severity": _normalize_severity(item.get("severity", "")) or "low",
            "severity_reason": truncate(normalize_text(str(item.get("severity_reason", ""))), 200),
        }
        if _should_drop_original_text(
            source_language=source_language,
            source_content_language=source_content_language,
            text=point["text"],
            original_text=point["original_text"],
        ):
            point["original_text"] = ""
        if not point["primary_tag"]:
            point["primary_tag"] = "Others"
        ts_label = _clean_optional_text(item.get("timestamp_label", ""))
        ts_seconds_raw = item.get("timestamp_seconds")
        ts_seconds: int | None = None
        try:
            if ts_seconds_raw is not None and str(ts_seconds_raw).strip() != "":
                ts_seconds = max(0, int(float(ts_seconds_raw)))
        except (TypeError, ValueError):
            ts_seconds = None
        if ts_label:
            point["timestamp_label"] = ts_label
        if ts_seconds is not None:
            point["timestamp_seconds"] = ts_seconds
        point["secondary_tags"] = normalize_secondary_tags_for_primary(
            primary_tag=point["primary_tag"],
            raw_tags=point["secondary_tags"],
            text=point["text"],
            limit=8,
        )
        result.append(point)
        if len(result) >= 40:
            break
    return result


def _clean_optional_text(raw: Any) -> str:
    text = normalize_text(str(raw or ""))
    if text.lower() in {"none", "null", "n/a", "na", "-"}:
        return ""
    return text


def _should_drop_original_text(
    source_language: str | None,
    source_content_language: str | None,
    text: str,
    original_text: str,
) -> bool:
    original = normalize_text(original_text)
    if not original:
        return False
    source_lang = normalize_text(source_language or "").lower()
    source_content_lang = normalize_text(source_content_language or "").lower()
    text_lang = detect_language(text)
    original_lang = detect_language(original)
    original_cjk_ratio = _cjk_ratio(original)
    original_latin_words = _latin_word_count(original)

    # If source is non-Chinese (or mixed) but original_text is Chinese,
    # this is usually an unintended translation and should be discarded.
    if source_lang in {"en", "mixed"} and original_lang == "zh":
        return True

    # If core source content is clearly English, original text should not be CJK-dominant.
    if source_content_lang == "en" and original_cjk_ratio >= 0.15:
        return True

    # For mixed-language sources, avoid CJK-dominant "original_text" that only keeps a token like "Pro".
    if source_lang in {"en", "mixed"} and source_content_lang in {"en", "mixed"}:
        if original_cjk_ratio >= 0.45 and original_latin_words < 2:
            return True

    # When source is clearly English, original snippet should keep English signal.
    if source_lang == "en" and original_lang not in {"en", "mixed"}:
        return True

    # Exact duplicate of translated text is low-value and often wrong as "原文".
    if normalize_text(text).lower() == original.lower() and text_lang == "zh" and source_lang in {"en", "mixed"}:
        return True

    return False


def _cjk_ratio(text: str) -> float:
    value = normalize_text(text)
    if not value:
        return 0.0
    cjk_count = len(re.findall(r"[\u4e00-\u9fff]", value))
    alpha_num_count = len(re.findall(r"[A-Za-z0-9\u4e00-\u9fff]", value))
    if alpha_num_count <= 0:
        return 0.0
    return cjk_count / alpha_num_count


def _latin_word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z]{3,}", normalize_text(text)))


def _extract_point_texts(points: list[dict[str, Any]], sentiment: str, limit: int = 6) -> list[str]:
    output: list[str] = []
    for point in points:
        if str(point.get("sentiment", "")).strip().lower() != sentiment:
            continue
        text = normalize_text(str(point.get("text", "")))
        if not text:
            continue
        output.append(truncate(text, 180))
        if len(output) >= limit:
            break
    return output


def _collect_secondary_tags(points: list[dict[str, Any]], limit: int = 8) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for point in points:
        for tag in point.get("secondary_tags", []) or []:
            clean = normalize_text(str(tag))
            if not clean:
                continue
            key = clean.lower()
            if key in seen:
                continue
            seen.add(key)
            output.append(clean)
            if len(output) >= limit:
                return output
    return output


def _majority_primary_tag(points: list[dict[str, Any]]) -> str:
    counts: dict[str, int] = {}
    for point in points:
        tag = _normalize_primary_tag(point.get("primary_tag", ""))
        if not tag:
            continue
        counts[tag] = counts.get(tag, 0) + 1
    if not counts:
        return "Others"
    return max(counts.items(), key=lambda item: item[1])[0]


def _max_severity(points: list[dict[str, Any]]) -> str:
    order = {"high": 3, "medium": 2, "low": 1}
    best = "low"
    best_rank = 1
    for point in points:
        sev = _normalize_severity(point.get("severity", "")) or "low"
        rank = order.get(sev, 1)
        if rank > best_rank:
            best = sev
            best_rank = rank
    return best


class LocalAIEnricher:
    def __init__(self, config: LocalAIConfig) -> None:
        self.config = config
        self.prompt_template = self._load_prompt(config.prompt_path)
        self._runtime_disabled = False

    def is_enabled(self) -> bool:
        return bool(self.config.enabled and self.config.base_url and self.config.model)

    def enrich(self, item: FeedbackItem) -> EnrichResult:
        if not self.is_enabled():
            return EnrichResult(ok=False, error="local_ai_disabled")
        if self._runtime_disabled:
            return EnrichResult(ok=False, error="local_ai_disabled")

        payload_text = self._build_payload_text(item)
        prompt = self.prompt_template.replace("{feedback_text}", payload_text)
        request_body = {
            "model": self.config.model,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "你是结构化信息提取助手。"
                        "你必须严格输出 JSON，且字段必须可被程序解析。"
                    ),
                },
                {"role": "user", "content": prompt},
            ],
        }

        tries = max(1, self.config.retries + 1)
        last_error = "unknown_error"
        saw_transport_error = False
        saw_non_transport_error = False
        for _ in range(tries):
            try:
                output_text = self._chat_completion(request_body)
                data = self._extract_json(output_text)
                self._apply(item, data)
                return EnrichResult(ok=True)
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                if isinstance(exc, requests.RequestException):
                    saw_transport_error = True
                else:
                    saw_non_transport_error = True
                continue
        if saw_transport_error and not saw_non_transport_error:
            self._runtime_disabled = True
            return EnrichResult(ok=False, error=f"local_ai_unreachable: {last_error}")
        return EnrichResult(ok=False, error=f"local_ai_failed: {last_error}")

    def _chat_completion(self, body: dict[str, Any]) -> str:
        base_url = self.config.base_url.rstrip("/")
        url = f"{base_url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"

        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=max(10, self.config.timeout_seconds),
        )
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError(f"empty choices in local ai response: {data}")
        content = (choices[0].get("message") or {}).get("content")
        if not content:
            raise RuntimeError("empty content in local ai response")
        return str(content)

    def _apply(self, item: FeedbackItem, data: dict[str, Any]) -> None:
        apply_structured_analysis(item, data)

    @staticmethod
    def _extract_json(raw_text: str) -> dict[str, Any]:
        return extract_json_object(raw_text)

    @staticmethod
    def _string_list(value: Any, limit: int = 6) -> list[str]:
        if not isinstance(value, list):
            return []
        results: list[str] = []
        for item in value:
            text = normalize_text(str(item))
            if not text:
                continue
            results.append(truncate(text, 180))
            if len(results) >= limit:
                break
        return results

    @staticmethod
    def _build_payload_text(item: FeedbackItem) -> str:
        content_lang = detect_language(item.content or "")
        video_context = _video_analysis_context(item.extra)
        lines = [
            f"source={item.source}",
            f"source_section={item.source_section or ''}",
            f"author={item.author or ''}",
            f"url={item.url}",
            f"title={item.title}",
            f"detected_language={item.language or ''}",
            f"content_language={content_lang}",
            f"summary={item.summary or ''}",
            "content=" + truncate(item.content or "", 3000),
        ]
        if video_context:
            lines.extend(video_context)
        return "\n".join(lines)

    @staticmethod
    def _load_prompt(path: str) -> str:
        if path:
            prompt_path = Path(path).expanduser().resolve()
            if prompt_path.exists():
                text = prompt_path.read_text(encoding="utf-8").strip()
                if text:
                    text = text.replace("{transcript_text}", "{feedback_text}")
                    if "{feedback_text}" not in text:
                        text += "\n\n{feedback_text}"
                    return text
        return DEFAULT_PROMPT


def _video_analysis_context(extra: Any) -> list[str]:
    if not isinstance(extra, dict):
        return []
    video_analysis = extra.get("video_analysis")
    if not isinstance(video_analysis, dict):
        return []
    output_file = str(video_analysis.get("output_file", "")).strip()
    if not output_file:
        return []

    payload = _load_video_analysis_payload(output_file)
    if not payload:
        return []

    lines: list[str] = []
    summary = normalize_text(str(payload.get("summary", "")))
    positives = [normalize_text(str(value)) for value in list(payload.get("positives") or []) if normalize_text(str(value))]
    negatives = [normalize_text(str(value)) for value in list(payload.get("negatives") or []) if normalize_text(str(value))]
    if summary:
        lines.append("video_analysis_summary=" + truncate(summary, 500))
    if positives:
        lines.append("video_analysis_positives=" + " | ".join(truncate(value, 220) for value in positives[:6]))
    if negatives:
        lines.append("video_analysis_negatives=" + " | ".join(truncate(value, 220) for value in negatives[:6]))
    return lines


def _load_video_analysis_payload(path_text: str) -> dict[str, Any]:
    path = str(path_text or "").strip()
    if not path:
        return {}
    file_path = Path(path).expanduser()
    if not file_path.exists():
        return {}
    try:
        raw_text = file_path.read_text(encoding="utf-8")
        payload = extract_json_object(raw_text)
    except (OSError, json.JSONDecodeError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        "summary": payload.get("summary", ""),
        "positives": list(payload.get("positives") or []),
        "negatives": list(payload.get("negatives") or []),
    }
