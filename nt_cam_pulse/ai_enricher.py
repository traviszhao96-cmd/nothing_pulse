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
5) secondary_tags 为二级标签数组，最多 8 个。优先英文短语，允许必要时中文专业词。
6) Camera 领域二级标签优先使用：
   TelephotoSharpness, PhotoExposure, PhotoColor, PhotoHDR, NightPhotography,
   VideoClarity, VideoSpecs, VideoColor, VideoExposure, Usability, Preset。
7) severity 只能是 high / medium / low：
   - high: 严重体验/功能 bug（无法使用、严重效果异常、崩溃、关键能力失效）
   - medium: 功能缺失、明确痛点、频繁影响体验
   - low: 一般建议、轻微优化项
   若为负面观点且措辞强烈，可上调一级；若偏建议型，可下调。
8) positives / neutrals / negatives 分别提取 0-4 条关键观点。
9) 如果是视频链接或文本不足以判断细节，请把 needs_video_transcript 设为 true。
10) 必须输出 points 数组：每个观点都要给出完整标签（sentiment / primary_tag / secondary_tags / severity）。
11) points[].text 建议中文总结；points[].original_text 必须是原文摘录且保持原语言，禁止翻译。拿不到就返回空字符串。
12) 若输入内容主要是英文，points[].original_text 应优先给英文原句，尽量直接引用输入文本中的片段。

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
        sub_tags = LocalAIEnricher._string_list(data.get("secondary_tags", data.get("sub_tags")), limit=8)
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


def _normalize_primary_tag(raw: Any) -> str:
    text = normalize_text(str(raw or ""))
    if not text:
        return ""
    key = text.replace(" ", "").replace("-", "").replace("_", "").lower()
    return PRIMARY_TAG_ALIASES.get(key, text if text in {"ID", "OS", "Camera", "Charge", "Signal", "Screen", "Battery", "PurchaseExperience", "Others"} else "Others")


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
        point: dict[str, Any] = {
            "text": truncate(text, 220),
            "original_text": truncate(_clean_optional_text(item.get("original_text", "")), 500),
            "sentiment": _normalize_sentiment(item.get("sentiment", "")),
            "primary_tag": _normalize_primary_tag(item.get("primary_tag", "")),
            "secondary_tags": LocalAIEnricher._string_list(item.get("secondary_tags"), limit=8),
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
        # secondary_tags 不要重复 primary_tag
        primary_key = str(point["primary_tag"]).strip().lower()
        secondary_clean: list[str] = []
        seen: set[str] = set()
        for tag in point["secondary_tags"]:
            clean = normalize_text(str(tag))
            if not clean:
                continue
            key = clean.lower()
            if key == primary_key or key in seen:
                continue
            seen.add(key)
            secondary_clean.append(clean)
        point["secondary_tags"] = secondary_clean[:8]
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
        output.append(truncate(text, 80))
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
        for _ in range(tries):
            try:
                output_text = self._chat_completion(request_body)
                data = self._extract_json(output_text)
                self._apply(item, data)
                return EnrichResult(ok=True)
            except Exception as exc:  # noqa: BLE001
                last_error = str(exc)
                continue
        self._runtime_disabled = True
        return EnrichResult(ok=False, error=f"local_ai_unreachable: {last_error}")

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
            results.append(text[:80])
            if len(results) >= limit:
                break
        return results

    @staticmethod
    def _build_payload_text(item: FeedbackItem) -> str:
        content_lang = detect_language(item.content or "")
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
