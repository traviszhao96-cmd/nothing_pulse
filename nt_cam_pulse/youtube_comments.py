from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

from .config import AppConfig
from .utils import clean_content_text, normalize_text, truncate

_TAG_ENUM = {"ID", "OS", "Camera", "Charge", "Signal", "Screen", "Battery", "PurchaseExperience", "Others"}
_SEVERITY_ORDER = {"high": 3, "medium": 2, "low": 1}
_SENTIMENT_ORDER = {"negative": 3, "neutral": 2, "positive": 1}

_NOISE_SHORT_TOKENS = {
    "lol",
    "lmao",
    "first",
    "wow",
    "ok",
    "nice",
    "cool",
    "bro",
    "gg",
    "w",
}

_PRODUCT_HINTS = (
    "nothing",
    "cmf",
    "phone",
    "np",
    "3a",
    "4a",
)

_DOMAIN_TERMS = {
    "camera",
    "photo",
    "video",
    "lens",
    "zoom",
    "hdr",
    "night",
    "portrait",
    "battery",
    "charging",
    "charge",
    "screen",
    "display",
    "signal",
    "network",
    "os",
    "update",
    "bug",
    "issue",
    "problem",
    "lag",
    "stutter",
    "overheat",
    "heating",
    "focus",
    "autofocus",
    "ois",
    "eis",
}

_OPINION_TERMS = {
    "good",
    "great",
    "bad",
    "better",
    "worse",
    "hate",
    "love",
    "like",
    "dislike",
    "smooth",
    "slow",
    "broken",
    "fixed",
    "improve",
    "improved",
    "annoying",
    "terrible",
    "awful",
    "amazing",
    "垃圾",
    "好",
    "差",
    "卡",
    "喜欢",
    "失望",
    "建议",
}

_OWNED_PATTERNS = (
    r"\bi\s+(?:have|own|bought|purchased|got)\b",
    r"\bi(?:'m| am)\s+using\b",
    r"\bi(?:'ve| have)\s+been\s+using\b",
    r"\bmy\s+(?:nothing\s+)?phone\b",
    r"\busing\s+it\s+for\s+\d+\s*(?:day|days|week|weeks|month|months)\b",
    r"(已买|买了|我在用|用了\d+天|用了\d+周|入手了)",
)

_CONSIDERING_PATTERNS = (
    r"\bthinking\s+of\s+(?:buying|get(?:ting)?)\b",
    r"\bconsider(?:ing)?\b",
    r"\bplan(?:ning)?\s+to\s+buy\b",
    r"\bwant\s+to\s+buy\b",
    r"\bworth\s+buying\b",
    r"\bshould\s+i\s+buy\b",
    r"(想买|考虑入手|观望|值不值得买|要不要买)",
)

_NEGATIVE_TERMS = {
    "bad",
    "worse",
    "issue",
    "problem",
    "bug",
    "lag",
    "stutter",
    "overheat",
    "heating",
    "broken",
    "annoying",
    "terrible",
    "awful",
    "refund",
    "crash",
    "can't",
    "cannot",
    "hate",
    "垃圾",
    "卡顿",
    "发热",
    "问题",
}

_POSITIVE_TERMS = {
    "good",
    "great",
    "better",
    "love",
    "like",
    "smooth",
    "amazing",
    "awesome",
    "improved",
    "fixed",
    "清晰",
    "稳定",
    "喜欢",
    "不错",
}

_MODEL_PATTERNS: list[tuple[re.Pattern[str], str, bool]] = [
    (re.compile(r"\b(?:np\s*)?4a\s*pro\b|\b4apro\b|\bphone\s*\(?4a\s*pro\)?\b", re.I), "phone_4a_pro", False),
    (re.compile(r"\b(?:np\s*)?3a\s*pro\b|\b3apro\b|\bphone\s*\(?3a\s*pro\)?\b", re.I), "phone_3a_pro", False),
    (re.compile(r"\b(?:np\s*)?4a\b|\bphone\s*\(?4a\)?\b", re.I), "phone_4a", True),
    (re.compile(r"\b(?:np\s*)?3a\b|\bphone\s*\(?3a\)?\b", re.I), "phone_3a", True),
    (re.compile(r"\b(?:np\s*)?3\b|\bphone\s*\(?3\)?\b", re.I), "phone_3", True),
]

_MODEL_ALIAS_MAP: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bnp\s*4a\s*pro\b|\b4apro\b", re.I), "phone 4a pro"),
    (re.compile(r"\bnp\s*3a\s*pro\b|\b3apro\b", re.I), "phone 3a pro"),
    (re.compile(r"\bnp\s*4a\b", re.I), "phone 4a"),
    (re.compile(r"\bnp\s*3a\b", re.I), "phone 3a"),
    (re.compile(r"\bnp\s*3\b", re.I), "phone 3"),
    (re.compile(r"\buwa?\b", re.I), "ultra wide"),
    (re.compile(r"\baf\b", re.I), "autofocus"),
]

_CAMERA_SUBTAG_HINTS: list[tuple[tuple[str, ...], str]] = [
    (("telephoto", "periscope", "zoom"), "TelephotoSharpness"),
    (("exposure", "overexposed", "underexposed"), "PhotoExposure"),
    (("color", "colour", "white balance", "wb"), "PhotoColor"),
    (("hdr",), "PhotoHDR"),
    (("night", "low light"), "NightPhotography"),
    (("video", "4k", "60fps", "fps"), "VideoSpecs"),
    (("stabilization", "ois", "eis", "shake"), "Usability"),
    (("focus", "autofocus", "af"), "Usability"),
]


@dataclass(slots=True)
class _CommentRow:
    comment_id: str
    text: str
    author: str
    like_count: int
    timestamp: int | None
    sort: str
    is_pinned: bool


@dataclass(slots=True)
class _RuleRow:
    row: _CommentRow
    is_valid: bool
    priority: int
    purchase_stage: str
    sentiment: str
    primary_tag: str
    secondary_tags: list[str]
    product_tags: list[str]
    severity: str
    score: float
    reason: str


class YouTubeCommentMiner:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        vp = config.video_processing
        youtube_cfg = dict(config.sources.get("youtube_yt_dlp", {}))
        self.executable = str(youtube_cfg.get("executable", "yt-dlp")).strip() or "yt-dlp"
        self.enabled = bool(vp.comment_mining_enabled)
        self.newest_limit = max(20, min(3000, int(vp.comment_newest_limit)))
        self.top_limit = max(20, min(1000, int(vp.comment_top_limit)))
        self.timeout_seconds = max(20, int(vp.comment_timeout_seconds))
        self.ai_batch_size = max(5, min(40, int(vp.comment_ai_batch_size)))
        self.ai_max_candidates = max(20, min(500, int(vp.comment_ai_max_candidates)))
        self.ai_max_p3 = max(0, min(300, int(vp.comment_ai_max_p3)))
        self.ai_max_p2_negative = max(0, min(300, int(vp.comment_ai_max_p2_negative)))
        self.max_points = max(5, min(80, int(vp.comment_max_points)))

    def is_enabled_for_url(self, url: str) -> bool:
        if not self.enabled:
            return False
        value = str(url or "").strip().lower()
        return "youtube.com" in value or "youtu.be" in value

    def analyze_video(self, url: str, context_text: str = "") -> dict[str, Any]:
        if not self.is_enabled_for_url(url):
            return {
                "ok": False,
                "error": "youtube_comment_mining_disabled",
                "points": [],
                "meta": {"status": "disabled"},
            }

        context_norm = self._normalize_text(context_text)
        context_models = self._detect_models(context_norm, context_norm, allow_contextless=True)[0]
        context_has_product = self._has_product_context(context_norm)
        try:
            newest_rows = self._fetch_comments(url, comment_sort="new", limit=self.newest_limit)
            top_rows = self._fetch_comments(url, comment_sort="top", limit=self.top_limit)
        except Exception as exc:  # noqa: BLE001
            return {
                "ok": False,
                "error": f"youtube_comment_fetch_failed: {exc}",
                "points": [],
                "meta": {"status": "failed"},
            }

        merged_rows = self._merge_rows(newest_rows, top_rows)
        rule_rows = [
            self._rule_classify(
                row=row,
                context_norm=context_norm,
                context_has_product=context_has_product,
                context_models=context_models,
            )
            for row in merged_rows
        ]
        selected_rows = self._select_ai_candidates(rule_rows)
        points, ai_batches_ok, ai_batches_failed = self._build_points(selected_rows)

        meta = {
            "status": "ok",
            "fetched_newest": len(newest_rows),
            "fetched_top": len(top_rows),
            "merged_total": len(merged_rows),
            "valid_total": sum(1 for row in rule_rows if row.is_valid),
            "priority_1": sum(1 for row in rule_rows if row.priority == 1),
            "priority_2": sum(1 for row in rule_rows if row.priority == 2),
            "priority_3": sum(1 for row in rule_rows if row.priority == 3),
            "priority_4": sum(1 for row in rule_rows if row.priority == 4),
            "selected_for_ai": len(selected_rows),
            "ai_batches_ok": ai_batches_ok,
            "ai_batches_failed": ai_batches_failed,
            "points": len(points),
        }
        return {"ok": True, "error": "", "points": points, "meta": meta}

    def _fetch_comments(self, url: str, comment_sort: str, limit: int) -> list[_CommentRow]:
        command = [
            self.executable,
            "--skip-download",
            "--dump-single-json",
            "--write-comments",
            "--no-warnings",
            "--ignore-no-formats-error",
            "--extractor-args",
            f"youtube:comment_sort={comment_sort};max_comments={max(1, limit)}",
            url,
        ]
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=self.timeout_seconds,
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(f"yt-dlp_not_found: {self.executable}") from exc
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"yt_dlp_run_failed: {exc}") from exc

        if completed.returncode != 0 and not clean_content_text(completed.stdout):
            error_text = clean_content_text(completed.stderr)
            raise RuntimeError(error_text[:240] or "unknown_error")

        payload = json.loads(completed.stdout or "{}")
        raw_comments = payload.get("comments") if isinstance(payload, dict) else []
        if not isinstance(raw_comments, list):
            return []

        rows: list[_CommentRow] = []
        for raw in raw_comments:
            if not isinstance(raw, dict):
                continue
            comment_id = clean_content_text(str(raw.get("id", "")))
            text = clean_content_text(str(raw.get("text", "")))
            if not comment_id or not text:
                continue
            parent = clean_content_text(str(raw.get("parent", ""))).lower()
            if parent and parent != "root":
                # Focus on top-level comments for signal stability.
                continue
            like_count = self._safe_int(raw.get("like_count"), default=0)
            timestamp = self._safe_int(raw.get("timestamp"), default=-1)
            rows.append(
                _CommentRow(
                    comment_id=comment_id,
                    text=text,
                    author=clean_content_text(str(raw.get("author", ""))),
                    like_count=max(0, like_count),
                    timestamp=(timestamp if timestamp >= 0 else None),
                    sort=("newest" if comment_sort == "new" else "top"),
                    is_pinned=bool(raw.get("is_pinned")),
                )
            )
        return rows

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            if value is None:
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    def _merge_rows(self, newest_rows: list[_CommentRow], top_rows: list[_CommentRow]) -> list[_CommentRow]:
        merged: dict[str, _CommentRow] = {}
        for row in newest_rows + top_rows:
            existing = merged.get(row.comment_id)
            if existing is None:
                merged[row.comment_id] = row
                continue
            merged_sort = set(existing.sort.split("+"))
            merged_sort.update(row.sort.split("+"))
            merged[row.comment_id] = _CommentRow(
                comment_id=row.comment_id,
                text=(row.text if len(row.text) > len(existing.text) else existing.text),
                author=(row.author or existing.author),
                like_count=max(existing.like_count, row.like_count),
                timestamp=(existing.timestamp if existing.timestamp and (not row.timestamp or existing.timestamp >= row.timestamp) else row.timestamp),
                sort="+".join(sorted(value for value in merged_sort if value)),
                is_pinned=bool(existing.is_pinned or row.is_pinned),
            )
        return sorted(
            merged.values(),
            key=lambda row: (
                self._is_newest_sort(row.sort),
                row.timestamp or 0,
                row.like_count,
            ),
            reverse=True,
        )

    @staticmethod
    def _is_newest_sort(sort_text: str) -> int:
        return 1 if "newest" in sort_text else 0

    def _rule_classify(
        self,
        row: _CommentRow,
        context_norm: str,
        context_has_product: bool,
        context_models: list[str],
    ) -> _RuleRow:
        normalized = self._normalize_text(row.text)
        if self._is_noise_comment(normalized):
            return _RuleRow(
                row=row,
                is_valid=False,
                priority=1,
                purchase_stage="none",
                sentiment="neutral",
                primary_tag="Others",
                secondary_tags=[],
                product_tags=[],
                severity="low",
                score=0.0,
                reason="noise_or_non_viewpoint",
            )

        models, uncertain_model = self._detect_models(normalized, context_norm, allow_contextless=False)
        if not models and context_models:
            models = context_models[:2]

        has_product_context = self._has_product_context(normalized) or context_has_product or bool(models)
        has_domain_signal = self._has_any_term(normalized, _DOMAIN_TERMS)
        has_opinion_signal = self._has_any_term(normalized, _OPINION_TERMS)
        has_min_text = len(re.findall(r"[a-z0-9\u4e00-\u9fff]+", normalized)) >= 4
        is_valid = bool(has_min_text and has_product_context and (has_domain_signal or has_opinion_signal))
        if uncertain_model and not has_domain_signal and not has_opinion_signal:
            is_valid = False

        purchase_stage = self._detect_purchase_stage(normalized)
        sentiment = self._guess_sentiment(normalized)
        primary_tag = self._guess_primary_tag(normalized)
        secondary_tags = self._guess_secondary_tags(normalized, primary_tag)
        severity = self._guess_severity(normalized, sentiment, purchase_stage)

        priority = 1
        if is_valid:
            if purchase_stage == "owned":
                priority = 4
            elif purchase_stage == "considering":
                priority = 3
            else:
                priority = 2

        product_tags = self._map_models_to_tags(models)
        score = self._score_rule_row(row=row, priority=priority, sentiment=sentiment)
        reason = "valid_viewpoint" if is_valid else "weak_product_or_opinion_signal"

        return _RuleRow(
            row=row,
            is_valid=is_valid,
            priority=priority,
            purchase_stage=purchase_stage,
            sentiment=sentiment,
            primary_tag=primary_tag,
            secondary_tags=secondary_tags,
            product_tags=product_tags,
            severity=severity,
            score=score,
            reason=reason,
        )

    @staticmethod
    def _normalize_text(text: str) -> str:
        value = clean_content_text(text).lower()
        value = re.sub(r"https?://\S+", " ", value)
        for pattern, replacement in _MODEL_ALIAS_MAP:
            value = pattern.sub(f" {replacement} ", value)
        value = re.sub(r"\s+", " ", value).strip()
        return value

    @staticmethod
    def _is_noise_comment(text: str) -> bool:
        if not text:
            return True
        alnum_tokens = re.findall(r"[a-z0-9\u4e00-\u9fff]+", text)
        if not alnum_tokens:
            return True
        if len(alnum_tokens) <= 2 and all(token in _NOISE_SHORT_TOKENS for token in alnum_tokens):
            return True
        if len("".join(alnum_tokens)) < 5:
            return True
        mention_stripped = re.sub(r"@\w+", "", text).strip()
        if not mention_stripped:
            return True
        if re.fullmatch(r"[\W_]+", mention_stripped):
            return True
        return False

    @staticmethod
    def _has_any_term(text: str, terms: set[str]) -> bool:
        return any(term in text for term in terms)

    @staticmethod
    def _has_product_context(text: str) -> bool:
        return any(token in text for token in _PRODUCT_HINTS)

    def _detect_models(
        self,
        text: str,
        context_norm: str,
        allow_contextless: bool,
    ) -> tuple[list[str], bool]:
        has_context = allow_contextless or self._has_product_context(text) or self._has_product_context(context_norm)
        results: list[str] = []
        uncertain = False
        for pattern, model, require_context in _MODEL_PATTERNS:
            if not pattern.search(text):
                continue
            if require_context and not has_context:
                uncertain = True
                continue
            if model not in results:
                results.append(model)
        return results, uncertain

    @staticmethod
    def _map_models_to_tags(models: list[str]) -> list[str]:
        mapping = {
            "phone_3": "phone3",
            "phone_3a": "3a",
            "phone_3a_pro": "3a pro",
            "phone_4a": "4a",
            "phone_4a_pro": "4a pro",
        }
        output: list[str] = []
        for model in models:
            tag = mapping.get(model)
            if tag and tag not in output:
                output.append(tag)
        return output

    @staticmethod
    def _detect_purchase_stage(text: str) -> str:
        if any(re.search(pattern, text, re.I) for pattern in _OWNED_PATTERNS):
            return "owned"
        if any(re.search(pattern, text, re.I) for pattern in _CONSIDERING_PATTERNS):
            return "considering"
        return "none"

    @staticmethod
    def _guess_sentiment(text: str) -> str:
        score = 0
        for word in _NEGATIVE_TERMS:
            if word in text:
                score -= 1
        for word in _POSITIVE_TERMS:
            if word in text:
                score += 1
        if score <= -1:
            return "negative"
        if score >= 1:
            return "positive"
        return "neutral"

    @staticmethod
    def _guess_primary_tag(text: str) -> str:
        lowered = text.lower()
        if any(token in lowered for token in ("camera", "photo", "video", "zoom", "lens", "hdr", "focus", "ois", "eis")):
            return "Camera"
        if any(token in lowered for token in ("battery", "sot", "drain")):
            return "Battery"
        if any(token in lowered for token in ("charge", "charging", "charger")):
            return "Charge"
        if any(token in lowered for token in ("signal", "network", "5g", "4g", "reception")):
            return "Signal"
        if any(token in lowered for token in ("screen", "display", "brightness", "flicker")):
            return "Screen"
        if any(token in lowered for token in ("os", "update", "android", "nothing os", "bug", "ui")):
            return "OS"
        if any(token in lowered for token in ("buy", "price", "availability", "ship", "stock", "purchase")):
            return "PurchaseExperience"
        return "Others"

    def _guess_secondary_tags(self, text: str, primary_tag: str) -> list[str]:
        output: list[str] = []
        lowered = text.lower()
        if primary_tag == "Camera":
            for keywords, tag in _CAMERA_SUBTAG_HINTS:
                if any(keyword in lowered for keyword in keywords):
                    output.append(tag)
        if "availability" in lowered or "usa" in lowered:
            output.append("Availability")
        if "price" in lowered:
            output.append("Pricing")
        return output[:8]

    def _guess_severity(self, text: str, sentiment: str, purchase_stage: str) -> str:
        if sentiment == "negative" and any(term in text for term in ("can't", "cannot", "broken", "dead", "crash", "black screen")):
            return "high"
        if purchase_stage == "owned" and sentiment == "negative":
            return "high"
        if purchase_stage == "owned":
            return "medium"
        if purchase_stage == "considering" and sentiment == "negative":
            return "medium"
        return "low"

    @staticmethod
    def _build_comment_meta(priority: int, purchase_stage: str) -> str:
        p = min(4, max(1, int(priority)))
        stage = clean_content_text(purchase_stage).lower()
        if stage not in {"none", "considering", "owned"}:
            stage = "none"
        return f"Priority:P{p},PurchaseStage:{stage}"

    def _score_rule_row(self, row: _CommentRow, priority: int, sentiment: str) -> float:
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        recency_bonus = 0.0
        if row.timestamp:
            age_days = max(0.0, (now_ts - row.timestamp) / 86400.0)
            recency_bonus = max(0.0, 30.0 - age_days)
        engagement_bonus = min(300, max(0, row.like_count)) * 0.3
        sort_bonus = 8.0 if "newest" in row.sort else 0.0
        sentiment_bonus = 20.0 if sentiment == "negative" else 0.0
        priority_bonus = {1: 0.0, 2: 40.0, 3: 90.0, 4: 150.0}.get(priority, 0.0)
        pin_bonus = 10.0 if row.is_pinned else 0.0
        return priority_bonus + sentiment_bonus + engagement_bonus + recency_bonus + sort_bonus + pin_bonus

    def _select_ai_candidates(self, rule_rows: list[_RuleRow]) -> list[_RuleRow]:
        valid_rows = [row for row in rule_rows if row.is_valid and row.priority >= 2]
        valid_rows.sort(key=lambda row: row.score, reverse=True)
        p4 = [row for row in valid_rows if row.priority == 4]
        p3 = [row for row in valid_rows if row.priority == 3]
        p2_negative = [row for row in valid_rows if row.priority == 2 and row.sentiment == "negative"]
        p2_other = [row for row in valid_rows if row.priority == 2 and row.sentiment != "negative"]

        selected: list[_RuleRow] = []
        selected_ids: set[str] = set()

        def append_many(rows: list[_RuleRow], limit: int | None = None) -> None:
            remaining = self.ai_max_candidates - len(selected)
            if remaining <= 0:
                return
            take = rows[:remaining] if limit is None else rows[: min(remaining, max(0, limit))]
            for row in take:
                if row.row.comment_id in selected_ids:
                    continue
                selected.append(row)
                selected_ids.add(row.row.comment_id)

        append_many(p4, limit=None)
        append_many(p3, limit=self.ai_max_p3)
        append_many(p2_negative, limit=self.ai_max_p2_negative)
        append_many(p2_other, limit=None)
        return selected[: self.ai_max_candidates]

    def _build_points(self, selected_rows: list[_RuleRow]) -> tuple[list[dict[str, Any]], int, int]:
        if not selected_rows:
            return [], 0, 0

        if not self._is_ai_available():
            fallback = self._build_fallback_points(selected_rows)
            return fallback[: self.max_points], 0, 0

        output_points: list[dict[str, Any]] = []
        batches_ok = 0
        batches_failed = 0
        for index in range(0, len(selected_rows), self.ai_batch_size):
            chunk = selected_rows[index : index + self.ai_batch_size]
            try:
                chunk_points = self._build_ai_points_for_chunk(chunk)
                output_points.extend(chunk_points)
                batches_ok += 1
            except Exception:
                output_points.extend(self._build_fallback_points(chunk))
                batches_failed += 1

        deduped = self._dedupe_points(output_points)
        deduped.sort(key=self._point_sort_key, reverse=True)
        return deduped[: self.max_points], batches_ok, batches_failed

    def _is_ai_available(self) -> bool:
        cfg = self.config.local_ai
        return bool(cfg.enabled and cfg.base_url and cfg.model)

    def _build_ai_points_for_chunk(self, chunk: list[_RuleRow]) -> list[dict[str, Any]]:
        cfg = self.config.local_ai
        payload_items: list[dict[str, Any]] = []
        for row in chunk:
            payload_items.append(
                {
                    "comment_id": row.row.comment_id,
                    "text": row.row.text,
                    "purchase_stage_guess": row.purchase_stage,
                    "priority_guess": f"P{row.priority}",
                    "sentiment_guess": row.sentiment,
                    "primary_tag_guess": row.primary_tag,
                    "model_tags": row.product_tags,
                    "like_count": row.row.like_count,
                    "sort": row.row.sort,
                }
            )

        prompt = (
            "你是手机评论观点抽取助手。请仅输出 JSON。\n"
            "输入是 YouTube 评论列表。请保留有价值观点，过滤无意义句子。\n"
            "输出 schema:\n"
            "{\n"
            '  "points": [\n'
            "    {\n"
            '      "comment_id": "string",\n'
            '      "text": "中文观点总结（简短）",\n'
            '      "original_text": "原文片段，不翻译",\n'
            '      "sentiment": "positive|neutral|negative",\n'
            '      "primary_tag": "ID|OS|Camera|Charge|Signal|Screen|Battery|PurchaseExperience|Others",\n'
            '      "secondary_tags": ["string"],\n'
            '      "severity": "high|medium|low",\n'
            '      "purchase_stage": "none|considering|owned",\n'
            '      "priority": "P2|P3|P4",\n'
            '      "product_tags": ["3a","3a pro","4a","4a pro","phone3"]\n'
            "    }\n"
            "  ]\n"
            "}\n"
            "规则：\n"
            "1) 只保留与产品体验相关观点；\n"
            "2) purchased/using -> owned(P4)，considering -> P3，其余有观点 -> P2；\n"
            "3) 保留 model 线索（3a/4a/3a pro/4a pro）。\n"
            "输入评论 JSON：\n"
            + json.dumps(payload_items, ensure_ascii=False)
        )

        body = {
            "model": cfg.model,
            "temperature": 0.1,
            "max_tokens": min(max(600, cfg.max_tokens), 2200),
            "messages": [
                {"role": "system", "content": "你必须严格输出可解析 JSON，不要 Markdown。"},
                {"role": "user", "content": prompt},
            ],
        }
        raw = self._chat_completion(body)
        data = self._extract_json_object(raw)
        points = data.get("points", []) if isinstance(data, dict) else []
        normalized: list[dict[str, Any]] = []
        rule_map = {row.row.comment_id: row for row in chunk}
        if not isinstance(points, list):
            return self._build_fallback_points(chunk)
        for point in points:
            if not isinstance(point, dict):
                continue
            comment_id = clean_content_text(str(point.get("comment_id", "")))
            base_rule = rule_map.get(comment_id)
            if base_rule is None:
                continue
            normalized_point = self._normalize_ai_point(point, base_rule)
            if normalized_point:
                normalized.append(normalized_point)
        if not normalized:
            return self._build_fallback_points(chunk)
        return normalized

    def _chat_completion(self, body: dict[str, Any]) -> str:
        cfg = self.config.local_ai
        base_url = cfg.base_url.rstrip("/")
        url = f"{base_url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if cfg.api_key:
            headers["Authorization"] = f"Bearer {cfg.api_key}"
        response = requests.post(
            url,
            headers=headers,
            json=body,
            timeout=max(10, cfg.timeout_seconds),
        )
        response.raise_for_status()
        payload = response.json()
        choices = payload.get("choices") or []
        if not choices:
            raise RuntimeError("empty_choices")
        message = choices[0].get("message") if isinstance(choices[0], dict) else {}
        content = message.get("content") if isinstance(message, dict) else ""
        if not content:
            raise RuntimeError("empty_content")
        return str(content)

    @staticmethod
    def _extract_json_object(raw_text: str) -> dict[str, Any]:
        text = str(raw_text or "").strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text.replace("json", "", 1).strip()
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end <= start:
            raise RuntimeError("json_not_found")
        return json.loads(text[start : end + 1])

    def _normalize_ai_point(self, point: dict[str, Any], base_rule: _RuleRow) -> dict[str, Any] | None:
        text = clean_content_text(str(point.get("text", "")))
        if not text:
            return None
        original_text = clean_content_text(str(point.get("original_text", ""))) or base_rule.row.text
        sentiment = self._normalize_sentiment(str(point.get("sentiment", "")) or base_rule.sentiment)
        primary_tag = self._normalize_primary_tag(str(point.get("primary_tag", "")) or base_rule.primary_tag)
        severity = self._normalize_severity(str(point.get("severity", "")) or base_rule.severity)
        purchase_stage = self._normalize_purchase_stage(str(point.get("purchase_stage", "")) or base_rule.purchase_stage)
        priority = self._normalize_priority(str(point.get("priority", "")))
        if priority <= 0:
            priority = base_rule.priority
        secondary_tags = self._normalize_secondary_tags(point.get("secondary_tags"), primary_tag=primary_tag)
        product_tags = self._normalize_product_tags(point.get("product_tags"), fallback=base_rule.product_tags)
        comment_meta = self._build_comment_meta(priority=priority, purchase_stage=purchase_stage)
        return {
            "text": truncate(text, 220),
            "original_text": truncate(original_text, 500),
            "sentiment": sentiment,
            "primary_tag": primary_tag,
            "secondary_tags": secondary_tags,
            "severity": severity,
            "severity_reason": "youtube_comment_mining",
            "product_tags": product_tags,
            "priority": f"P{priority}",
            "purchase_stage": purchase_stage,
            "comment_meta": comment_meta,
            "source_label": "评论区",
            "comment_author": truncate(clean_content_text(base_rule.row.author), 120),
            "comment_id": base_rule.row.comment_id,
        }

    @staticmethod
    def _normalize_sentiment(value: str) -> str:
        lowered = clean_content_text(value).lower()
        if lowered in {"positive", "neutral", "negative"}:
            return lowered
        return "neutral"

    @staticmethod
    def _normalize_primary_tag(value: str) -> str:
        cleaned = clean_content_text(value)
        if cleaned in _TAG_ENUM:
            return cleaned
        lowered = cleaned.lower()
        alias = {
            "camera": "Camera",
            "os": "OS",
            "battery": "Battery",
            "charge": "Charge",
            "signal": "Signal",
            "screen": "Screen",
            "purchaseexperience": "PurchaseExperience",
            "purchase": "PurchaseExperience",
            "id": "ID",
        }
        return alias.get(lowered, "Others")

    @staticmethod
    def _normalize_severity(value: str) -> str:
        lowered = clean_content_text(value).lower()
        if lowered in _SEVERITY_ORDER:
            return lowered
        if lowered in {"严重", "high"}:
            return "high"
        if lowered in {"中", "medium"}:
            return "medium"
        return "low"

    @staticmethod
    def _normalize_purchase_stage(value: str) -> str:
        lowered = clean_content_text(value).lower()
        if lowered in {"owned", "considering", "none"}:
            return lowered
        return "none"

    @staticmethod
    def _normalize_priority(value: str) -> int:
        cleaned = clean_content_text(value).upper()
        matched = re.search(r"P([1-4])", cleaned)
        if matched:
            return int(matched.group(1))
        return 0

    def _normalize_secondary_tags(self, raw: Any, primary_tag: str) -> list[str]:
        if not isinstance(raw, list):
            return []
        output: list[str] = []
        primary_key = clean_content_text(primary_tag).lower()
        for value in raw:
            clean = clean_content_text(str(value))
            if not clean:
                continue
            key = clean.lower()
            if key == primary_key or key in {item.lower() for item in output}:
                continue
            output.append(clean)
            if len(output) >= 8:
                break
        return output

    @staticmethod
    def _normalize_product_tags(raw: Any, fallback: list[str]) -> list[str]:
        if not isinstance(raw, list):
            return fallback[:4]
        output: list[str] = []
        for value in raw:
            clean = clean_content_text(str(value)).lower()
            if not clean:
                continue
            mapped = {
                "phone3": "phone3",
                "3a": "3a",
                "3a pro": "3a pro",
                "4a": "4a",
                "4a pro": "4a pro",
            }.get(clean)
            if mapped and mapped not in output:
                output.append(mapped)
        if not output:
            output = fallback[:4]
        return output[:4]

    def _build_fallback_points(self, rows: list[_RuleRow]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        for row in rows:
            if not row.is_valid or row.priority <= 1:
                continue
            prefix = {
                4: "已购用户反馈",
                3: "潜在购机意向",
                2: "普通观点",
            }.get(row.priority, "观点")
            output.append(
                {
                    "text": truncate(f"{prefix}: {row.row.text}", 220),
                    "original_text": truncate(row.row.text, 500),
                    "sentiment": row.sentiment,
                    "primary_tag": row.primary_tag,
                    "secondary_tags": row.secondary_tags,
                    "severity": row.severity,
                    "severity_reason": row.reason,
                    "product_tags": row.product_tags,
                    "priority": f"P{row.priority}",
                    "purchase_stage": row.purchase_stage,
                    "comment_meta": self._build_comment_meta(priority=row.priority, purchase_stage=row.purchase_stage),
                    "source_label": "评论区",
                    "comment_author": truncate(clean_content_text(row.row.author), 120),
                    "comment_id": row.row.comment_id,
                }
            )
        return output

    @staticmethod
    def _dedupe_points(points: list[dict[str, Any]]) -> list[dict[str, Any]]:
        deduped: list[dict[str, Any]] = []
        seen: set[str] = set()
        for point in points:
            text = clean_content_text(str(point.get("text", "")))
            original = clean_content_text(str(point.get("original_text", "")))
            if not text:
                continue
            key = normalize_text(f"{text}||{original}").lower()
            if key in seen:
                continue
            seen.add(key)
            deduped.append(point)
        return deduped

    @classmethod
    def _point_sort_key(cls, point: dict[str, Any]) -> tuple[int, int, int]:
        secondary = [clean_content_text(str(tag)) for tag in (point.get("secondary_tags") or [])]
        priority = 2
        raw_priority = clean_content_text(str(point.get("priority", ""))).upper()
        matched = re.search(r"P([1-4])", raw_priority)
        if matched:
            priority = int(matched.group(1))
        else:
            comment_meta = clean_content_text(str(point.get("comment_meta", "")))
            matched = re.search(r"priority\s*:\s*p?([1-4])", comment_meta, re.I)
            if matched:
                priority = int(matched.group(1))
            else:
                for tag in secondary:
                    matched = re.search(r"priority\s*:\s*p?([1-4])", tag, re.I)
                    if matched:
                        priority = int(matched.group(1))
                        break
        severity = cls._normalize_severity(str(point.get("severity", "")))
        sentiment = cls._normalize_sentiment(str(point.get("sentiment", "")))
        return priority, _SEVERITY_ORDER.get(severity, 1), _SENTIMENT_ORDER.get(sentiment, 2)
