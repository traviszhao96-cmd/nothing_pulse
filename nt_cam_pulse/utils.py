from __future__ import annotations

import difflib
import hashlib
import html
import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


def ensure_parent(path: str | Path) -> None:
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def ensure_dir(path: str | Path) -> Path:
    directory = Path(path).expanduser().resolve()
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def clean_content_text(value: str | None) -> str:
    if not value:
        return ""
    text = html.unescape(str(value))
    text = re.sub(r"(?is)<(script|style)\b[^>]*>.*?</\1>", " ", text)
    text = re.sub(r"(?is)<\s*/?\s*a\b[^>]*", " ", text)
    text = re.sub(r"(?is)<a\b[^>]*>", " ", text)
    text = re.sub(r"(?is)</a>", " ", text)
    text = re.sub(r'(?i)\b(?:href|ref)\s*=\s*"[^"]*"', " ", text)
    text = re.sub(r"(?i)\b(?:href|ref)\s*=\s*'[^']*'", " ", text)
    text = re.sub(r"(?i)\b(?:href|ref)\s*=\s*\S+", " ", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = text.replace("<", " ").replace(">", " ")
    return normalize_text(text)


def is_summary_redundant(title: str | None, summary: str | None) -> bool:
    title_text = _canonical_compare_text(title)
    summary_text = _canonical_compare_text(summary)
    if not title_text or not summary_text:
        return False
    if title_text == summary_text:
        return True
    if summary_text.startswith(title_text) or title_text.startswith(summary_text):
        return True
    ratio = difflib.SequenceMatcher(None, title_text, summary_text).ratio()
    return ratio >= 0.92


def _canonical_compare_text(value: str | None) -> str:
    text = clean_content_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", text)
    return normalize_text(text)


def canonical_url(value: str) -> str:
    parsed = urlparse(value.strip())
    safe_params = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=False) if not k.startswith("utm_")]
    cleaned = parsed._replace(query=urlencode(safe_params), fragment="")
    return urlunparse(cleaned)


def build_fingerprint(source: str, source_item_id: str | None, title: str, url: str) -> str:
    payload = "||".join(
        [
            normalize_text(source).lower(),
            normalize_text(source_item_id or ""),
            normalize_text(title).lower(),
            canonical_url(url).lower(),
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_exact_dedupe_key(title: str, url: str) -> str:
    payload = "||".join([normalize_text(title).lower(), canonical_url(url).lower()])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)

    text = str(value).strip()
    if not text:
        return None

    parsers = (
        lambda raw: datetime.fromisoformat(raw.replace("Z", "+00:00")),
        parsedate_to_datetime,
    )
    for parser in parsers:
        try:
            dt = parser(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (TypeError, ValueError):
            continue
    return None


def isoformat(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def load_json(text: str, default: Any) -> Any:
    if not text:
        return default
    return json.loads(text)


def dump_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."


def detect_language(text: str) -> str:
    if not text:
        return "unknown"
    has_cjk = re.search(r"[\u4e00-\u9fff]", text)
    has_latin = re.search(r"[A-Za-z]", text)
    if has_cjk and has_latin:
        return "mixed"
    if has_cjk:
        return "zh"
    if has_latin:
        return "en"
    return "unknown"


def report_day_for(dt: datetime) -> date:
    return dt.astimezone().date()


def since_hours(hours: int) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(hours=hours)


def since_days(days: int) -> datetime:
    return datetime.now(tz=timezone.utc) - timedelta(days=days)


def expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_env(item) for item in value]
    if isinstance(value, str):
        return os.path.expandvars(value)
    return value


def tokenize_text(value: str) -> list[str]:
    text = normalize_text(value).lower()
    if not text:
        return []
    # Keep both latin words and CJK chunks as tokens for mixed-language posts.
    tokens = re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]{1,4}", text)
    seen: set[str] = set()
    unique_tokens: list[str] = []
    for token in tokens:
        if token not in seen:
            seen.add(token)
            unique_tokens.append(token)
    return unique_tokens


def jaccard_similarity(tokens_a: Iterable[str], tokens_b: Iterable[str]) -> float:
    set_a = set(tokens_a)
    set_b = set(tokens_b)
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return intersection / union


def is_video_url(url: str | None) -> bool:
    if not url:
        return False
    parsed = urlparse(str(url).strip())
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    if any(
        token in host
        for token in (
            "youtube.com",
            "youtu.be",
            "bilibili.com",
            "b23.tv",
            "vimeo.com",
            "x.com",
            "twitter.com",
            "instagram.com",
            "tiktok.com",
            "douyin.com",
        )
    ):
        return True
    return any(token in path for token in ("/video/", "/watch", "/shorts/"))


def build_timestamped_video_url(url: str | None, seconds: int | None) -> str:
    base = str(url or "").strip()
    if not base:
        return ""
    if seconds is None:
        return base
    try:
        offset = max(0, int(seconds))
    except (TypeError, ValueError):
        return base

    try:
        parsed = urlparse(base)
        host = (parsed.hostname or "").lower()
        params = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True)]
        if "youtube.com" in host or "youtu.be" in host:
            params = [(k, v) for k, v in params if k not in {"t", "start", "time_continue"}]
            params.append(("t", str(offset)))
            return urlunparse(parsed._replace(query=urlencode(params), fragment=""))
        if "bilibili.com" in host or "b23.tv" in host:
            params = [(k, v) for k, v in params if k != "t"]
            params.append(("t", str(offset)))
            return urlunparse(parsed._replace(query=urlencode(params), fragment=""))
        return urlunparse(parsed._replace(fragment=f"t={offset}"))
    except Exception:  # noqa: BLE001
        return base


def int_to_base36(value: int) -> str:
    number = max(0, int(value))
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if number == 0:
        return "0"
    digits: list[str] = []
    while number > 0:
        number, remainder = divmod(number, 36)
        digits.append(alphabet[remainder])
    return "".join(reversed(digits))


def build_feedback_uid(row_id: int, source_code: str = "ot") -> str:
    code = re.sub(r"[^a-z0-9]+", "", str(source_code or "").strip().lower())[:3] or "ot"
    return f"{code}-{int_to_base36(int(row_id)).upper().zfill(4)}"


def build_feedback_point_uid(feedback_uid: str, sentiment: str, point_text: str, ordinal: int) -> str:
    sentiment_key = str(sentiment or "neutral").strip().lower()
    sentiment_code = {"positive": "p", "neutral": "n", "negative": "d"}.get(sentiment_key, "u")
    normalized = normalize_text(point_text).lower()
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:4].upper() if normalized else "0000"
    return f"{feedback_uid}-{sentiment_code}{max(1, int(ordinal)):02d}{digest}"


_POINT_TS_PATTERN = re.compile(r"^\s*(?:\[|\()?\s*((?:\d{1,2}:)?\d{1,2}:\d{2})\s*(?:\]|\))?\s*[-:：]?\s*(.*)$")


def parse_point_timestamp(raw: str | None) -> tuple[int | None, str, str]:
    text = clean_content_text(raw or "")
    if not text:
        return None, "", ""
    match = _POINT_TS_PATTERN.match(text)
    if not match:
        return None, "", text
    seconds = parse_timestamp_to_seconds(match.group(1))
    if seconds is None:
        return None, "", text
    point_text = clean_content_text(match.group(2) or "")
    return seconds, format_seconds_label(seconds), (point_text or text)


def parse_timestamp_to_seconds(raw: str | None) -> int | None:
    token = normalize_text(raw or "")
    if not token:
        return None
    chunks = token.split(":")
    if len(chunks) not in {2, 3}:
        return None
    if any(not piece.isdigit() for piece in chunks):
        return None
    values = [int(piece) for piece in chunks]
    if len(values) == 2:
        return values[0] * 60 + values[1]
    return values[0] * 3600 + values[1] * 60 + values[2]


def format_seconds_label(total_seconds: int) -> str:
    value = max(0, int(total_seconds))
    hour = value // 3600
    minute = (value % 3600) // 60
    second = value % 60
    if hour > 0:
        return f"{hour:02d}:{minute:02d}:{second:02d}"
    return f"{minute:02d}:{second:02d}"
