from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qs, urlparse

from .utils import canonical_url, normalize_text

TITLE_STOPWORDS = {
    "a",
    "an",
    "and",
    "the",
    "for",
    "with",
    "review",
    "hands",
    "on",
    "video",
    "camera",
    "test",
    "vs",
}


def extract_video_signatures(
    url: str,
    title: str = "",
    author: str = "",
    source_item_id: str = "",
) -> list[str]:
    signatures: set[str] = set()
    normalized_url = _normalize_http_url(url)
    if normalized_url:
        signatures.add(f"url:{normalized_url.lower()}")
    signatures.update(_platform_signatures(normalized_url or url))

    source_id = normalize_text(source_item_id).lower()
    if source_id:
        signatures.add(f"source_item:{source_id}")

    title_sig = _title_signature(title=title, author=author)
    if title_sig:
        signatures.add(title_sig)
    return sorted(signatures)


def parse_video_signatures(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    result: list[str] = []
    for raw in value:
        item = normalize_text(str(raw)).lower()
        if not item or item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _normalize_http_url(raw: str) -> str:
    text = normalize_text(raw)
    if not text:
        return ""
    if text.startswith("www."):
        text = "https://" + text
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return canonical_url(text)


def _platform_signatures(url: str) -> set[str]:
    signatures: set[str] = set()
    if not url:
        return signatures
    try:
        parsed = urlparse(url)
    except ValueError:
        return signatures

    host = (parsed.hostname or "").lower()
    path = parsed.path or ""
    query = parse_qs(parsed.query or "")

    if "youtube.com" in host:
        video_id = normalize_text((query.get("v") or [""])[0]).strip()
        if video_id:
            signatures.add(f"vid:youtube:{video_id.lower()}")
    elif "youtu.be" in host:
        token = path.strip("/").split("/")
        if token and token[0]:
            signatures.add(f"vid:youtube:{token[0].lower()}")

    if "bilibili.com" in host or "b23.tv" in host:
        bvid_match = re.search(r"(BV[0-9A-Za-z]+)", path, flags=re.IGNORECASE)
        if bvid_match:
            signatures.add(f"vid:bilibili:{bvid_match.group(1).lower()}")
        av_match = re.search(r"(?:av)(\d+)", path, flags=re.IGNORECASE)
        if av_match:
            signatures.add(f"vid:bilibili:av{av_match.group(1)}")

    if host in {"x.com", "twitter.com"}:
        status_match = re.search(r"/status/(\d+)", path)
        if status_match:
            signatures.add(f"vid:x:{status_match.group(1)}")

    if "instagram.com" in host:
        reel_match = re.search(r"/(?:reel|p|tv)/([^/?#]+)", path)
        if reel_match:
            signatures.add(f"vid:instagram:{reel_match.group(1).lower()}")

    if "tiktok.com" in host:
        tiktok_match = re.search(r"/video/(\d+)", path)
        if tiktok_match:
            signatures.add(f"vid:tiktok:{tiktok_match.group(1)}")

    if "douyin.com" in host:
        douyin_match = re.search(r"/video/(\d+)", path)
        if douyin_match:
            signatures.add(f"vid:douyin:{douyin_match.group(1)}")

    if "vimeo.com" in host:
        vimeo_match = re.search(r"/(\d+)", path)
        if vimeo_match:
            signatures.add(f"vid:vimeo:{vimeo_match.group(1)}")

    return signatures


def _title_signature(title: str, author: str) -> str:
    normalized_title = normalize_text(title).lower()
    if not normalized_title:
        return ""
    cleaned = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", normalized_title)
    tokens = [token for token in cleaned.split() if token and token not in TITLE_STOPWORDS]
    if len(tokens) < 3:
        return ""
    phrase = " ".join(tokens[:14])
    author_key = normalize_text(author).lower()
    payload = f"{phrase}|{author_key}" if author_key else phrase
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
    return f"title:{digest}"
