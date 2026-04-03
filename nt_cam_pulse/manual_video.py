from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

from .models import FeedbackItem
from .utils import canonical_url, clean_content_text, is_summary_redundant, normalize_text, parse_datetime, truncate
from .video_identity import extract_video_signatures

URL_PATTERN = re.compile(r"https?://[^\s<>\"]+")
MANUAL_FETCH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


@dataclass(slots=True)
class ManualVideoMeta:
    title: str = ""
    description: str = ""
    author: str | None = None
    published_at: datetime | None = None
    final_url: str = ""
    fetch_error: str = ""


def collect_manual_video_urls(raw_urls: list[str], file_path: str | None = None) -> list[str]:
    candidates: list[str] = []
    for raw in raw_urls:
        candidates.extend(_extract_url_candidates(raw))

    if file_path:
        lines = Path(file_path).expanduser().resolve().read_text(encoding="utf-8").splitlines()
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            candidates.extend(_extract_url_candidates(stripped))

    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        valid = _normalize_http_url(candidate)
        if not valid:
            continue
        if valid in seen:
            continue
        seen.add(valid)
        normalized.append(valid)
    return normalized


def build_manual_video_item(url: str, timeout_seconds: int = 12) -> FeedbackItem:
    normalized_url = _normalize_http_url(url)
    if not normalized_url:
        raise ValueError(f"invalid_url: {url}")

    meta = fetch_video_page_meta(normalized_url, timeout_seconds=timeout_seconds)
    final_url = _normalize_http_url(meta.final_url) or normalized_url
    domain = _domain_of_url(final_url)
    source = _source_for_domain(domain)
    source_item_id = _source_item_id(final_url)
    published_at = meta.published_at or datetime.now(tz=timezone.utc)

    title = clean_content_text(meta.title)
    if not title:
        title = f"Manual Video · {domain or final_url}"
    description = clean_content_text(meta.description)
    resolved_author = normalize_text(meta.author or "") or _default_author_for_domain(domain)
    summary = ""
    if description and not is_summary_redundant(title, description):
        summary = truncate(description, 240)

    extra = {
        "manual_input": True,
        "manual_source": "cli",
        "meta_fetch_error": meta.fetch_error,
        "meta_final_url": final_url,
        "video_signatures": extract_video_signatures(
            url=final_url,
            title=title,
            author=resolved_author,
            source_item_id=source_item_id,
        ),
    }

    return FeedbackItem(
        source=source,
        source_item_id=source_item_id,
        title=title,
        url=final_url,
        content=description or title,
        summary=summary,
        published_at=published_at,
        author=resolved_author or None,
        source_section=domain or "manual",
        video_candidate=True,
        extra=extra,
    )


def fetch_video_page_meta(url: str, timeout_seconds: int = 12) -> ManualVideoMeta:
    try:
        response = requests.get(url, headers=MANUAL_FETCH_HEADERS, timeout=max(5, timeout_seconds))
        response.raise_for_status()
        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        title = _first_meta_content(
            soup,
            [
                ("property", "og:title"),
                ("name", "twitter:title"),
                ("itemprop", "name"),
            ],
        )
        if not title and soup.title:
            title = soup.title.get_text(" ", strip=True)

        description = _first_meta_content(
            soup,
            [
                ("property", "og:description"),
                ("name", "description"),
                ("name", "twitter:description"),
            ],
        )
        author = _first_meta_content(
            soup,
            [
                ("name", "author"),
                ("property", "article:author"),
                ("itemprop", "author"),
            ],
        )
        site_name = _first_meta_content(
            soup,
            [
                ("property", "og:site_name"),
                ("name", "application-name"),
            ],
        )
        published_raw = _first_meta_content(
            soup,
            [
                ("property", "article:published_time"),
                ("itemprop", "datePublished"),
                ("name", "publishdate"),
                ("name", "date"),
            ],
        )
        if _is_youtube_url(url):
            yt_title, yt_author = _fetch_youtube_oembed_meta(url, timeout_seconds=timeout_seconds)
            if not title:
                title = yt_title or title
            if not author:
                author = yt_author or author
        if not author:
            author = site_name or author
        return ManualVideoMeta(
            title=clean_content_text(title),
            description=clean_content_text(description),
            author=clean_content_text(author) or None,
            published_at=parse_datetime(published_raw),
            final_url=str(response.url or url),
            fetch_error="",
        )
    except Exception as exc:  # noqa: BLE001
        return ManualVideoMeta(
            title="",
            description="",
            author=None,
            published_at=None,
            final_url=url,
            fetch_error=str(exc)[:240],
        )


def _extract_url_candidates(raw: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []
    urls = URL_PATTERN.findall(text)
    if urls:
        return [candidate.rstrip(".,;)]}>'\"") for candidate in urls]
    return [text]


def _normalize_http_url(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("www."):
        text = "https://" + text
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return ""
    if not parsed.netloc:
        return ""
    return canonical_url(text)


def _domain_of_url(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower()
    except ValueError:
        return ""


def _source_for_domain(domain: str) -> str:
    host = domain.lower()
    if "youtube.com" in host or "youtu.be" in host:
        return "youtube_manual"
    if "bilibili.com" in host or "b23.tv" in host:
        return "bilibili_manual"
    if host in {"x.com", "twitter.com"}:
        return "x_manual"
    if "instagram.com" in host:
        return "instagram_manual"
    if "tiktok.com" in host or "douyin.com" in host:
        return "short_video_manual"
    if "vimeo.com" in host:
        return "vimeo_manual"
    return "manual_video"


def _source_item_id(url: str) -> str:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if "youtube.com" in host:
        query = parse_qs(parsed.query or "")
        video_id = (query.get("v") or [""])[0]
        if video_id:
            return video_id
    if "youtu.be" in host:
        token = parsed.path.strip("/").split("/")
        if token and token[0]:
            return token[0]
    if "bilibili.com" in host or "b23.tv" in host:
        token = parsed.path.strip("/").split("/")
        if token and token[0]:
            return token[0]
    return truncate(url, 120)


def _first_meta_content(soup: BeautifulSoup, pairs: list[tuple[str, str]]) -> str:
    for attr, value in pairs:
        node = soup.find("meta", attrs={attr: value})
        if not node:
            continue
        content = clean_content_text(node.get("content", ""))
        if content:
            return content
    return ""


def _is_youtube_url(url: str) -> bool:
    host = _domain_of_url(url)
    return ("youtube.com" in host) or ("youtu.be" in host)


def _fetch_youtube_oembed_meta(url: str, timeout_seconds: int = 12) -> tuple[str, str]:
    try:
        response = requests.get(
            "https://www.youtube.com/oembed",
            params={"url": url, "format": "json"},
            headers=MANUAL_FETCH_HEADERS,
            timeout=max(5, min(20, timeout_seconds)),
        )
        response.raise_for_status()
        data = response.json()
        title = clean_content_text(data.get("title", ""))
        author = clean_content_text(data.get("author_name", ""))
        return title, author
    except Exception:  # noqa: BLE001
        return "", ""


def _default_author_for_domain(domain: str) -> str:
    host = domain.lower()
    if "youtube.com" in host or "youtu.be" in host:
        return "YouTube Creator"
    if "bilibili.com" in host or "b23.tv" in host:
        return "Bilibili UP"
    if "instagram.com" in host:
        return "Instagram Creator"
    if "tiktok.com" in host:
        return "TikTok Creator"
    if "douyin.com" in host:
        return "抖音创作者"
    if host in {"x.com", "twitter.com"}:
        return "X Creator"
    return domain or "Unknown Creator"
