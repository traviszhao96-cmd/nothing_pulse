from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..models import FeedbackItem
from ..utils import normalize_text, parse_datetime, truncate
from .base import BaseCollector


class NothingCommunityCollector(BaseCollector):
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        base_url = self.config.get("base_url", "https://nothing.community")
        pages = int(self.config.get("pages", 2))
        include_keywords = [keyword.lower() for keyword in self.config.get("include_keywords", [])]

        discussion_links: dict[str, str] = {}
        for page in range(1, pages + 1):
            url = base_url if page == 1 else f"{base_url}?page={page}"
            response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            for anchor in soup.select('a[href^="/d/"]'):
                href = anchor.get("href", "")
                title = normalize_text(anchor.get_text(" ", strip=True))
                if not href or not title:
                    continue
                if href in discussion_links:
                    continue
                discussion_links[href] = title

        items: list[FeedbackItem] = []
        for href, title in discussion_links.items():
            full_url = urljoin(base_url, href)
            detail = self._fetch_detail(full_url)
            if not detail:
                continue
            published_at = detail["published_at"]
            if published_at < since:
                continue

            summary = detail["summary"]
            content = detail["content"]
            blob = " ".join([title.lower(), summary.lower(), content.lower()])
            if include_keywords and not any(keyword in blob for keyword in include_keywords):
                continue
            if not self.is_relevant(title, summary, content):
                continue

            items.append(
                FeedbackItem(
                    source="nothing_community",
                    source_item_id=detail["source_item_id"],
                    title=detail["title"],
                    url=full_url,
                    content=content,
                    summary=truncate(summary or content, 240),
                    published_at=published_at,
                    author=detail.get("author"),
                    source_section="Nothing Community",
                    extra={"updated_at": detail.get("updated_at")},
                )
            )
        return items

    def _fetch_detail(self, url: str) -> dict[str, Any] | None:
        response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        title = normalize_text(_meta_content(soup, property_name="og:title") or soup.title.string if soup.title else "")
        description = normalize_text(_meta_content(soup, property_name="og:description") or "")
        published_at = parse_datetime(_meta_content(soup, property_name="article:published_time"))
        updated_at = parse_datetime(_meta_content(soup, property_name="article:updated_time"))
        if not title or not published_at:
            return None

        schema = _find_discussion_schema(soup)
        content = normalize_text(schema.get("articleBody") or schema.get("text") or description or title)
        author = None
        author_data = schema.get("author")
        if isinstance(author_data, dict):
            author = author_data.get("name")

        source_item_id = str(schema.get("identifier") or url.rstrip("/").split("/")[-1])
        return {
            "title": title,
            "summary": description or content,
            "content": content,
            "author": author,
            "published_at": published_at,
            "updated_at": updated_at.isoformat() if updated_at else None,
            "source_item_id": source_item_id,
        }


def _find_discussion_schema(soup: BeautifulSoup) -> dict[str, Any]:
    for node in soup.select('script[type="application/ld+json"]'):
        raw = node.string or node.get_text()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            continue
        candidates = data if isinstance(data, list) else [data]
        for candidate in candidates:
            if candidate.get("@type") == "DiscussionForumPosting":
                return candidate
    return {}


def _meta_content(soup: BeautifulSoup, property_name: str) -> str | None:
    node = soup.find("meta", attrs={"property": property_name})
    if not node:
        return None
    return node.get("content")
