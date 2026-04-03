from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import quote_plus

import feedparser
import requests

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .article_body import ArticleBodyExtractor
from .base import BaseCollector


class GoogleNewsCollector(BaseCollector):
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        query = self.config.get("query", "\"Nothing Phone\"")
        language = self.config.get("language", "en-US")
        country = self.config.get("country", "US")
        edition = self.config.get("edition", "US:en")
        limit = int(self.config.get("limit", 30))
        fetch_article_body = bool(self.config.get("fetch_article_body", True))
        article_timeout = int(self.config.get("article_timeout_seconds", 20))
        article_max_chars = int(self.config.get("article_max_chars", 6000))
        extractor = ArticleBodyExtractor(timeout=article_timeout, max_chars=article_max_chars) if fetch_article_body else None
        url = (
            "https://news.google.com/rss/search?"
            f"q={quote_plus(query)}&hl={language}&gl={country}&ceid={edition}"
        )
        response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        feed = feedparser.parse(response.text)

        items: list[FeedbackItem] = []
        for entry in feed.entries[:limit]:
            published_at = parse_datetime(_pick(entry, "published", "updated"))
            if not published_at or published_at < since:
                continue

            title = clean_content_text(getattr(entry, "title", ""))
            summary = clean_content_text(getattr(entry, "summary", ""))
            if not self.is_relevant(title, summary):
                continue
            link = getattr(entry, "link", "")
            article_body = ""
            resolved_link = link
            if extractor and link:
                body_result = extractor.fetch(link)
                article_body = body_result.text
                resolved_link = body_result.resolved_url or link

            summary_for_store = "" if is_summary_redundant(title, summary) else summary
            if not summary_for_store and article_body and not is_summary_redundant(title, article_body):
                summary_for_store = truncate(article_body, 240)

            source_name = getattr(getattr(entry, "source", None), "title", "Google News")
            items.append(
                FeedbackItem(
                    source="google_news",
                    source_item_id=getattr(entry, "id", None),
                    title=title,
                    url=resolved_link or link,
                    content=article_body or summary or title,
                    summary=truncate(summary_for_store, 240) if summary_for_store else "",
                    published_at=published_at,
                    source_section=source_name,
                    extra={"query": query, "article_body_fetched": bool(article_body)},
                )
            )
        return items


def _pick(entry: Any, *names: str) -> Any:
    for name in names:
        value = getattr(entry, name, None)
        if value:
            return value
    return None
