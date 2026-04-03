from __future__ import annotations

from datetime import datetime

import feedparser
import requests

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .article_body import ArticleBodyExtractor
from .base import BaseCollector


class CustomRSSCollector(BaseCollector):
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        results: list[FeedbackItem] = []
        fetch_article_body = bool(self.config.get("fetch_article_body", True))
        article_timeout = int(self.config.get("article_timeout_seconds", 20))
        article_max_chars = int(self.config.get("article_max_chars", 6000))
        extractor = ArticleBodyExtractor(timeout=article_timeout, max_chars=article_max_chars) if fetch_article_body else None
        for feed in self.config.get("feeds", []):
            url = feed["url"]
            response = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            parsed = feedparser.parse(response.text)

            include_keywords = [keyword.lower() for keyword in feed.get("include_keywords", [])]
            for entry in parsed.entries:
                published_at = parse_datetime(
                    getattr(entry, "published", None) or getattr(entry, "updated", None)
                )
                if not published_at or published_at < since:
                    continue
                title = clean_content_text(getattr(entry, "title", ""))
                summary = clean_content_text(getattr(entry, "summary", ""))
                summary_for_store = "" if is_summary_redundant(title, summary) else summary
                if not self.is_relevant(title, summary):
                    continue
                link = getattr(entry, "link", "")
                article_body = ""
                resolved_link = link
                if extractor and link:
                    body_result = extractor.fetch(link)
                    article_body = body_result.text
                    resolved_link = body_result.resolved_url or link
                if not summary_for_store and article_body and not is_summary_redundant(title, article_body):
                    summary_for_store = truncate(article_body, 240)
                blob = " ".join([title.lower(), summary.lower()])
                if include_keywords and not any(keyword in blob for keyword in include_keywords):
                    continue
                results.append(
                    FeedbackItem(
                        source="custom_rss",
                        source_item_id=getattr(entry, "id", None),
                        title=title,
                        url=resolved_link or link,
                        content=article_body or summary or title,
                        summary=truncate(summary_for_store, 240) if summary_for_store else "",
                        published_at=published_at,
                        source_section=feed.get("name", "Custom RSS"),
                        extra={"feed_url": url, "article_body_fetched": bool(article_body)},
                    )
                )
        return results
