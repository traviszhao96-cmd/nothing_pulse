from __future__ import annotations

from datetime import datetime, timezone

import feedparser
import requests

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class XSnscrapeCollector(BaseCollector):
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        try:
            from snscrape.modules.twitter import TwitterSearchScraper
        except Exception:
            return self._fetch_via_nitter_rss(since)

        query = str(self.config.get("query", "")).strip() or self._default_query()
        limit = max(1, min(200, int(self.config.get("limit", 50))))
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]

        since_utc = since.astimezone(timezone.utc)
        items: list[FeedbackItem] = []
        scraper = TwitterSearchScraper(query)
        for post in scraper.get_items():
            published_at = parse_datetime(getattr(post, "date", None))
            if not published_at:
                continue
            if published_at.astimezone(timezone.utc) < since_utc:
                break

            text = clean_content_text(getattr(post, "rawContent", "") or "")
            if not text:
                continue
            if not self.is_relevant(text):
                continue
            if include_keywords:
                if not any(keyword in text.lower() for keyword in include_keywords):
                    continue

            post_id = str(getattr(post, "id", "")).strip()
            url = str(getattr(post, "url", "")).strip()
            username = clean_content_text(getattr(getattr(post, "user", None), "username", "") or "")
            display_name = clean_content_text(getattr(getattr(post, "user", None), "displayname", "") or "")
            author = display_name or username
            title = truncate(text, 120)
            summary = "" if is_summary_redundant(title, text) else truncate(text, 240)
            has_video = bool(getattr(post, "media", None))

            items.append(
                FeedbackItem(
                    source="x_snscrape",
                    source_item_id=post_id or None,
                    title=title,
                    url=url,
                    content=text,
                    summary=summary,
                    published_at=published_at,
                    author=author or None,
                    source_section="X",
                    video_candidate=has_video,
                    extra={
                        "collector": "snscrape",
                        "query": query,
                        "username": username,
                        "lang": getattr(post, "lang", None),
                        "like_count": getattr(post, "likeCount", None),
                        "reply_count": getattr(post, "replyCount", None),
                        "retweet_count": getattr(post, "retweetCount", None),
                        "quote_count": getattr(post, "quoteCount", None),
                    },
                )
            )
            if len(items) >= limit:
                break

        return items

    def _fetch_via_nitter_rss(self, since: datetime) -> list[FeedbackItem]:
        query = str(self.config.get("query", "")).strip() or self._default_query()
        limit = max(1, min(200, int(self.config.get("limit", 50))))
        timeout = max(8, int(self.config.get("timeout_seconds", 30)))
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]
        instances = [
            str(url).rstrip("/")
            for url in self.config.get(
                "nitter_instances",
                [
                    "https://nitter.net",
                    "https://nitter.poast.org",
                ],
            )
            if str(url).strip()
        ]
        since_utc = since.astimezone(timezone.utc)
        items: list[FeedbackItem] = []

        for instance in instances:
            rss_url = f"{instance}/search/rss"
            try:
                response = requests.get(
                    rss_url,
                    params={"f": "tweets", "q": query},
                    timeout=timeout,
                    headers={"User-Agent": "NothingCameraPulse/1.0"},
                )
                response.raise_for_status()
                feed = feedparser.parse(response.text)
            except Exception:
                continue

            for entry in feed.entries:
                published_at = parse_datetime(getattr(entry, "published", "") or getattr(entry, "updated", ""))
                if not published_at:
                    continue
                if published_at.astimezone(timezone.utc) < since_utc:
                    continue

                link = clean_content_text(getattr(entry, "link", "") or "")
                title = clean_content_text(getattr(entry, "title", "") or "")
                summary = clean_content_text(getattr(entry, "summary", "") or "")
                content = summary or title
                if not content:
                    continue
                if not self.is_relevant(title, content):
                    continue
                if include_keywords:
                    blob = f"{title} {content}".lower()
                    if not any(keyword in blob for keyword in include_keywords):
                        continue

                author = clean_content_text(getattr(entry, "author", "") or "")
                source_item_id = link.rstrip("/").split("/")[-1] if link else None
                item_title = truncate(title or content, 120)
                item_summary = "" if is_summary_redundant(item_title, content) else truncate(content, 240)
                if link and "nitter" in link and "/status/" in link:
                    # Nitter link usually mirrors x.com URL structure.
                    link = link.replace("nitter.net", "x.com")

                items.append(
                    FeedbackItem(
                        source="x_snscrape",
                        source_item_id=source_item_id,
                        title=item_title,
                        url=link or instance,
                        content=content,
                        summary=item_summary,
                        published_at=published_at,
                        author=author or None,
                        source_section="X",
                        video_candidate=False,
                        extra={
                            "collector": "nitter_rss_fallback",
                            "query": query,
                            "nitter_instance": instance,
                        },
                    )
                )
                if len(items) >= limit:
                    return items
            if items:
                return items
        return items

    def _default_query(self) -> str:
        if self.product_keywords:
            joined = " OR ".join(f"\"{item}\"" for item in self.product_keywords[:3] if item)
            if joined:
                return f"({joined}) (camera OR photo OR video)"
        return "\"Nothing Phone\" (camera OR photo OR video)"
