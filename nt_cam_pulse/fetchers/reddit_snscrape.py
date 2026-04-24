from __future__ import annotations

from datetime import datetime, timezone

import requests

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class RedditSNScrapeCollector(BaseCollector):
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        try:
            from snscrape.modules.reddit import RedditSearchScraper
        except Exception:
            return self._fetch_via_reddit_json(since)

        query = str(self.config.get("query", "")).strip() or self._default_query()
        subreddits = [str(name).strip() for name in self.config.get("subreddits", []) if str(name).strip()]
        limit = max(1, min(300, int(self.config.get("limit", 80))))
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]
        since_utc = since.astimezone(timezone.utc)

        queries = [query]
        if subreddits:
            queries = [f"subreddit:{name} {query}" for name in subreddits]

        items: list[FeedbackItem] = []
        for search_query in queries:
            scraper = RedditSearchScraper(search_query)
            for post in scraper.get_items():
                published_at = parse_datetime(getattr(post, "date", None))
                if not published_at:
                    continue
                if published_at.tzinfo is None:
                    published_at = published_at.replace(tzinfo=timezone.utc)
                if published_at.astimezone(timezone.utc) < since_utc:
                    break

                title = clean_content_text(getattr(post, "title", "") or "")
                body = clean_content_text(getattr(post, "selftext", "") or "")
                if not title:
                    continue
                if not self.is_relevant(title, body):
                    continue
                if include_keywords:
                    blob = f"{title} {body}".lower()
                    if not any(keyword in blob for keyword in include_keywords):
                        continue

                source_item_id = str(getattr(post, "id", "")).strip() or None
                url = str(getattr(post, "url", "")).strip()
                if not url:
                    continue
                author = clean_content_text(str(getattr(post, "author", "") or ""))
                subreddit_name = clean_content_text(str(getattr(post, "subreddit", "") or ""))
                summary = ""
                if body and not is_summary_redundant(title, body):
                    summary = truncate(body, 240)

                items.append(
                    FeedbackItem(
                        source="reddit_snscrape",
                        source_item_id=source_item_id,
                        title=title,
                        url=url,
                        content=body or title,
                        summary=summary,
                        published_at=published_at,
                        author=author or None,
                        source_section=f"r/{subreddit_name}" if subreddit_name else "reddit",
                        extra={
                            "collector": "snscrape",
                            "query": search_query,
                            "score": getattr(post, "score", None),
                            "num_comments": getattr(post, "numComments", None),
                        },
                    )
                )
                if len(items) >= limit:
                    return items
        return items

    def _fetch_via_reddit_json(self, since: datetime) -> list[FeedbackItem]:
        query = str(self.config.get("query", "")).strip() or self._default_query()
        subreddits = [str(name).strip() for name in self.config.get("subreddits", []) if str(name).strip()]
        limit = max(1, min(300, int(self.config.get("limit", 80))))
        timeout = max(8, int(self.config.get("timeout_seconds", 30)))
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]
        user_agent = str(self.config.get("user_agent", "MediaPulse/1.0")).strip() or "MediaPulse/1.0"
        since_utc = since.astimezone(timezone.utc)

        if not subreddits:
            subreddits = ["all"]

        items: list[FeedbackItem] = []
        for subreddit in subreddits:
            url = f"https://www.reddit.com/r/{subreddit}/search.json"
            try:
                response = requests.get(
                    url,
                    params={
                        "q": query,
                        "restrict_sr": "1",
                        "sort": "new",
                        "limit": min(100, limit),
                        "type": "link",
                    },
                    headers={"User-Agent": user_agent},
                    timeout=timeout,
                )
                response.raise_for_status()
                payload = response.json()
            except Exception:
                continue

            children = payload.get("data", {}).get("children", []) or []
            for child in children:
                data = child.get("data", {}) if isinstance(child, dict) else {}
                published_at = parse_datetime(data.get("created_utc"))
                if not published_at:
                    continue
                if published_at.astimezone(timezone.utc) < since_utc:
                    continue

                title = clean_content_text(data.get("title", ""))
                body = clean_content_text(data.get("selftext", ""))
                if not title:
                    continue
                if not self.is_relevant(title, body):
                    continue
                if include_keywords:
                    blob = f"{title} {body}".lower()
                    if not any(keyword in blob for keyword in include_keywords):
                        continue

                permalink = clean_content_text(data.get("permalink", ""))
                if permalink and not permalink.startswith("http"):
                    post_url = f"https://www.reddit.com{permalink}"
                else:
                    post_url = clean_content_text(data.get("url", ""))
                if not post_url:
                    continue

                summary = ""
                if body and not is_summary_redundant(title, body):
                    summary = truncate(body, 240)

                items.append(
                    FeedbackItem(
                        source="reddit_snscrape",
                        source_item_id=str(data.get("id", "")).strip() or None,
                        title=title,
                        url=post_url,
                        content=body or title,
                        summary=summary,
                        published_at=published_at,
                        author=clean_content_text(data.get("author", "")) or None,
                        source_section=f"r/{subreddit}",
                        extra={
                            "collector": "reddit_json_fallback",
                            "query": query,
                            "score": data.get("score"),
                            "num_comments": data.get("num_comments"),
                        },
                    )
                )
                if len(items) >= limit:
                    return items
        return items

    def _default_query(self) -> str:
        if self.product_keywords:
            joined = " OR ".join(f"\"{item}\"" for item in self.product_keywords[:3] if item)
            if joined:
                return f"({joined}) (camera OR photo OR video)"
        return "\"Target Phone\" (camera OR photo OR video)"
