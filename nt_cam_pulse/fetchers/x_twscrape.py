from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class XTWScrapeCollector(BaseCollector):
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        try:
            from twscrape import API
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("twscrape_not_installed_for_x") from exc

        query = str(self.config.get("query", "")).strip() or self._default_query()
        limit = max(1, min(200, int(self.config.get("limit", 50))))
        db_path = str(self.config.get("db_path", "./data/twscrape_accounts.db")).strip() or "./data/twscrape_accounts.db"
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]
        since_utc = since.astimezone(timezone.utc)

        async def _run() -> list[FeedbackItem]:
            api = API(db_path)
            items: list[FeedbackItem] = []
            async for tweet in api.search(query, limit=limit):
                parsed = _tweet_to_feedback(
                    tweet=tweet,
                    query=query,
                    include_keywords=include_keywords,
                    since=since_utc,
                    relevance_check=self.is_relevant,
                )
                if not parsed:
                    continue
                items.append(parsed)
                if len(items) >= limit:
                    break
            return items

        try:
            return asyncio.run(_run())
        except RuntimeError:
            # Already in an event loop (rare in our CLI usage); fallback.
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(_run())
            finally:
                loop.close()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"twscrape_fetch_failed: {exc}") from exc

    def _default_query(self) -> str:
        if self.product_keywords:
            joined = " OR ".join(f"\"{item}\"" for item in self.product_keywords[:3] if item)
            if joined:
                return f"({joined}) (camera OR photo OR video)"
        return "\"Nothing Phone\" (camera OR photo OR video)"


def _tweet_to_feedback(
    tweet: Any,
    query: str,
    include_keywords: list[str],
    since: datetime,
    relevance_check: Any,
) -> FeedbackItem | None:
    published_at = parse_datetime(getattr(tweet, "date", None))
    if not published_at:
        return None
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)
    if published_at.astimezone(timezone.utc) < since:
        return None

    text = clean_content_text(getattr(tweet, "rawContent", "") or getattr(tweet, "content", "") or "")
    if not text:
        return None
    if not relevance_check(text):
        return None
    if include_keywords and not any(keyword in text.lower() for keyword in include_keywords):
        return None

    tweet_id = str(getattr(tweet, "id", "")).strip()
    user = getattr(tweet, "user", None)
    username = clean_content_text(getattr(user, "username", "") or "")
    display_name = clean_content_text(getattr(user, "displayname", "") or "")
    author = display_name or username

    url = str(getattr(tweet, "url", "")).strip()
    if not url and username and tweet_id:
        url = f"https://x.com/{username}/status/{tweet_id}"
    if not url:
        return None

    title = truncate(text, 120)
    summary = "" if is_summary_redundant(title, text) else truncate(text, 240)
    media = getattr(tweet, "media", None)
    has_video = bool(media)

    return FeedbackItem(
        source="x_twscrape",
        source_item_id=tweet_id or None,
        title=title,
        url=url,
        content=text,
        summary=summary,
        published_at=published_at,
        author=author or None,
        source_section="X",
        video_candidate=has_video,
        extra={
            "collector": "twscrape",
            "query": query,
            "username": username,
            "lang": getattr(tweet, "lang", None),
            "like_count": getattr(tweet, "likeCount", None),
            "reply_count": getattr(tweet, "replyCount", None),
            "retweet_count": getattr(tweet, "retweetCount", None),
            "quote_count": getattr(tweet, "quoteCount", None),
        },
    )
