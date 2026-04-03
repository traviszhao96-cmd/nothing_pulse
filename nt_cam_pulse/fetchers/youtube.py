from __future__ import annotations

from datetime import datetime, timezone

import requests

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class YouTubeSearchCollector(BaseCollector):
    SEARCH_API_URL = "https://www.googleapis.com/youtube/v3/search"

    def fetch(self, since: datetime) -> list[FeedbackItem]:
        api_key = str(self.config.get("api_key", "")).strip()
        if not api_key:
            raise RuntimeError("youtube.api_key is required when sources.youtube.enabled=true")

        query = str(self.config.get("query", "")).strip() or self._default_query()
        limit = max(1, min(200, int(self.config.get("limit", 30))))
        region_code = str(self.config.get("region_code", "")).strip().upper()
        language = str(self.config.get("language", "")).strip()
        order = str(self.config.get("order", "date")).strip().lower() or "date"
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]
        user_agent = str(self.config.get("user_agent", "NothingCameraPulse/1.0")).strip() or "NothingCameraPulse/1.0"

        if order not in {"date", "rating", "relevance", "title", "videoCount", "viewCount"}:
            order = "date"

        since_utc = since.astimezone(timezone.utc)
        since_text = since_utc.isoformat().replace("+00:00", "Z")

        results: list[FeedbackItem] = []
        page_token: str | None = None
        fetched_pages = 0
        while len(results) < limit and fetched_pages < 20:
            batch_size = min(50, max(1, limit - len(results)))
            params: dict[str, str | int] = {
                "part": "snippet",
                "type": "video",
                "maxResults": batch_size,
                "q": query,
                "order": order,
                "publishedAfter": since_text,
                "key": api_key,
            }
            if region_code:
                params["regionCode"] = region_code
            if language:
                params["relevanceLanguage"] = language
            if page_token:
                params["pageToken"] = page_token

            response = requests.get(
                self.SEARCH_API_URL,
                params=params,
                timeout=30,
                headers={"User-Agent": user_agent},
            )
            response.raise_for_status()
            payload = response.json()
            if "error" in payload:
                raise RuntimeError(f"YouTube search failed: {payload['error']}")

            items = payload.get("items", []) or []
            for item in items:
                parsed_item = self._parse_item(
                    item=item,
                    query=query,
                    include_keywords=include_keywords,
                    since=since_utc,
                )
                if not parsed_item:
                    continue
                results.append(parsed_item)
                if len(results) >= limit:
                    break

            fetched_pages += 1
            page_token = str(payload.get("nextPageToken", "")).strip() or None
            if not page_token:
                break

        return results

    def _parse_item(
        self,
        item: dict,
        query: str,
        include_keywords: list[str],
        since: datetime,
    ) -> FeedbackItem | None:
        item_id = item.get("id", {}) if isinstance(item.get("id"), dict) else {}
        snippet = item.get("snippet", {}) if isinstance(item.get("snippet"), dict) else {}
        video_id = str(item_id.get("videoId", "")).strip()
        if not video_id:
            return None

        published_at = parse_datetime(snippet.get("publishedAt"))
        if not published_at:
            return None
        if published_at.astimezone(timezone.utc) < since:
            return None

        title = clean_content_text(snippet.get("title", ""))
        description = clean_content_text(snippet.get("description", ""))
        if not title:
            return None
        if not self.is_relevant(title, description):
            return None

        if include_keywords:
            blob = f"{title} {description}".lower()
            if not any(keyword in blob for keyword in include_keywords):
                return None

        summary = ""
        if description and not is_summary_redundant(title, description):
            summary = truncate(description, 240)

        channel_title = clean_content_text(snippet.get("channelTitle", ""))
        url = f"https://www.youtube.com/watch?v={video_id}"

        return FeedbackItem(
            source="youtube",
            source_item_id=video_id,
            title=title,
            url=url,
            content=description or title,
            summary=summary,
            published_at=published_at,
            author=channel_title or None,
            source_section="YouTube",
            video_candidate=True,
            extra={
                "query": query,
                "channel_id": str(snippet.get("channelId", "")).strip(),
                "youtube_kind": str(item_id.get("kind", "")).strip(),
            },
        )

    def _default_query(self) -> str:
        if self.product_keywords:
            items = [keyword for keyword in self.product_keywords[:3] if keyword]
            if items:
                joined = " OR ".join(f"\"{item}\"" for item in items)
                return f"({joined}) camera review"
        return "Nothing Phone camera review"
