from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class XAPICollector(BaseCollector):
    SEARCH_API_BASE = "https://api.x.com/2/tweets/search"
    TWEET_FIELDS = ",".join(
        [
            "author_id",
            "attachments",
            "conversation_id",
            "created_at",
            "in_reply_to_user_id",
            "lang",
            "public_metrics",
            "referenced_tweets",
        ]
    )
    USER_FIELDS = "id,name,username,verified"
    MEDIA_FIELDS = "media_key,type,preview_image_url,url"
    EXPANSIONS = "author_id,attachments.media_keys"

    def fetch(self, since: datetime) -> list[FeedbackItem]:
        bearer_token = clean_content_text(str(self.config.get("bearer_token", "")))
        if not bearer_token:
            raise RuntimeError("x_api.bearer_token is required when sources.x_api.enabled=true")

        base_query = str(self.config.get("query", "")).strip() or self._default_query()
        limit = max(1, min(500, int(self.config.get("limit", 50))))
        max_pages = max(1, min(20, int(self.config.get("max_pages", 5))))
        search_scope = str(self.config.get("search_scope", "recent")).strip().lower() or "recent"
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]
        timeout = max(10, int(self.config.get("timeout_seconds", 30)))
        user_agent = str(self.config.get("user_agent", "NothingCameraPulse/1.0")).strip() or "NothingCameraPulse/1.0"
        exclude_retweets = bool(self.config.get("exclude_retweets", True))
        exclude_replies_in_search = bool(self.config.get("exclude_replies_in_search", True))
        include_replies = bool(self.config.get("include_replies", True))
        include_self_replies = bool(self.config.get("include_self_replies", True))
        replies_per_post = max(0, min(200, int(self.config.get("replies_per_post", 20))))
        max_total_replies = max(0, min(1000, int(self.config.get("max_total_replies", 200))))
        max_reply_pages = max(1, min(10, int(self.config.get("max_reply_pages", 3))))

        if search_scope not in {"recent", "all"}:
            search_scope = "recent"

        since_utc = since.astimezone(timezone.utc)
        effective_since = self._effective_since(since_utc, search_scope)
        effective_query = self._build_search_query(
            base_query,
            exclude_retweets=exclude_retweets,
            exclude_replies=exclude_replies_in_search,
        )

        session = requests.Session()
        session.headers.update(
            {
                "Authorization": f"Bearer {bearer_token}",
                "User-Agent": user_agent,
            }
        )

        root_rows = self._search_posts(
            session=session,
            search_scope=search_scope,
            query=effective_query,
            since=effective_since,
            limit=limit,
            max_pages=max_pages,
            timeout=timeout,
        )

        items: list[FeedbackItem] = []
        eligible_root_rows: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for row in root_rows:
            item = self._parse_post(
                row=row,
                query=base_query,
                effective_query=effective_query,
                since=effective_since,
                include_keywords=include_keywords,
                record_type="post",
            )
            if not item:
                continue
            item_id = str(item.source_item_id or "").strip()
            if item_id and item_id in seen_ids:
                continue
            if item_id:
                seen_ids.add(item_id)
            items.append(item)
            eligible_root_rows.append(row)

        if not include_replies or replies_per_post <= 0 or max_total_replies <= 0:
            return items

        reply_total = 0
        for row in eligible_root_rows:
            if reply_total >= max_total_replies:
                break
            remaining = min(replies_per_post, max_total_replies - reply_total)
            if remaining <= 0:
                break

            root_post_id = clean_content_text(str(row.get("id", "")))
            if not root_post_id:
                continue
            reply_query = self._build_reply_query(
                root_row=row,
                include_self_replies=include_self_replies,
            )
            if not reply_query:
                continue
            reply_rows = self._search_posts(
                session=session,
                search_scope=search_scope,
                query=reply_query,
                since=effective_since,
                limit=remaining,
                max_pages=max_reply_pages,
                timeout=timeout,
            )
            for reply_row in reply_rows:
                reply_id = clean_content_text(str(reply_row.get("id", "")))
                if not reply_id or reply_id == root_post_id or reply_id in seen_ids:
                    continue
                item = self._parse_post(
                    row=reply_row,
                    query=base_query,
                    effective_query=reply_query,
                    since=effective_since,
                    include_keywords=include_keywords,
                    record_type="reply",
                    root_row=row,
                )
                if not item:
                    continue
                seen_ids.add(reply_id)
                items.append(item)
                reply_total += 1
                if reply_total >= max_total_replies:
                    break

        return items

    def _effective_since(self, since: datetime, search_scope: str) -> datetime:
        if search_scope != "recent":
            return since
        seven_days_ago = datetime.now(tz=timezone.utc) - timedelta(days=7)
        return max(since, seven_days_ago)

    def _search_posts(
        self,
        session: requests.Session,
        search_scope: str,
        query: str,
        since: datetime,
        limit: int,
        max_pages: int,
        timeout: int,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []

        endpoint = f"{self.SEARCH_API_BASE}/{search_scope}"
        params: dict[str, Any] = {
            "query": query,
            "tweet.fields": self.TWEET_FIELDS,
            "user.fields": self.USER_FIELDS,
            "media.fields": self.MEDIA_FIELDS,
            "expansions": self.EXPANSIONS,
            "start_time": since.isoformat().replace("+00:00", "Z"),
        }

        rows: list[dict[str, Any]] = []
        next_token: str | None = None
        page_count = 0

        while len(rows) < limit and page_count < max_pages:
            page_size = min(100, max(10, limit - len(rows)))
            params["max_results"] = page_size
            if next_token:
                params["next_token"] = next_token
            else:
                params.pop("next_token", None)

            response = session.get(endpoint, params=params, timeout=timeout)
            payload = self._decode_response(response)
            rows.extend(self._hydrate_rows(payload))

            meta = payload.get("meta", {}) if isinstance(payload, dict) else {}
            next_token = clean_content_text(str(meta.get("next_token", ""))) or None
            page_count += 1
            if not next_token:
                break

        return rows[:limit]

    def _decode_response(self, response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError:
            payload = {}

        if response.status_code >= 400:
            message = self._error_message(payload) or response.text[:200]
            raise RuntimeError(f"x_api_request_failed[{response.status_code}]: {message}")
        if not isinstance(payload, dict):
            raise RuntimeError("x_api_request_failed: invalid_json_payload")
        if payload.get("errors"):
            raise RuntimeError(f"x_api_request_failed: {self._error_message(payload)}")
        return payload

    @staticmethod
    def _error_message(payload: dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return "unknown_error"
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            parts: list[str] = []
            for error in errors:
                if not isinstance(error, dict):
                    continue
                detail = clean_content_text(str(error.get("detail", "") or error.get("message", "")))
                title = clean_content_text(str(error.get("title", "")))
                value = detail or title
                if value:
                    parts.append(value)
            if parts:
                return "; ".join(parts)
        title = clean_content_text(str(payload.get("title", "")))
        detail = clean_content_text(str(payload.get("detail", "")))
        return detail or title or "unknown_error"

    @staticmethod
    def _hydrate_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
        includes = payload.get("includes", {}) if isinstance(payload.get("includes"), dict) else {}
        users = includes.get("users", []) if isinstance(includes.get("users"), list) else []
        media = includes.get("media", []) if isinstance(includes.get("media"), list) else []
        users_by_id = {clean_content_text(str(item.get("id", ""))): item for item in users if isinstance(item, dict)}
        media_by_key = {
            clean_content_text(str(item.get("media_key", ""))): item for item in media if isinstance(item, dict)
        }

        hydrated: list[dict[str, Any]] = []
        raw_rows = payload.get("data", []) if isinstance(payload.get("data"), list) else []
        for raw in raw_rows:
            if not isinstance(raw, dict):
                continue
            row = dict(raw)
            author_id = clean_content_text(str(row.get("author_id", "")))
            attachments = row.get("attachments", {}) if isinstance(row.get("attachments"), dict) else {}
            media_keys = attachments.get("media_keys", []) if isinstance(attachments.get("media_keys"), list) else []
            row["__author"] = users_by_id.get(author_id)
            row["__media"] = [media_by_key[key] for key in media_keys if key in media_by_key]
            hydrated.append(row)
        return hydrated

    def _parse_post(
        self,
        row: dict[str, Any],
        query: str,
        effective_query: str,
        since: datetime,
        include_keywords: list[str],
        record_type: str,
        root_row: dict[str, Any] | None = None,
    ) -> FeedbackItem | None:
        published_at = parse_datetime(row.get("created_at"))
        if not published_at:
            return None
        if published_at.astimezone(timezone.utc) < since:
            return None

        text = clean_content_text(str(row.get("text", "")))
        if not text:
            return None
        root_text = clean_content_text(str(root_row.get("text", ""))) if isinstance(root_row, dict) else ""
        is_reply = record_type == "reply"
        text_is_relevant = self.is_relevant(text)
        root_is_relevant = self.is_relevant(root_text) if root_text else False
        if not text_is_relevant and not (is_reply and root_is_relevant):
            return None

        if include_keywords:
            text_has_keyword = any(keyword in text.lower() for keyword in include_keywords)
            root_has_keyword = any(keyword in root_text.lower() for keyword in include_keywords) if root_text else False
            if not text_has_keyword and not (is_reply and root_has_keyword):
                return None

        post_id = clean_content_text(str(row.get("id", "")))
        if not post_id:
            return None

        author_info = row.get("__author", {}) if isinstance(row.get("__author"), dict) else {}
        username = clean_content_text(str(author_info.get("username", "")))
        display_name = clean_content_text(str(author_info.get("name", "")))
        author = display_name or username
        url = self._status_url(username=username, post_id=post_id)
        if not url:
            return None

        title = truncate(text, 120)
        summary = "" if is_summary_redundant(title, text) else truncate(text, 240)
        public_metrics = row.get("public_metrics", {}) if isinstance(row.get("public_metrics"), dict) else {}
        media_rows = row.get("__media", []) if isinstance(row.get("__media"), list) else []
        media_types = [
            clean_content_text(str(item.get("type", ""))).lower() for item in media_rows if isinstance(item, dict)
        ]
        root_author = root_row.get("__author", {}) if isinstance(root_row, dict) else {}
        root_username = ""
        if isinstance(root_author, dict):
            root_username = clean_content_text(str(root_author.get("username", "")))
        root_post_id = clean_content_text(str(root_row.get("id", ""))) if isinstance(root_row, dict) else ""

        return FeedbackItem(
            source="x_api",
            source_item_id=post_id,
            title=title,
            url=url,
            content=text,
            summary=summary,
            published_at=published_at,
            author=author or None,
            source_section="X · reply" if record_type == "reply" else "X",
            video_candidate=any(media_type in {"video", "animated_gif"} for media_type in media_types),
            extra={
                "collector": "x_api",
                "query": query,
                "effective_query": effective_query,
                "record_type": record_type,
                "username": username,
                "author_id": clean_content_text(str(row.get("author_id", ""))) or None,
                "conversation_id": clean_content_text(str(row.get("conversation_id", ""))) or None,
                "in_reply_to_user_id": clean_content_text(str(row.get("in_reply_to_user_id", ""))) or None,
                "lang": clean_content_text(str(row.get("lang", ""))) or None,
                "like_count": public_metrics.get("like_count"),
                "reply_count": public_metrics.get("reply_count"),
                "retweet_count": public_metrics.get("retweet_count", public_metrics.get("repost_count")),
                "repost_count": public_metrics.get("repost_count"),
                "quote_count": public_metrics.get("quote_count"),
                "bookmark_count": public_metrics.get("bookmark_count"),
                "impression_count": public_metrics.get("impression_count"),
                "media_types": media_types,
                "verified_author": bool(author_info.get("verified", False)),
                "root_post_id": root_post_id or None,
                "root_author_username": root_username or None,
                "root_url": self._status_url(root_username, root_post_id) if root_post_id and root_username else None,
            },
        )

    def _build_search_query(self, query: str, *, exclude_retweets: bool, exclude_replies: bool) -> str:
        parts = [query.strip()]
        lower = query.lower()
        if exclude_retweets and "is:retweet" not in lower and "-is:retweet" not in lower:
            parts.append("-is:retweet")
        if exclude_replies and "is:reply" not in lower and "-is:reply" not in lower and "conversation_id:" not in lower:
            parts.append("-is:reply")
        return " ".join(part for part in parts if part).strip()

    def _build_reply_query(self, root_row: dict[str, Any], include_self_replies: bool) -> str:
        root_post_id = clean_content_text(str(root_row.get("id", "")))
        if not root_post_id:
            return ""
        query = f"conversation_id:{root_post_id} -is:retweet"
        if not include_self_replies:
            author = root_row.get("__author", {}) if isinstance(root_row.get("__author"), dict) else {}
            username = clean_content_text(str(author.get("username", "")))
            if username:
                query = f"{query} -from:{username}"
        return query

    @staticmethod
    def _status_url(username: str, post_id: str) -> str:
        clean_username = clean_content_text(username).lstrip("@")
        clean_post_id = clean_content_text(post_id)
        if not clean_username or not clean_post_id:
            return ""
        return f"https://x.com/{clean_username}/status/{clean_post_id}"

    def _default_query(self) -> str:
        if self.product_keywords:
            joined = " OR ".join(f"\"{item}\"" for item in self.product_keywords[:3] if item)
            if joined:
                return f"({joined}) (camera OR photo OR video)"
        return "\"Nothing Phone\" (camera OR photo OR video)"
