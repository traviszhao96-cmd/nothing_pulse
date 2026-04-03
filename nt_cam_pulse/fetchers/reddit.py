from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import requests

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class RedditOAuthCollector(BaseCollector):
    TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
    API_BASE = "https://oauth.reddit.com"
    PULLPUSH_BASE = "https://api.pullpush.io/reddit/search"

    def fetch(self, since: datetime) -> list[FeedbackItem]:
        since = self._effective_since(since)
        client_id = clean_content_text(str(self.config.get("client_id", "")))
        client_secret = clean_content_text(str(self.config.get("client_secret", "")))
        user_agent = str(self.config.get("user_agent", "NothingFeedbackBot/1.0")).strip() or "NothingFeedbackBot/1.0"
        query = str(self.config.get("query", "\"Nothing Phone\"")).strip() or "\"Nothing Phone\""
        limit = max(1, min(300, int(self.config.get("limit", 80))))
        per_subreddit_limit = max(1, min(200, int(self.config.get("per_subreddit_limit", limit))))
        subreddits = [str(name).strip() for name in self.config.get("subreddits", []) if str(name).strip()]
        include_comments = bool(self.config.get("include_comments", True))
        include_submissions = bool(self.config.get("include_submissions", True))
        allow_fallback = bool(self.config.get("allow_pullpush_fallback", True))

        if not subreddits:
            subreddits = ["all"]

        if client_id and client_secret:
            try:
                token = self._get_token(client_id, client_secret, user_agent)
                return self._fetch_via_oauth(
                    token=token,
                    user_agent=user_agent,
                    query=query,
                    since=since,
                    subreddits=subreddits,
                    limit=limit,
                    per_subreddit_limit=per_subreddit_limit,
                    include_submissions=include_submissions,
                    include_comments=include_comments,
                )
            except Exception:
                if not allow_fallback:
                    raise

        return self._fetch_via_pullpush(
            query=query,
            since=since,
            subreddits=subreddits,
            limit=limit,
            per_subreddit_limit=per_subreddit_limit,
            include_submissions=include_submissions,
            include_comments=include_comments,
        )

    def _effective_since(self, since: datetime) -> datetime:
        try:
            lookback_days = int(self.config.get("lookback_days", 0))
        except (TypeError, ValueError):
            lookback_days = 0
        lookback_days = max(0, min(3650, lookback_days))
        if lookback_days <= 0:
            return since
        override_since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        current = since.astimezone(timezone.utc)
        return override_since if override_since < current else current

    def _fetch_via_oauth(
        self,
        token: str,
        user_agent: str,
        query: str,
        since: datetime,
        subreddits: list[str],
        limit: int,
        per_subreddit_limit: int,
        include_submissions: bool,
        include_comments: bool,
    ) -> list[FeedbackItem]:
        headers = {
            "Authorization": f"bearer {token}",
            "User-Agent": user_agent,
        }
        since_utc = since.astimezone(timezone.utc)
        seen: set[str] = set()
        items: list[FeedbackItem] = []

        for subreddit in subreddits:
            if len(items) >= limit:
                break
            fetch_plan: list[tuple[str, str]] = []
            if include_submissions:
                fetch_plan.append(("search", "link"))
                fetch_plan.append(("new", "link"))
            if include_comments:
                fetch_plan.append(("search", "comment"))

            for mode, record_type in fetch_plan:
                if len(items) >= limit:
                    break
                payload = self._oauth_fetch(
                    headers=headers,
                    subreddit=subreddit,
                    query=query,
                    mode=mode,
                    record_type=record_type,
                    per_subreddit_limit=per_subreddit_limit,
                )
                children = payload.get("data", {}).get("children", []) if isinstance(payload, dict) else []
                for child in children:
                    if not isinstance(child, dict):
                        continue
                    kind = str(child.get("kind", "")).strip().lower()
                    data = child.get("data", {}) if isinstance(child.get("data"), dict) else {}
                    if kind == "t3":
                        item = self._parse_submission(
                            data=data,
                            subreddit=subreddit,
                            source="reddit",
                            collector="reddit_oauth_submission",
                        )
                    elif kind == "t1":
                        item = self._parse_comment(
                            data=data,
                            subreddit=subreddit,
                            source="reddit",
                            collector="reddit_oauth_comment",
                        )
                    else:
                        item = None

                    if not item:
                        continue
                    if item.published_at.astimezone(timezone.utc) < since_utc:
                        continue
                    if not self.is_relevant(item.title, item.content, item.summary or ""):
                        continue
                    dedupe_key = f"{item.source_item_id}|{item.url}"
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    items.append(item)
                    if len(items) >= limit:
                        break
        return items

    def _oauth_fetch(
        self,
        headers: dict[str, str],
        subreddit: str,
        query: str,
        mode: str,
        record_type: str,
        per_subreddit_limit: int,
    ) -> dict[str, Any]:
        if mode == "new":
            url = f"{self.API_BASE}/r/{subreddit}/new"
            params: dict[str, Any] = {"limit": per_subreddit_limit}
        else:
            url = f"{self.API_BASE}/r/{subreddit}/search"
            params = {
                "q": query,
                "sort": "new",
                "restrict_sr": "true",
                "limit": per_subreddit_limit,
                "t": str(self.config.get("time_filter", "month")),
                "type": record_type,
            }
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("error"):
            raise RuntimeError(f"reddit_oauth_error: {payload.get('message') or payload.get('error')}")
        return payload

    def _fetch_via_pullpush(
        self,
        query: str,
        since: datetime,
        subreddits: list[str],
        limit: int,
        per_subreddit_limit: int,
        include_submissions: bool,
        include_comments: bool,
    ) -> list[FeedbackItem]:
        since_utc = since.astimezone(timezone.utc)
        after = int(since_utc.timestamp())
        pullpush_base = str(self.config.get("pullpush_base_url", self.PULLPUSH_BASE)).rstrip("/")
        timeout = max(10, int(self.config.get("pullpush_timeout_seconds", 30)))
        user_agent = str(self.config.get("user_agent", "NothingFeedbackBot/1.0")).strip() or "NothingFeedbackBot/1.0"

        seen: set[str] = set()
        items: list[FeedbackItem] = []

        for subreddit in subreddits:
            if len(items) >= limit:
                break
            if include_submissions:
                endpoint = f"{pullpush_base}/submission/"
                for row in self._pullpush_fetch(
                    endpoint=endpoint,
                    query=query,
                    subreddit=subreddit,
                    size=per_subreddit_limit,
                    after=after,
                    timeout=timeout,
                    user_agent=user_agent,
                ):
                    item = self._parse_submission(
                        data=row,
                        subreddit=subreddit,
                        source="reddit",
                        collector="pullpush_submission",
                    )
                    if not item:
                        continue
                    if item.published_at.astimezone(timezone.utc) < since_utc:
                        continue
                    if not self.is_relevant(item.title, item.content, item.summary or ""):
                        continue
                    key = f"{item.source_item_id}|{item.url}"
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append(item)
                    if len(items) >= limit:
                        return items

            if include_comments:
                endpoint = f"{pullpush_base}/comment/"
                for row in self._pullpush_fetch(
                    endpoint=endpoint,
                    query=query,
                    subreddit=subreddit,
                    size=per_subreddit_limit,
                    after=after,
                    timeout=timeout,
                    user_agent=user_agent,
                ):
                    item = self._parse_comment(
                        data=row,
                        subreddit=subreddit,
                        source="reddit",
                        collector="pullpush_comment",
                    )
                    if not item:
                        continue
                    if item.published_at.astimezone(timezone.utc) < since_utc:
                        continue
                    if not self.is_relevant(item.title, item.content, item.summary or ""):
                        continue
                    key = f"{item.source_item_id}|{item.url}"
                    if key in seen:
                        continue
                    seen.add(key)
                    items.append(item)
                    if len(items) >= limit:
                        return items

        return items

    def _pullpush_fetch(
        self,
        endpoint: str,
        query: str,
        subreddit: str,
        size: int,
        after: int,
        timeout: int,
        user_agent: str,
    ) -> list[dict[str, Any]]:
        params = {
            "q": query,
            "subreddit": subreddit,
            "size": max(1, min(500, int(size))),
            "sort": "desc",
            "sort_type": "created_utc",
            "after": after,
        }
        response = requests.get(
            endpoint,
            params=params,
            timeout=timeout,
            headers={"User-Agent": user_agent},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return []
        rows = payload.get("data", [])
        if not isinstance(rows, list):
            return []
        return [row for row in rows if isinstance(row, dict)]

    @staticmethod
    def _parse_submission(
        data: dict[str, Any],
        subreddit: str,
        source: str,
        collector: str,
    ) -> FeedbackItem | None:
        title = clean_content_text(data.get("title", ""))
        body = clean_content_text(data.get("selftext", ""))
        if not title:
            return None
        published_at = parse_datetime(data.get("created_utc"))
        if not published_at:
            return None

        permalink = clean_content_text(data.get("permalink", ""))
        url = f"https://www.reddit.com{permalink}" if permalink and not permalink.startswith("http") else clean_content_text(data.get("url", ""))
        if not url:
            return None

        summary = ""
        if body and not is_summary_redundant(title, body):
            summary = truncate(body, 240)

        return FeedbackItem(
            source=source,
            source_item_id=clean_content_text(str(data.get("id", ""))) or None,
            title=title,
            url=url,
            content=body or title,
            summary=summary,
            published_at=published_at,
            author=clean_content_text(str(data.get("author", ""))) or None,
            source_section=f"r/{subreddit}",
            extra={
                "collector": collector,
                "score": data.get("score"),
                "num_comments": data.get("num_comments"),
            },
        )

    @staticmethod
    def _parse_comment(
        data: dict[str, Any],
        subreddit: str,
        source: str,
        collector: str,
    ) -> FeedbackItem | None:
        body = clean_content_text(data.get("body", ""))
        link_title = clean_content_text(data.get("link_title", "")) or clean_content_text(data.get("title", ""))
        if not body:
            return None
        published_at = parse_datetime(data.get("created_utc"))
        if not published_at:
            return None
        permalink = clean_content_text(data.get("permalink", ""))
        if permalink and not permalink.startswith("http"):
            url = f"https://www.reddit.com{permalink}"
        else:
            url = clean_content_text(str(data.get("url", "")))
        if not url:
            return None

        title = truncate(link_title or f"Comment in r/{subreddit}", 120)
        summary = "" if is_summary_redundant(title, body) else truncate(body, 240)

        return FeedbackItem(
            source=source,
            source_item_id=clean_content_text(str(data.get("id", ""))) or None,
            title=title,
            url=url,
            content=body,
            summary=summary,
            published_at=published_at,
            author=clean_content_text(str(data.get("author", ""))) or None,
            source_section=f"r/{subreddit} · comment",
            extra={
                "collector": collector,
                "score": data.get("score"),
                "link_id": data.get("link_id"),
                "parent_id": data.get("parent_id"),
            },
        )

    def _get_token(self, client_id: str, client_secret: str, user_agent: str) -> str:
        response = requests.post(
            self.TOKEN_URL,
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            headers={"User-Agent": user_agent},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        access_token = str(payload.get("access_token", "")).strip()
        if not access_token:
            raise RuntimeError(f"reddit_auth_failed: {payload}")
        return access_token
