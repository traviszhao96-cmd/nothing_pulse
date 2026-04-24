from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timezone

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class YouTubeYtDlpCollector(BaseCollector):
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        executable = str(self.config.get("executable", "yt-dlp")).strip() or "yt-dlp"
        raw_queries = self.config.get("queries", [])
        queries = [str(value).strip() for value in raw_queries if str(value).strip()]
        if not queries:
            fallback_query = str(self.config.get("query", "")).strip() or self._default_query()
            queries = [fallback_query]
        limit = max(1, min(100, int(self.config.get("limit", 30))))
        timeout = max(15, int(self.config.get("timeout_seconds", 90)))
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]

        since_utc = since.astimezone(timezone.utc)
        items: list[FeedbackItem] = []
        seen_ids: set[str] = set()
        total_queries = len(queries)
        for index, query in enumerate(queries, start=1):
            print(
                f"[youtube_yt_dlp] query {index}/{total_queries} start limit={limit} timeout={timeout}s q={query}",
                flush=True,
            )
            started_at = time.monotonic()
            completed = self._run_yt_dlp(executable=executable, query=query, limit=limit, timeout=timeout)
            raw_lines = [line for line in (completed.stdout or "").splitlines() if line.strip()]
            print(
                f"[youtube_yt_dlp] query {index}/{total_queries} done elapsed={time.monotonic() - started_at:.2f}s "
                f"returncode={completed.returncode} raw_results={len(raw_lines)}",
                flush=True,
            )
            accepted_before = len(items)
            for raw_line in raw_lines:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                item = self._parse_item(
                    data=data,
                    query=query,
                    include_keywords=include_keywords,
                    since=since_utc,
                )
                if not item:
                    continue
                if item.source_item_id in seen_ids:
                    continue
                seen_ids.add(item.source_item_id)
                items.append(item)
            print(
                f"[youtube_yt_dlp] query {index}/{total_queries} accepted={len(items) - accepted_before} cumulative={len(items)}",
                flush=True,
            )
        return items

    def _run_yt_dlp(self, executable: str, query: str, limit: int, timeout: int) -> subprocess.CompletedProcess:
        command = [
            executable,
            "--dump-json",
            "--skip-download",
            "--no-warnings",
            "--ignore-no-formats-error",
            f"ytsearch{limit}:{query}",
        ]
        try:
            completed = subprocess.run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(f"yt-dlp_not_found: {executable}") from exc
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"yt_dlp_run_failed: {exc}") from exc

        if completed.returncode != 0 and not (completed.stdout or "").strip():
            stderr = clean_content_text(completed.stderr or "")
            raise RuntimeError(f"yt_dlp_search_failed: {stderr[:240] or 'unknown_error'}")
        return completed

    def _parse_item(
        self,
        data: dict,
        query: str,
        include_keywords: list[str],
        since: datetime,
    ) -> FeedbackItem | None:
        video_id = str(data.get("id", "")).strip()
        title = clean_content_text(data.get("title", ""))
        description = clean_content_text(data.get("description", ""))
        if not video_id or not title:
            return None

        published_at = self._parse_published_at(data)
        if not published_at:
            return None
        if published_at.astimezone(timezone.utc) < since:
            return None

        if not self.is_relevant(title, description):
            return None
        if include_keywords:
            blob = f"{title} {description}".lower()
            if not any(keyword in blob for keyword in include_keywords):
                return None

        url = str(data.get("webpage_url", "")).strip() or f"https://www.youtube.com/watch?v={video_id}"
        uploader = clean_content_text(data.get("uploader", "") or data.get("channel", ""))
        summary = ""
        if description and not is_summary_redundant(title, description):
            summary = truncate(description, 240)

        return FeedbackItem(
            source="youtube_yt_dlp",
            source_item_id=video_id,
            title=title,
            url=url,
            content=description or title,
            summary=summary,
            published_at=published_at,
            author=uploader or None,
            source_section="YouTube",
            video_candidate=True,
            extra={
                "collector": "yt_dlp",
                "query": query,
                "channel_id": str(data.get("channel_id", "")).strip(),
                "view_count": data.get("view_count"),
                "like_count": data.get("like_count"),
                "duration": data.get("duration"),
            },
        )

    @staticmethod
    def _parse_published_at(data: dict) -> datetime | None:
        value = data.get("timestamp")
        if value is not None:
            parsed = parse_datetime(value)
            if parsed:
                return parsed

        date_text = clean_content_text(str(data.get("upload_date", "")))
        if len(date_text) == 8 and date_text.isdigit():
            try:
                return datetime.strptime(date_text, "%Y%m%d").replace(tzinfo=timezone.utc)
            except ValueError:
                return None
        return None

    def _default_query(self) -> str:
        if self.product_keywords:
            items = [keyword for keyword in self.product_keywords[:3] if keyword]
            if items:
                joined = " OR ".join(f"\"{item}\"" for item in items)
                return f"({joined}) camera review"
        return "Target Phone camera review"
