from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from ..models import FeedbackItem
from ..utils import normalize_text, parse_datetime, truncate
from .base import BaseCollector


class MockFileCollector(BaseCollector):
    """Offline sample source for smoke tests and local demos."""

    def fetch(self, since: datetime) -> list[FeedbackItem]:
        file_path = Path(str(self.config.get("path", "./examples/sample_feedback.json"))).expanduser().resolve()
        if not file_path.exists():
            return []

        payload = json.loads(file_path.read_text(encoding="utf-8"))
        items: list[FeedbackItem] = []
        for raw in payload:
            published_at = parse_datetime(raw.get("published_at"))
            if not published_at or published_at < since:
                continue

            title = normalize_text(str(raw.get("title", "")))
            summary = normalize_text(str(raw.get("summary", "")))
            content = normalize_text(str(raw.get("content", summary or title)))
            if not self.is_relevant(title, summary, content):
                continue

            items.append(
                FeedbackItem(
                    source="mock_file",
                    source_item_id=str(raw.get("id") or ""),
                    title=title,
                    url=str(raw.get("url") or "https://example.com"),
                    content=content,
                    summary=truncate(summary or content, 240),
                    published_at=published_at,
                    author=raw.get("author"),
                    source_section=raw.get("source_section") or "mock",
                )
            )
        return items
