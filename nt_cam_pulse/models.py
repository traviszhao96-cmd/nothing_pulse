from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class FeedbackItem:
    source: str
    title: str
    url: str
    content: str
    published_at: datetime
    source_item_id: str | None = None
    author: str | None = None
    source_section: str | None = None
    summary: str | None = None

    camera_category: str = "未分类"
    sentiment: str = "neutral"
    severity: str = "low"
    source_actor_type: str = "unknown"
    source_actor_reason: str = ""
    domain_tag: str = "未分类"
    domain_subtags: list[str] = field(default_factory=list)
    sentiment_reason: str = ""
    ai_positive_points: list[str] = field(default_factory=list)
    ai_neutral_points: list[str] = field(default_factory=list)
    ai_negative_points: list[str] = field(default_factory=list)
    video_candidate: bool = False

    product_tags: list[str] = field(default_factory=list)
    camera_keyword_hits: list[str] = field(default_factory=list)
    camera_related: bool = True
    token_set: list[str] = field(default_factory=list)

    language: str = "unknown"
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class PipelineResult:
    fetched: int = 0
    kept_camera_only: int = 0
    skipped_non_camera: int = 0
    retained_non_camera: int = 0
    skipped_duplicates: int = 0
    inserted: int = 0
    ai_enriched: int = 0
    ai_failed: int = 0
    synced_to_lark: int = 0
    report_path: str | None = None
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class DailyStats:
    report_date: date
    total: int
    high_risk: int
    categories: dict[str, int]
    sentiments: dict[str, int]
