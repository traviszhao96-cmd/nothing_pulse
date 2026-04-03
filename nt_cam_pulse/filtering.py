from __future__ import annotations

from dataclasses import dataclass

from .models import FeedbackItem
from .utils import build_exact_dedupe_key, jaccard_similarity, normalize_text, since_days, tokenize_text


class CameraScopeFilter:
    def __init__(self, camera_keywords: list[str]) -> None:
        self.camera_keywords = [keyword.lower() for keyword in camera_keywords]

    def is_camera_related(self, item: FeedbackItem) -> tuple[bool, list[str]]:
        blob = normalize_text(" ".join([item.title, item.summary or "", item.content])).lower()
        if not self.camera_keywords:
            return True, []
        hits = [keyword for keyword in self.camera_keywords if keyword in blob]
        return bool(hits), hits


@dataclass(slots=True)
class DedupeCandidate:
    exact_key: str
    tokens: list[str]


class SimilarityDeduper:
    def __init__(self, threshold: float, lookback_days: int, recent_candidates: list[DedupeCandidate]) -> None:
        self.threshold = threshold
        self.lookback_days = lookback_days
        self.candidates: list[DedupeCandidate] = list(recent_candidates)
        self.exact_keys = {candidate.exact_key for candidate in self.candidates}

    @classmethod
    def from_repository(
        cls,
        repository: "FeedbackRepository",
        threshold: float,
        lookback_days: int,
    ) -> "SimilarityDeduper":
        since = since_days(lookback_days)
        rows = repository.fetch_recent_dedupe_candidates(since)
        candidates = [DedupeCandidate(exact_key=row["dedupe_exact_key"], tokens=row["token_set"]) for row in rows]
        return cls(threshold=threshold, lookback_days=lookback_days, recent_candidates=candidates)

    def is_duplicate(self, item: FeedbackItem) -> tuple[bool, str | None]:
        exact_key = build_exact_dedupe_key(item.title, item.url)
        token_set = tokenize_text(" ".join([item.title, item.content, item.summary or ""]))
        item.token_set = token_set

        if exact_key in self.exact_keys:
            return True, "exact"

        if len(token_set) >= 4:
            for candidate in self.candidates:
                similarity = jaccard_similarity(token_set, candidate.tokens)
                if similarity >= self.threshold:
                    return True, f"near:{similarity:.2f}"

        self.exact_keys.add(exact_key)
        self.candidates.append(DedupeCandidate(exact_key=exact_key, tokens=token_set))
        item.extra["dedupe_exact_key"] = exact_key
        return False, None


# Import guard for type checking only.
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .storage import FeedbackRepository
