from __future__ import annotations

from collections import Counter
import re
from typing import Iterable

from .models import FeedbackItem
from .utils import detect_language, is_summary_redundant, normalize_text, tokenize_text, truncate

PRODUCT_KEYWORDS = {
    "4a pro": ["phone 4a pro", "phone4a pro", "4a pro", "4apro"],
    "4a": ["phone 4a", "phone4a", "phone (4a)", "4a"],
    "3a pro": ["phone 3a pro", "phone3a pro", "3a pro", "3apro", "a059p"],
    "3a": ["phone 3a", "phone3a", "phone (3a)", "3a", "a059"],
    "phone3": ["phone 3", "phone3", "phone (3)"],
    "2a": ["phone 2a", "phone2a", "phone (2a)", "2a", "a142"],
    "phone2": ["phone 2", "phone2", "phone (2)", "a063"],
    "accessory phone1": ["accessory phone 1", "accessory phone1", "accessory phone"],
    "target_os": ["target os", "tos"],
}

SENTIMENT_KEYWORDS = {
    "negative": [
        "bad",
        "worse",
        "terrible",
        "awful",
        "issue",
        "problem",
        "hate",
        "annoying",
        "broken",
        "失望",
        "糟糕",
        "问题",
        "投诉",
        "崩溃",
        "闪退",
    ],
    "positive": [
        "love",
        "great",
        "good",
        "better",
        "fixed",
        "improved",
        "awesome",
        "满意",
        "喜欢",
        "惊喜",
        "好评",
        "清晰",
        "稳定",
    ],
}

HIGH_SEVERITY_KEYWORDS = {
    "无法使用",
    "cannot use",
    "can't use",
    "camera dead",
    "black screen",
    "crash every",
    "cannot focus",
    "无法对焦",
    "严重发热",
    "过热关机",
}

MEDIUM_SEVERITY_KEYWORDS = {
    "issue",
    "problem",
    "lag",
    "stutter",
    "blurry",
    "noise",
    "focus",
    "exposure",
    "night mode",
    "卡顿",
    "模糊",
    "噪点",
    "对焦",
    "曝光",
    "发热",
}


class CameraClassifier:
    def __init__(self, camera_categories: dict[str, list[str]]) -> None:
        self.camera_categories = camera_categories

    def classify(self, item: FeedbackItem) -> FeedbackItem:
        title_text = normalize_text(item.title).lower()
        title_and_summary = normalize_text(" ".join([item.title, item.summary or ""])).lower()
        text = normalize_text(" ".join([item.title, item.content, item.summary or ""])).lower()
        item.language = detect_language(text)
        item.camera_category = self._match_primary(text, self.camera_categories, fallback="未分类")
        item.sentiment = self._score_sentiment(text)
        item.severity = self._score_severity(text, item.camera_category, item.sentiment)
        if not item.domain_tag or item.domain_tag == "未分类":
            item.domain_tag = item.camera_category
        if not item.sentiment_reason:
            item.sentiment_reason = "规则关键词打分"
        if not item.domain_subtags and item.camera_keyword_hits:
            item.domain_subtags = item.camera_keyword_hits[:4]
        # Prefer model evidence in title/summary to avoid over-tagging from long article body comparisons.
        item.product_tags = self._match_product_tags(title_and_summary)
        if not item.product_tags and title_text:
            item.product_tags = self._match_product_tags(title_text)
        if not item.product_tags:
            item.product_tags = self._match_product_tags(text)
        if item.summary:
            if is_summary_redundant(item.title, item.summary):
                item.summary = ""
        else:
            fallback_summary = truncate(normalize_text(item.content), 240)
            item.summary = "" if is_summary_redundant(item.title, fallback_summary) else fallback_summary
        if item.summary:
            if item.sentiment == "positive" and not item.ai_positive_points:
                item.ai_positive_points = [truncate(item.summary, 80)]
            elif item.sentiment == "negative" and not item.ai_negative_points:
                item.ai_negative_points = [truncate(item.summary, 80)]
            elif item.sentiment == "neutral" and not item.ai_neutral_points:
                item.ai_neutral_points = [truncate(item.summary, 80)]

        # Persist useful debug evidence for analysts.
        item.extra.setdefault("tokens", tokenize_text(text)[:80])
        item.extra.setdefault("category_keywords", self._matched_keywords(text, self.camera_categories))
        return item

    def _match_primary(self, text: str, mapping: dict[str, list[str]], fallback: str) -> str:
        counts = Counter()
        for label, keywords in mapping.items():
            for keyword in keywords:
                if keyword.lower() in text:
                    counts[label] += 1
        if not counts:
            return fallback
        return counts.most_common(1)[0][0]

    def _match_many(self, text: str, mapping: dict[str, list[str]]) -> list[str]:
        matched: list[str] = []
        for label, keywords in mapping.items():
            if any(keyword.lower() in text for keyword in keywords):
                matched.append(label)
        return matched

    def _match_product_tags(self, text: str) -> list[str]:
        normalized_text = self._normalize_for_keyword_match(text)
        matched: list[str] = []
        for label, keywords in PRODUCT_KEYWORDS.items():
            if any(self._contains_keyword(normalized_text, keyword) for keyword in keywords):
                if label == "4a" and "4a pro" in matched:
                    continue
                if label == "3a" and "3a pro" in matched:
                    continue
                matched.append(label)
        return matched

    @staticmethod
    def _normalize_for_keyword_match(text: str) -> str:
        compact = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", (text or "").lower())
        compact = normalize_text(compact)
        return f" {compact} " if compact else " "

    @classmethod
    def _contains_keyword(cls, normalized_text: str, keyword: str) -> bool:
        needle = cls._normalize_for_keyword_match(keyword).strip()
        if not needle:
            return False
        return f" {needle} " in normalized_text

    def _score_sentiment(self, text: str) -> str:
        score = 0
        for keyword in SENTIMENT_KEYWORDS["negative"]:
            if keyword in text:
                score -= 1
        for keyword in SENTIMENT_KEYWORDS["positive"]:
            if keyword in text:
                score += 1
        if score >= 2:
            return "positive"
        if score <= -1:
            return "negative"
        return "neutral"

    def _score_severity(self, text: str, category: str, sentiment: str) -> str:
        if any(keyword in text for keyword in HIGH_SEVERITY_KEYWORDS):
            return "high"

        medium_hit_count = sum(1 for keyword in MEDIUM_SEVERITY_KEYWORDS if keyword in text)
        critical_categories = {"对焦", "曝光", "视频", "性能发热"}

        if medium_hit_count >= 2:
            return "medium"
        if sentiment == "negative" and category in critical_categories:
            return "medium"
        return "low"

    def _matched_keywords(self, text: str, mapping: dict[str, list[str]]) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {}
        for label, keywords in mapping.items():
            hits = [keyword for keyword in keywords if keyword.lower() in text]
            if hits:
                result[label] = hits[:6]
        return result


def flatten_keywords(values: dict[str, Iterable[str]]) -> list[str]:
    merged: list[str] = []
    for keywords in values.values():
        merged.extend(keywords)
    return merged
