from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from ..models import FeedbackItem


class BaseCollector(ABC):
    def __init__(self, name: str, config: dict, product_keywords: list[str] | None = None) -> None:
        self.name = name
        self.config = config
        self.product_keywords = [keyword.lower() for keyword in (product_keywords or [])]

    @abstractmethod
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        raise NotImplementedError

    def is_relevant(self, *texts: str) -> bool:
        if not self.product_keywords:
            return True
        blob = " ".join(text.lower() for text in texts if text)
        return any(keyword in blob for keyword in self.product_keywords)
