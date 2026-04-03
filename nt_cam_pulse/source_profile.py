from __future__ import annotations

from urllib.parse import urlparse

from .models import FeedbackItem
from .utils import normalize_text

SOURCE_LABELS = {
    "real_user": "真实购买用户",
    "official_kol": "官方KOL/媒体",
    "core_koc": "核心KOC/自媒体",
    "unknown": "待确认",
}

OFFICIAL_MEDIA_HINTS = {
    "gsmarena",
    "android authority",
    "engadget",
    "the verge",
    "xda",
    "techradar",
    "tom's guide",
    "trusted reviews",
    "pocket-lint",
    "cnet",
    "digital trends",
    "forbes",
    "phonearena",
    "mkbhd",
    "mrwhosetheboss",
}

VIDEO_PLATFORM_HINTS = {
    "youtube.com",
    "youtu.be",
    "bilibili.com",
    "douyin.com",
    "tiktok.com",
    "x.com",
    "twitter.com",
    "instagram.com",
}

REAL_USER_HINTS = {
    "i bought",
    "i purchased",
    "i own",
    "my phone",
    "my unit",
    "我买了",
    "我入手",
    "我的手机",
    "刚买",
    "用了",
    "购买",
    "退货",
}

REVIEW_HINTS = {"review", "hands-on", "vs", "camera test", "测评", "评测", "上手"}


class SourceProfiler:
    def classify(self, item: FeedbackItem) -> FeedbackItem:
        source = (item.source or "").lower()
        source_section = (item.source_section or "").lower()
        author = (item.author or "").lower()
        title = normalize_text(item.title).lower()
        text = normalize_text(" ".join([item.title, item.summary or "", item.content])).lower()
        source_blob = " ".join([source, source_section, author, self._domain(item.url)])

        if source in {"nothing_community", "reddit", "reddit_snscrape"}:
            item.source_actor_type = "real_user"
            item.source_actor_reason = "来源于社区/论坛用户发帖"
            return item

        if any(keyword in source_blob for keyword in OFFICIAL_MEDIA_HINTS):
            item.source_actor_type = "official_kol"
            item.source_actor_reason = "命中媒体/KOL来源站点"
            return item

        if any(keyword in source_blob for keyword in VIDEO_PLATFORM_HINTS):
            item.video_candidate = True
            if author:
                item.source_actor_type = "core_koc"
                item.source_actor_reason = "视频平台创作者内容"
            else:
                item.source_actor_type = "unknown"
                item.source_actor_reason = "视频来源但作者信息不足"
            return item

        if any(keyword in text for keyword in REAL_USER_HINTS):
            item.source_actor_type = "real_user"
            item.source_actor_reason = "文本中出现明显购买/持有表达"
            return item

        if any(keyword in title for keyword in REVIEW_HINTS):
            if source in {"google_news", "custom_rss"}:
                item.source_actor_type = "official_kol"
                item.source_actor_reason = "新闻/媒体渠道评测内容"
            else:
                item.source_actor_type = "core_koc"
                item.source_actor_reason = "评测向内容，偏自媒体创作者"
            return item

        item.source_actor_type = "unknown"
        item.source_actor_reason = "规则无法稳定判断"
        return item

    @staticmethod
    def _domain(url: str) -> str:
        if not url:
            return ""
        try:
            return urlparse(url).netloc.lower()
        except ValueError:
            return ""
