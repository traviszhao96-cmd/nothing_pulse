from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class InstagramInstaloaderCollector(BaseCollector):
    def fetch(self, since: datetime) -> list[FeedbackItem]:
        try:
            import instaloader
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError("instaloader_not_installed_for_instagram") from exc

        timeout = max(10, int(self.config.get("timeout_seconds", 45)))
        max_posts = max(1, min(500, int(self.config.get("limit", 80))))
        hashtags = [str(tag).strip().lstrip("#") for tag in self.config.get("hashtags", []) if str(tag).strip()]
        profiles = [str(name).strip().lstrip("@") for name in self.config.get("profiles", []) if str(name).strip()]
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]

        loader = instaloader.Instaloader(
            quiet=True,
            download_pictures=False,
            download_videos=False,
            download_video_thumbnails=False,
            download_comments=False,
            save_metadata=False,
            compress_json=False,
            max_connection_attempts=3,
            request_timeout=timeout,
        )
        username = str(self.config.get("username", "")).strip()
        password = str(self.config.get("password", "")).strip()
        if username and password:
            loader.login(username, password)

        if not hashtags and not profiles:
            if self.product_keywords:
                hashtags = [self.product_keywords[0].replace(" ", "")]

        since_utc = since.astimezone(timezone.utc)
        items: list[FeedbackItem] = []

        for tag in hashtags:
            if len(items) >= max_posts:
                break
            try:
                hashtag = instaloader.Hashtag.from_name(loader.context, tag)
                posts = hashtag.get_posts()
            except Exception:
                continue
            self._collect_posts(
                posts=posts,
                since=since_utc,
                include_keywords=include_keywords,
                source_section=f"#{tag}",
                max_posts=max_posts,
                output=items,
            )

        for profile_name in profiles:
            if len(items) >= max_posts:
                break
            try:
                profile = instaloader.Profile.from_username(loader.context, profile_name)
                posts = profile.get_posts()
            except Exception:
                continue
            self._collect_posts(
                posts=posts,
                since=since_utc,
                include_keywords=include_keywords,
                source_section=f"@{profile_name}",
                max_posts=max_posts,
                output=items,
            )

        return items

    def _collect_posts(
        self,
        posts: Iterable,
        since: datetime,
        include_keywords: list[str],
        source_section: str,
        max_posts: int,
        output: list[FeedbackItem],
    ) -> None:
        for post in posts:
            if len(output) >= max_posts:
                break
            published_at = parse_datetime(getattr(post, "date_utc", None) or getattr(post, "date", None))
            if not published_at:
                continue
            if published_at.tzinfo is None:
                published_at = published_at.replace(tzinfo=timezone.utc)
            if published_at.astimezone(timezone.utc) < since:
                break

            caption = clean_content_text(getattr(post, "caption", "") or "")
            owner = clean_content_text(getattr(post, "owner_username", "") or "")
            shortcode = clean_content_text(getattr(post, "shortcode", "") or "")
            if not shortcode:
                continue
            title = truncate(caption, 120) if caption else f"Instagram post by @{owner or 'unknown'}"
            if not self.is_relevant(title, caption):
                continue
            if include_keywords:
                blob = f"{title} {caption}".lower()
                if not any(keyword in blob for keyword in include_keywords):
                    continue

            url = f"https://www.instagram.com/p/{shortcode}/"
            summary = ""
            if caption and not is_summary_redundant(title, caption):
                summary = truncate(caption, 240)

            output.append(
                FeedbackItem(
                    source="instagram_instaloader",
                    source_item_id=shortcode,
                    title=title,
                    url=url,
                    content=caption or title,
                    summary=summary,
                    published_at=published_at,
                    author=owner or None,
                    source_section=source_section,
                    video_candidate=bool(getattr(post, "is_video", False)),
                    extra={
                        "collector": "instaloader",
                        "likes": getattr(post, "likes", None),
                        "comments": getattr(post, "comments", None),
                    },
                )
            )
