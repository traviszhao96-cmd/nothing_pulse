from __future__ import annotations

from datetime import datetime, timezone
import html
import re

import requests

from ..models import FeedbackItem
from ..utils import clean_content_text, is_summary_redundant, parse_datetime, truncate
from .base import BaseCollector


class BilibiliSearchCollector(BaseCollector):
    SEARCH_API_URL = "https://api.bilibili.com/x/web-interface/search/type"
    SEARCH_HTML_URL = "https://search.bilibili.com/all"
    VALID_ORDERS = {"totalrank", "click", "pubdate", "dm", "stow", "scores"}

    def fetch(self, since: datetime) -> list[FeedbackItem]:
        raw_queries = self.config.get("queries", [])
        queries = [str(value).strip() for value in raw_queries if str(value).strip()]
        if not queries:
            queries = [str(self.config.get("query", "")).strip() or self._default_query()]
        queries = self._expand_queries(queries)

        limit = max(1, min(200, int(self.config.get("limit", 30))))
        page_size = max(1, min(50, int(self.config.get("page_size", 20))))
        max_pages = max(1, min(10, int(self.config.get("max_pages", 5))))
        timeout = max(10, int(self.config.get("timeout_seconds", 30)))
        orders = self._resolve_orders()
        include_keywords = [str(keyword).lower() for keyword in self.config.get("include_keywords", []) if keyword]
        user_agent = str(self.config.get("user_agent", "MediaPulse/1.0")).strip() or "MediaPulse/1.0"

        since_utc = since.astimezone(timezone.utc)
        items: list[FeedbackItem] = []
        seen_ids: set[str] = set()

        session = requests.Session()
        session.headers.update({"User-Agent": user_agent, "Referer": "https://search.bilibili.com/"})

        for query in queries:
            if len(items) >= limit:
                break
            for order in orders:
                if len(items) >= limit:
                    break
                page = 1
                while len(items) < limit and page <= max_pages:
                    rows = self._search_page(
                        session=session,
                        query=query,
                        page=page,
                        page_size=min(page_size, limit - len(items)),
                        order=order,
                        timeout=timeout,
                    )
                    if not rows:
                        break
                    inserted_this_page = 0
                    for row in rows:
                        item = self._parse_item(
                            row=row,
                            query=query,
                            include_keywords=include_keywords,
                            since=since_utc,
                            order=order,
                        )
                        if not item:
                            continue
                        item_id = str(item.source_item_id or "").strip()
                        if not item_id or item_id in seen_ids:
                            continue
                        seen_ids.add(item_id)
                        items.append(item)
                        inserted_this_page += 1
                        if len(items) >= limit:
                            break
                    if inserted_this_page == 0 and order == "pubdate":
                        oldest = self._oldest_published_at(rows)
                        if oldest and oldest.astimezone(timezone.utc) < since_utc:
                            break
                    page += 1

        return items

    def _resolve_orders(self) -> list[str]:
        raw_orders = self.config.get("orders", [])
        values = [str(value).strip().lower() for value in raw_orders if str(value).strip()]
        if not values:
            values = [str(self.config.get("order", "pubdate")).strip().lower() or "pubdate"]

        orders: list[str] = []
        seen: set[str] = set()
        for value in values:
            normalized = value if value in self.VALID_ORDERS else "pubdate"
            if normalized in seen:
                continue
            seen.add(normalized)
            orders.append(normalized)
        return orders or ["pubdate"]

    def _expand_queries(self, queries: list[str]) -> list[str]:
        enable_variants = bool(self.config.get("expand_query_variants"))
        seen: set[str] = set()
        expanded: list[str] = []
        for query in queries:
            for candidate in ([query] + (self._query_variants(query) if enable_variants else [])):
                normalized = clean_content_text(candidate)
                if not normalized:
                    continue
                key = normalized.lower()
                if key in seen:
                    continue
                seen.add(key)
                expanded.append(normalized)
        return expanded

    @staticmethod
    def _query_variants(query: str) -> list[str]:
        base = clean_content_text(query)
        if not base:
            return []

        variants: list[str] = []
        compact = re.sub(r"\s+", "", base)
        spaced = re.sub(r"(?i)(x\d{3})(ultra|pro|max)", r"\1 \2", compact)
        if compact != base:
            variants.append(compact)
        if spaced != base and spaced != compact:
            variants.append(spaced)
        if "vivo" in base.lower():
            without_brand = re.sub(r"(?i)\bvivo\b", "", base).strip()
            if without_brand:
                variants.append(without_brand)
                compact_without_brand = re.sub(r"\s+", "", without_brand)
                if compact_without_brand != without_brand:
                    variants.append(compact_without_brand)
        return variants

    def _search_page(
        self,
        session: requests.Session,
        query: str,
        page: int,
        page_size: int,
        order: str,
        timeout: int,
    ) -> list[dict]:
        try:
            response = session.get(
                self.SEARCH_API_URL,
                params={
                    "search_type": "video",
                    "keyword": query,
                    "page": page,
                    "page_size": page_size,
                    "order": order,
                },
                timeout=timeout,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("bilibili_search_failed: invalid_json_payload")
            code = payload.get("code", -1)
            try:
                code_value = int(code)
            except (TypeError, ValueError):
                code_value = -1
            if code_value != 0:
                raise RuntimeError(
                    f"bilibili_search_failed[{code}]: {clean_content_text(str(payload.get('message', 'unknown_error')))}"
                )
            data = payload.get("data", {}) if isinstance(payload.get("data"), dict) else {}
            rows = data.get("result", [])
            return [row for row in rows if isinstance(row, dict)]
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            if int(status_code or 0) != 412:
                raise
        return self._search_page_html(
            session=session,
            query=query,
            page=page,
            page_size=page_size,
            timeout=timeout,
        )

    def _search_page_html(
        self,
        session: requests.Session,
        query: str,
        page: int,
        page_size: int,
        timeout: int,
    ) -> list[dict]:
        response = session.get(
            self.SEARCH_HTML_URL,
            params={
                "keyword": query,
                "page": page,
            },
            timeout=timeout,
        )
        response.raise_for_status()
        return self._parse_html_cards(response.text, limit=page_size)

    def _parse_html_cards(self, text: str, limit: int) -> list[dict]:
        rows: list[dict] = []
        seen_bvids: set[str] = set()
        pattern = re.compile(
            r'<a href="(?P<href>//www\.bilibili\.com/video/(?P<bvid>BV[0-9A-Za-z]+)[^"]*)"[^>]*>'
            r'.*?<img [^>]*alt="(?P<alt>[^"]*)"[^>]*>'
            r'.*?<span class="bili-video-card__stats__duration"[^>]*>(?P<duration>[^<]*)</span>'
            r'.*?<h3 class="bili-video-card__info--tit" title="(?P<title>[^"]*)"[^>]*>.*?</h3>'
            r'.*?<span class="bili-video-card__info--author"[^>]*>(?P<author>[^<]*)</span>'
            r'.*?<span class="bili-video-card__info--date"[^>]*>\s*[·]?\s*(?P<pubdate>[^<]*)</span>',
            re.S,
        )
        for match in pattern.finditer(text):
            bvid = clean_content_text(match.group("bvid"))
            if not bvid or bvid in seen_bvids:
                continue
            seen_bvids.add(bvid)
            title = self._strip_html(match.group("title") or match.group("alt"))
            if not title:
                continue
            href = clean_content_text(match.group("href"))
            if href.startswith("//"):
                href = "https:" + href
            rows.append(
                {
                    "bvid": bvid,
                    "arcurl": href,
                    "title": title,
                    "description": "",
                    "author": self._strip_html(match.group("author")),
                    "pubdate": self._parse_html_pubdate(match.group("pubdate")),
                    "duration": clean_content_text(match.group("duration")),
                    "tag": "",
                }
            )
            if len(rows) >= limit:
                break
        return rows

    @staticmethod
    def _strip_html(value: str) -> str:
        text = html.unescape(value or "")
        text = re.sub(r"<[^>]+>", "", text)
        return clean_content_text(text)

    @staticmethod
    def _parse_html_pubdate(raw: str) -> int | str:
        text = clean_content_text(raw)
        if not text:
            return ""
        normalized = text.replace(".", "-").replace("/", "-")
        if len(normalized) == 5 and normalized.count("-") == 1:
            normalized = f"{datetime.now().year}-{normalized}"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return text
        return int(parsed.replace(tzinfo=timezone.utc).timestamp())

    def _parse_item(
        self,
        row: dict,
        query: str,
        include_keywords: list[str],
        since: datetime,
        order: str,
    ) -> FeedbackItem | None:
        bvid = clean_content_text(str(row.get("bvid", "")))
        aid = clean_content_text(str(row.get("aid", "")))
        source_item_id = bvid or (f"av{aid}" if aid else "")
        if not source_item_id:
            return None

        title = clean_content_text(row.get("title", ""))
        description = clean_content_text(row.get("description", "") or row.get("desc", ""))
        if not title:
            return None

        published_at = parse_datetime(row.get("pubdate") or row.get("senddate"))
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

        summary = ""
        if description and not is_summary_redundant(title, description):
            summary = truncate(description, 240)

        url = self._build_url(row=row, bvid=bvid, aid=aid)
        if not url:
            return None

        return FeedbackItem(
            source="bilibili",
            source_item_id=source_item_id,
            title=title,
            url=url,
            content=description or title,
            summary=summary,
            published_at=published_at,
            author=clean_content_text(str(row.get("author", ""))) or None,
            source_section="Bilibili",
            video_candidate=True,
            extra={
                "collector": "bilibili_web_search",
                "query": query,
                "search_order": order,
                "bvid": bvid or None,
                "aid": aid or None,
                "play_count": row.get("play"),
                "danmaku_count": row.get("video_review") if row.get("video_review") is not None else row.get("danmaku"),
                "favorite_count": row.get("favorites"),
                "comment_count": row.get("review"),
                "like_count": row.get("like"),
                "duration": clean_content_text(str(row.get("duration", ""))) or None,
                "typename": clean_content_text(str(row.get("typename", ""))) or None,
                "tag": clean_content_text(str(row.get("tag", ""))) or None,
            },
        )

    @staticmethod
    def _build_url(row: dict, bvid: str, aid: str) -> str:
        if bvid:
            return f"https://www.bilibili.com/video/{bvid}"
        arcurl = clean_content_text(str(row.get("arcurl", "")))
        if arcurl:
            if arcurl.startswith("http://"):
                arcurl = "https://" + arcurl[7:]
            return arcurl
        if aid:
            return f"https://www.bilibili.com/video/av{aid}"
        return ""

    @staticmethod
    def _oldest_published_at(rows: list[dict]) -> datetime | None:
        published_at_values = [parse_datetime(row.get("pubdate") or row.get("senddate")) for row in rows]
        values = [value for value in published_at_values if value is not None]
        if not values:
            return None
        return min(values)

    def _default_query(self) -> str:
        if self.product_keywords:
            items = [keyword for keyword in self.product_keywords[:3] if keyword]
            if items:
                joined = " OR ".join(f"\"{item}\"" for item in items)
                return f"({joined}) 相机 评测"
        return "Target Phone 相机 评测"
