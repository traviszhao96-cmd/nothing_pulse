from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import quote, urlparse

import requests
from bs4 import BeautifulSoup

from ..utils import clean_content_text, normalize_text, truncate


@dataclass(slots=True)
class ArticleBodyResult:
    text: str
    resolved_url: str


class ArticleBodyExtractor:
    def __init__(self, timeout: int = 20, max_chars: int = 6000) -> None:
        self.timeout = max(8, int(timeout))
        self.max_chars = max(1000, int(max_chars))
        self._cache: dict[str, ArticleBodyResult] = {}
        self._google_decode_cache: dict[str, str] = {}
        self._headers = {"User-Agent": "Mozilla/5.0 (compatible; NothingCameraPulse/1.0)"}

    def fetch(self, url: str) -> ArticleBodyResult:
        if not url:
            return ArticleBodyResult(text="", resolved_url=url)
        if url in self._cache:
            return self._cache[url]

        fetch_url = self._resolve_google_news_url(url)
        response = self._fetch_page(fetch_url)
        if response is None and fetch_url != url:
            response = self._fetch_page(url)

        if response is None:
            result = ArticleBodyResult(text="", resolved_url=fetch_url or url)
            self._cache[url] = result
            return result

        resolved_url = response.url or fetch_url or url
        html = response.text or ""
        text = self._extract_text(html)
        result = ArticleBodyResult(text=text, resolved_url=resolved_url)
        self._cache[url] = result
        return result

    def _fetch_page(self, url: str) -> requests.Response | None:
        if not url:
            return None
        try:
            response = requests.get(url, timeout=self.timeout, headers=self._headers, allow_redirects=True)
            response.raise_for_status()
            return response
        except Exception:  # noqa: BLE001
            return None

    def _resolve_google_news_url(self, url: str) -> str:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if "news.google." not in host:
            return url

        path_parts = [part for part in parsed.path.split("/") if part]
        if len(path_parts) < 2:
            return url

        base64_id = ""
        if path_parts[-2] in {"articles", "read"}:
            base64_id = path_parts[-1]
        elif len(path_parts) >= 3 and path_parts[-3] == "rss" and path_parts[-2] == "articles":
            base64_id = path_parts[-1]
        if not base64_id:
            return url

        if base64_id in self._google_decode_cache:
            return self._google_decode_cache[base64_id] or url

        decoded = self._decode_google_article_url(base64_id)
        self._google_decode_cache[base64_id] = decoded
        return decoded or url

    def _decode_google_article_url(self, base64_id: str) -> str:
        params = self._fetch_google_decode_params(base64_id)
        if not params:
            return ""
        payload = [
            "Fbv4je",
            (
                f"[\"garturlreq\",[[\"X\",\"X\",[\"X\",\"X\"],null,null,1,1,\"US:en\",null,1,null,null,null,"
                f"null,null,0,1],\"X\",\"X\",1,[1,1,1],1,1,null,0,0,null,0],\"{base64_id}\",{params['timestamp']},"
                f"\"{params['signature']}\"]"
            ),
        ]
        headers = {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Referer": "https://news.google.com/",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
            ),
        }
        try:
            response = requests.post(
                "https://news.google.com/_/DotsSplashUi/data/batchexecute",
                headers=headers,
                data=f"f.req={quote(json.dumps([[payload]]))}",
                timeout=self.timeout,
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            return ""

        return self._parse_google_batch_execute(response.text)

    def _fetch_google_decode_params(self, base64_id: str) -> dict[str, str]:
        targets = [
            f"https://news.google.com/articles/{base64_id}",
            f"https://news.google.com/rss/articles/{base64_id}",
        ]
        for target in targets:
            response = self._fetch_page(target)
            if response is None:
                continue
            soup = BeautifulSoup(response.text or "", "html.parser")
            node = soup.select_one("c-wiz > div[jscontroller]")
            if not node:
                continue
            signature = clean_content_text(node.get("data-n-a-sg", ""))
            timestamp = clean_content_text(node.get("data-n-a-ts", ""))
            if signature and timestamp.isdigit():
                return {"signature": signature, "timestamp": timestamp}
        return {}

    @staticmethod
    def _parse_google_batch_execute(text: str) -> str:
        if not text:
            return ""
        parts = text.split("\n\n", 1)
        if len(parts) != 2:
            return ""
        try:
            payload = json.loads(parts[1])
        except json.JSONDecodeError:
            return ""
        if not isinstance(payload, list):
            return ""
        for entry in payload:
            if not isinstance(entry, list) or len(entry) < 3 or not entry[2]:
                continue
            try:
                inner = json.loads(entry[2])
            except (TypeError, json.JSONDecodeError):
                continue
            if (
                isinstance(inner, list)
                and len(inner) >= 2
                and str(inner[0]) == "garturlres"
                and str(inner[1]).startswith("http")
            ):
                return str(inner[1])
        return ""

    def _extract_text(self, html: str) -> str:
        if not html:
            return ""
        soup = BeautifulSoup(html, "html.parser")
        page_title = clean_content_text(soup.title.get_text(" ", strip=True) if soup.title else "")
        if page_title.lower() == "google news":
            return ""
        for node in soup(["script", "style", "noscript", "svg", "footer", "header", "nav", "aside", "form"]):
            node.decompose()

        blocks: list[str] = []
        article = soup.find("article")
        if article:
            blocks.extend(self._collect_paragraphs(article.find_all("p")))
        if not blocks:
            blocks.extend(self._collect_paragraphs(soup.select("main p")))
        if not blocks:
            blocks.extend(self._collect_paragraphs(soup.find_all("p")))

        if len(" ".join(blocks)) < 120:
            description = clean_content_text(_meta(soup, "description") or _meta(soup, "og:description"))
            if description:
                blocks.append(description)

        merged: list[str] = []
        seen: set[str] = set()
        for block in blocks:
            key = normalize_text(block).lower()
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(block)

        return truncate(" ".join(merged), self.max_chars)

    @staticmethod
    def _collect_paragraphs(nodes: list) -> list[str]:
        chunks: list[str] = []
        for node in nodes:
            text = clean_content_text(node.get_text(" ", strip=True))
            if len(text) < 40:
                continue
            low = text.lower()
            if low.startswith(("copyright", "all rights reserved", "advertisement", "ad choices")):
                continue
            chunks.append(text)
        return chunks


def _meta(soup: BeautifulSoup, name: str) -> str | None:
    if name.startswith("og:"):
        node = soup.find("meta", attrs={"property": name})
    else:
        node = soup.find("meta", attrs={"name": name})
    if not node:
        return None
    return node.get("content")
