from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

from .fetchers import BilibiliSearchCollector, YouTubeYtDlpCollector
from .filtering import SimilarityDeduper
from .manual_video import fetch_video_page_meta
from .models import FeedbackItem
from .utils import clean_content_text, is_video_url, load_json, normalize_text, since_days, truncate
from .video_analysis import VideoAnalysisService


@dataclass(slots=True)
class CompetitorVideoRequest:
    targets: list[str] = field(default_factory=list)
    compare_to: list[str] = field(default_factory=list)
    direct_queries: list[str] = field(default_factory=list)
    platforms: list[str] = field(default_factory=lambda: ["youtube", "bilibili"])
    lookback_days: int = 30
    limit_per_query: int = 8
    max_total: int = 80
    run_ai: bool = True
    analyze_video: bool = False
    sync_lark: bool = False
    dry_run: bool = False
    campaign_name: str = ""


def run_competitor_video_task(config: Any, request: CompetitorVideoRequest) -> dict[str, Any]:
    from .pipeline import CameraPulsePipeline

    pipeline = CameraPulsePipeline(config)
    repository = pipeline.repository
    since = since_days(max(1, int(request.lookback_days)))
    deduper = SimilarityDeduper.from_repository(
        repository,
        threshold=config.dedupe.jaccard_threshold,
        lookback_days=config.dedupe.lookback_days,
    )

    targets = _pick_targets(config, request.targets)
    compare_to = _pick_compare_to(config, request.compare_to)
    platforms = _pick_platforms(request.platforms)
    direct_queries = [value for value in request.direct_queries if clean_content_text(value)]
    campaign_name = clean_content_text(request.campaign_name) or "competitor-video"

    collectors = _build_collectors(
        targets=targets,
        compare_to=compare_to,
        direct_queries=direct_queries,
        platforms=platforms,
        limit_per_query=max(1, int(request.limit_per_query)),
    )

    fetched = 0
    inserted = 0
    ai_enriched = 0
    ai_failed = 0
    analyzed = 0
    skipped_duplicates = 0
    errors: list[str] = []
    inserted_ids: list[int] = []
    seen_urls: set[str] = set()
    video_service = VideoAnalysisService(config, repository)

    for collector, search_meta in collectors:
        try:
            items = collector.fetch(since)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"collector={collector.name} error={exc}")
            continue

        for item in items:
            if fetched >= max(1, int(request.max_total)):
                break
            fetched += 1
            normalized_url = str(item.url or "").strip()
            if normalized_url in seen_urls:
                skipped_duplicates += 1
                continue
            seen_urls.add(normalized_url)
            _decorate_competitor_item(item, search_meta)
            duplicated, reason = deduper.is_duplicate(item)
            if duplicated:
                skipped_duplicates += 1
                item.extra["dedupe_reason"] = reason
                continue

            if (not clean_content_text(item.author or "")) and item.video_candidate:
                meta = fetch_video_page_meta(item.url, timeout_seconds=8)
                author = clean_content_text(meta.author or "")
                if author:
                    item.author = author

            pipeline.classifier.classify(item)
            pipeline.source_profiler.classify(item)
            if request.run_ai:
                enrich_result = pipeline.ai_enricher.enrich(item)
                if enrich_result.ok:
                    ai_enriched += 1
                    item.extra.pop("local_ai_error", None)
                elif enrich_result.error and enrich_result.error != "local_ai_disabled":
                    ai_failed += 1
                    item.extra["local_ai_error"] = enrich_result.error

            if request.dry_run:
                inserted += 1
                continue

            try:
                row_id = repository.insert(item)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"insert url={item.url} error={exc}")
                continue
            if not row_id:
                continue
            inserted += 1
            inserted_ids.append(int(row_id))

            if request.analyze_video and item.video_candidate and video_service.is_enabled():
                result = video_service.process(
                    target_date=None,
                    row_id=int(row_id),
                    limit=1,
                    only_unprocessed=True,
                )
                if result.get("ok"):
                    analyzed += sum(1 for result_item in result.get("items", []) if result_item.get("ok"))
        if fetched >= max(1, int(request.max_total)):
            break

    lark_synced = 0
    if (not request.dry_run) and request.sync_lark and pipeline.lark_client.is_available() and inserted_ids:
        rows = [repository.fetch_by_id(row_id) for row_id in inserted_ids]
        sync_rows = [row for row in rows if row is not None]
        lark_synced = pipeline.lark_client.sync_rows(
            sync_rows,
            mark_synced=repository.mark_synced,
            mark_failed=repository.mark_lark_sync_failed,
            list_point_links=repository.list_lark_point_links,
            get_point_record_id=repository.get_lark_point_record_id,
            upsert_point_link=repository.upsert_lark_point_link,
            delete_point_link=repository.delete_lark_point_link,
            mark_point_failed=repository.mark_lark_point_failed,
        )

    return {
        "targets": targets,
        "compare_to": compare_to,
        "platforms": platforms,
        "fetched": fetched,
        "inserted": inserted,
        "ai_enriched": ai_enriched,
        "ai_failed": ai_failed,
        "analyzed": analyzed,
        "skipped_duplicates": skipped_duplicates,
        "lark_synced": lark_synced,
        "inserted_ids": inserted_ids,
        "errors": errors,
        "campaign_name": campaign_name,
    }


def _pick_targets(config: Any, cli_targets: list[str]) -> list[str]:
    if cli_targets:
        return _unique_clean(cli_targets)
    raw = config.competitor_video.get("targets", []) if hasattr(config, "competitor_video") else []
    return _unique_clean(raw)


def _pick_compare_to(config: Any, cli_compare_to: list[str]) -> list[str]:
    if cli_compare_to:
        return _unique_clean(cli_compare_to)
    configured = config.competitor_video.get("compare_to", []) if hasattr(config, "competitor_video") else []
    if configured:
        return _unique_clean(configured)
    return _unique_clean(config.product_keywords[:3] if getattr(config, "product_keywords", None) else [])


def _pick_platforms(raw_platforms: list[str]) -> list[str]:
    values = [clean_content_text(value).lower() for value in raw_platforms if clean_content_text(value)]
    picked = [value for value in values if value in {"youtube", "bilibili"}]
    return picked or ["youtube", "bilibili"]


def _build_collectors(
    *,
    targets: list[str],
    compare_to: list[str],
    direct_queries: list[str],
    platforms: list[str],
    limit_per_query: int,
) -> list[tuple[Any, dict[str, Any]]]:
    collectors: list[tuple[Any, dict[str, Any]]] = []
    query_specs = _build_query_specs(targets=targets, compare_to=compare_to, direct_queries=direct_queries)
    for platform in platforms:
        for spec in query_specs:
            target = clean_content_text(spec.get("target", ""))
            query = clean_content_text(spec.get("query", ""))
            if not query:
                continue
            include_keywords = _build_include_keywords(target=target, compare_to=spec.get("compare_to"))
            if platform == "youtube":
                collector = YouTubeYtDlpCollector(
                    name="competitor_youtube_yt_dlp",
                    config={
                        "queries": [query],
                        "limit": limit_per_query,
                        "timeout_seconds": 90,
                        "include_keywords": include_keywords,
                        "executable": "yt-dlp",
                    },
                    product_keywords=_keyword_variants(target) if target else include_keywords,
                )
            else:
                collector = BilibiliSearchCollector(
                    name="competitor_bilibili",
                    config={
                        "queries": [_to_bilibili_query(query, target)],
                        "limit": limit_per_query,
                        "page_size": min(limit_per_query, 20),
                        "max_pages": 2,
                        "timeout_seconds": 30,
                        "include_keywords": include_keywords,
                        "order": "pubdate",
                    },
                    product_keywords=_keyword_variants(target) if target else include_keywords,
                )
            meta = dict(spec)
            meta["platform"] = platform
            meta["query"] = query if platform == "youtube" else _to_bilibili_query(query, target)
            collectors.append((collector, meta))
    return collectors


def _build_query_specs(
    *,
    targets: list[str],
    compare_to: list[str],
    direct_queries: list[str],
) -> list[dict[str, str]]:
    specs: list[dict[str, str]] = []
    for query in direct_queries:
        specs.append({"target": "", "compare_to": "", "query": clean_content_text(query), "intent": "manual"})
    for target in targets:
        specs.append({"target": target, "compare_to": "", "query": f"\"{target}\" camera review", "intent": "review"})
        specs.append({"target": target, "compare_to": "", "query": f"\"{target}\" camera test", "intent": "camera_test"})
        specs.append({"target": target, "compare_to": "", "query": f"\"{target}\" camera comparison", "intent": "comparison"})
        for other in compare_to:
            specs.append(
                {
                    "target": target,
                    "compare_to": other,
                    "query": f"\"{target}\" vs \"{other}\" camera",
                    "intent": "versus",
                }
            )
    seen: set[str] = set()
    output: list[dict[str, str]] = []
    for spec in specs:
        key = "||".join([spec.get("target", ""), spec.get("compare_to", ""), spec.get("query", "")]).lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(spec)
    return output


def _build_include_keywords(target: str, compare_to: str | None) -> list[str]:
    values: list[str] = []
    for raw in [target, compare_to or ""]:
        values.extend(_keyword_variants(raw))
    for raw in ["camera", "photo", "video", "review", "vs", "comparison", "相机", "评测", "测评", "影像", "拍照"]:
        text = clean_content_text(raw).lower()
        if text:
            values.append(text)
    return _unique_clean(values)


def _to_bilibili_query(query: str, target: str) -> str:
    text = clean_content_text(query)
    if any(token in text.lower() for token in ("review", "camera test", "comparison", "vs")):
        base = clean_content_text(target) or text
        if "vs" in text.lower():
            return text.replace("camera", "相机").replace("review", "评测").replace("comparison", "对比")
        return f"{base} 相机 评测"
    return text


def _decorate_competitor_item(item: FeedbackItem, search_meta: dict[str, Any]) -> None:
    target = clean_content_text(search_meta.get("target", ""))
    compare_to = clean_content_text(search_meta.get("compare_to", ""))
    platform = clean_content_text(search_meta.get("platform", ""))
    query = clean_content_text(search_meta.get("query", ""))
    item.source_section = f"Competitor {platform.title() or 'Video'}"
    item.extra["competitor_video"] = {
        "target": target,
        "target_slug": _slugify(target),
        "brand": _infer_brand(target),
        "compare_to": compare_to,
        "compare_to_slug": _slugify(compare_to),
        "platform": platform,
        "query": query,
        "intent": clean_content_text(search_meta.get("intent", "")) or "review",
        "video_type": _classify_video_type(item.title, item.summary or item.content),
        "focus_tags": _classify_focus_tags(item.title, item.summary or item.content),
    }
    if target:
        item.extra["competitor_brand"] = _infer_brand(target)
        item.extra["competitor_model"] = target
    if compare_to:
        item.extra["comparison_target"] = compare_to
    item.camera_related = True
    item.video_candidate = True if is_video_url(item.url) else item.video_candidate
    if target:
        product_tags = list(item.product_tags)
        token = f"competitor:{_slugify(target)}"
        if token and token not in product_tags:
            product_tags.append(token)
        item.product_tags = product_tags[:6]


def _classify_video_type(title: str, content: str) -> str:
    blob = normalize_text(" ".join([title, content])).lower()
    if any(token in blob for token in (" vs ", "对比", "comparison")):
        return "comparison"
    if any(token in blob for token in ("camera test", "blind test", "sample", "样张")):
        return "camera_test"
    if any(token in blob for token in ("review", "评测", "测评", "hands-on", "上手")):
        return "review"
    if any(token in blob for token in ("tips", "tricks", "first things to do", "教程", "设置")):
        return "tips"
    return "general"


def _classify_focus_tags(title: str, content: str) -> list[str]:
    blob = normalize_text(" ".join([title, content])).lower()
    mapping = {
        "photo": ("photo", "拍照", "照片"),
        "video": ("video", "录像", "视频"),
        "portrait": ("portrait", "人像", "bokeh"),
        "zoom": ("zoom", "telephoto", "长焦"),
        "night": ("night", "low light", "夜景", "暗光"),
        "selfie": ("selfie", "front camera", "前置", "自拍"),
    }
    output = [name for name, tokens in mapping.items() if any(token in blob for token in tokens)]
    return output[:4] or ["general"]


def _infer_brand(target: str) -> str:
    text = clean_content_text(target)
    if not text:
        return ""
    return text.split(" ", 1)[0]


def _slugify(value: str) -> str:
    text = clean_content_text(value).lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def _keyword_variants(value: str) -> list[str]:
    text = clean_content_text(value).lower()
    if not text:
        return []
    compact = re.sub(r"\s+", "", text)
    dashless = text.replace("-", "")
    compact_dashless = compact.replace("-", "")
    variants = [text, compact, dashless, compact_dashless]
    return _unique_clean([item for item in variants if item])


def _unique_clean(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for raw in values:
        text = clean_content_text(raw)
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(text)
    return result
