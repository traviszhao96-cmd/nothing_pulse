from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from typing import Any

import requests

from ..config import AppConfig
from ..source_profile import SOURCE_LABELS
from ..storage import FeedbackRepository
from ..utils import clean_content_text, is_summary_redundant, is_video_url, load_json, truncate


def build_summary_payload(
    repository: FeedbackRepository,
    target_date: date | None = None,
    scope: str = "camera",
    app_config: AppConfig | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict:
    scope = scope if scope in {"all", "camera"} else "camera"
    if start_date and end_date:
        if start_date > end_date:
            start_date, end_date = end_date, start_date
        all_rows = repository.fetch_by_published_date_range(
            start_date=start_date,
            end_date=end_date,
            camera_only=None,
        )
        if scope == "camera":
            rows = [row for row in all_rows if int(row["camera_related"] or 0) == 1 or _is_tracked_video_row(row)]
        else:
            rows = list(all_rows)
        trend = _trend_from_rows(rows if scope == "camera" else all_rows, start_date=start_date, end_date=end_date)
        report_date = end_date
    else:
        report_date = target_date or resolve_default_date(repository, None)
        all_rows = repository.fetch_by_report_date(report_date, camera_only=None)
        if scope == "camera":
            rows = [row for row in all_rows if int(row["camera_related"] or 0) == 1 or _is_tracked_video_row(row)]
        else:
            rows = list(all_rows)
        start_date = report_date
        end_date = report_date
        trend_start = report_date - timedelta(days=13)
        trend_all_rows = repository.fetch_by_published_date_range(
            start_date=trend_start,
            end_date=report_date,
            camera_only=None,
        )
        if scope == "camera":
            trend_rows = [row for row in trend_all_rows if int(row["camera_related"] or 0) == 1 or _is_tracked_video_row(row)]
        else:
            trend_rows = list(trend_all_rows)
        trend = _trend_from_rows(trend_rows, start_date=trend_start, end_date=report_date)

    categories = Counter(row["camera_category"] for row in rows)
    sentiments: Counter[str] = Counter()
    severities = Counter(row["severity"] for row in rows)
    sources = Counter((row["source_section"] or row["source"]) for row in rows)
    source_roles = Counter(_source_role_label(row["source_actor_type"]) for row in rows)
    media_types = Counter(_media_type_of_row(row) for row in rows)
    domain_tags = Counter((row["domain_tag"] or "未分类") for row in rows)
    camera_related_total = sum(1 for row in all_rows if int(row["camera_related"] or 0) == 1)
    video_candidate_total = sum(1 for row in all_rows if int(row["video_candidate"] or 0) == 1)
    tracked_video_total = sum(1 for row in all_rows if _is_tracked_video_row(row))
    processed_video_total = sum(1 for row in all_rows if _video_analysis_status(row) == "ok")

    keyword_counter: Counter[str] = Counter()
    sub_tag_counter: Counter[str] = Counter()
    positive_counter: Counter[str] = Counter()
    neutral_counter: Counter[str] = Counter()
    negative_counter: Counter[str] = Counter()
    for row in rows:
        pos_eval, neu_eval, neg_eval = _evaluation_counts_for_row(row)
        sentiments["positive"] += pos_eval
        sentiments["neutral"] += neu_eval
        sentiments["negative"] += neg_eval
        for keyword in load_json(row["camera_keyword_hits"], []):
            keyword_counter[keyword] += 1
        for sub_tag in load_json(row["domain_subtags_json"], []):
            sub_tag_counter[str(sub_tag)] += 1
        for point in _clean_point_list(load_json(row["ai_positive_points_json"], [])):
            positive_counter[point] += 1
        for point in _clean_point_list(load_json(row["ai_neutral_points_json"], [])):
            neutral_counter[point] += 1
        for point in _clean_point_list(load_json(row["ai_negative_points_json"], [])):
            negative_counter[point] += 1

    high_risk_cases = [row for row in rows if row["severity"] == "high"]
    if not high_risk_cases:
        high_risk_cases = [row for row in rows if row["severity"] == "medium"]
    if not high_risk_cases:
        high_risk_cases = rows
    high_risk_cases = sorted(high_risk_cases, key=lambda item: item["published_at"], reverse=True)[:8]

    cases = [
        {
            "title": row["title"],
            "url": row["url"],
            "category": row["camera_category"],
            "severity": row["severity"],
            "source": row["source_section"] or row["source"],
            "published_at": row["published_at"],
            "summary": _display_summary(row["title"], row["summary"], row["content"]),
            "source_actor_type": _source_role_label(row["source_actor_type"]),
            "media_type": _media_type_of_row(row),
            "domain_tag": row["domain_tag"] or "未分类",
            "domain_subtags": load_json(row["domain_subtags_json"], []),
            "sentiment_reason": clean_content_text(row["sentiment_reason"] or ""),
        }
        for row in high_risk_cases
    ]

    sorted_rows = sorted(rows, key=lambda item: item["published_at"], reverse=True)
    latest_rows = sorted_rows[:30]
    pinned_video_rows = [
        row
        for row in sorted_rows
        if _is_tracked_video_row(row) and (_video_analysis_status(row) or int(row["video_candidate"] or 0) == 1)
    ][:6]
    pinned_ids = {int(row["id"]) for row in pinned_video_rows}
    merged_rows = _merge_unique_rows(pinned_video_rows + latest_rows, limit=36)

    latest_items = [
        {
            "id": int(row["id"]),
            "title": row["title"],
            "url": row["url"],
            "category": row["camera_category"],
            "severity": row["severity"],
            "source": row["source_section"] or row["source"],
            "published_at": row["published_at"],
            "summary": _display_summary(row["title"], row["summary"], row["content"]),
            "camera_related": int(row["camera_related"] or 0) == 1,
            "is_video": is_video_url(row["url"]),
            "sentiment": row["sentiment"] or "neutral",
            "source_actor_type": _source_role_label(row["source_actor_type"]),
            "media_type": _media_type_of_row(row),
            "source_actor_reason": clean_content_text(row["source_actor_reason"] or ""),
            "domain_tag": row["domain_tag"] or "未分类",
            "domain_subtags": load_json(row["domain_subtags_json"], []),
            "product_tags": load_json(row["product_tags"], []),
            "sentiment_reason": clean_content_text(row["sentiment_reason"] or ""),
            "ai_positive_points": _clean_point_list(load_json(row["ai_positive_points_json"], [])),
            "ai_neutral_points": _clean_point_list(load_json(row["ai_neutral_points_json"], [])),
            "ai_negative_points": _clean_point_list(load_json(row["ai_negative_points_json"], [])),
            "video_candidate": int(row["video_candidate"] or 0) == 1,
            "video_analysis_status": _video_analysis_status(row),
            "video_analysis_output_file": _video_analysis_output_file(row),
            "video_pinned": int(row["id"]) in pinned_ids,
        }
        for row in merged_rows
    ]

    return {
        "scope": scope,
        "report_date": report_date.isoformat(),
        "start_date": start_date.isoformat() if start_date else "",
        "end_date": end_date.isoformat() if end_date else "",
        "total": len(rows),
        "total_all": len(all_rows),
        "camera_related_total": camera_related_total,
        "non_camera_total": max(0, len(all_rows) - camera_related_total),
        "video_candidate_total": video_candidate_total,
        "video_total": tracked_video_total,
        "video_done_total": processed_video_total,
        "video_pending_total": max(0, tracked_video_total - processed_video_total),
        "positive_eval_total": sentiments.get("positive", 0),
        "neutral_eval_total": sentiments.get("neutral", 0),
        "negative_eval_total": sentiments.get("negative", 0),
        "open_followups": sum(
            1 for row in rows if int(row["lark_dirty"] or 0) == 1 or not str(row["lark_record_id"] or "").strip()
        ),
        "high_risk": severities.get("high", 0),
        "medium_risk": severities.get("medium", 0),
        "low_risk": severities.get("low", 0),
        "categories": [{"name": name, "count": count} for name, count in categories.most_common()],
        "sentiments": [{"name": name, "count": count} for name, count in sentiments.most_common()],
        "sources": [{"name": name, "count": count} for name, count in sources.most_common(8)],
        "source_roles": [{"name": name, "count": count} for name, count in source_roles.most_common()],
        "media_types": [{"name": name, "count": count} for name, count in media_types.most_common()],
        "domains": [{"name": name, "count": count} for name, count in domain_tags.most_common()],
        "top_sub_tags": [{"name": name, "count": count} for name, count in sub_tag_counter.most_common(12)],
        "top_keywords": [{"name": name, "count": count} for name, count in keyword_counter.most_common(12)],
        "sentiment_insights": {
            "positive": [{"name": name, "count": count} for name, count in positive_counter.most_common(8)],
            "neutral": [{"name": name, "count": count} for name, count in neutral_counter.most_common(8)],
            "negative": [{"name": name, "count": count} for name, count in negative_counter.most_common(8)],
        },
        "cases": cases,
        "latest_items": latest_items,
        "trend": trend,
    }


def resolve_default_date(repository: FeedbackRepository, preferred: date | None) -> date:
    if preferred:
        return preferred
    available = repository.list_report_dates(limit=1)
    if available:
        return date.fromisoformat(available[0])
    return date.today()


def parse_report_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def _source_role_label(raw: str | None) -> str:
    key = str(raw or "unknown").strip().lower()
    return SOURCE_LABELS.get(key, SOURCE_LABELS["unknown"])


def _display_summary(title: str | None, summary: str | None, content: str | None = None) -> str:
    title_text = title or ""
    summary_text = clean_content_text(summary or "")
    if summary_text and not is_summary_redundant(title_text, summary_text):
        return truncate(summary_text, 320)

    content_text = clean_content_text(content or "")
    if not content_text:
        return ""
    if is_summary_redundant(title_text, content_text):
        return ""
    return truncate(content_text, 320)


def _merge_unique_rows(rows: list[Any], limit: int) -> list[Any]:
    merged: list[Any] = []
    seen: set[int] = set()
    for row in rows:
        row_id = int(row["id"])
        if row_id in seen:
            continue
        seen.add(row_id)
        merged.append(row)
        if len(merged) >= limit:
            break
    return merged


def _video_analysis_status(row: Any) -> str:
    extra = load_json(row["extra_json"], {})
    if not isinstance(extra, dict):
        return ""
    video_analysis = extra.get("video_analysis", {})
    if not isinstance(video_analysis, dict):
        return ""
    return str(video_analysis.get("status", "")).strip().lower()


def _video_analysis_output_file(row: Any) -> str:
    extra = load_json(row["extra_json"], {})
    if not isinstance(extra, dict):
        return ""
    video_analysis = extra.get("video_analysis", {})
    if not isinstance(video_analysis, dict):
        return ""
    return str(video_analysis.get("output_file", "")).strip()


def _is_tracked_video_row(row: Any) -> bool:
    return int(row["video_candidate"] or 0) == 1 or is_video_url(str(row["url"] or ""))


def _evaluation_counts_for_row(row: Any) -> tuple[int, int, int]:
    positive_points = _clean_point_list(load_json(row["ai_positive_points_json"], []))
    neutral_points = _clean_point_list(load_json(row["ai_neutral_points_json"], []))
    negative_points = _clean_point_list(load_json(row["ai_negative_points_json"], []))
    positive_total = len(positive_points)
    neutral_total = len(neutral_points)
    negative_total = len(negative_points)
    if positive_total + neutral_total + negative_total > 0:
        return positive_total, neutral_total, negative_total

    sentiment = str(row["sentiment"] or "").strip().lower()
    if sentiment == "positive":
        return 1, 0, 0
    if sentiment == "negative":
        return 0, 0, 1
    return 0, 1, 0


def _media_type_of_row(row: Any) -> str:
    url = str(row["url"] or "").lower()
    source = str(row["source"] or "").lower()
    if _is_tracked_video_row(row):
        if any(token in url for token in ("/shorts/", "shorts/", "tiktok.com", "douyin.com", "instagram.com/reel/")):
            return "短视频"
        return "长视频"
    if source == "reddit" or "/comments/" in url:
        return "评论"
    if any(url.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".svg")):
        return "图片"
    return "文章"


def _trend_from_rows(rows: list[Any], start_date: date, end_date: date) -> list[dict[str, int | str]]:
    bucket: dict[str, dict[str, int]] = {}
    for row in rows:
        day = str(row["published_at"] or "")[:10]
        if not day:
            continue
        item = bucket.setdefault(
            day,
            {"total": 0, "positive_total": 0, "neutral_total": 0, "negative_total": 0},
        )
        item["total"] += 1
        positive_total, neutral_total, negative_total = _evaluation_counts_for_row(row)
        item["positive_total"] += positive_total
        item["neutral_total"] += neutral_total
        item["negative_total"] += negative_total

    trend: list[dict[str, int | str]] = []
    current = start_date
    while current <= end_date:
        day_key = current.isoformat()
        values = bucket.get(day_key, {"total": 0, "positive_total": 0, "neutral_total": 0, "negative_total": 0})
        trend.append(
            {
                "report_date": day_key,
                "total": int(values["total"]),
                "positive_total": int(values["positive_total"]),
                "neutral_total": int(values["neutral_total"]),
                "negative_total": int(values["negative_total"]),
            }
        )
        current += timedelta(days=1)
    return trend


def build_runtime_status_payload(
    repository: FeedbackRepository,
    target_date: date,
    app_config: AppConfig | None = None,
) -> dict[str, Any]:
    rows = repository.fetch_by_report_date(target_date, camera_only=None)
    local_ai = _local_ai_status(rows, app_config)
    video_status = _video_status(rows, app_config)
    lark_status = _lark_status(rows, repository, target_date, app_config)
    return {
        "report_date": target_date.isoformat(),
        "local_ai": local_ai,
        "video_processing": video_status,
        "lark_sync": lark_status,
    }


def build_video_candidates_payload(
    repository: FeedbackRepository,
    target_date: date,
    limit: int = 80,
) -> dict[str, Any]:
    rows = repository.fetch_by_report_date(target_date, camera_only=None)
    items: list[dict[str, Any]] = []
    for row in rows:
        url = str(row["url"] or "")
        extra = load_json(row["extra_json"], {})
        video_analysis = extra.get("video_analysis", {}) if isinstance(extra, dict) else {}
        analysis_status = video_analysis.get("status", "") if isinstance(video_analysis, dict) else ""
        is_tracked_video = int(row["video_candidate"] or 0) == 1 or is_video_url(url)
        if not is_tracked_video:
            continue
        items.append(
            {
                "id": int(row["id"]),
                "title": row["title"],
                "url": url,
                "source": row["source_section"] or row["source"],
                "published_at": row["published_at"],
                "domain_tag": row["domain_tag"] or "未分类",
                "video_candidate": int(row["video_candidate"] or 0) == 1,
                "analysis_status": str(analysis_status or "pending"),
                "analysis_output_file": video_analysis.get("output_file", "") if isinstance(video_analysis, dict) else "",
            }
        )
    items.sort(key=lambda item: str(item.get("published_at") or ""), reverse=True)
    items = items[: max(1, limit)]
    return {"report_date": target_date.isoformat(), "total": len(items), "items": items}


def build_video_detail_payload(repository: FeedbackRepository, row_id: int) -> dict[str, Any]:
    row = repository.fetch_by_id(int(row_id))
    if row is None:
        return {"error": "not_found", "id": int(row_id)}
    extra = load_json(row["extra_json"], {})
    video_analysis = extra.get("video_analysis", {}) if isinstance(extra, dict) else {}
    return {
        "id": int(row["id"]),
        "title": row["title"],
        "url": row["url"],
        "source": row["source_section"] or row["source"],
        "published_at": row["published_at"],
        "summary": _display_summary(row["title"], row["summary"], row["content"]),
        "content": clean_content_text(row["content"] or ""),
        "camera_category": row["camera_category"] or "未分类",
        "domain_tag": row["domain_tag"] or "未分类",
        "domain_subtags": load_json(row["domain_subtags_json"], []),
        "sentiment": row["sentiment"] or "neutral",
        "sentiment_reason": clean_content_text(row["sentiment_reason"] or ""),
        "source_actor_type": _source_role_label(row["source_actor_type"]),
        "source_actor_reason": clean_content_text(row["source_actor_reason"] or ""),
        "ai_positive_points": _clean_point_list(load_json(row["ai_positive_points_json"], [])),
        "ai_neutral_points": _clean_point_list(load_json(row["ai_neutral_points_json"], [])),
        "ai_negative_points": _clean_point_list(load_json(row["ai_negative_points_json"], [])),
        "video_candidate": int(row["video_candidate"] or 0) == 1,
        "is_video": is_video_url(row["url"]),
        "video_analysis": video_analysis if isinstance(video_analysis, dict) else {},
    }


def _local_ai_status(rows: list[Any], app_config: AppConfig | None) -> dict[str, Any]:
    enabled = bool(app_config and app_config.local_ai.enabled)
    configured = bool(enabled and app_config and app_config.local_ai.base_url and app_config.local_ai.model)
    latest_error = ""
    unreachable_count = 0
    for row in rows:
        extra = load_json(row["extra_json"], {})
        error = str(extra.get("local_ai_error", "")).strip() if isinstance(extra, dict) else ""
        if not error:
            continue
        latest_error = latest_error or error
        if "local_ai_unreachable" in error:
            unreachable_count += 1

    reachable = False
    message = "未启用"
    base_url = app_config.local_ai.base_url if app_config else ""
    is_cloud_api = "api.deepseek.com" in str(base_url)
    if configured and app_config:
        reachable, probe_error = _probe_local_ai(app_config)
        if reachable:
            message = "云端 AI 可用" if is_cloud_api else "本地 AI 可用"
        else:
            message = probe_error or ("云端 AI 不可用" if is_cloud_api else "本地 AI 不可用")
    elif enabled:
        message = "已启用但配置不完整"

    if unreachable_count > 0 and not reachable:
        message = "云端 AI 不可用（最近请求失败）" if is_cloud_api else "本地 AI 不可用（最近请求失败）"

    return {
        "enabled": enabled,
        "configured": configured,
        "reachable": reachable,
        "message": message,
        "last_error": latest_error,
        "base_url": app_config.local_ai.base_url if app_config else "",
        "model": app_config.local_ai.model if app_config else "",
    }


def _video_status(rows: list[Any], app_config: AppConfig | None) -> dict[str, Any]:
    total_video = 0
    pending = 0
    done = 0
    failed = 0
    for row in rows:
        url = str(row["url"] or "")
        extra = load_json(row["extra_json"], {})
        status = ""
        if isinstance(extra, dict):
            video_analysis = extra.get("video_analysis", {})
            if isinstance(video_analysis, dict):
                status = str(video_analysis.get("status", "")).strip().lower()
        is_tracked_video = int(row["video_candidate"] or 0) == 1 or is_video_url(url)
        if not is_tracked_video:
            continue
        total_video += 1
        if status == "ok":
            done += 1
        elif status == "failed":
            failed += 1
            pending += 1
        else:
            pending += 1

    vp = app_config.video_processing if app_config else None
    return {
        "enabled": bool(vp and vp.enabled),
        "nightly_enabled": bool(vp and vp.nightly_enabled),
        "nightly_time": f"{vp.nightly_hour:02d}:{vp.nightly_minute:02d}" if vp else "",
        "nightly_timezone": vp.nightly_timezone if vp else "",
        "total_video_items": total_video,
        "pending": pending,
        "done": done,
        "failed": failed,
    }


def _lark_status(
    rows: list[Any],
    repository: FeedbackRepository,
    target_date: date,
    app_config: AppConfig | None,
) -> dict[str, Any]:
    enabled = bool(app_config and app_config.lark_enabled)
    lark_cfg = dict(app_config.lark) if app_config else {}
    required_fields = ("app_id", "app_secret", "bitable_app_token", "bitable_table_id")
    configured = bool(enabled and all(str(lark_cfg.get(name, "")).strip() for name in required_fields))

    pending_in_range = 0
    synced_in_range = 0
    failed_in_range = 0
    last_error = ""
    for row in rows:
        record_id = str(row["lark_record_id"] or "").strip()
        dirty = int(row["lark_dirty"] or 0) == 1
        error = str(row["lark_last_sync_error"] or "").strip()
        if dirty or not record_id:
            pending_in_range += 1
        else:
            synced_in_range += 1
        if error:
            failed_in_range += 1
            if not last_error:
                last_error = error

    pending_total = repository.count_lark_pending(None)
    pending_today = repository.count_lark_pending(target_date)

    if not enabled:
        message = "未启用"
    elif not configured:
        message = "已启用但配置不完整"
    elif pending_total > 0:
        message = f"待同步 {pending_total} 条"
    else:
        message = "同步正常"

    return {
        "enabled": enabled,
        "configured": configured,
        "message": message,
        "pending_total": pending_total,
        "pending_for_date": pending_today,
        "pending_in_view": pending_in_range,
        "synced_in_view": synced_in_range,
        "failed_in_view": failed_in_range,
        "last_error": last_error,
    }


def _probe_local_ai(app_config: AppConfig) -> tuple[bool, str]:
    base = app_config.local_ai.base_url.rstrip("/")
    if not base:
        return False, "local_ai.base_url 为空"
    try:
        headers = {}
        if app_config.local_ai.api_key:
            headers["Authorization"] = f"Bearer {app_config.local_ai.api_key}"
        response = requests.get(f"{base}/models", headers=headers, timeout=4)
        if response.status_code >= 400:
            return False, f"HTTP {response.status_code}"
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _clean_point_list(values: list[Any]) -> list[str]:
    if not isinstance(values, list):
        return []
    results: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = clean_content_text(str(raw or ""))
        if not text:
            continue
        if text.lower() in {"none", "null", "-", "暂无"}:
            continue
        normalized = truncate(text, 120)
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        results.append(normalized)
    return results
