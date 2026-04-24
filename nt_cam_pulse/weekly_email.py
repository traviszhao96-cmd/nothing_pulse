from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import date, timedelta
import html
import json
import math
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from .backend.service import build_summary_payload
from .config import AppConfig
from .email_summary import send_email_message
from .storage import FeedbackRepository
from .utils import (
    build_exact_dedupe_key,
    canonical_url,
    clean_content_text,
    ensure_parent,
    is_summary_redundant,
    load_json,
    parse_datetime,
    truncate,
)

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
HOT_SCORE_METRICS = (
    ("view_count", 34.0),
    ("like_count", 26.0),
    ("comment_count", 20.0),
    ("favorite_count", 18.0),
    ("score", 18.0),
    ("retweet_count", 20.0),
    ("repost_count", 20.0),
    ("reply_count", 16.0),
)
SOURCE_ROLE_LABELS = {
    "official_kol": "官方KOL/媒体",
    "core_koc": "核心KOC/自媒体",
    "real_user": "真实购买用户",
    "unknown": "待确认",
}
PURCHASE_STAGE_LABELS = {
    "owned": "已购用户",
    "considering": "意向用户",
    "none": "",
}
THEME_TITLES = {
    "positive": {
        "photo_daylight": "日光/HDR/综合色彩",
        "telephoto_zoom": "长焦/人像/变焦",
        "design_os": "设计/手感/系统体验",
        "battery_perf": "续航/充电/流畅度",
        "general_positive": "综合正面体验",
    },
    "negative": {
        "video_capability": "视频能力/录制体验",
        "low_light_aux": "低光/副摄/长焦短板",
        "exposure_focus": "曝光/对焦/炫光/一致性",
        "price_weight": "价格/重量/套装负担",
        "hardware_misc": "外围体验/硬件细节",
        "general_negative": "综合负面反馈",
    },
}
THEME_SUMMARIES = {
    "positive": {
        "photo_daylight": "高热内容里最稳定的正向反馈，集中在白天样张的细节、HDR 和综合色彩控制。",
        "telephoto_zoom": "另一类强好评落在长焦、人像和变焦体验，普遍认为这个价位给到的可玩性和成片能力有越级感。",
        "design_os": "设计辨识度、握持手感和系统交互体验仍然是最容易打动创作者和用户的部分。",
        "battery_perf": "续航、充电和日常流畅度整体口碑偏稳，没有出现明显的共性负面。",
        "general_positive": "本周正向反馈仍以综合体验为主，强调产品辨识度和整体完成度。",
    },
    "negative": {
        "video_capability": "负面反馈主要围绕视频拍摄稳定性、暗光录制表现和专业视频工作流体验，说明它虽然强，但离完全成熟仍有距离。",
        "low_light_aux": "另一个反复出现的槽点是低光和副摄表现，集中在超广角解析力一般、长焦夜景噪点重、细节容易塌。",
        "exposure_focus": "部分评测把问题指向成像一致性，包括逆光泛光、曝光控制和镜头切换后的对焦不够稳。",
        "price_weight": "另一个明确的负面方向是价格、重量和外接套装的使用门槛，说明它更像影像玩家设备，而不是轻松上手的普适旗舰。",
        "hardware_misc": "除了影像外，扬声器、按键、信号和屏幕反光也在评论区被多次点名。",
        "general_negative": "本周负面反馈偏向综合体验短板，但影像相关问题依旧最受关注。",
    },
}
_TIMESTAMP_RE = re.compile(r"^\[(?P<ts>[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)\]\s*(?P<text>.+)$")
_VIDEO_OUTPUT_CACHE: dict[str, dict[str, Any] | None] = {}
FEATURED_DETAIL_LIMIT = 5
FEATURED_COMMENT_LIMIT = 4
FOCUS_EVIDENCE_LIMIT = 5
FOCUS_GROUP_LIMIT = 3


@dataclass(slots=True)
class WeeklyMediaEmail:
    subject: str
    text_body: str
    html_body: str
    metrics: dict[str, Any]
    output_path: str = ""


def build_weekly_media_email(
    config: AppConfig,
    repository: FeedbackRepository,
    start_date: date | None = None,
    end_date: date | None = None,
    scope: str = "all",
    top_limit: int = 10,
) -> WeeklyMediaEmail:
    resolved_end = _resolve_end_date(repository, end_date)
    resolved_start = start_date or (resolved_end - timedelta(days=6))
    if resolved_start > resolved_end:
        resolved_start, resolved_end = resolved_end, resolved_start

    scope = scope if scope in {"all", "camera"} else "all"
    summary = build_summary_payload(
        repository=repository,
        scope=scope,
        app_config=config,
        start_date=resolved_start,
        end_date=resolved_end,
    )
    rows = repository.fetch_by_published_date_range(
        start_date=resolved_start,
        end_date=resolved_end,
        camera_only=None,
    )
    scoped_rows = _filter_rows_for_scope(rows, scope)
    featured_items = _build_featured_items(
        scoped_rows,
        limit=max(1, min(12, int(top_limit))),
        reference_date=resolved_end,
    )
    product_counts = _build_product_counts(scoped_rows)
    platform_counts = _build_platform_counts(scoped_rows)
    focus_groups = _build_focus_groups(featured_items)
    overview_lines = _build_overview_lines(
        summary=summary,
        featured_items=featured_items,
        product_counts=product_counts,
        platform_counts=platform_counts,
        focus_groups=focus_groups,
    )
    subject_prefix = str(config.email_summary.subject_prefix or "[Media Pulse]").strip()
    report_title = _weekly_report_title(config)
    subject = f"{subject_prefix} {report_title} {resolved_start.isoformat()} ~ {resolved_end.isoformat()}"

    metrics = {
        "scope": scope,
        "start_date": resolved_start.isoformat(),
        "end_date": resolved_end.isoformat(),
        "featured_total": len(featured_items),
        "featured_titles": [item["title"] for item in featured_items],
        "product_counts": dict(product_counts),
        "platform_counts": dict(platform_counts),
        "summary_total": int(summary.get("total") or 0),
    }
    text_body = _render_text_body(
        summary=summary,
        featured_items=featured_items,
        overview_lines=overview_lines,
        focus_groups=focus_groups,
        report_title=report_title,
    )
    html_body = _render_html_body(
        summary=summary,
        featured_items=featured_items,
        overview_lines=overview_lines,
        focus_groups=focus_groups,
        report_title=report_title,
    )
    return WeeklyMediaEmail(
        subject=subject,
        text_body=text_body,
        html_body=html_body,
        metrics=metrics,
    )


def export_weekly_media_email_html(
    config: AppConfig,
    repository: FeedbackRepository,
    start_date: date | None = None,
    end_date: date | None = None,
    scope: str = "all",
    top_limit: int = 10,
    output_path: str | None = None,
) -> WeeklyMediaEmail:
    email = build_weekly_media_email(
        config=config,
        repository=repository,
        start_date=start_date,
        end_date=end_date,
        scope=scope,
        top_limit=top_limit,
    )
    resolved_output = Path(output_path).expanduser() if output_path else (
        Path(config.report_dir).expanduser()
        / f"weekly-media-email-{email.metrics['start_date']}-to-{email.metrics['end_date']}.html"
    )
    ensure_parent(resolved_output)
    resolved_output.write_text(email.html_body, encoding="utf-8")
    return replace(email, output_path=str(resolved_output.resolve()))


def send_weekly_media_email(
    config: AppConfig,
    repository: FeedbackRepository,
    start_date: date | None = None,
    end_date: date | None = None,
    scope: str = "all",
    top_limit: int = 10,
) -> WeeklyMediaEmail:
    email = build_weekly_media_email(
        config=config,
        repository=repository,
        start_date=start_date,
        end_date=end_date,
        scope=scope,
        top_limit=top_limit,
    )
    send_email_message(config, email.subject, email.text_body, email.html_body)
    return email


def _resolve_end_date(repository: FeedbackRepository, preferred: date | None) -> date:
    if preferred:
        return preferred
    published_dates = repository.list_published_dates(limit=1)
    if published_dates:
        return date.fromisoformat(published_dates[0])
    return date.today()


def _weekly_report_title(config: AppConfig) -> str:
    subject_prefix = str(config.email_summary.subject_prefix or "").strip().lower()
    if "pocket 4" in subject_prefix or "pocket4" in subject_prefix:
        return "DJI Pocket 4 影像观点周报"
    if "x300" in subject_prefix:
        return "vivo X300 系列影像观点周报"
    return "Media Pulse 影像观点周报"


def _filter_rows_for_scope(rows: list[Any], scope: str) -> list[Any]:
    if scope == "all":
        return list(rows)
    return [row for row in rows if int(row["camera_related"] or 0) == 1 or _is_video_row(row)]


def _build_product_counts(rows: list[Any]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for row in rows:
        for product_tag in load_json(row["product_tags"], []):
            tag = str(product_tag or "").strip()
            if tag:
                counter[tag] += 1
    return counter


def _build_platform_counts(rows: list[Any]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for row in rows:
        counter[_platform_label(row)] += 1
    return counter


def _build_featured_items(rows: list[Any], limit: int, reference_date: date) -> list[dict[str, Any]]:
    ranked = sorted((_build_featured_item(row, reference_date) for row in rows), key=_featured_sort_key, reverse=True)
    featured: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    ranked = sorted(
        ranked,
        key=lambda item: (
            1 if int(item.get("insight_density") or 0) > 0 else 0,
            float(item.get("featured_score") or item.get("hot_score") or 0.0),
        ),
        reverse=True,
    )
    for item in ranked:
        dedupe_key = str(item["dedupe_key"] or "").strip()
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        featured.append(item)
        if len(featured) >= limit:
            break
    return featured


def _build_featured_item(row: Any, reference_date: date) -> dict[str, Any]:
    extra = load_json(row["extra_json"], {})
    title = clean_content_text(row["title"] or "") or "未命名内容"
    source_name = clean_content_text(row["source_section"] or row["source"] or "") or _hostname_label(str(row["url"] or ""))
    source_role = str(row["source_actor_type"] or "unknown").strip().lower() or "unknown"
    product_tags = [str(tag) for tag in load_json(row["product_tags"], []) if str(tag).strip()]
    output_data = _load_output_analysis(str((extra.get("video_analysis") or {}).get("output_file") or "").strip())
    analysis_tags = _merge_tags(output_data.get("sub_tags", []), load_json(row["domain_subtags_json"], []))
    hot_score = _hot_score(extra, row, reference_date)
    positive_details = _build_detail_entries(
        source_title=title,
        raw_values=output_data.get("positives", []),
        fallback_values=load_json(row["ai_positive_points_json"], []),
        sentiment="positive",
        hot_score=hot_score,
        theme_tags=analysis_tags,
        source_kind="review",
    )
    negative_details = _build_detail_entries(
        source_title=title,
        raw_values=output_data.get("negatives", []),
        fallback_values=load_json(row["ai_negative_points_json"], []),
        sentiment="negative",
        hot_score=hot_score,
        theme_tags=analysis_tags,
        source_kind="review",
    )
    comment_details = _build_comment_entries(extra, source_title=title, hot_score=hot_score)
    camera_signal_score = _camera_signal_score(row, analysis_tags, positive_details, negative_details, comment_details)
    insight_density = len(positive_details) + len(negative_details) + len(comment_details)
    insight_penalty = 12.0 if insight_density == 0 else 0.0
    published_at = parse_datetime(row["published_at"])
    dedupe_key = str(extra.get("dedupe_exact_key") or "").strip() if isinstance(extra, dict) else ""
    if not dedupe_key:
        dedupe_key = build_exact_dedupe_key(title, str(row["url"] or ""))

    return {
        "title": title,
        "url": str(row["url"] or "").strip(),
        "canonical_url": canonical_url(str(row["url"] or "").strip()),
        "source_name": source_name,
        "source_role": source_role,
        "source_role_label": SOURCE_ROLE_LABELS.get(source_role, SOURCE_ROLE_LABELS["unknown"]),
        "media_type": _media_type_label(row),
        "published_at": published_at,
        "published_label": _format_item_datetime(published_at),
        "product_tags": product_tags,
        "domain_subtags": analysis_tags[:4],
        "engagement_labels": _engagement_labels(extra),
        "hot_score": hot_score,
        "camera_signal_score": camera_signal_score,
        "featured_score": hot_score + camera_signal_score + insight_density * 1.2 - insight_penalty,
        "insight_density": insight_density,
        "positive_point_count": len(positive_details),
        "negative_point_count": len(negative_details),
        "comment_point_count": len(comment_details),
        "dedupe_key": dedupe_key,
        "analysis_summary": clean_content_text(output_data.get("summary") or "") or _row_summary(row),
        "positive_details": positive_details[:FEATURED_DETAIL_LIMIT],
        "negative_details": negative_details[:FEATURED_DETAIL_LIMIT],
        "comment_details": comment_details[:FEATURED_COMMENT_LIMIT],
        "focus_positive_points": positive_details + [item for item in comment_details if item["sentiment"] == "positive"],
        "focus_negative_points": negative_details + [item for item in comment_details if item["sentiment"] == "negative"],
    }


def _build_focus_groups(featured_items: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    positive_points: list[dict[str, Any]] = []
    negative_points: list[dict[str, Any]] = []
    for item in featured_items:
        positive_points.extend(item.get("focus_positive_points", []))
        negative_points.extend(item.get("focus_negative_points", []))
    return {
        "positive": _build_focus_group_items(positive_points, "positive"),
        "negative": _build_focus_group_items(negative_points, "negative"),
    }


def _build_focus_group_items(points: list[dict[str, Any]], sentiment: str) -> list[dict[str, Any]]:
    buckets: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for point in points:
        theme_key = str(point.get("theme_key") or _default_theme_key(sentiment))
        buckets[theme_key].append(point)

    groups: list[dict[str, Any]] = []
    for theme_key, theme_points in buckets.items():
        ranked = _unique_points(theme_points, limit=6)
        if not ranked:
            continue
        title = THEME_TITLES.get(sentiment, {}).get(theme_key, THEME_TITLES[sentiment][_default_theme_key(sentiment)])
        summary = THEME_SUMMARIES.get(sentiment, {}).get(theme_key, THEME_SUMMARIES[sentiment][_default_theme_key(sentiment)])
        groups.append(
            {
                "theme_key": theme_key,
                "title": title,
                "summary": summary,
                "overview": _group_overview_text(sentiment, theme_key),
                "evidence": ranked[:FOCUS_EVIDENCE_LIMIT],
                "point_count": len(theme_points),
                "source_count": len({clean_content_text(str(point.get("source_title") or "")) for point in theme_points if clean_content_text(str(point.get("source_title") or ""))}),
                "score": len(theme_points) * 4.0
                + sum(float(point.get("score") or 0.0) for point in ranked[:FOCUS_EVIDENCE_LIMIT])
                + len(ranked)
                + _theme_group_bonus(sentiment, theme_key),
            }
        )
    groups.sort(key=lambda item: (_theme_priority(sentiment, str(item["theme_key"])), -float(item["score"])))
    return groups[:FOCUS_GROUP_LIMIT]


def _build_overview_lines(
    summary: dict[str, Any],
    featured_items: list[dict[str, Any]],
    product_counts: Counter[str],
    platform_counts: Counter[str],
    focus_groups: dict[str, list[dict[str, Any]]],
) -> list[str]:
    media_breakdown = Counter()
    for item in summary.get("media_types", []):
        media_breakdown[str(item.get("name") or "")] = int(item.get("count") or 0)
    video_total = media_breakdown.get("长视频", 0) + media_breakdown.get("短视频", 0)
    article_total = max(0, int(summary.get("total") or 0) - video_total)
    top_products = " / ".join(_format_product_label(name) for name, _ in product_counts.most_common(2)) or "暂无明显集中机型"
    platform_text = "、".join(f"{name}（{count}条）" for name, count in platform_counts.most_common(4)) or "暂无明显平台集中"
    top_positive = focus_groups.get("positive", [])
    top_negative = focus_groups.get("negative", [])
    positive_line = top_positive[0]["overview"] if top_positive else "暂无明显集中好评。"
    negative_line = top_negative[0]["overview"] if top_negative else "暂无明显集中差评。"
    positive_points_total = sum(int(item.get("positive_point_count") or 0) for item in featured_items)
    negative_points_total = sum(int(item.get("negative_point_count") or 0) for item in featured_items)

    lines = [
        f"本周收录 {int(summary.get('total') or 0)} 条内容，其中视频 {video_total} 条、非视频 {article_total} 条。",
        f"讨论热度主要集中在 {top_products}，主要来源类型为 {platform_text}。",
        f"样本内共提炼出好评观点 {positive_points_total} 条、差评观点 {negative_points_total} 条，可直接作为周报结论池。",
        f"最热好评：{positive_line}",
        f"最热差评：{negative_line}",
    ]
    if featured_items:
        top_item = featured_items[0]
        top_signal = " / ".join(top_item["engagement_labels"][:2]) or "按发布时间优先"
        lines.append(
            f"本周最值得优先看的内容是《{top_item['title']}》，热度信号：{top_signal}，"
            f"相机讨论权重 {top_item['camera_signal_score']:.1f}，共覆盖 {top_item['insight_density']} 条有效观点。"
        )
    return lines


def _render_text_body(
    summary: dict[str, Any],
    featured_items: list[dict[str, Any]],
    overview_lines: list[str],
    focus_groups: dict[str, list[dict[str, Any]]],
    report_title: str,
) -> str:
    range_text = f"{summary.get('start_date')} ~ {summary.get('end_date')}"
    metrics_lines = [
        f"- 收录内容: {int(summary.get('total') or 0)}",
        f"- 视频内容: {_summary_count(summary, {'长视频', '短视频'})}",
        f"- 文章/评论: {_summary_non_video_count(summary)}",
        f"- 相机关联: {int(summary.get('camera_related_total') or 0)}",
        f"- 好评观点: {sum(int(item.get('positive_point_count') or 0) for item in featured_items)}",
        f"- 差评观点: {sum(int(item.get('negative_point_count') or 0) for item in featured_items)}",
    ]

    focus_lines: list[str] = []
    for label, key in (("核心好评", "positive"), ("核心差评", "negative")):
        for group in focus_groups.get(key, []):
            focus_lines.append(
                f"- {label} / {group['title']}（{int(group.get('point_count') or 0)}条观点，覆盖{int(group.get('source_count') or 0)}条内容）: {group['summary']}"
            )
            for point in _visible_points(group["evidence"], limit=FOCUS_EVIDENCE_LIMIT):
                focus_lines.append(f"  - {_format_point_reference(point)}")

    featured_lines: list[str] = []
    for index, item in enumerate(featured_items, start=1):
        meta = " | ".join(part for part in [item["source_name"], item["media_type"], item["published_label"]] if part)
        metrics = " / ".join(item["engagement_labels"][:3]) or "按发布时间入选"
        featured_lines.extend(
            [
                f"{index}. {item['title']}",
                f"   {meta}",
                f"   热度信号: {metrics}",
                f"   相机讨论权重: {item['camera_signal_score']:.1f} | 观点总量: {item['insight_density']}",
                f"   链接: {item['url']}",
                f"   核心观点: {item['analysis_summary'] or '暂无'}",
            ]
        )
        for heading, values in (
            ("好评", item.get("positive_details", [])),
            ("差评", item.get("negative_details", [])),
            ("评论区", item.get("comment_details", [])),
        ):
            if not values:
                continue
            featured_lines.append(f"   {heading}:")
            limit = FEATURED_COMMENT_LIMIT if heading == "评论区" else FEATURED_DETAIL_LIMIT
            for point in _visible_points(values, limit=limit):
                featured_lines.append(f"   - {_format_point_text(point)}")

    body_lines = [
        f"{report_title} - {range_text}",
        "",
        "本周概览",
        *[f"- {line}" for line in overview_lines],
        "",
        "核心指标",
        *metrics_lines,
        "",
        "本周关注方向",
        *(focus_lines or ["- 暂无明显焦点"]),
        "",
        "热门内容精选",
        *(featured_lines or ["- 本周暂无可展示内容"]),
        "",
        "说明",
        "- 热门排序优先参考 view/like/comment/score 等互动数据，缺失时回退到发布时间。",
    ]
    return "\n".join(body_lines)


def _render_html_body(
    summary: dict[str, Any],
    featured_items: list[dict[str, Any]],
    overview_lines: list[str],
    focus_groups: dict[str, list[dict[str, Any]]],
    report_title: str,
) -> str:
    range_text = f"{summary.get('start_date')} ~ {summary.get('end_date')}"
    metric_cards = [
        ("收录内容", str(int(summary.get("total") or 0))),
        ("视频内容", str(_summary_count(summary, {"长视频", "短视频"}))),
        ("文章/评论", str(_summary_non_video_count(summary))),
        ("相机关联", str(int(summary.get("camera_related_total") or 0))),
        ("好评观点", str(sum(int(item.get("positive_point_count") or 0) for item in featured_items))),
        ("差评观点", str(sum(int(item.get("negative_point_count") or 0) for item in featured_items))),
    ]
    featured_html = "".join(_render_featured_card(index, item) for index, item in enumerate(featured_items, start=1))
    if not featured_html:
        featured_html = '<div class="empty-state">本周暂无可展示的热门内容。</div>'

    positive_focus_html = _render_focus_column("核心好评", focus_groups.get("positive", []), "positive")
    negative_focus_html = _render_focus_column("核心差评", focus_groups.get("negative", []), "negative")
    trend_items = (summary.get("trend") or [])[-7:]
    trend_chart_html = _render_trend_chart(trend_items)
    trend_rows = "".join(_render_trend_row(item) for item in trend_items)

    return f"""<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{_escape(report_title)}</title>
    <style>
      body {{
        margin: 0;
        padding: 0;
        background: #f4f6f8;
        color: #17212f;
        font-family: Arial, "PingFang SC", "Microsoft YaHei", sans-serif;
      }}
      .wrapper {{
        max-width: 980px;
        margin: 0 auto;
        padding: 24px 16px 40px;
      }}
      .hero {{
        background: linear-gradient(135deg, #171717 0%, #232323 100%);
        color: #ffffff;
        border-radius: 8px;
        padding: 24px;
      }}
      .kicker {{
        margin: 0;
        font-size: 12px;
        line-height: 1.4;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #ff8a3d;
      }}
      .hero h1 {{
        margin: 10px 0 8px;
        font-size: 30px;
        line-height: 1.2;
      }}
      .hero p {{
        margin: 0;
        font-size: 14px;
        line-height: 1.6;
        color: rgba(255, 255, 255, 0.82);
      }}
      .section {{
        margin-top: 16px;
        background: #ffffff;
        border: 1px solid #e4e7eb;
        border-radius: 8px;
        padding: 20px;
      }}
      .section h2 {{
        margin: 0 0 14px;
        font-size: 18px;
        line-height: 1.3;
      }}
      .overview-list {{
        margin: 0;
        padding-left: 20px;
      }}
      .overview-list li {{
        margin: 0 0 10px;
        line-height: 1.7;
      }}
      .metrics {{
        margin: 0 -6px;
      }}
      .metric {{
        display: inline-block;
        vertical-align: top;
        width: calc(25% - 12px);
        min-width: 160px;
        margin: 0 6px 12px;
        background: #fafbfc;
        border: 1px solid #e9edf2;
        border-radius: 8px;
        padding: 14px;
      }}
      .metric-label {{
        margin: 0;
        font-size: 12px;
        line-height: 1.4;
        color: #6b7280;
      }}
      .metric-value {{
        margin: 8px 0 0;
        font-size: 28px;
        line-height: 1.15;
        font-weight: 700;
      }}
      .focus-grid {{
        font-size: 0;
      }}
      .focus-col {{
        display: inline-block;
        vertical-align: top;
        width: calc(50% - 8px);
        margin-right: 16px;
      }}
      .focus-col:last-child {{
        margin-right: 0;
      }}
      .focus-col h3 {{
        margin: 0 0 12px;
        font-size: 16px;
        line-height: 1.4;
      }}
      .focus-card {{
        background: #fafbfc;
        border: 1px solid #e9edf2;
        border-radius: 8px;
        padding: 14px;
        margin-bottom: 12px;
      }}
      .focus-card h4 {{
        margin: 0 0 8px;
        font-size: 14px;
        line-height: 1.5;
      }}
      .focus-summary {{
        margin: 0 0 10px;
        font-size: 14px;
        line-height: 1.7;
        color: #334155;
      }}
      .detail-list {{
        margin: 0;
        padding-left: 18px;
      }}
      .detail-list li {{
        margin: 0 0 8px;
        font-size: 13px;
        line-height: 1.7;
        color: #334155;
      }}
      .featured-card {{
        border-top: 1px solid #edf1f5;
        padding: 18px 0;
      }}
      .featured-card:first-child {{
        border-top: none;
        padding-top: 0;
      }}
      .featured-rank {{
        display: inline-block;
        min-width: 28px;
        margin-right: 10px;
        color: #ff7a00;
        font-weight: 700;
      }}
      .featured-title {{
        display: inline;
        font-size: 18px;
        line-height: 1.45;
        color: #111827;
        text-decoration: none;
      }}
      .featured-title:hover {{
        text-decoration: underline;
      }}
      .featured-meta {{
        margin: 8px 0 0;
        font-size: 13px;
        line-height: 1.6;
        color: #6b7280;
      }}
      .featured-summary {{
        margin: 10px 0 0;
        font-size: 14px;
        line-height: 1.75;
        color: #334155;
      }}
      .detail-block {{
        margin-top: 12px;
      }}
      .detail-block h4 {{
        margin: 0 0 8px;
        font-size: 14px;
        line-height: 1.4;
      }}
      .trend-table {{
        width: 100%;
        border-collapse: collapse;
        margin-top: 2px;
      }}
      .trend-chart-card {{
        margin-bottom: 16px;
        padding: 14px 14px 10px;
        background: linear-gradient(180deg, #fafbfc 0%, #f5f7fa 100%);
        border: 1px solid #e9edf2;
        border-radius: 8px;
      }}
      .trend-legend {{
        margin: 0 0 10px;
        font-size: 12px;
        line-height: 1.4;
        color: #475569;
      }}
      .trend-legend span {{
        display: inline-block;
        margin-right: 14px;
        white-space: nowrap;
      }}
      .trend-dot {{
        display: inline-block;
        width: 8px;
        height: 8px;
        margin-right: 6px;
        border-radius: 999px;
      }}
      .trend-svg {{
        display: block;
        width: 100%;
        height: auto;
      }}
      .trend-table td {{
        border-top: 1px solid #edf1f5;
        padding: 10px 0;
        font-size: 13px;
        line-height: 1.4;
      }}
      .trend-table tr:first-child td {{
        border-top: none;
      }}
      .empty-state {{
        color: #6b7280;
        font-size: 14px;
        line-height: 1.6;
      }}
      .footer {{
        margin-top: 16px;
        color: #6b7280;
        font-size: 12px;
        line-height: 1.7;
      }}
      @media screen and (max-width: 720px) {{
        .metric,
        .focus-col {{
          display: block;
          width: 100%;
          margin-right: 0;
        }}
      }}
    </style>
  </head>
  <body>
    <div class="wrapper">
      <section class="hero">
        <p class="kicker">Media Pulse</p>
        <h1>{_escape(report_title)}</h1>
        <p>{_escape(range_text)}</p>
      </section>

      <section class="section">
        <h2>本周概览</h2>
        <ul class="overview-list">
          {''.join(f"<li>{_escape(line)}</li>" for line in overview_lines)}
        </ul>
      </section>

      <section class="section">
        <h2>核心指标</h2>
        <div class="metrics">
          {''.join(_render_metric_card(label, value) for label, value in metric_cards)}
        </div>
      </section>

      <section class="section">
        <h2>本周关注方向</h2>
        <div class="focus-grid">
          <div class="focus-col">
            {positive_focus_html}
          </div>
          <div class="focus-col">
            {negative_focus_html}
          </div>
        </div>
      </section>

      <section class="section">
        <h2>热门内容精选</h2>
        {featured_html}
      </section>

      <section class="section">
        <h2>近 7 天趋势</h2>
        {trend_chart_html}
        <table class="trend-table" role="presentation">
          <tbody>
            {trend_rows or '<tr><td class="empty-state">暂无趋势数据</td></tr>'}
          </tbody>
        </table>
      </section>

      <div class="footer">
        热门排序优先参考 view / like / comment / score 等互动数据，缺失时回退到发布时间。<br />
        当前版本优先把“媒体观点 + 评论区信号 + 场景化问题”合并进周报正文，便于继续打磨成最终邮件模板。
      </div>
    </div>
  </body>
</html>
"""


def _render_metric_card(label: str, value: str) -> str:
    return (
        '<div class="metric">'
        f'<p class="metric-label">{_escape(label)}</p>'
        f'<p class="metric-value">{_escape(value)}</p>'
        "</div>"
    )


def _render_trend_chart(items: list[dict[str, Any]]) -> str:
    if not items:
        return '<div class="empty-state">暂无趋势图数据</div>'
    feedback_values = [int(item.get("total") or 0) for item in items]
    duration_values = [int(item.get("duration_total_seconds") or 0) for item in items]
    feedback_line = _sparkline(feedback_values)
    duration_line = _sparkline(duration_values)
    labels = " ".join(str(item.get("report_date") or "")[5:] for item in items)
    rows_html = "".join(
        (
            "<tr>"
            f'<td style="padding:6px 8px 6px 0;color:#64748b;font-size:12px;white-space:nowrap;">{_escape(str(item.get("report_date") or "")[5:])}</td>'
            f'<td style="padding:6px 8px;color:#17212f;font-size:12px;">{int(item.get("total") or 0)} 条</td>'
            f'<td style="padding:6px 0;color:#7c3aed;font-size:12px;">{_escape(_format_duration_label(int(item.get("duration_total_seconds") or 0)))}</td>'
            "</tr>"
        )
        for item in items
    )

    return (
        '<div class="trend-chart-card">'
        '<div class="trend-legend">'
        '<span><i class="trend-dot" style="background:#2563eb"></i>反馈量</span>'
        '<span><i class="trend-dot" style="background:#7c3aed"></i>总时长</span>'
        "</div>"
        '<table role="presentation" style="width:100%;border-collapse:collapse;margin-top:8px;">'
        '<tbody>'
        '<tr>'
        '<td style="width:92px;padding:6px 10px 6px 0;color:#475569;font-size:12px;white-space:nowrap;">反馈量走势</td>'
        f'<td style="padding:6px 0;font-family:Menlo,Consolas,monospace;font-size:18px;line-height:1.4;color:#2563eb;">{_escape(feedback_line)}</td>'
        '</tr>'
        '<tr>'
        '<td style="width:92px;padding:6px 10px 6px 0;color:#475569;font-size:12px;white-space:nowrap;">总时长走势</td>'
        f'<td style="padding:6px 0;font-family:Menlo,Consolas,monospace;font-size:18px;line-height:1.4;color:#7c3aed;">{_escape(duration_line)}</td>'
        '</tr>'
        '<tr>'
        '<td style="padding:4px 10px 0 0;color:#94a3b8;font-size:11px;white-space:nowrap;">日期</td>'
        f'<td style="padding:4px 0 0;color:#94a3b8;font-size:11px;font-family:Menlo,Consolas,monospace;">{_escape(labels)}</td>'
        '</tr>'
        '</tbody>'
        '</table>'
        '<table role="presentation" style="width:100%;border-collapse:collapse;margin-top:10px;">'
        '<tbody>'
        f'{rows_html}'
        '</tbody>'
        '</table>'
        "</div>"
    )


def _render_focus_column(title: str, groups: list[dict[str, Any]], tone: str) -> str:
    if not groups:
        return f'<h3>{_escape(title)}</h3><div class="empty-state">暂无明显信号</div>'
    blocks = "".join(_render_focus_card(group, tone) for group in groups)
    return f"<h3>{_escape(title)}</h3>{blocks}"


def _render_focus_card(group: dict[str, Any], tone: str) -> str:
    evidence_html = "".join(
        f"<li>{_escape(_format_point_reference(point))}</li>"
        for point in _visible_points(group.get("evidence", []), limit=FOCUS_EVIDENCE_LIMIT)
    )
    return (
        '<div class="focus-card">'
        f"<h4>{_escape(group['title'])}（{int(group.get('point_count') or 0)}条观点 / {int(group.get('source_count') or 0)}条内容）</h4>"
        f"<p class=\"focus-summary\">{_escape(group['summary'])}</p>"
        f"<ul class=\"detail-list\">{evidence_html}</ul>"
        "</div>"
    )


def _render_featured_card(index: int, item: dict[str, Any]) -> str:
    meta_parts = [item["source_name"], item["source_role_label"], item["media_type"], item["published_label"]]
    if item["engagement_labels"]:
        meta_parts.append(" / ".join(item["engagement_labels"][:3]))
    meta_parts.append(f"相机权重 {item['camera_signal_score']:.1f}")
    meta_parts.append(f"观点 {item['insight_density']} 条")

    detail_blocks = []
    if item.get("positive_details"):
        detail_blocks.append(_render_detail_block("好评点", item["positive_details"]))
    if item.get("negative_details"):
        detail_blocks.append(_render_detail_block("差评点", item["negative_details"]))
    if item.get("comment_details"):
        detail_blocks.append(_render_detail_block("评论区有价值信息", item["comment_details"]))

    return (
        '<div class="featured-card">'
        f'<div><span class="featured-rank">#{index}</span><a class="featured-title" href="{_escape(item["url"])}">{_escape(item["title"])}</a></div>'
        f'<p class="featured-meta">{_escape(" | ".join(part for part in meta_parts if part))}</p>'
        f'<p class="featured-summary"><strong>核心观点：</strong>{_escape(item["analysis_summary"] or "暂无")}</p>'
        f'{"".join(detail_blocks) or "<div class=\"empty-state\">暂无详细观点。</div>"}'
        "</div>"
    )


def _render_detail_block(title: str, values: list[dict[str, Any]]) -> str:
    limit = FEATURED_COMMENT_LIMIT if title == "评论区有价值信息" else FEATURED_DETAIL_LIMIT
    items = "".join(f"<li>{_escape(_format_point_text(item))}</li>" for item in _visible_points(values, limit=limit))
    if not items:
        return ""
    return (
        '<div class="detail-block">'
        f"<h4>{_escape(title)}（{len(values)}）</h4>"
        f'<ul class="detail-list">{items}</ul>'
        "</div>"
    )


def _render_trend_row(item: dict[str, Any]) -> str:
    day = str(item.get("report_date") or "")
    total = int(item.get("total") or 0)
    duration_total_seconds = int(item.get("duration_total_seconds") or 0)
    return (
        "<tr>"
        f"<td>{_escape(day)}</td>"
        f"<td>{_escape(str(total))} 条</td>"
        f"<td>{_escape(_format_duration_label(duration_total_seconds))}</td>"
        "</tr>"
    )


def _format_duration_label(seconds: int) -> str:
    value = max(0, int(seconds or 0))
    hours = value // 3600
    minutes = (value % 3600) // 60
    secs = value % 60
    if hours > 0:
        return f"{hours}小时{minutes}分"
    if minutes > 0:
        return f"{minutes}分{secs}秒"
    return f"{secs}秒"


def _summary_count(summary: dict[str, Any], names: set[str]) -> int:
    total = 0
    for item in summary.get("media_types", []):
        if str(item.get("name") or "") in names:
            total += int(item.get("count") or 0)
    return total


def _summary_non_video_count(summary: dict[str, Any]) -> int:
    return max(0, int(summary.get("total") or 0) - _summary_count(summary, {"长视频", "短视频"}))


def _build_detail_entries(
    source_title: str,
    raw_values: list[Any],
    fallback_values: list[Any],
    sentiment: str,
    hot_score: float,
    theme_tags: list[str],
    source_kind: str,
) -> list[dict[str, Any]]:
    values = _merge_detail_values(raw_values, fallback_values)
    items: list[dict[str, Any]] = []
    for raw in values:
        timestamp, text = _split_timestamp(str(raw or ""))
        if not text:
            continue
        detail_bonus = _detail_relevance_bonus(sentiment, text, source_kind)
        items.append(
            {
                "text": text,
                "display_text": f"{timestamp} {text}" if timestamp else text,
                "timestamp": timestamp,
                "source_title": source_title,
                "source_kind": source_kind,
                "sentiment": sentiment,
                "theme_key": _classify_theme_key(sentiment, text, theme_tags),
                "score": hot_score + (1.0 if timestamp else 0.0) + detail_bonus + _detail_length_bonus(text),
                "stage_label": "",
            }
        )
    return _unique_points(items, limit=6)


def _build_comment_entries(extra: dict[str, Any], source_title: str, hot_score: float) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for point in extra.get("ai_structured_points") or []:
        if not _is_comment_point(point):
            continue
        text = clean_content_text(str(point.get("text") or point.get("original_text") or ""))
        if not text:
            continue
        secondary_tags = [clean_content_text(value) for value in list(point.get("secondary_tags") or []) if clean_content_text(value)]
        purchase_stage = _extract_purchase_stage(secondary_tags)
        severity = clean_content_text(str(point.get("severity") or "")).lower()
        sentiment = clean_content_text(str(point.get("sentiment") or "neutral")).lower()
        if sentiment not in {"positive", "neutral", "negative"}:
            sentiment = "neutral"
        score = (
            hot_score
            + _severity_bonus(severity)
            + _purchase_stage_bonus(purchase_stage)
            + 2.0
            + _detail_relevance_bonus(sentiment, text, "comment")
        )
        items.append(
            {
                "text": text,
                "display_text": text,
                "timestamp": "",
                "source_title": source_title,
                "source_kind": "comment",
                "sentiment": sentiment,
                "theme_key": _classify_theme_key(sentiment, text, secondary_tags),
                "score": score,
                "stage_label": PURCHASE_STAGE_LABELS.get(purchase_stage, ""),
                "severity": severity,
            }
        )
    items.sort(key=lambda item: float(item["score"]), reverse=True)
    return _unique_points(items, limit=5)


def _unique_points(items: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    sorted_items = sorted(items, key=lambda item: float(item.get("score") or 0.0), reverse=True)
    for item in sorted_items:
        text = clean_content_text(str(item.get("text") or ""))
        signature = _point_signature(text)
        if not text or not signature:
            continue
        duplicate_index = -1
        for index, existing in enumerate(result):
            existing_signature = _point_signature(str(existing.get("text") or ""))
            if not existing_signature:
                continue
            if signature == existing_signature or signature in existing_signature or existing_signature in signature:
                duplicate_index = index
                break
        if duplicate_index >= 0:
            existing = result[duplicate_index]
            existing_text = clean_content_text(str(existing.get("text") or ""))
            existing_score = float(existing.get("score") or 0.0) + _detail_length_bonus(existing_text)
            current_score = float(item.get("score") or 0.0) + _detail_length_bonus(text)
            if len(signature) > len(_point_signature(existing_text)) + 6 or current_score > existing_score + 0.8:
                result[duplicate_index] = item
            continue
        result.append(item)
    result.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return result[:limit]


def _point_signature(text: str) -> str:
    _, content = _split_timestamp(text)
    normalized = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", clean_content_text(content).lower())
    return normalized


def _visible_points(values: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen: list[str] = []
    for point in values:
        formatted = _format_point_text(point)
        signature = _point_signature(formatted)
        if not signature:
            continue
        duplicate_index = -1
        for index, existing_signature in enumerate(seen):
            if signature == existing_signature or signature in existing_signature or existing_signature in signature:
                duplicate_index = index
                break
        if duplicate_index >= 0:
            existing_point = selected[duplicate_index]
            if len(formatted) > len(_format_point_text(existing_point)) + 6:
                selected[duplicate_index] = point
                seen[duplicate_index] = signature
            continue
        selected.append(point)
        seen.append(signature)
        if len(selected) >= limit:
            break
    return selected


def _group_overview_text(sentiment: str, theme_key: str) -> str:
    if sentiment == "positive":
        if theme_key == "photo_daylight":
            return "白天成像、HDR 和综合色彩是热度最高的好评点，尤其在对比测评里被反复提到。"
        if theme_key == "telephoto_zoom":
            return "长焦、人像和变焦是另一块高频好评，大家普遍觉得越级感明显。"
        if theme_key == "design_os":
            return "设计辨识度、握持手感和系统体验依旧是最容易收获好感的部分。"
        if theme_key == "battery_perf":
            return "续航、充电和流畅度整体口碑稳定，没有形成明显短板。"
        return "整体体验偏正向，围绕辨识度和完成度的评价更集中。"

    if theme_key == "video_capability":
        return "视频能力相关的争议主要集中在录制稳定性、暗光表现和专业视频工作流细节。"
    if theme_key == "low_light_aux":
        return "低光和副摄是另一块主要槽点，尤其是超广角和长焦夜景。"
    if theme_key == "exposure_focus":
        return "曝光、对焦和炫光问题在多条评测里重复出现。"
    if theme_key == "price_weight":
        return "价格、重量和套装使用门槛是另一类明确负面，说明这台机器更偏重度影像玩家。"
    if theme_key == "hardware_misc":
        return "评论区对扬声器、按键、信号和屏幕反光也有比较具体的抱怨。"
    return "本周负面反馈仍然集中在影像相关短板。"


def _load_output_analysis(path_text: str) -> dict[str, Any]:
    path = str(path_text or "").strip()
    if not path:
        return {}
    cached = _VIDEO_OUTPUT_CACHE.get(path)
    if cached is not None:
        return cached
    file_path = Path(path).expanduser()
    if not file_path.exists():
        _VIDEO_OUTPUT_CACHE[path] = {}
        return {}
    try:
        raw_text = file_path.read_text(encoding="utf-8")
        payload = _extract_json_object(raw_text)
    except (OSError, json.JSONDecodeError, RuntimeError):
        _VIDEO_OUTPUT_CACHE[path] = {}
        return {}
    result = {
        "summary": clean_content_text(payload.get("summary", "")),
        "positives": [clean_content_text(value) for value in list(payload.get("positives") or []) if clean_content_text(value)],
        "neutrals": [clean_content_text(value) for value in list(payload.get("neutrals") or []) if clean_content_text(value)],
        "negatives": [clean_content_text(value) for value in list(payload.get("negatives") or []) if clean_content_text(value)],
        "sub_tags": [clean_content_text(value) for value in list(payload.get("sub_tags") or []) if clean_content_text(value)],
    }
    _VIDEO_OUTPUT_CACHE[path] = result
    return result


def _extract_json_object(raw_text: str) -> dict[str, Any]:
    text = str(raw_text or "").strip()
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end <= start:
        raise RuntimeError("json_not_found")
    payload = json.loads(text[start : end + 1])
    return payload if isinstance(payload, dict) else {}


def _split_timestamp(raw_text: str) -> tuple[str, str]:
    cleaned = clean_content_text(raw_text)
    if not cleaned:
        return "", ""
    matched = _TIMESTAMP_RE.match(cleaned)
    if not matched:
        return "", cleaned
    return matched.group("ts"), clean_content_text(matched.group("text"))


def _classify_theme_key(sentiment: str, text: str, tags: list[str]) -> str:
    if sentiment not in {"positive", "negative"}:
        return _default_theme_key(sentiment)

    text_blob = clean_content_text(text).lower()
    tag_blob = " ".join(clean_content_text(tag) for tag in tags).lower()

    if sentiment == "positive":
        if any(token in text_blob for token in ("hdr", "阴影", "细节", "色彩", "曝光", "动态范围", "日光", "自拍", "自然", "真实", "相机", "照片", "video")):
            return "photo_daylight"
        if any(token in text_blob for token in ("长焦", "人像", "变焦", "telephoto", "zoom", "80mm", "潜望")):
            return "telephoto_zoom"
        if any(token in text_blob for token in ("glyph", "设计", "系统", "os", "软件", "界面", "屏幕", "亮度", "工业", "质感")):
            return "design_os"
        if any(token in text_blob for token in ("电池", "续航", "充电", "性能", "流畅", "芯片", "快充")):
            return "battery_perf"
        if not text_blob and any(token in tag_blob for token in ("hdr", "photo", "exposure", "color", "自拍", "相机", "照片", "video")):
            return "photo_daylight"
        if not text_blob and any(token in tag_blob for token in ("telephoto", "zoom", "portrait", "长焦", "人像", "变焦", "潜望")):
            return "telephoto_zoom"
        if not text_blob and any(token in tag_blob for token in ("glyph", "设计", "系统", "os", "软件", "界面", "亮度", "工业", "质感")):
            return "design_os"
        if not text_blob and any(token in tag_blob for token in ("电池", "续航", "充电", "性能", "流畅", "快充")):
            return "battery_perf"
        return "general_positive"

    if any(
        token in text_blob
        for token in (
            "价格",
            "昂贵",
            "太贵",
            "很贵",
            "售价",
            "卢布",
            "欧元",
            "元",
            "重量",
            "很重",
            "头重脚轻",
            "平衡感",
            "配件",
            "更换镜头",
            "套装",
            "缺货",
            "模块",
        )
    ):
        return "price_weight"
    if any(
        token in text_blob
        for token in ("信号", "wifi", "扬声器", "按钮", "反光", "扫描仪", "音质", "volume", "speaker", "松动", "机身", "做工", "发热", "glyph", "保护膜", "贴膜")
    ):
        return "hardware_misc"
    if any(token in text_blob for token in ("4k", "fps", "防抖", "稳定", "抖动", "视频", "录制", "stabil", "video", "掉帧", "卡顿")):
        return "video_capability"
    if any(token in text_blob for token in ("曝光", "对焦", "炫光", "flare", "色彩一致性", "模糊", "阴影", "focus", "glow", "光晕", "泛光")):
        return "exposure_focus"
    if any(token in text_blob for token in ("夜景", "低光", "噪点", "超广角", "长焦", "微距", "传感器", "副摄", "telephoto", "暗光")):
        return "low_light_aux"
    if not text_blob and any(token in tag_blob for token in ("price", "weight", "kit", "accessory", "价格", "重量", "配件", "套装")):
        return "price_weight"
    if not text_blob and any(token in tag_blob for token in ("4k", "fps", "防抖", "稳定", "视频", "video", "stabil")):
        return "video_capability"
    if not text_blob and any(token in tag_blob for token in ("exposure", "focus", "flare", "glow", "曝光", "对焦", "炫光")):
        return "exposure_focus"
    if not text_blob and any(token in tag_blob for token in ("night", "telephoto", "超广角", "长焦", "副摄", "低光", "夜景", "暗光")):
        return "low_light_aux"
    if not text_blob and any(token in tag_blob for token in ("speaker", "button", "wifi", "信号", "扬声器", "按钮", "反光", "音质")):
        return "hardware_misc"
    return "general_negative"


def _merge_detail_values(primary: list[Any], secondary: list[Any]) -> list[Any]:
    merged: list[Any] = []
    seen: set[str] = set()
    for raw in list(primary or []) + list(secondary or []):
        text = clean_content_text(str(raw or ""))
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        merged.append(raw)
    return merged


def _default_theme_key(sentiment: str) -> str:
    return "general_positive" if sentiment == "positive" else "general_negative"


def _theme_group_bonus(sentiment: str, theme_key: str) -> float:
    if sentiment == "positive":
        bonuses = {
            "photo_daylight": 18.0,
            "telephoto_zoom": 14.0,
            "design_os": 5.0,
            "battery_perf": 4.0,
            "general_positive": 0.0,
        }
        return bonuses.get(theme_key, 0.0)
    bonuses = {
        "video_capability": 40.0,
        "low_light_aux": 28.0,
        "exposure_focus": 18.0,
        "price_weight": 10.0,
        "hardware_misc": 4.0,
        "general_negative": 0.0,
    }
    return bonuses.get(theme_key, 0.0)


def _theme_priority(sentiment: str, theme_key: str) -> int:
    if sentiment == "positive":
        order = {
            "photo_daylight": 0,
            "telephoto_zoom": 1,
            "design_os": 2,
            "battery_perf": 3,
            "general_positive": 9,
        }
        return order.get(theme_key, 9)
    order = {
        "video_capability": 0,
        "low_light_aux": 1,
        "exposure_focus": 2,
        "price_weight": 3,
        "hardware_misc": 4,
        "general_negative": 9,
    }
    return order.get(theme_key, 9)


def _detail_length_bonus(text: str) -> float:
    length = len(clean_content_text(text))
    if length >= 60:
        return 1.8
    if length >= 36:
        return 1.2
    if length >= 22:
        return 0.6
    return 0.0


def _detail_relevance_bonus(sentiment: str, text: str, source_kind: str) -> float:
    blob = clean_content_text(text).lower()
    bonus = 0.0

    if any(token in blob for token in ("附赠", "包装", "保护壳", "保护膜", "usb-c", "贴膜", "开箱")):
        bonus -= 6.0
    if any(token in blob for token in ("整体评价积极", "整体体验积极", "整体认为", "有优缺点", "整体不错")):
        bonus -= 3.0

    if any(
        token in blob
        for token in (
            "日光",
            "夜景",
            "低光",
            "hdr",
            "动态范围",
            "阴影",
            "曝光",
            "色彩",
            "细节",
            "长焦",
            "人像",
            "变焦",
            "超广角",
            "自拍视频",
            "自拍",
            "4k",
            "fps",
            "防抖",
            "对焦",
            "镜头",
            "flare",
            "video",
        )
    ):
        bonus += 5.0

    if any(token in blob for token in ("glyph", "设计", "系统", "os", "软件", "界面", "工业感", "质感")):
        bonus += 3.0

    if any(token in blob for token in ("续航", "电池", "充电", "流畅", "性能", "亮度")):
        bonus += 2.0

    if source_kind == "comment":
        bonus += 1.0
    return bonus


def _camera_signal_score(
    row: Any,
    analysis_tags: list[str],
    positive_details: list[dict[str, Any]],
    negative_details: list[dict[str, Any]],
    comment_details: list[dict[str, Any]],
) -> float:
    score = 0.0
    domain_tag = clean_content_text(str(row["domain_tag"] or "")).lower()
    camera_related = int(row["camera_related"] or 0) == 1
    is_video = _is_video_row(row)

    if camera_related:
        score += 12.0
    if domain_tag == "camera":
        score += 10.0
    if is_video:
        score += 4.0

    tag_blob = " ".join(clean_content_text(tag).lower() for tag in analysis_tags)
    if any(
        token in tag_blob
        for token in ("photo", "video", "telephoto", "zoom", "portrait", "hdr", "night", "focus", "exposure")
    ):
        score += 6.0

    camera_theme_keys = {
        "photo_daylight",
        "telephoto_zoom",
        "video_capability",
        "low_light_aux",
        "exposure_focus",
    }
    score += 1.8 * sum(1 for point in positive_details if str(point.get("theme_key") or "") in camera_theme_keys)
    score += 2.2 * sum(1 for point in negative_details if str(point.get("theme_key") or "") in camera_theme_keys)
    score += 1.2 * sum(1 for point in comment_details if str(point.get("theme_key") or "") in camera_theme_keys)
    return score


def _severity_bonus(value: str) -> float:
    if value == "high":
        return 4.0
    if value == "medium":
        return 2.0
    return 0.5


def _purchase_stage_bonus(value: str) -> float:
    if value == "owned":
        return 2.0
    if value == "considering":
        return 1.0
    return 0.0


def _extract_purchase_stage(tags: list[str]) -> str:
    for tag in tags:
        lowered = tag.lower()
        if lowered.startswith("purchasestage:"):
            return lowered.split(":", 1)[1].strip()
    return "none"


def _is_comment_point(point: Any) -> bool:
    if not isinstance(point, dict):
        return False
    source_label = clean_content_text(str(point.get("source_label") or ""))
    severity_reason = clean_content_text(str(point.get("severity_reason") or ""))
    return source_label == "评论区" or severity_reason == "youtube_comment_mining"


def _merge_tags(primary: list[Any], fallback: list[Any]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for raw in list(primary or []) + list(fallback or []):
        text = clean_content_text(str(raw or ""))
        key = text.lower()
        if not text or key in seen:
            continue
        seen.add(key)
        merged.append(text)
    return merged


def _hot_score(extra: Any, row: Any, reference_date: date) -> float:
    score = 0.0
    data = extra if isinstance(extra, dict) else {}
    for key, weight in HOT_SCORE_METRICS:
        value = _safe_int(data.get(key))
        if value > 0:
            score += math.log10(value + 1) * weight
    if data:
        score += 2.5

    role = str(row["source_actor_type"] or "unknown").strip().lower()
    if role == "official_kol":
        score += 10.0
    elif role == "core_koc":
        score += 7.0
    elif role == "real_user":
        score += 3.0

    if _is_video_row(row):
        score += 6.0
    if clean_content_text(row["summary"] or ""):
        score += 1.0
    published_at = parse_datetime(row["published_at"])
    if published_at:
        age_days = max(0.0, (reference_date - published_at.astimezone(SHANGHAI_TZ).date()).days)
        score += max(0.0, 8.0 - min(age_days, 8.0))
    return score


def _engagement_labels(extra: Any) -> list[str]:
    data = extra if isinstance(extra, dict) else {}
    labels: list[str] = []
    for key, label in (
        ("view_count", "播放"),
        ("like_count", "赞"),
        ("comment_count", "评论"),
        ("favorite_count", "收藏"),
        ("score", "热度"),
        ("retweet_count", "转发"),
        ("repost_count", "转发"),
        ("reply_count", "回复"),
    ):
        value = _safe_int(data.get(key))
        if value <= 0:
            continue
        candidate = f"{_format_compact_int(value)}{label}"
        if candidate not in labels:
            labels.append(candidate)
        if len(labels) >= 4:
            break
    return labels


def _row_summary(row: Any) -> str:
    title = clean_content_text(row["title"] or "")
    summary = clean_content_text(row["summary"] or "")
    if summary and not is_summary_redundant(title, summary):
        return truncate(summary, 220)
    content = clean_content_text(row["content"] or "")
    if content and not is_summary_redundant(title, content):
        return truncate(content, 220)
    return ""


def _is_video_row(row: Any) -> bool:
    source = str(row["source"] or "").strip().lower()
    if source in {"x_api", "x_twscrape", "x_snscrape"}:
        return int(row["video_candidate"] or 0) == 1
    url = str(row["url"] or "").lower()
    return int(row["video_candidate"] or 0) == 1 or any(
        token in url
        for token in ("youtube.com", "youtu.be", "bilibili.com", "b23.tv", "x.com", "twitter.com", "instagram.com", "/video/", "/watch", "/shorts/")
    )


def _media_type_label(row: Any) -> str:
    if _is_video_row(row):
        url = str(row["url"] or "").lower()
        if any(token in url for token in ("/shorts/", "tiktok.com", "douyin.com", "instagram.com/reel/")):
            return "短视频"
        return "长视频"
    url = str(row["url"] or "").lower()
    if "/comments/" in url:
        return "评论"
    return "文章"


def _platform_label(row: Any) -> str:
    source = str(row["source"] or "").strip().lower()
    source_section = clean_content_text(row["source_section"] or "")
    host = _hostname_label(str(row["url"] or "")).lower()
    if "youtube" in source or "youtube" in source_section.lower() or "youtu" in host:
        return "YouTube"
    if "bilibili" in source or "bilibili" in host or "b23.tv" in host:
        return "Bilibili"
    if source.startswith("x_") or host in {"x.com", "twitter.com"}:
        return "X"
    if "instagram" in source or "instagram.com" in host:
        return "Instagram"
    if source.startswith("reddit") or "reddit.com" in host:
        return "Reddit"
    if source == "brand_community" or source.endswith("_community") or "community." in host:
        return "品牌社区"
    if source in {"google_news", "custom_rss"}:
        return "媒体站点"
    if source_section and source_section.lower() in {"news", "article", "blog", "rss"}:
        return "媒体站点"
    if host:
        return "媒体站点"
    return "其他平台"


def _format_compact_int(value: int) -> str:
    if value >= 10000:
        return f"{value / 10000:.1f}万"
    return str(value)


def _format_item_datetime(value: Any) -> str:
    parsed = value if hasattr(value, "astimezone") else parse_datetime(value)
    if not parsed:
        return ""
    return parsed.astimezone(SHANGHAI_TZ).strftime("%m-%d %H:%M")


def _format_product_label(value: str) -> str:
    raw = str(value or "").strip()
    aliases = {
        "4a pro": "Phone (4a) Pro",
        "4a": "Phone (4a)",
        "3a pro": "Phone (3a) Pro",
        "3a": "Phone (3a)",
        "phone3": "Phone (3)",
        "phone2": "Phone (2)",
        "2a": "Phone (2a)",
        "cmf phone1": "Accessory Phone 1",
    }
    return aliases.get(raw.lower(), raw)


def _hostname_label(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").replace("www.", "")
    except ValueError:
        return ""


def _featured_sort_key(item: dict[str, Any]) -> tuple[float, str]:
    published = item["published_at"]
    published_key = published.isoformat() if published else ""
    return float(item.get("featured_score") or item["hot_score"]), published_key


def _format_point_text(point: dict[str, Any]) -> str:
    text = str(point.get("display_text") or point.get("text") or "").strip()
    if _should_localize_point_as_comment(point):
        text = _localize_comment_text(point)
        prefix_bits = []
        stage_label = str(point.get("stage_label") or "").strip()
        sentiment = str(point.get("sentiment") or "").strip()
        source_kind = str(point.get("source_kind") or "").strip()
        if stage_label and source_kind == "comment":
            prefix_bits.append(stage_label)
        if sentiment == "positive" and source_kind == "comment":
            prefix_bits.append("好评")
        elif sentiment == "negative" and source_kind == "comment":
            prefix_bits.append("差评")
        prefix = f"{' / '.join(prefix_bits)}: " if prefix_bits else ""
        return prefix + text
    return text


def _format_point_reference(point: dict[str, Any]) -> str:
    if point.get("source_kind") == "comment":
        return _format_point_text(point)
    title = truncate(str(point.get("source_title") or ""), 60)
    return f"《{title}》：{_format_point_text(point)}"


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _escape(value: Any) -> str:
    return html.escape(str(value or ""))


def _sparkline(values: list[int]) -> str:
    if not values:
        return ""
    blocks = "▁▂▃▄▅▆▇█"
    min_value = min(values)
    max_value = max(values)
    if max_value <= 0 or max_value == min_value:
        return blocks[0] * len(values)
    span = max_value - min_value
    output: list[str] = []
    for value in values:
        index = round((value - min_value) * (len(blocks) - 1) / span)
        output.append(blocks[max(0, min(len(blocks) - 1, index))])
    return "".join(output)


def _localize_comment_text(point: dict[str, Any]) -> str:
    text = clean_content_text(str(point.get("text") or ""))
    original = clean_content_text(str(point.get("original_text") or ""))
    source = original or text
    normalized = source.lower()

    for prefix in ("已购用户反馈:", "意向用户反馈:", "评论区:"):
        text = text.replace(prefix, "").strip()
        source = source.replace(prefix, "").strip()
    normalized = source.lower()

    patterns = [
        (("hdr", "turning on", "camera app"), "HDR 会自动开启，手动关闭后重新打开相机又会恢复，影响拍照体验。"),
        (("doesn't match", "gallery"), "取景预览和最终保存到相册的成片不一致，这已经不是个别现象。"),
        (("140", "zoom"), "超高倍变焦更像宣传卖点，实际使用频率和成片质量都比较有限。"),
        (("degoogled", "target os"), "有去 Google 化系统用户对目标系统感兴趣，但对生态迁移仍有顾虑。"),
        (("degoogled", "brand os"), "有去 Google 化系统用户对目标系统感兴趣，但对生态迁移仍有顾虑。"),
        (("battery", "draining"), "有用户反馈续航下降较快，希望后续更新继续优化功耗表现。"),
        (("heating",), "有用户提到发热问题，说明日常使用稳定性还有优化空间。"),
        (("dslr", "cinematic"), "有用户认为相机依然是换机关键，说明影像能力仍有机会帮助目标品牌争取更多潜在用户。"),
        (("bought", "samsung", "zoom"), "有用户直言高倍变焦在真实使用中的频率很低，说明这类能力更像加分项而不是核心卖点。"),
    ]
    for cues, result in patterns:
        if all(cue in normalized for cue in cues):
            return result

    if _looks_mostly_ascii(source):
        primary_tag = clean_content_text(str(point.get("primary_tag") or "")).lower()
        sentiment = clean_content_text(str(point.get("sentiment") or "")).lower()
        if primary_tag == "camera" and sentiment == "negative":
            return "评论区反馈了相机体验问题，核心集中在稳定性、一致性或功能控制不够可靠。"
        if primary_tag == "camera" and sentiment == "positive":
            return "评论区对相机表现给出了正向评价，认为影像仍然是目标品牌的关键吸引点。"
        if sentiment == "negative":
            return "评论区出现了明确负面反馈，说明用户对当前体验仍有实际顾虑。"
        if sentiment == "positive":
            return "评论区有正向信号，说明用户认可这部分体验带来的吸引力。"
    return text or source


def _looks_mostly_ascii(text: str) -> bool:
    cleaned = clean_content_text(text)
    if not cleaned:
        return False
    ascii_count = sum(1 for char in cleaned if ord(char) < 128)
    return ascii_count / max(1, len(cleaned)) >= 0.8


def _should_localize_point_as_comment(point: dict[str, Any]) -> bool:
    if str(point.get("source_kind") or "") == "comment":
        return True
    text = clean_content_text(str(point.get("text") or point.get("display_text") or ""))
    normalized = text.lower()
    if any(prefix in text for prefix in ("已购用户反馈:", "意向用户反馈:", "评论区:")):
        return True
    if "comment" in normalized and _looks_mostly_ascii(text):
        return True
    return _looks_mostly_ascii(text) and any(
        cue in normalized
        for cue in ("hdr", "gallery", "degoogled", "target os", "brand os", "zoom", "dslr", "cinematic", "camera issue")
    )
