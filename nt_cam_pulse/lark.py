from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
import re
import time
from typing import Any, Protocol
from urllib.parse import urlparse

import requests

from .ai_enricher import normalize_secondary_tags_for_primary
from .classifier import PRODUCT_KEYWORDS
from .source_profile import SOURCE_LABELS
from .utils import (
    build_timestamped_video_url,
    build_feedback_point_uid,
    build_feedback_uid,
    clean_content_text,
    detect_language,
    format_seconds_label,
    is_video_url,
    load_json,
    parse_point_timestamp,
    parse_timestamp_to_seconds,
    truncate,
)


POINT_HIGH_SEVERITY_KEYWORDS = {
    "无法使用",
    "无法对焦",
    "不能拍照",
    "黑屏",
    "崩溃",
    "死机",
    "过热关机",
    "unusable",
    "cannot use",
    "can't use",
    "cannot focus",
    "black screen",
    "crash",
    "dead",
    "overheat",
}

POINT_MEDIUM_SEVERITY_KEYWORDS = {
    "问题",
    "模糊",
    "噪点",
    "对焦",
    "跑焦",
    "曝光",
    "过曝",
    "欠曝",
    "快门延迟",
    "发热",
    "抖动",
    "不稳定",
    "issue",
    "problem",
    "blurry",
    "noise",
    "focus",
    "exposure",
    "delay",
    "heating",
    "unstable",
}

POINT_NEGATIVE_SEVERITY_CUES = {
    "很差",
    "差评",
    "糟糕",
    "严重",
    "bad",
    "poor",
    "terrible",
    "awful",
}

POINT_SECONDARY_TAG_RULES: list[tuple[str, tuple[str, ...]]] = [
    ("HDR处理", ("hdr", "高光", "动态范围", "dynamic range")),
    ("色彩表现", ("色彩", "饱和", "saturation", "color")),
    ("对焦速度", ("对焦", "autofocus", "focus hunting", "跑焦")),
    ("快门延迟", ("快门", "shutter lag", "延迟")),
    ("噪点", ("噪点", "noise", "grain")),
    ("防抖", ("防抖", "stabilization", "ois", "eis", "抖动")),
    ("低光表现", ("夜景", "暗光", "low light", "night mode")),
    ("人像虚化", ("人像", "portrait", "bokeh")),
    ("视频稳定", ("视频", "录像", "fps", "frame")),
    ("算法调校", ("算法", "锐化", "processing", "algorithm")),
    ("发热", ("发热", "overheat", "heating", "温度")),
    ("续航", ("续航", "battery", "耗电")),
    ("屏幕亮度", ("亮度", "brightness", "屏幕")),
    ("系统体验", ("系统", "software", "os", "ui", "交互")),
    ("工业设计", ("设计", "外观", "做工", "质感", "design")),
]


@dataclass(slots=True)
class LarkConfig:
    enabled: bool
    base_url: str
    app_id: str
    app_secret: str
    bitable_app_token: str
    bitable_table_id: str
    field_mapping: dict[str, str]
    auto_create_fields: bool


@dataclass(slots=True)
class PointRecord:
    feedback_uid: str
    point_uid: str
    point_index: int
    source_code: str
    sentiment: str
    sentiment_label: str
    severity: str
    severity_label: str
    point_text_zh: str
    point_text_original: str
    point_language: str
    primary_tag: str
    secondary_tags: list[str]
    product_tags: list[str]
    timestamp_seconds: int | None
    timestamp_label: str
    source_label: str
    comment_meta: str
    comment_author: str


DEFAULT_FIELD_MAPPING = {
    "feedback_uid": "反馈ID",
    "point_uid": "观点ID",
    "point_index": "观点序号",
    "point_source_code": "渠道缩写",
    "point_sentiment": "观点情绪",
    "point_severity": "观点严重级别",
    "point_text": "观点内容",
    "point_text_original": "观点原文",
    "point_language": "原文语言",
    "point_primary_tag": "一级标签",
    "point_secondary_tags": "二级标签",
    "point_source_label": "来源标签",
    "comment_meta": "评论属性",
    "point_timestamp": "观点时间点",
    "point_timestamp_seconds": "观点时间秒",
    "title": "标题",
    "url": "链接",
    "source": "平台",
    "source_section": "来源补充",
    "author": "作者",
    "published_at": "发布时间",
    "camera_category": "问题类型",
    "camera_related": "相机关联",
    "sentiment": "情绪",
    "sentiment_reason": "情绪依据",
    "severity": "严重级别",
    "source_actor_type": "来源身份",
    "source_actor_reason": "来源判断依据",
    "domain_subtags": "二级标签",
    "product_tags": "产品标签",
    "primary_product": "主产品",
    "platform_group": "平台大类",
    "published_date": "发布时间日期",
    "is_negative": "是否负向",
    "is_high_severity": "是否高严重",
    "camera_keyword_hits": "命中关键词",
    "video_candidate": "待补视频转写",
    "summary": "摘要",
    "content": "原文",
    "report_date": "入库日期",
    "status": "跟进状态",
}

LARK_FIELD_TYPE_LABELS = {
    1: "多行文本",
    2: "数字",
    3: "单选",
    4: "多选",
    5: "日期",
    7: "复选框",
    15: "超链接",
}

DASHBOARD_FIELD_TYPE_RECOMMENDATIONS = {
    "point_sentiment": 3,
    "point_severity": 3,
    "point_primary_tag": 3,
    "point_secondary_tags": 4,
    "point_source_label": 3,
    "source": 3,
    "source_actor_type": 3,
    "product_tags": 4,
    "primary_product": 3,
    "platform_group": 3,
    "is_negative": 7,
    "is_high_severity": 7,
    "point_timestamp_seconds": 2,
    "camera_related": 7,
    "video_candidate": 7,
}

DASHBOARD_VIEW_SPECS: tuple[dict[str, Any], ...] = (
    {
        "name": "全量观点",
        "filters": [],
    },
    {
        "name": "负向问题",
        "filters": [
            {"field_key": "point_sentiment", "operator": "is", "value": "负向"},
        ],
    },
    {
        "name": "高严重负向",
        "filters": [
            {"field_key": "point_sentiment", "operator": "is", "value": "负向"},
            {"field_key": "point_severity", "operator": "is", "value": "高"},
        ],
    },
    {
        "name": "评论区观点",
        "filters": [
            {"field_key": "point_source_label", "operator": "is", "value": "评论区"},
        ],
    },
    {
        "name": "原视频观点",
        "filters": [
            {"field_key": "point_source_label", "operator": "is", "value": "原视频"},
        ],
    },
    {
        "name": "原帖子观点",
        "filters": [
            {"field_key": "point_source_label", "operator": "is", "value": "原帖子"},
        ],
    },
)


class LarkBitableClient:
    def __init__(self, raw_config: dict[str, Any]) -> None:
        self.config = LarkConfig(
            enabled=bool(raw_config.get("enabled", False)),
            base_url=str(raw_config.get("base_url", "https://open.feishu.cn")).rstrip("/"),
            app_id=str(raw_config.get("app_id", "")),
            app_secret=str(raw_config.get("app_secret", "")),
            bitable_app_token=str(raw_config.get("bitable_app_token", "")),
            bitable_table_id=str(raw_config.get("bitable_table_id", "")),
            field_mapping=dict(DEFAULT_FIELD_MAPPING | raw_config.get("field_mapping", {})),
            auto_create_fields=bool(raw_config.get("auto_create_fields", True)),
        )
        raw_categories = raw_config.get("_camera_categories", {})
        self._camera_categories: dict[str, list[str]] = {}
        if isinstance(raw_categories, dict):
            for category, values in raw_categories.items():
                if not isinstance(values, list):
                    continue
                keywords = [clean_content_text(value) for value in values if clean_content_text(value)]
                if keywords:
                    self._camera_categories[clean_content_text(category)] = keywords

        self._tenant_access_token: str | None = None
        self._table_field_names: set[str] | None = None
        self._table_primary_field_name: str | None = None
        self._table_field_ids: dict[str, str] = {}
        self._table_field_types: dict[str, int] = {}
        self._table_field_option_ids: dict[str, dict[str, str]] = {}
        self._transcript_cache: dict[str, list[tuple[int, str]]] = {}
        self._video_timestamp_hint_cache: dict[str, dict[str, list[tuple[int, str]]]] = {}
        self._request_timeout_seconds = 30
        self._max_request_attempts = 3

    def is_available(self) -> bool:
        required = [
            self.config.app_id,
            self.config.app_secret,
            self.config.bitable_app_token,
            self.config.bitable_table_id,
        ]
        return self.config.enabled and all(required)

    def prepare_dashboard_views(self) -> dict[str, Any]:
        if not self.is_available():
            return {
                "created": [],
                "updated": [],
                "skipped": [],
                "warnings": ["Lark 配置未启用或缺少必要凭证"],
            }

        self._get_table_field_names()
        existing_views = self._list_views()
        created: list[str] = []
        updated: list[str] = []
        skipped: list[str] = []
        warnings: list[str] = []

        for spec in DASHBOARD_VIEW_SPECS:
            view_name = clean_content_text(spec.get("name", ""))
            if not view_name:
                continue
            property_payload, missing_fields = self._build_dashboard_view_property(spec.get("filters") or [])
            if missing_fields:
                warnings.append(f"{view_name} 缺少字段: {', '.join(missing_fields)}")
                skipped.append(view_name)
                continue

            view_id = existing_views.get(view_name)
            if not view_id:
                view_id = self._create_view(view_name)
                created.append(view_name)
            if property_payload:
                self._update_view(view_id=view_id, view_name=view_name, property_payload=property_payload)
                updated.append(view_name)
            elif view_name not in created:
                skipped.append(view_name)

        return {
            "created": created,
            "updated": updated,
            "skipped": skipped,
            "warnings": warnings,
        }

    def inspect_dashboard_field_types(self) -> list[dict[str, str]]:
        self._get_table_field_names()
        rows: list[dict[str, str]] = []
        for field_key, expected_type in DASHBOARD_FIELD_TYPE_RECOMMENDATIONS.items():
            field_name = self._mapped_field_name(field_key)
            current_type = int(self._table_field_types.get(field_name, 0) or 0)
            rows.append(
                {
                    "field_key": field_key,
                    "field_name": field_name,
                    "current_type": self._field_type_label(current_type),
                    "expected_type": self._field_type_label(expected_type),
                    "ok": "1" if current_type == expected_type else "0",
                }
            )
        return rows

    def sync_rows(
        self,
        rows: list[Any],
        mark_synced: "MarkSyncedFn",
        mark_failed: "MarkLarkSyncFailedFn | None" = None,
        list_point_links: "ListPointLinksFn | None" = None,
        get_point_record_id: "GetPointRecordIdFn | None" = None,
        upsert_point_link: "UpsertPointLinkFn | None" = None,
        delete_point_link: "DeletePointLinkFn | None" = None,
        mark_point_failed: "MarkPointSyncFailedFn | None" = None,
        on_row_result: "SyncRowResultFn | None" = None,
    ) -> int:
        if not self.is_available():
            return 0

        synced_rows = 0
        for row in rows:
            row_id = int(row["id"])
            points: list[PointRecord] = []
            try:
                points = self._extract_points(row)
                existing_map = self._build_existing_point_record_map(row_id, list_point_links)
                legacy_record_id = str(row["lark_record_id"] or "").strip()
                if legacy_record_id and not existing_map:
                    try:
                        self._delete_record(legacy_record_id)
                    except Exception as exc:  # noqa: BLE001
                        if not self._is_record_missing_error(exc):
                            raise

                current_uids = {point.point_uid for point in points}
                stale_uids = set(existing_map) - current_uids
                for stale_uid in stale_uids:
                    stale_record_id = str(existing_map.get(stale_uid) or "").strip()
                    if stale_record_id:
                        try:
                            self._delete_record(stale_record_id)
                        except Exception as exc:  # noqa: BLE001
                            if not self._is_record_missing_error(exc):
                                raise
                    if delete_point_link:
                        delete_point_link(stale_uid)

                first_record_id = ""
                for point in points:
                    fields = self._build_point_fields(row, point)
                    fields = self._prepare_fields_for_table(fields)
                    record_id = str(existing_map.get(point.point_uid) or "").strip()
                    if (not record_id) and get_point_record_id:
                        record_id = str(get_point_record_id(point.point_uid) or "").strip()
                    synced_record_id = self._upsert_record(record_id=record_id, fields=fields)
                    if not synced_record_id:
                        raise RuntimeError(f"lark_empty_record_id: {point.point_uid}")
                    if not first_record_id:
                        first_record_id = synced_record_id
                    if upsert_point_link:
                        upsert_point_link(row_id, point.point_uid, synced_record_id)

                mark_synced(row_id, first_record_id)
                synced_rows += 1
                if on_row_result:
                    on_row_result(
                        {
                            "row_id": row_id,
                            "title": str(row["title"] or ""),
                            "url": str(row["url"] or ""),
                            "status": "synced",
                            "point_count": len(points),
                            "record_id": first_record_id,
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                if mark_failed:
                    mark_failed(row_id, str(exc))
                if mark_point_failed:
                    for point in points or self._extract_points(row):
                        mark_point_failed(row_id, point.point_uid, str(exc))
                if on_row_result:
                    on_row_result(
                        {
                            "row_id": row_id,
                            "title": str(row["title"] or ""),
                            "url": str(row["url"] or ""),
                            "status": "failed",
                            "point_count": len(points),
                            "error": str(exc),
                        }
                    )
        return synced_rows

    def _build_existing_point_record_map(self, row_id: int, list_point_links: "ListPointLinksFn | None") -> dict[str, str]:
        if not list_point_links:
            return {}
        mapping: dict[str, str] = {}
        for link in list_point_links(row_id):
            if isinstance(link, dict):
                point_uid = str(link.get("point_uid", "")).strip()
                record_id = str(link.get("lark_record_id", "")).strip()
            else:
                point_uid = str(link["point_uid"] or "").strip()
                record_id = str(link["lark_record_id"] or "").strip()
            if point_uid:
                mapping[point_uid] = record_id
        return mapping

    def _extract_points(self, row: Any) -> list[PointRecord]:
        source_code = self._source_code_of_row(row)
        feedback_uid = build_feedback_uid(int(row["id"]), source_code=source_code)
        items: list[PointRecord] = []
        seen_uids: set[str] = set()
        row_products = [clean_content_text(tag) for tag in load_json(row["product_tags"], []) if clean_content_text(tag)]
        structured_points = self._load_structured_points(row)
        if structured_points:
            return self._build_points_from_structured(
                row=row,
                source_code=source_code,
                feedback_uid=feedback_uid,
                row_products=row_products,
                structured_points=structured_points,
                seen_uids=seen_uids,
            )

        sentiment_sources = [
            ("positive", "正向", load_json(row["ai_positive_points_json"], [])),
            ("neutral", "中性", load_json(row["ai_neutral_points_json"], [])),
            ("negative", "负向", load_json(row["ai_negative_points_json"], [])),
        ]

        point_index = 1
        for sentiment, label, points in sentiment_sources:
            for ordinal, raw_point in enumerate(points, start=1):
                text = clean_content_text(raw_point)
                if not text:
                    continue
                ts_seconds, ts_label, point_text = parse_point_timestamp(text)
                normalized_point = truncate(clean_content_text(point_text or text), 1200)
                if not normalized_point:
                    continue

                point_uid = build_feedback_point_uid(feedback_uid, sentiment, normalized_point, ordinal)
                while point_uid in seen_uids:
                    ordinal += 1
                    point_uid = build_feedback_point_uid(feedback_uid, sentiment, normalized_point, ordinal)
                seen_uids.add(point_uid)

                original_text = self._extract_point_original_text(row=row, timestamp_seconds=ts_seconds, point_text=normalized_point)
                primary_tag, secondary_tags = self._classify_point_tags(normalized_point, row)
                point_products = self._classify_point_products(normalized_point, row_products)
                severity = self._score_point_severity(normalized_point, sentiment)

                items.append(
                    PointRecord(
                        feedback_uid=feedback_uid,
                        point_uid=point_uid,
                        point_index=point_index,
                        source_code=source_code,
                        sentiment=sentiment,
                        sentiment_label=label,
                        severity=severity,
                        severity_label=self._severity_label(severity),
                        point_text_zh=normalized_point,
                        point_text_original=truncate(original_text, 1200),
                        point_language=detect_language(original_text or normalized_point),
                        primary_tag=primary_tag,
                        secondary_tags=secondary_tags,
                        product_tags=point_products,
                        timestamp_seconds=ts_seconds,
                        timestamp_label=ts_label,
                        source_label=self._infer_source_label(row=row, explicit_raw=""),
                        comment_meta="",
                        comment_author="",
                    )
                )
                point_index += 1

        if items:
            return items

        fallback_sentiment = str(row["sentiment"] or "neutral").strip().lower() or "neutral"
        fallback_label = {"positive": "正向", "neutral": "中性", "negative": "负向"}.get(fallback_sentiment, "中性")
        fallback_text = truncate(clean_content_text(row["summary"] or row["content"] or row["title"] or ""), 1200)
        fallback_uid = build_feedback_point_uid(feedback_uid, fallback_sentiment, fallback_text, 1)
        primary_tag, secondary_tags = self._classify_point_tags(fallback_text, row)
        point_products = self._classify_point_products(fallback_text, row_products)
        severity = self._score_point_severity(fallback_text, fallback_sentiment)
        return [
            PointRecord(
                feedback_uid=feedback_uid,
                point_uid=fallback_uid,
                point_index=1,
                source_code=source_code,
                sentiment=fallback_sentiment,
                sentiment_label=fallback_label,
                severity=severity,
                severity_label=self._severity_label(severity),
                point_text_zh=fallback_text,
                point_text_original="",
                point_language=detect_language(fallback_text),
                primary_tag=primary_tag,
                secondary_tags=secondary_tags,
                product_tags=point_products,
                timestamp_seconds=None,
                timestamp_label="",
                source_label=self._infer_source_label(row=row, explicit_raw=""),
                comment_meta="",
                comment_author="",
            )
        ]

    def _build_points_from_structured(
        self,
        row: Any,
        source_code: str,
        feedback_uid: str,
        row_products: list[str],
        structured_points: list[dict[str, Any]],
        seen_uids: set[str],
    ) -> list[PointRecord]:
        items: list[PointRecord] = []
        per_sentiment_ordinal: dict[str, int] = {"positive": 0, "neutral": 0, "negative": 0}
        timestamp_hints = self._collect_timestamp_hints(row)
        point_index = 1
        for point in structured_points:
            sentiment = self._normalize_sentiment(point.get("sentiment"))
            per_sentiment_ordinal[sentiment] += 1
            ordinal = per_sentiment_ordinal[sentiment]
            raw_text = clean_content_text(point.get("text", ""))
            parsed_seconds, parsed_label, parsed_text = parse_point_timestamp(raw_text)
            point_text = truncate(clean_content_text(parsed_text or raw_text), 1200)
            if not point_text:
                continue

            raw_seconds = point.get("timestamp_seconds")
            timestamp_seconds: int | None = None
            if raw_seconds is not None and str(raw_seconds).strip() != "":
                try:
                    timestamp_seconds = max(0, int(float(raw_seconds)))
                except (TypeError, ValueError):
                    timestamp_seconds = None
            if timestamp_seconds is None:
                timestamp_seconds = parsed_seconds
            timestamp_label = clean_content_text(point.get("timestamp_label", "")) or parsed_label
            if (timestamp_seconds is None) or (timestamp_seconds == 0 and not timestamp_label):
                hint = self._pick_timestamp_hint(timestamp_hints, sentiment=sentiment, ordinal=ordinal)
                if hint:
                    timestamp_seconds, timestamp_label = hint
            if timestamp_seconds is not None and not timestamp_label:
                timestamp_label = format_seconds_label(timestamp_seconds)

            point_uid = build_feedback_point_uid(feedback_uid, sentiment, point_text, ordinal)
            while point_uid in seen_uids:
                ordinal += 1
                point_uid = build_feedback_point_uid(feedback_uid, sentiment, point_text, ordinal)
            seen_uids.add(point_uid)

            primary_tag = clean_content_text(point.get("primary_tag", "")) or clean_content_text(row["camera_category"] or "") or "Others"
            secondary_tags = self._normalize_secondary_tags(
                point.get("secondary_tags"),
                primary_tag=primary_tag,
                point_text=point_text,
            )
            severity = self._normalize_severity(point.get("severity", "")) or self._score_point_severity(point_text, sentiment)
            sentiment_label = {"positive": "正向", "neutral": "中性", "negative": "负向"}.get(sentiment, "中性")
            original_text_candidate = clean_content_text(point.get("original_text", ""))
            original_text_candidate = self._sanitize_original_text(
                row=row,
                point_text=point_text,
                original_text=original_text_candidate,
            )
            original_text = original_text_candidate or self._extract_point_original_text(
                row=row,
                timestamp_seconds=timestamp_seconds,
                point_text=point_text,
            )

            point_products = [clean_content_text(value) for value in (point.get("product_tags") or []) if clean_content_text(value)]
            if not point_products:
                point_products = self._extract_product_tags_from_secondary_markers(point.get("secondary_tags"))
            if not point_products:
                point_products = self._classify_point_products(point_text, row_products)

            comment_meta = self._build_comment_meta(point=point)
            severity_reason = clean_content_text(point.get("severity_reason", "")).lower()
            has_comment_cue = bool(
                comment_meta
                or clean_content_text(point.get("comment_id", ""))
                or clean_content_text(point.get("comment_author", ""))
                or ("youtube_comment_mining" in severity_reason)
            )
            source_label = self._infer_source_label(
                row=row,
                explicit_raw=point.get("source_label", "") or point.get("source_origin", "") or point.get("origin", ""),
            )
            if has_comment_cue and source_label != "评论区":
                source_label = "评论区"

            comment_author = ""
            if source_label == "评论区":
                comment_author = clean_content_text(point.get("comment_author", "") or point.get("author", ""))
            else:
                comment_meta = ""

            items.append(
                PointRecord(
                    feedback_uid=feedback_uid,
                    point_uid=point_uid,
                    point_index=point_index,
                    source_code=source_code,
                    sentiment=sentiment,
                    sentiment_label=sentiment_label,
                    severity=severity,
                    severity_label=self._severity_label(severity),
                    point_text_zh=point_text,
                    point_text_original=truncate(original_text, 1200),
                    point_language=detect_language(original_text or point_text),
                    primary_tag=primary_tag,
                    secondary_tags=secondary_tags,
                    product_tags=self._unique_list(point_products)[:4],
                    timestamp_seconds=timestamp_seconds,
                    timestamp_label=timestamp_label,
                    source_label=source_label,
                    comment_meta=comment_meta,
                    comment_author=comment_author,
                )
            )
            point_index += 1
        return items

    @staticmethod
    def _pick_timestamp_hint(
        hints: dict[str, list[tuple[int, str]]],
        sentiment: str,
        ordinal: int,
    ) -> tuple[int, str] | None:
        rows = hints.get(str(sentiment).strip().lower(), [])
        if not rows:
            return None
        index = max(0, int(ordinal) - 1)
        if index < len(rows):
            return rows[index]
        return None

    def _collect_timestamp_hints(self, row: Any) -> dict[str, list[tuple[int, str]]]:
        # Prefer timestamp hints from videosummary output. Fall back to existing
        # point strings in DB if output file is missing.
        from_output = self._load_video_output_timestamp_hints(row)
        from_row = self._load_row_point_timestamp_hints(row)
        merged: dict[str, list[tuple[int, str]]] = {"positive": [], "neutral": [], "negative": []}
        for key in ("positive", "neutral", "negative"):
            output_rows = from_output.get(key, [])
            row_rows = from_row.get(key, [])
            if output_rows and len(output_rows) >= len(row_rows):
                merged[key] = output_rows
            elif row_rows:
                merged[key] = row_rows
            else:
                merged[key] = output_rows
        return merged

    def _load_row_point_timestamp_hints(self, row: Any) -> dict[str, list[tuple[int, str]]]:
        result: dict[str, list[tuple[int, str]]] = {"positive": [], "neutral": [], "negative": []}
        buckets = {
            "positive": load_json(row["ai_positive_points_json"], []),
            "neutral": load_json(row["ai_neutral_points_json"], []),
            "negative": load_json(row["ai_negative_points_json"], []),
        }
        for sentiment, values in buckets.items():
            if not isinstance(values, list):
                continue
            for raw in values:
                sec, label, _text = parse_point_timestamp(clean_content_text(str(raw)))
                if sec is None:
                    continue
                result[sentiment].append((int(sec), label or format_seconds_label(sec)))
        return result

    def _load_video_output_timestamp_hints(self, row: Any) -> dict[str, list[tuple[int, str]]]:
        empty: dict[str, list[tuple[int, str]]] = {"positive": [], "neutral": [], "negative": []}
        extra = load_json(row["extra_json"], {})
        if not isinstance(extra, dict):
            return empty
        video_analysis = extra.get("video_analysis", {})
        if not isinstance(video_analysis, dict):
            return empty
        output_file = str(video_analysis.get("output_file", "")).strip()
        if not output_file:
            return empty
        if output_file in self._video_timestamp_hint_cache:
            return self._video_timestamp_hint_cache[output_file]

        output_path = Path(output_file).expanduser()
        if not output_path.exists():
            self._video_timestamp_hint_cache[output_file] = empty
            return empty

        try:
            raw_text = output_path.read_text(encoding="utf-8", errors="ignore")
            start = raw_text.find("{")
            end = raw_text.rfind("}")
            if start < 0 or end <= start:
                self._video_timestamp_hint_cache[output_file] = empty
                return empty
            payload = json.loads(raw_text[start : end + 1])
            if not isinstance(payload, dict):
                self._video_timestamp_hint_cache[output_file] = empty
                return empty
        except Exception:  # noqa: BLE001
            self._video_timestamp_hint_cache[output_file] = empty
            return empty

        result: dict[str, list[tuple[int, str]]] = {"positive": [], "neutral": [], "negative": []}
        key_map = {"positive": "positives", "neutral": "neutrals", "negative": "negatives"}
        for sentiment, source_key in key_map.items():
            values = payload.get(source_key, [])
            if not isinstance(values, list):
                continue
            for raw in values:
                sec, label, _text = parse_point_timestamp(clean_content_text(str(raw)))
                if sec is None:
                    continue
                result[sentiment].append((int(sec), label or format_seconds_label(sec)))

        self._video_timestamp_hint_cache[output_file] = result
        return result

    def _load_structured_points(self, row: Any) -> list[dict[str, Any]]:
        extra = load_json(row["extra_json"], {})
        if not isinstance(extra, dict):
            return []
        points = extra.get("ai_structured_points")
        if not isinstance(points, list):
            return []
        result: list[dict[str, Any]] = []
        for item in points:
            if not isinstance(item, dict):
                continue
            text = clean_content_text(item.get("text", ""))
            if not text:
                continue
            result.append(item)
            if len(result) >= 80:
                break
        return result

    @staticmethod
    def _normalize_sentiment(raw: Any) -> str:
        value = clean_content_text(raw or "").lower()
        if value in {"positive", "neutral", "negative"}:
            return value
        return "neutral"

    @staticmethod
    def _normalize_severity(raw: Any) -> str:
        value = clean_content_text(raw or "").lower()
        if value in {"high", "medium", "low"}:
            return value
        if value in {"高", "严重"}:
            return "high"
        if value in {"中", "中等"}:
            return "medium"
        if value in {"低", "轻微"}:
            return "low"
        return ""

    def _normalize_secondary_tags(self, raw: Any, primary_tag: str, point_text: str = "") -> list[str]:
        values = raw if isinstance(raw, list) else []
        normalized = self._unique_list([clean_content_text(value) for value in values if clean_content_text(value)])
        filtered: list[str] = []
        primary_key = clean_content_text(primary_tag).lower()
        for tag in normalized:
            lowered = clean_content_text(tag).lower()
            if lowered == primary_key or self._is_comment_meta_token(lowered):
                continue
            filtered.append(tag)
        return normalize_secondary_tags_for_primary(
            primary_tag=primary_tag,
            raw_tags=filtered,
            text=point_text,
            limit=6,
        )

    @staticmethod
    def _is_comment_meta_token(value: str) -> bool:
        lowered = clean_content_text(value).lower()
        return (
            lowered.startswith("priority:")
            or lowered.startswith("purchasestage:")
            or lowered.startswith("model:")
        )

    @staticmethod
    def _extract_product_tags_from_secondary_markers(raw: Any) -> list[str]:
        if not isinstance(raw, list):
            return []
        model_map = {
            "phone_3": "phone3",
            "phone 3": "phone3",
            "phone3": "phone3",
            "3": "phone3",
            "phone_3a": "3a",
            "phone 3a": "3a",
            "phone3a": "3a",
            "3a": "3a",
            "phone_3a_pro": "3a pro",
            "phone 3a pro": "3a pro",
            "phone3apro": "3a pro",
            "3a pro": "3a pro",
            "phone_4a": "4a",
            "phone 4a": "4a",
            "phone4a": "4a",
            "4a": "4a",
            "phone_4a_pro": "4a pro",
            "phone 4a pro": "4a pro",
            "phone4apro": "4a pro",
            "4a pro": "4a pro",
        }
        output: list[str] = []
        for value in raw:
            text = clean_content_text(value)
            if not text:
                continue
            matched = re.search(r"model\s*:\s*(.+)$", text, re.I)
            if not matched:
                continue
            raw_value = clean_content_text(matched.group(1)).lower().replace("-", " ").replace("_", " ")
            raw_value = re.sub(r"\s+", " ", raw_value).strip()
            mapped = model_map.get(raw_value)
            if mapped and mapped not in output:
                output.append(mapped)
        return output[:4]

    def _infer_source_label(self, row: Any, explicit_raw: Any) -> str:
        explicit = clean_content_text(explicit_raw or "")
        explicit_key = explicit.lower()
        if explicit_key in {"评论区", "comment", "comments", "reply", "replies"}:
            return "评论区"
        if explicit_key in {"原视频", "video", "source_video"}:
            return "原视频"
        if explicit_key in {"原帖子", "post", "source_post"}:
            return "原帖子"

        source_section = clean_content_text(row["source_section"] or "").lower()
        source = clean_content_text(row["source"] or "").lower()
        if self._looks_like_comment_source(source_section) or self._looks_like_comment_source(source):
            return "评论区"
        if self._is_video_row(row):
            return "原视频"
        return "原帖子"

    @staticmethod
    def _looks_like_comment_source(text: str) -> bool:
        value = clean_content_text(text).lower()
        if not value:
            return False
        return any(token in value for token in ("comment", "comments", "reply", "评论", "评论区"))

    @staticmethod
    def _is_video_row(row: Any) -> bool:
        source = clean_content_text(row["source"] or "").lower()
        if source in {"youtube", "youtube_yt_dlp", "youtube_manual"}:
            return True
        if int(row["video_candidate"] or 0) == 1:
            return True
        url = clean_content_text(row["url"] or "")
        if not url:
            return False
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:  # noqa: BLE001
            host = ""
        if any(token in host for token in ("youtube.com", "youtu.be", "bilibili.com", "b23.tv", "vimeo.com", "tiktok.com", "douyin.com")):
            return True
        return False

    def _build_comment_meta(self, point: dict[str, Any]) -> str:
        priority = self._extract_priority_token(point.get("priority", ""))
        purchase_stage = self._extract_purchase_stage(point.get("purchase_stage", ""))
        raw_meta = clean_content_text(point.get("comment_meta", ""))
        if not priority:
            priority = self._extract_priority_token(raw_meta)
        if not purchase_stage:
            purchase_stage = self._extract_purchase_stage(raw_meta)
        if (not priority) or (not purchase_stage):
            raw_secondary = point.get("secondary_tags")
            if isinstance(raw_secondary, list):
                for tag in raw_secondary:
                    if not priority:
                        priority = self._extract_priority_token(tag)
                    if not purchase_stage:
                        purchase_stage = self._extract_purchase_stage(tag)
                    if priority and purchase_stage:
                        break

        if not priority and not purchase_stage:
            return ""
        if not priority:
            priority = "P2"
        if not purchase_stage:
            purchase_stage = "none"
        return f"Priority:{priority},PurchaseStage:{purchase_stage}"

    @staticmethod
    def _extract_priority_token(raw: Any) -> str:
        text = clean_content_text(raw or "")
        if not text:
            return ""
        matched = re.search(r"(?:priority\s*:\s*)?p?\s*([1-4])", text, re.I)
        if not matched:
            return ""
        return f"P{matched.group(1)}"

    @staticmethod
    def _extract_purchase_stage(raw: Any) -> str:
        text = clean_content_text(raw or "").lower()
        if not text:
            return ""
        if any(token in text for token in ("owned", "已购", "买了", "在用", "使用中")):
            return "owned"
        if any(token in text for token in ("considering", "想买", "考虑", "观望", "purchase_intent")):
            return "considering"
        if "none" in text or "无意向" in text:
            return "none"
        matched = re.search(r"purchasestage\s*:\s*([a-z_]+)", text, re.I)
        if matched:
            value = clean_content_text(matched.group(1)).lower()
            if value in {"owned", "considering", "none"}:
                return value
        return ""

    def _sanitize_original_text(self, row: Any, point_text: str, original_text: str) -> str:
        original = clean_content_text(original_text)
        if not original:
            return ""
        source_lang = clean_content_text(row["language"] or "").lower()
        source_content_lang = detect_language(clean_content_text(row["content"] or ""))
        point_lang = detect_language(point_text)
        original_lang = detect_language(original)
        original_cjk_ratio = self._cjk_ratio(original)
        original_latin_words = self._latin_word_count(original)
        if source_lang in {"en", "mixed"} and original_lang == "zh":
            return ""
        if source_content_lang == "en" and original_cjk_ratio >= 0.15:
            return ""
        if source_lang in {"en", "mixed"} and source_content_lang in {"en", "mixed"}:
            if original_cjk_ratio >= 0.45 and original_latin_words < 2:
                return ""
        if source_lang == "en" and original_lang not in {"en", "mixed"}:
            return ""
        if clean_content_text(point_text).lower() == original.lower() and point_lang == "zh" and source_lang in {"en", "mixed"}:
            return ""
        return original

    @staticmethod
    def _cjk_ratio(text: str) -> float:
        value = clean_content_text(text)
        if not value:
            return 0.0
        cjk_count = len(re.findall(r"[\u4e00-\u9fff]", value))
        alpha_num_count = len(re.findall(r"[A-Za-z0-9\u4e00-\u9fff]", value))
        if alpha_num_count <= 0:
            return 0.0
        return cjk_count / alpha_num_count

    @staticmethod
    def _latin_word_count(text: str) -> int:
        return len(re.findall(r"[A-Za-z]{3,}", clean_content_text(text)))

    def _extract_point_original_text(self, row: Any, timestamp_seconds: int | None, point_text: str) -> str:
        if timestamp_seconds is None or int(timestamp_seconds) <= 0:
            point_lang = detect_language(point_text)
            return point_text if point_lang in {"en", "mixed"} else ""

        transcript_lines = self._load_transcript_lines(row)
        if not transcript_lines:
            point_lang = detect_language(point_text)
            return point_text if point_lang in {"en", "mixed"} else ""

        nearest = min(transcript_lines, key=lambda item: abs(item[0] - int(timestamp_seconds)))
        if abs(nearest[0] - int(timestamp_seconds)) > 12:
            point_lang = detect_language(point_text)
            return point_text if point_lang in {"en", "mixed"} else ""
        return clean_content_text(nearest[1])

    def _load_transcript_lines(self, row: Any) -> list[tuple[int, str]]:
        extra = load_json(row["extra_json"], {})
        if not isinstance(extra, dict):
            return []
        video_analysis = extra.get("video_analysis", {})
        if not isinstance(video_analysis, dict):
            return []
        output_file = str(video_analysis.get("output_file", "")).strip()
        if not output_file:
            return []
        if output_file in self._transcript_cache:
            return self._transcript_cache[output_file]

        output_path = Path(output_file).expanduser()
        raw_path: Path
        if output_path.name.endswith("_camera_feedback.md"):
            raw_path = output_path.with_name(output_path.name.replace("_camera_feedback.md", "_raw.md"))
        else:
            raw_path = output_path.with_name(output_path.stem + "_raw.md")

        lines: list[tuple[int, str]] = []
        if raw_path.exists():
            for raw_line in raw_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw_line.strip()
                if not line.startswith("["):
                    continue
                if "]" not in line:
                    continue
                token = line[1 : line.find("]")].strip()
                seconds = parse_timestamp_to_seconds(token)
                if seconds is None:
                    continue
                content = clean_content_text(line[line.find("]") + 1 :])
                if not content:
                    continue
                lines.append((int(seconds), content))

        self._transcript_cache[output_file] = lines
        return lines

    def _classify_point_tags(self, point_text: str, row: Any) -> tuple[str, list[str]]:
        text = clean_content_text(point_text).lower()
        counts: Counter[str] = Counter()
        for category, keywords in self._camera_categories.items():
            category_key = clean_content_text(category)
            if not category_key:
                continue
            for keyword in keywords:
                needle = clean_content_text(keyword).lower()
                if needle and needle in text:
                    counts[category_key] += 1

        secondary_tags: list[str] = []
        for label, tokens in POINT_SECONDARY_TAG_RULES:
            if any(token in text for token in tokens):
                secondary_tags.append(label)

        if counts:
            primary_tag = counts.most_common(1)[0][0]
            for category, _hits in counts.most_common():
                if category != primary_tag:
                    secondary_tags.append(category)
        elif secondary_tags:
            primary_tag = secondary_tags[0]
        else:
            primary_tag = clean_content_text(row["camera_category"] or "") or "未分类"

        secondary_tags = self._unique_list([tag for tag in secondary_tags if tag != primary_tag])
        return primary_tag, secondary_tags[:6]

    def _classify_point_products(self, point_text: str, row_products: list[str]) -> list[str]:
        text = clean_content_text(point_text).lower()
        tags: list[str] = []
        for product_tag, keywords in PRODUCT_KEYWORDS.items():
            if any(clean_content_text(keyword).lower() in text for keyword in keywords):
                tags.append(product_tag)
        if not tags:
            tags = list(row_products)
        return self._unique_list(tags)[:4]

    def _score_point_severity(self, point_text: str, sentiment: str) -> str:
        text = clean_content_text(point_text).lower()
        if any(token in text for token in POINT_HIGH_SEVERITY_KEYWORDS):
            return "high"
        medium_hits = sum(1 for token in POINT_MEDIUM_SEVERITY_KEYWORDS if token in text)
        if medium_hits >= 2:
            return "high"
        if medium_hits >= 1:
            return "medium"
        if str(sentiment).strip().lower() == "negative" and any(
            cue in text for cue in POINT_NEGATIVE_SEVERITY_CUES
        ):
            return "medium"
        return "low"

    @staticmethod
    def _severity_label(level: str) -> str:
        return {"high": "高", "medium": "中", "low": "低"}.get(str(level), "低")

    @staticmethod
    def _source_code_of_row(row: Any) -> str:
        blob = " ".join(
            [
                clean_content_text(row["source"] or "").lower(),
                clean_content_text(row["source_section"] or "").lower(),
                clean_content_text(row["url"] or "").lower(),
            ]
        )
        if "youtube" in blob or "youtu.be" in blob:
            return "yt"
        if "facebook" in blob:
            return "fb"
        if "instagram" in blob:
            return "ig"
        if "twitter" in blob or "x.com" in blob:
            return "x"
        if "bilibili" in blob or "b23.tv" in blob:
            return "bl"
        if "tiktok" in blob:
            return "tt"
        if "douyin" in blob:
            return "dy"
        if "reddit" in blob:
            return "rd"
        if "community." in blob or "/community" in blob:
            return "nc"
        if "google_news" in blob or "rss" in blob:
            return "nw"
        return "ot"

    @staticmethod
    def _unique_list(values: list[str]) -> list[str]:
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

    def _upsert_record(self, record_id: str, fields: dict[str, Any]) -> str | None:
        if record_id:
            try:
                self._update_record(record_id=record_id, fields=fields)
                return record_id
            except Exception as exc:  # noqa: BLE001
                if not self._is_record_missing_error(exc):
                    raise
        return self._create_record(fields)

    def _create_record(self, fields: dict[str, Any]) -> str | None:
        url = (
            f"{self.config.base_url}/open-apis/bitable/v1/apps/{self.config.bitable_app_token}"
            f"/tables/{self.config.bitable_table_id}/records"
        )
        payload = self._request_json(
            "post",
            url,
            use_auth=True,
            json={"fields": fields},
        )
        if payload.get("code") != 0:
            raise RuntimeError(f"Lark create record failed: {payload}")
        return payload.get("data", {}).get("record", {}).get("record_id")

    def _update_record(self, record_id: str, fields: dict[str, Any]) -> None:
        url = (
            f"{self.config.base_url}/open-apis/bitable/v1/apps/{self.config.bitable_app_token}"
            f"/tables/{self.config.bitable_table_id}/records/{record_id}"
        )
        payload = self._request_json(
            "put",
            url,
            use_auth=True,
            json={"fields": fields},
        )
        if payload.get("code") != 0:
            raise RuntimeError(f"Lark update record failed: {payload}")

    def _delete_record(self, record_id: str) -> None:
        url = (
            f"{self.config.base_url}/open-apis/bitable/v1/apps/{self.config.bitable_app_token}"
            f"/tables/{self.config.bitable_table_id}/records/{record_id}"
        )
        payload = self._request_json("delete", url, use_auth=True)
        if payload.get("code") != 0:
            raise RuntimeError(f"Lark delete record failed: {payload}")

    @staticmethod
    def _is_record_missing_error(exc: Exception) -> bool:
        message = str(exc).lower()
        return (
            "recordidnotfound" in message
            or "record not found" in message
            or ("record_id" in message and "not found" in message)
            or "1254047" in message
        )

    def _get_tenant_access_token(self, force_refresh: bool = False) -> str:
        if self._tenant_access_token and not force_refresh:
            return self._tenant_access_token
        url = f"{self.config.base_url}/open-apis/auth/v3/tenant_access_token/internal"
        payload = self._request_json(
            "post",
            url,
            use_auth=False,
            json={"app_id": self.config.app_id, "app_secret": self.config.app_secret},
        )
        if payload.get("code") != 0:
            raise RuntimeError(f"Lark auth failed: {payload}")
        self._tenant_access_token = payload["tenant_access_token"]
        return self._tenant_access_token

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        use_auth: bool,
        json: Any | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        allow_token_refresh = use_auth
        for attempt in range(1, self._max_request_attempts + 1):
            headers: dict[str, str] = {}
            if use_auth:
                token = self._get_tenant_access_token(force_refresh=False)
                headers["Authorization"] = f"Bearer {token}"
            if json is not None:
                headers["Content-Type"] = "application/json"

            try:
                response = requests.request(
                    method=method.upper(),
                    url=url,
                    headers=headers or None,
                    json=json,
                    params=params,
                    timeout=self._request_timeout_seconds,
                )
                should_refresh_token = use_auth and response.status_code == 401 and allow_token_refresh
                if should_refresh_token:
                    self._tenant_access_token = None
                    self._get_tenant_access_token(force_refresh=True)
                    allow_token_refresh = False
                    continue

                if response.status_code in {429, 500, 502, 503, 504} and attempt < self._max_request_attempts:
                    time.sleep(self._retry_delay_seconds(attempt))
                    continue

                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise RuntimeError(f"Lark invalid response payload: {payload!r}")

                if self._should_refresh_token_from_payload(payload) and allow_token_refresh:
                    self._tenant_access_token = None
                    self._get_tenant_access_token(force_refresh=True)
                    allow_token_refresh = False
                    continue
                return payload
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= self._max_request_attempts:
                    break
                time.sleep(self._retry_delay_seconds(attempt))

        if last_error:
            raise last_error
        raise RuntimeError(f"Lark request failed without response: {method.upper()} {url}")

    @staticmethod
    def _retry_delay_seconds(attempt: int) -> float:
        return min(8.0, float(max(1, attempt)))

    @staticmethod
    def _should_refresh_token_from_payload(payload: dict[str, Any]) -> bool:
        code = str(payload.get("code", "")).strip()
        message = str(payload.get("msg", "")).lower()
        if not code or code == "0":
            return False
        token_cues = ("tenant_access_token", "access token", "token expired", "token invalid", "401")
        return any(cue in message for cue in token_cues)

    def _prepare_fields_for_table(self, fields: dict[str, Any]) -> dict[str, Any]:
        try:
            supported = self._get_table_field_names()
        except Exception:  # noqa: BLE001
            return fields
        if self.config.auto_create_fields:
            missing = {key for key in fields if key not in supported}
            if missing:
                self._create_missing_fields(missing)
                supported = self._table_field_names or supported
        fields = self._fill_primary_field_with_title(fields, supported)
        if not supported:
            return fields
        filtered = {key: value for key, value in fields.items() if key in supported}
        if not filtered:
            return fields
        return {key: self._coerce_value_for_field(key, value) for key, value in filtered.items()}

    def _get_table_field_names(self) -> set[str]:
        if self._table_field_names is not None:
            return self._table_field_names

        url = (
            f"{self.config.base_url}/open-apis/bitable/v1/apps/{self.config.bitable_app_token}"
            f"/tables/{self.config.bitable_table_id}/fields"
        )
        page_token: str | None = None
        names: set[str] = set()

        while True:
            params: dict[str, Any] = {"page_size": 500}
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json(
                "get",
                url,
                use_auth=True,
                params=params,
            )
            if payload.get("code") != 0:
                raise RuntimeError(f"Lark list fields failed: {payload}")
            data = payload.get("data", {}) or {}
            items = data.get("items", []) or []
            for index, item in enumerate(items):
                field_name = str(item.get("field_name", "")).strip()
                if field_name:
                    names.add(field_name)
                    field_id = clean_content_text(item.get("field_id", ""))
                    if field_id:
                        self._table_field_ids[field_name] = field_id
                    field_type = int(item.get("type", 1) or 1)
                    self._table_field_types[field_name] = field_type
                    options = (item.get("property", {}) or {}).get("options", []) or []
                    if options:
                        option_ids: dict[str, str] = {}
                        for option in options:
                            option_name = clean_content_text(option.get("name", ""))
                            option_id = clean_content_text(option.get("id", ""))
                            if option_name and option_id:
                                option_ids[option_name] = option_id
                        if option_ids:
                            self._table_field_option_ids[field_name] = option_ids
                    if self._table_primary_field_name is None and index == 0:
                        self._table_primary_field_name = field_name
            if not data.get("has_more"):
                break
            page_token = str(data.get("page_token", "")).strip() or None
            if not page_token:
                break

        self._table_field_names = names
        return names

    def _create_missing_fields(self, missing_fields: set[str]) -> None:
        if not missing_fields:
            return
        for field_name in sorted(missing_fields):
            self._create_text_field(field_name)
            if self._table_field_names is None:
                self._table_field_names = set()
            self._table_field_names.add(field_name)
            self._table_field_types[field_name] = 1

    def _create_text_field(self, field_name: str) -> None:
        url = (
            f"{self.config.base_url}/open-apis/bitable/v1/apps/{self.config.bitable_app_token}"
            f"/tables/{self.config.bitable_table_id}/fields"
        )
        payload = self._request_json(
            "post",
            url,
            use_auth=True,
            json={"field_name": field_name, "type": 1},
        )
        if payload.get("code") == 0:
            return
        if self._is_field_name_duplicated(payload):
            return
        raise RuntimeError(f"Lark create field failed: {payload}")

    @staticmethod
    def _is_field_name_duplicated(payload: dict[str, Any]) -> bool:
        code = str(payload.get("code", ""))
        message = str(payload.get("msg", "")).lower()
        return code == "1254014" or "fieldnameduplicated" in message

    def _coerce_value_for_field(self, field_name: str, value: Any) -> Any:
        field_type = int(self._table_field_types.get(field_name, 1) or 1)
        if field_type == 2:  # number
            return self._normalize_number_value(value)
        if field_type == 5:  # date/datetime
            return self._normalize_date_value(value)
        if field_type == 4:  # multi-select
            return self._normalize_multi_select_value(value)
        if field_type == 3:  # single-select
            if isinstance(value, list):
                for item in value:
                    text = clean_content_text(str(item))
                    if text:
                        return text
                return ""
            return clean_content_text(str(value)) if value is not None else ""
        if field_type == 7:  # checkbox
            text = clean_content_text(str(value)).lower()
            return text in {"1", "true", "yes", "是", "y"}
        return value

    @staticmethod
    def _normalize_date_value(value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(value)

        text = clean_content_text(str(value))
        if not text:
            return None
        parsed: datetime | None = None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is None:
            for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.astimezone()
        else:
            parsed = parsed.astimezone()
        return int(parsed.timestamp() * 1000)

    @staticmethod
    def _normalize_number_value(value: Any) -> int | float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return value

        text = clean_content_text(str(value))
        if not text:
            return None
        text = text.replace(",", "")
        if re.fullmatch(r"[-+]?\d+", text):
            try:
                return int(text)
            except ValueError:
                return None
        if re.fullmatch(r"[-+]?(?:\d+\.\d+|\d+\.\d*|\.\d+)", text):
            try:
                return float(text)
            except ValueError:
                return None
        return None

    @staticmethod
    def _normalize_multi_select_value(value: Any) -> list[str]:
        candidates: list[str] = []
        if isinstance(value, list):
            candidates = [clean_content_text(str(item)) for item in value]
        else:
            text = clean_content_text(str(value)) if value is not None else ""
            if text:
                text = text.replace(" / ", ",").replace("|", ",").replace("，", ",").replace("、", ",")
                candidates = [clean_content_text(part) for part in text.split(",")]
        seen: set[str] = set()
        output: list[str] = []
        for item in candidates:
            if not item:
                continue
            key = item.lower()
            if key in seen:
                continue
            seen.add(key)
            output.append(item)
        return output[:20]

    def _list_views(self) -> dict[str, str]:
        url = (
            f"{self.config.base_url}/open-apis/bitable/v1/apps/{self.config.bitable_app_token}"
            f"/tables/{self.config.bitable_table_id}/views"
        )
        page_token: str | None = None
        output: dict[str, str] = {}

        while True:
            params: dict[str, Any] = {"page_size": 200}
            if page_token:
                params["page_token"] = page_token
            payload = self._request_json("get", url, use_auth=True, params=params)
            if payload.get("code") != 0:
                raise RuntimeError(f"Lark list views failed: {payload}")
            data = payload.get("data", {}) or {}
            for item in data.get("items", []) or []:
                view_name = clean_content_text(item.get("view_name", ""))
                view_id = clean_content_text(item.get("view_id", ""))
                if view_name and view_id:
                    output[view_name] = view_id
            if not data.get("has_more"):
                break
            page_token = clean_content_text(data.get("page_token", "")) or None
            if not page_token:
                break
        return output

    def _create_view(self, view_name: str) -> str:
        url = (
            f"{self.config.base_url}/open-apis/bitable/v1/apps/{self.config.bitable_app_token}"
            f"/tables/{self.config.bitable_table_id}/views"
        )
        payload = self._request_json(
            "post",
            url,
            use_auth=True,
            json={"view_name": view_name, "view_type": "grid"},
        )
        if payload.get("code") == 0:
            return clean_content_text(payload.get("data", {}).get("view", {}).get("view_id", ""))
        if self._is_view_name_duplicated(payload):
            existing = self._list_views().get(view_name)
            if existing:
                return existing
        raise RuntimeError(f"Lark create view failed: {payload}")

    def _update_view(self, view_id: str, view_name: str, property_payload: dict[str, Any]) -> None:
        url = (
            f"{self.config.base_url}/open-apis/bitable/v1/apps/{self.config.bitable_app_token}"
            f"/tables/{self.config.bitable_table_id}/views/{view_id}"
        )
        payload = self._request_json(
            "patch",
            url,
            use_auth=True,
            json={"view_name": view_name, "property": property_payload},
        )
        code = str(payload.get("code", "")).strip()
        if code in {"", "0", "1254606"}:
            return
        raise RuntimeError(f"Lark update view failed: {payload}")

    def _build_dashboard_view_property(self, filters: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, list[str]]:
        if not filters:
            return None, []
        conditions: list[dict[str, Any]] = []
        missing_fields: list[str] = []
        for raw_filter in filters:
            field_name = self._mapped_field_name(raw_filter.get("field_key", ""))
            field_id = self._table_field_ids.get(field_name, "")
            if not field_id:
                missing_fields.append(field_name)
                continue
            value = clean_content_text(raw_filter.get("value", ""))
            if not value:
                continue
            filter_value = self._normalize_filter_value_for_field(field_name=field_name, value=value)
            conditions.append(
                {
                    "field_id": field_id,
                    "operator": clean_content_text(raw_filter.get("operator", "")) or "is",
                    "value": filter_value,
                }
            )
        if missing_fields:
            return None, missing_fields
        return {
            "filter_info": {
                "conditions": conditions,
                "conjunction": "and",
            },
            "hidden_fields": None,
        }, []

    def _mapped_field_name(self, field_key: Any) -> str:
        key = clean_content_text(field_key)
        if not key:
            return ""
        return clean_content_text(self.config.field_mapping.get(key, key))

    def _normalize_filter_value_for_field(self, field_name: str, value: str) -> str:
        field_type = int(self._table_field_types.get(field_name, 1) or 1)
        if field_type in {3, 4}:
            option_id = self._table_field_option_ids.get(field_name, {}).get(value, "")
            if option_id:
                return json.dumps([option_id], ensure_ascii=False)
        return json.dumps([value], ensure_ascii=False)

    @staticmethod
    def _is_view_name_duplicated(payload: dict[str, Any]) -> bool:
        code = str(payload.get("code", "")).strip()
        message = str(payload.get("msg", "")).lower()
        return code == "1254020" or "viewnameduplicated" in message

    @staticmethod
    def _field_type_label(field_type: int) -> str:
        value = int(field_type or 0)
        if value <= 0:
            return "缺失"
        return LARK_FIELD_TYPE_LABELS.get(value, str(value))

    def _fill_primary_field_with_title(self, fields: dict[str, Any], supported: set[str]) -> dict[str, Any]:
        primary = str(self._table_primary_field_name or "").strip()
        if not primary or primary in fields or primary not in supported:
            return fields

        point_uid_field = str(self.config.field_mapping.get("point_uid", "观点ID")).strip()
        feedback_uid_field = str(self.config.field_mapping.get("feedback_uid", "反馈ID")).strip()
        point_text_field = str(self.config.field_mapping.get("point_text", "观点内容")).strip()
        title_field = str(self.config.field_mapping.get("title", "标题")).strip()

        primary_value = (
            fields.get(point_uid_field)
            or fields.get(feedback_uid_field)
            or fields.get(point_text_field)
            or fields.get(title_field)
        )
        if primary_value:
            fields[primary] = str(primary_value)
        return fields

    def _build_point_fields(self, row: Any, point: PointRecord) -> dict[str, Any]:
        mapping = self.config.field_mapping
        keyword_hits = ", ".join(load_json(row["camera_keyword_hits"], []))
        point_subtags = " / ".join(point.secondary_tags)
        point_products = ", ".join(point.product_tags)
        point_url = build_timestamped_video_url(row["url"], point.timestamp_seconds)
        source_name = self._display_source_name(str(row["source"] or ""), str(row["url"] or ""))
        primary_product = self._pick_primary_product(point.product_tags)
        platform_group = self._platform_group_label(
            source_name=source_name,
            raw_source=str(row["source"] or ""),
            raw_url=str(row["url"] or ""),
        )
        author_display = self._format_author_for_point(row=row, point=point)

        published_at = row["published_at"]
        published_date = ""
        if published_at:
            published_dt = datetime.fromisoformat(published_at).astimezone()
            published_date = published_dt.strftime("%Y-%m-%d")
            published_at = published_dt.strftime("%Y-%m-%d %H:%M")

        fields = {
            mapping["feedback_uid"]: point.feedback_uid,
            mapping["point_uid"]: point.point_uid,
            mapping["point_index"]: str(point.point_index),
            mapping["point_source_code"]: point.source_code,
            mapping["point_sentiment"]: point.sentiment_label,
            mapping["point_severity"]: point.severity_label,
            mapping["point_text"]: point.point_text_zh,
            mapping["point_text_original"]: point.point_text_original,
            mapping["point_language"]: point.point_language,
            mapping["point_primary_tag"]: point.primary_tag,
            mapping["point_secondary_tags"]: point_subtags,
            mapping["point_source_label"]: point.source_label,
            mapping["comment_meta"]: point.comment_meta,
            mapping["point_timestamp"]: point.timestamp_label,
            mapping["point_timestamp_seconds"]: "" if point.timestamp_seconds is None else str(point.timestamp_seconds),
            mapping["title"]: row["title"],
            mapping["url"]: point_url or row["url"],
            mapping["source"]: source_name,
            mapping["source_section"]: row["source_section"] or "",
            mapping["author"]: author_display,
            mapping["published_at"]: published_at or "",
            mapping["camera_category"]: point.primary_tag,
            mapping["camera_related"]: "是" if int(row["camera_related"] or 0) == 1 else "否",
            mapping["sentiment"]: point.sentiment,
            mapping["sentiment_reason"]: row["sentiment_reason"] or "",
            mapping["severity"]: point.severity,
            mapping["source_actor_type"]: SOURCE_LABELS.get(
                str(row["source_actor_type"] or "unknown"),
                SOURCE_LABELS["unknown"],
            ),
            mapping["source_actor_reason"]: row["source_actor_reason"] or "",
            mapping["domain_subtags"]: point_subtags,
            mapping["product_tags"]: point_products,
            mapping["primary_product"]: primary_product,
            mapping["platform_group"]: platform_group,
            mapping["published_date"]: published_date,
            mapping["is_negative"]: "是" if point.sentiment == "negative" else "否",
            mapping["is_high_severity"]: "是" if point.severity == "high" else "否",
            mapping["camera_keyword_hits"]: keyword_hits,
            mapping["video_candidate"]: "是" if int(row["video_candidate"] or 0) == 1 else "否",
            mapping["summary"]: row["summary"] or "",
            mapping["content"]: point.point_text_original or point.point_text_zh,
            mapping["report_date"]: row["report_date"],
            mapping["status"]: "待跟进",
        }
        domain_tag_field = str(mapping.get("domain_tag", "")).strip()
        if domain_tag_field:
            # Legacy compatibility: explicitly clear deprecated "领域标签" to avoid
            # duplicated semantics with point-level primary tag.
            fields[domain_tag_field] = ""
        return fields

    @staticmethod
    def _display_source_name(raw_source: str, raw_url: str) -> str:
        source = clean_content_text(raw_source).lower()
        if source.startswith("youtube"):
            return "YouTube"
        if source.startswith("bilibili"):
            return "Bilibili"
        if source.startswith("reddit"):
            return "Reddit"
        if "google_news" in source:
            return "Google News"
        if source.startswith("x_") or source == "x":
            return "X"
        if source.startswith("instagram"):
            return "Instagram"
        if source.startswith("brand_community") or source.endswith("_community"):
            return "品牌社区"
        if source.startswith("custom_rss"):
            return "RSS"
        url = clean_content_text(raw_url).lower()
        if "youtube.com" in url or "youtu.be" in url:
            return "YouTube"
        if "bilibili.com" in url or "b23.tv" in url:
            return "Bilibili"
        if "reddit.com" in url:
            return "Reddit"
        if "x.com" in url or "twitter.com" in url:
            return "X"
        if "instagram.com" in url:
            return "Instagram"
        return clean_content_text(raw_source) or "Unknown"

    @staticmethod
    def _pick_primary_product(product_tags: list[str]) -> str:
        for item in product_tags:
            text = clean_content_text(item)
            if text:
                return text
        return "未识别"

    @staticmethod
    def _platform_group_label(source_name: str, raw_source: str, raw_url: str) -> str:
        display = clean_content_text(source_name)
        source = clean_content_text(raw_source).lower()
        url = clean_content_text(raw_url).lower()
        if display in {"YouTube", "Bilibili", "Instagram"}:
            return "视频平台"
        if "youtu" in url or "bilibili.com" in url or "b23.tv" in url or "instagram.com" in url:
            return "视频平台"
        if display in {"Reddit", "品牌社区"}:
            return "社区论坛"
        if "reddit.com" in url or "community" in source:
            return "社区论坛"
        if display == "X" or "x.com" in url or "twitter.com" in url:
            return "社交平台"
        if display in {"Google News", "RSS"} or "google_news" in source or "rss" in source:
            return "媒体站点"
        return "其他"

    def _format_author_for_point(self, row: Any, point: PointRecord) -> str:
        base_author = clean_content_text(row["author"] or "")
        source_label = clean_content_text(point.source_label or "")
        comment_author = clean_content_text(point.comment_author or "")
        if source_label == "评论区":
            if self._is_video_row(row):
                if base_author and comment_author:
                    return f"{base_author}，原视频； {base_author}，评论区，{comment_author}"
                if base_author:
                    return f"{base_author}，原视频； {base_author}，评论区"
                if comment_author:
                    return f"评论区，{comment_author}"
                return "评论区"
            fallback_author = comment_author or base_author
            if fallback_author:
                return f"评论区，{fallback_author}"
            return "评论区"
        if source_label == "原视频":
            return f"{base_author}，原视频" if base_author else "原视频"
        if source_label == "原帖子":
            return f"{base_author}，原帖子" if base_author else "原帖子"
        return base_author


class MarkSyncedFn(Protocol):
    def __call__(self, row_id: int, record_id: str) -> None: ...


class MarkLarkSyncFailedFn(Protocol):
    def __call__(self, row_id: int, error: str) -> None: ...


class ListPointLinksFn(Protocol):
    def __call__(self, feedback_item_id: int) -> list[Any]: ...


class GetPointRecordIdFn(Protocol):
    def __call__(self, point_uid: str) -> str: ...


class UpsertPointLinkFn(Protocol):
    def __call__(self, feedback_item_id: int, point_uid: str, record_id: str) -> None: ...


class DeletePointLinkFn(Protocol):
    def __call__(self, point_uid: str) -> None: ...


class MarkPointSyncFailedFn(Protocol):
    def __call__(self, feedback_item_id: int, point_uid: str, error: str) -> None: ...


class SyncRowResultFn(Protocol):
    def __call__(self, payload: dict[str, Any]) -> None: ...
