from __future__ import annotations

from datetime import date
from urllib.parse import parse_qs

from ..config import AppConfig
from ..storage import FeedbackRepository
from ..video_analysis import VideoAnalysisService
from ..video_tasks import export_video_tasks
from .service import (
    build_competitor_video_payload,
    build_runtime_status_payload,
    build_summary_payload,
    build_video_candidates_payload,
    build_video_detail_payload,
    parse_report_date,
    resolve_default_date,
)


def handle_api_get(
    repository: FeedbackRepository,
    path: str,
    query: dict[str, list[str]],
    default_date: date | None = None,
    app_config: AppConfig | None = None,
) -> tuple[int, dict]:
    if path == "/api/health":
        return 200, {"status": "ok"}

    if path == "/api/dates":
        published_dates = repository.list_published_dates(limit=180)
        default_report_date = resolve_default_date(repository, default_date).isoformat()
        default_end_date = published_dates[0] if published_dates else default_report_date
        default_start_date = published_dates[min(len(published_dates) - 1, 13)] if published_dates else default_end_date
        return 200, {
            "dates": repository.list_report_dates(limit=60),
            "default_date": default_report_date,
            "published_dates": published_dates,
            "default_start_date": default_start_date,
            "default_end_date": default_end_date,
        }

    if path == "/api/summary":
        raw_date = (query.get("date") or [None])[0]
        raw_start_date = (query.get("start_date") or [None])[0]
        raw_end_date = (query.get("end_date") or [None])[0]
        scope = (query.get("scope") or ["camera"])[0]
        start_date = parse_report_date(raw_start_date)
        end_date = parse_report_date(raw_end_date)
        target_date = parse_report_date(raw_date) or resolve_default_date(repository, default_date)
        return 200, build_summary_payload(
            repository,
            target_date,
            scope=scope,
            app_config=app_config,
            start_date=start_date,
            end_date=end_date,
        )

    if path == "/api/status":
        raw_date = (query.get("date") or [None])[0]
        target_date = parse_report_date(raw_date) or resolve_default_date(repository, default_date)
        return 200, build_runtime_status_payload(repository, target_date, app_config)

    if path == "/api/video/candidates":
        raw_date = (query.get("date") or [None])[0]
        raw_limit = (query.get("limit") or ["50"])[0]
        try:
            limit = max(1, min(200, int(raw_limit)))
        except ValueError:
            limit = 50
        target_date = parse_report_date(raw_date) or resolve_default_date(repository, default_date)
        return 200, build_video_candidates_payload(repository, target_date, limit=limit)

    if path == "/api/competitor/videos":
        raw_start_date = (query.get("start_date") or [None])[0]
        raw_end_date = (query.get("end_date") or [None])[0]
        raw_limit = (query.get("limit") or ["30"])[0]
        try:
            limit = max(1, min(100, int(raw_limit)))
        except ValueError:
            limit = 30
        start_date = parse_report_date(raw_start_date)
        end_date = parse_report_date(raw_end_date)
        target_date = resolve_default_date(repository, default_date)
        if not start_date:
            start_date = target_date
        if not end_date:
            end_date = target_date
        return 200, build_competitor_video_payload(
            repository,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    if path == "/api/video/item":
        raw_id = (query.get("id") or [None])[0]
        try:
            row_id = int(raw_id or "")
        except ValueError:
            return 400, {"error": "invalid_id"}
        payload = build_video_detail_payload(repository, row_id=row_id)
        if payload.get("error") == "not_found":
            return 404, payload
        return 200, payload

    if path == "/api/trend":
        raw_days = (query.get("days") or ["14"])[0]
        try:
            days = max(3, min(60, int(raw_days)))
        except ValueError:
            days = 14
        return 200, {"trend": repository.trend_by_report_date(days=days)}

    return 404, {"error": "Not Found", "path": path}


def handle_api_post(
    repository: FeedbackRepository,
    path: str,
    query: dict[str, list[str]],
    payload: dict,
    default_date: date | None = None,
    app_config: AppConfig | None = None,
) -> tuple[int, dict]:
    if path == "/api/lark/sync":
        if not app_config:
            return 500, {"error": "missing_app_config"}
        if not bool(app_config.lark.get("enabled")):
            return 400, {"ok": False, "error": "lark_disabled"}

        from ..lark import LarkBitableClient

        raw_date = str(payload.get("date") or (query.get("date") or [None])[0] or "")
        raw_limit = payload.get("limit") or (query.get("limit") or [None])[0]
        target_date = parse_report_date(raw_date) or resolve_default_date(repository, default_date)
        limit = 200
        if raw_limit is not None:
            try:
                limit = max(1, min(1000, int(raw_limit)))
            except (TypeError, ValueError):
                limit = 200

        lark_client = LarkBitableClient(app_config.lark)
        if not lark_client.is_available():
            return 400, {"ok": False, "error": "lark_config_incomplete"}
        pending_before = repository.count_lark_pending(target_date)
        rows = repository.fetch_lark_pending(target_date=target_date, limit=limit)
        synced = lark_client.sync_rows(
            rows,
            mark_synced=repository.mark_synced,
            mark_failed=repository.mark_lark_sync_failed,
        )
        pending_after = repository.count_lark_pending(target_date)
        return 200, {
            "ok": True,
            "report_date": target_date.isoformat(),
            "limit": limit,
            "synced": synced,
            "pending_before": pending_before,
            "pending_after": pending_after,
        }

    if path == "/api/video/tasks/export":
        if not app_config:
            return 500, {"error": "missing_app_config"}
        raw_date = str(payload.get("date") or (query.get("date") or [None])[0] or "")
        target_date = parse_report_date(raw_date) or resolve_default_date(repository, default_date)
        output_dir = str(payload.get("output_dir") or app_config.report_dir)
        path_obj = export_video_tasks(repository, target_date, output_dir)
        return 200, {"ok": True, "path": str(path_obj), "report_date": target_date.isoformat()}

    if path == "/api/video/process":
        if not app_config:
            return 500, {"error": "missing_app_config"}
        raw_date = str(payload.get("date") or (query.get("date") or [None])[0] or "")
        raw_limit = payload.get("limit") or (query.get("limit") or [None])[0]
        raw_id = payload.get("id") or (query.get("id") or [None])[0]
        only_unprocessed = bool(payload.get("only_unprocessed", True))

        target_date = parse_report_date(raw_date) or resolve_default_date(repository, default_date)
        limit = app_config.video_processing.max_items_per_run
        if raw_limit is not None:
            try:
                limit = max(1, min(50, int(raw_limit)))
            except (TypeError, ValueError):
                limit = app_config.video_processing.max_items_per_run

        row_id: int | None = None
        if raw_id is not None and str(raw_id).strip():
            try:
                row_id = int(raw_id)
            except ValueError:
                return 400, {"error": "invalid_id"}

        service = VideoAnalysisService(app_config, repository)
        result = service.process(
            target_date=target_date,
            row_id=row_id,
            limit=limit,
            only_unprocessed=only_unprocessed,
        )
        return 200 if result.get("ok") else 400, result

    return 404, {"error": "Not Found", "path": path}


def parse_request_query(raw_query: str) -> dict[str, list[str]]:
    return parse_qs(raw_query, keep_blank_values=False)
