from __future__ import annotations

import argparse
import time
from datetime import date, datetime

from .config import load_config
from .dashboard import run_dashboard

DEFAULT_VIDEO_LINKS_FILE = "reports/video-links.txt"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Media Pulse")
    parser.add_argument("--config", default="config.yaml", help="Path to config yaml")

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Fetch + filter + classify + report + optional Lark sync")
    run_parser.add_argument("--date", help="Report date (YYYY-MM-DD), default today")
    run_parser.add_argument("--skip-lark", action="store_true", help="Skip Lark sync")
    run_parser.add_argument("--dry-run", action="store_true", help="Fetch and classify only, do not persist")

    report_parser = subparsers.add_parser("report", help="Generate report only")
    report_parser.add_argument("--date", required=True, help="Report date (YYYY-MM-DD)")

    email_parser = subparsers.add_parser("send-email-summary", help="Send daily summary email")
    email_parser.add_argument("--date", help="Report date (YYYY-MM-DD), default today")
    email_parser.add_argument("--run-log", help="Optional process log path for the related run")

    email_check_parser = subparsers.add_parser("email-check", help="Check SMTP connection and login only")

    test_email_parser = subparsers.add_parser("send-test-email", help="Send a test email using current SMTP config")
    test_email_parser.add_argument("--subject", help="Override subject")
    test_email_parser.add_argument("--body", help="Override plain text body")
    test_email_parser.add_argument("--html-file", help="Optional HTML file to attach as email HTML body")
    test_email_parser.add_argument("--to", action="append", default=[], help="Override recipient address; repeatable")

    weekly_email_parser = subparsers.add_parser("weekly-email", help="Render weekly media email HTML")
    weekly_email_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD), default end_date - 6 days")
    weekly_email_parser.add_argument("--end-date", help="End date (YYYY-MM-DD), default latest published date")
    weekly_email_parser.add_argument(
        "--scope",
        choices=["all", "camera"],
        default="all",
        help="Content scope for weekly email",
    )
    weekly_email_parser.add_argument("--top-limit", type=int, default=10, help="Max featured items")
    weekly_email_parser.add_argument("--output", help="Output HTML path, default reports/weekly-media-email-*.html")
    weekly_email_parser.add_argument("--send", action="store_true", help="Send email after rendering HTML")

    sync_parser = subparsers.add_parser("sync-lark", help="Sync unsynced rows to Lark")
    sync_parser.add_argument("--date", help="Only sync rows from report date YYYY-MM-DD")
    sync_parser.add_argument("--limit", type=int, default=500, help="Max rows per sync")
    sync_parser.add_argument(
        "--force-all-updates",
        action="store_true",
        help="Ignore only_sync_new_records and sync dirty rows even when record_id exists",
    )

    mark_lark_dirty_parser = subparsers.add_parser(
        "mark-lark-dirty",
        help="Mark existing rows dirty so they will be re-synced to Lark",
    )
    mark_lark_dirty_parser.add_argument("--date", help="Only mark rows from report date YYYY-MM-DD")

    prepare_dashboard_parser = subparsers.add_parser(
        "prepare-lark-dashboard",
        help="Bootstrap Lark views and inspect dashboard field types",
    )
    prepare_dashboard_parser.add_argument(
        "--skip-views",
        action="store_true",
        help="Only inspect field types, do not create/update views",
    )

    sync_loop_parser = subparsers.add_parser("sync-lark-loop", help="Sync Lark in loop for near real-time updates")
    sync_loop_parser.add_argument("--date", help="Only sync rows from report date YYYY-MM-DD")
    sync_loop_parser.add_argument("--limit", type=int, default=200, help="Max rows per round")
    sync_loop_parser.add_argument("--interval", type=int, default=60, help="Sleep seconds between rounds")
    sync_loop_parser.add_argument("--max-rounds", type=int, default=0, help="Stop after N rounds (0 = infinite)")
    sync_loop_parser.add_argument(
        "--force-all-updates",
        action="store_true",
        help="Ignore only_sync_new_records and sync dirty rows even when record_id exists",
    )

    backfill_parser = subparsers.add_parser("backfill", help="Backfill source/domain/AI tags for existing rows")
    backfill_parser.add_argument("--date", help="Only backfill report date YYYY-MM-DD")
    backfill_parser.add_argument("--limit", type=int, default=500, help="Max rows to backfill")

    retag_parser = subparsers.add_parser(
        "retag",
        help="Re-analyze tags with AI prompt and optionally force full Lark update",
    )
    retag_parser.add_argument("--date", help="Only retag rows from report date YYYY-MM-DD")
    retag_parser.add_argument("--limit", type=int, default=500, help="Max rows to retag")
    retag_parser.add_argument("--all", action="store_true", help="Retag all rows in database")
    retag_parser.add_argument("--sync-lark", action="store_true", help="Force-update Lark after retag")
    retag_parser.add_argument("--sync-batch-limit", type=int, default=200, help="Rows per Lark sync round")

    backend_parser = subparsers.add_parser("backend", help="Start backend API service")
    backend_parser.add_argument("--date", help="Default report date YYYY-MM-DD")
    backend_parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    backend_parser.add_argument("--port", type=int, default=8788, help="Bind port")

    frontend_parser = subparsers.add_parser("frontend", help="Start frontend static server")
    frontend_parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    frontend_parser.add_argument("--port", type=int, default=8787, help="Bind port")
    frontend_parser.add_argument(
        "--api-base-url",
        default="http://127.0.0.1:8788",
        help="Backend API base URL, e.g. http://127.0.0.1:8788",
    )

    video_tasks_parser = subparsers.add_parser("video-tasks", help="Export video transcription task list")
    video_tasks_parser.add_argument("--date", required=True, help="Report date YYYY-MM-DD")
    video_tasks_parser.add_argument("--output-dir", default="./reports", help="Output directory")

    video_process_parser = subparsers.add_parser("video-process", help="Run videosummary analysis for pending video items")
    video_process_parser.add_argument("--date", help="Report date YYYY-MM-DD, default latest")
    video_process_parser.add_argument("--id", type=int, help="Only process this row id")
    video_process_parser.add_argument("--limit", type=int, default=8, help="Max videos to process")
    video_process_parser.add_argument(
        "--include-processed",
        action="store_true",
        help="Include items that were already processed before",
    )

    ingest_video_parser = subparsers.add_parser(
        "ingest-video",
        help="Manually ingest video URLs (YouTube/Bilibili/X/Instagram etc.)",
    )
    ingest_video_parser.add_argument(
        "--url",
        action="append",
        default=[],
        help="Video URL. Repeat this flag for multiple URLs.",
    )
    ingest_video_parser.add_argument(
        "--file",
        help=f"Path to a text file (one URL per line, # for comments). Default: {DEFAULT_VIDEO_LINKS_FILE}",
    )
    ingest_video_parser.add_argument(
        "--run-ai",
        action="store_true",
        help="Run local AI enrichment while importing",
    )
    ingest_video_parser.add_argument(
        "--analyze",
        action="store_true",
        help="Run video-process immediately after import",
    )
    ingest_video_parser.add_argument(
        "--limit",
        type=int,
        default=8,
        help="Max items for post-import video-process when --analyze is enabled",
    )
    ingest_video_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview import result without writing to database",
    )

    competitor_video_parser = subparsers.add_parser(
        "competitor-video",
        help="Search, analyze, and classify competitor camera videos",
    )
    competitor_video_parser.add_argument(
        "--target",
        action="append",
        default=[],
        help="Competitor target model, e.g. 'iPhone 17 Pro'. Repeatable.",
    )
    competitor_video_parser.add_argument(
        "--compare-to",
        action="append",
        default=[],
        help="Reference model for VS queries, e.g. 'Target Device Pro'. Repeatable.",
    )
    competitor_video_parser.add_argument(
        "--query",
        action="append",
        default=[],
        help="Optional direct search query. Repeatable.",
    )
    competitor_video_parser.add_argument(
        "--platform",
        action="append",
        default=[],
        help="Search platform: youtube or bilibili. Repeatable; default runs both.",
    )
    competitor_video_parser.add_argument("--lookback-days", type=int, default=30, help="Only keep videos newer than N days")
    competitor_video_parser.add_argument("--limit-per-query", type=int, default=8, help="Max videos per generated query")
    competitor_video_parser.add_argument("--max-total", type=int, default=80, help="Max inserted candidate videos")
    competitor_video_parser.add_argument("--campaign-name", help="Optional campaign label for this search batch")
    competitor_video_parser.add_argument("--skip-ai", action="store_true", help="Skip local AI enrichment")
    competitor_video_parser.add_argument("--analyze", action="store_true", help="Run video transcript analysis after insert")
    competitor_video_parser.add_argument("--sync-lark", action="store_true", help="Sync newly inserted rows to Lark")
    competitor_video_parser.add_argument("--dry-run", action="store_true", help="Preview and classify without writing")

    dashboard_parser = subparsers.add_parser("dashboard", help="Start fullstack dashboard (UI + API)")
    dashboard_parser.add_argument("--date", help="Default report date YYYY-MM-DD")
    dashboard_parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    dashboard_parser.add_argument("--port", type=int, default=8787, help="Bind port")

    subparsers.add_parser("schedule", help="Run daily scheduler")

    return parser


def parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def run_once(config_path: str, report_date: date | None, skip_lark: bool, dry_run: bool) -> int:
    from .email_summary import send_daily_summary_email
    from .pipeline import CameraPulsePipeline
    from .process_log import ProcessingRunLogger

    config = load_config(config_path)
    pipeline = CameraPulsePipeline(config)
    logger = ProcessingRunLogger(config.report_dir, command="run")
    logger.start(
        config_path=config_path,
        report_date=report_date.isoformat() if report_date else None,
        skip_lark=bool(skip_lark),
        dry_run=bool(dry_run),
    )
    try:
        result = pipeline.run(
            target_date=report_date,
            skip_lark=skip_lark,
            dry_run=dry_run,
            run_logger=logger,
        )
    except Exception as exc:  # noqa: BLE001
        logger.finish("failed", error=str(exc))
        print("=== Media Pulse ===")
        print("ok=False")
        print(f"error={exc}")
        print(f"process_log_path={logger.path}")
        return 1

    final_status = "ok_with_errors" if result.errors else "ok"
    logger.finish(
        final_status,
        fetched=result.fetched,
        kept_camera_only=result.kept_camera_only,
        retained_non_camera=result.retained_non_camera,
        skipped_non_camera=result.skipped_non_camera,
        skipped_duplicates=result.skipped_duplicates,
        inserted=result.inserted,
        ai_enriched=result.ai_enriched,
        ai_failed=result.ai_failed,
        synced_to_lark=result.synced_to_lark,
        report_path=result.report_path,
        errors=result.errors,
    )

    print("=== Media Pulse ===")
    print(f"fetched={result.fetched}")
    print(f"kept_camera_only={result.kept_camera_only}")
    print(f"retained_non_camera={result.retained_non_camera}")
    print(f"skipped_non_camera={result.skipped_non_camera}")
    print(f"skipped_duplicates={result.skipped_duplicates}")
    print(f"inserted={result.inserted}")
    print(f"ai_enriched={result.ai_enriched}")
    print(f"ai_failed={result.ai_failed}")
    print(f"synced_to_lark={result.synced_to_lark}")
    if result.report_path:
        print(f"report_path={result.report_path}")
    print(f"process_log_path={logger.path}")
    if result.errors:
        print("errors:")
        for line in result.errors:
            print(f"- {line}")
    if (not dry_run) and config.email_summary.enabled and config.email_summary.auto_send_after_run:
        try:
            summary = send_daily_summary_email(
                config=config,
                repository=pipeline.repository,
                target_date=report_date or date.today(),
                run_log_path=str(logger.path),
            )
            logger.note(step="email-summary", status="sent", subject=summary.subject)
            print("email_summary_sent=1")
        except Exception as exc:  # noqa: BLE001
            logger.note(step="email-summary", status="failed", error=str(exc))
            print("email_summary_sent=0")
            print(f"email_summary_error={exc}")
    return 0


def run_schedule(config_path: str) -> int:
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "schedule 命令需要安装 APScheduler，请先执行: pip install APScheduler==3.10.4"
        ) from exc

    config = load_config(config_path)
    timezone = config.schedule.get("timezone", "Asia/Shanghai")
    hour = int(config.schedule.get("hour", 9))
    minute = int(config.schedule.get("minute", 0))

    scheduler = BlockingScheduler(timezone=timezone)
    scheduler.add_job(
        lambda: run_once(config_path, None, skip_lark=False, dry_run=False),
        trigger=CronTrigger(hour=hour, minute=minute),
        id="media_pulse_daily",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    if config.video_processing.enabled and config.video_processing.nightly_enabled:
        nightly_tz = config.video_processing.nightly_timezone or timezone
        scheduler.add_job(
            lambda: run_video_process(
                config_path=config_path,
                report_date=None,
                row_id=None,
                limit=config.video_processing.max_items_per_run,
                include_processed=False,
            ),
            trigger=CronTrigger(
                hour=config.video_processing.nightly_hour,
                minute=config.video_processing.nightly_minute,
                timezone=nightly_tz,
            ),
            id="media_pulse_video_nightly",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        print(
            "Video nightly schedule "
            f"{config.video_processing.nightly_hour:02d}:{config.video_processing.nightly_minute:02d} {nightly_tz}"
        )

    print(f"Scheduler started at {hour:02d}:{minute:02d} {timezone}")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        pass
    return 0


def run_video_process(
    config_path: str,
    report_date: date | None,
    row_id: int | None,
    limit: int,
    include_processed: bool,
) -> int:
    from .pipeline import CameraPulsePipeline
    from .process_log import ProcessingRunLogger
    from .video_analysis import VideoAnalysisService

    config = load_config(config_path)
    pipeline = CameraPulsePipeline(config)
    service = VideoAnalysisService(config, pipeline.repository)
    logger = ProcessingRunLogger(config.report_dir, command="video-process")
    logger.start(
        config_path=config_path,
        report_date=report_date.isoformat() if report_date else None,
        row_id=row_id,
        limit=max(1, int(limit)),
        include_processed=bool(include_processed),
    )
    try:
        result = service.process(
            target_date=report_date,
            row_id=row_id,
            limit=max(1, int(limit)),
            only_unprocessed=not include_processed,
        )
    except Exception as exc:  # noqa: BLE001
        logger.finish(
            "failed",
            error=str(exc),
        )
        print("=== Video Process ===")
        print("ok=False")
        print(f"error={exc}")
        print(f"process_log_path={logger.path}")
        return 1

    for item in result.get("items", []):
        item_status = "duplicate" if item.get("duplicate_of") else ("ok" if item.get("ok") else "failed")
        row_id = int(item.get("id") or 0)
        logger.item(
            step="video-process",
            status=item_status,
            row_id=row_id,
            title=item.get("title"),
            url=item.get("url"),
            error=item.get("error", ""),
            output_file=item.get("output_file", ""),
            duplicate_of=item.get("duplicate_of"),
        )
        if row_id > 0:
            pipeline.repository.upsert_processing_checkpoint(
                feedback_item_id=row_id,
                step="video-process",
                run_id=logger.run_id,
                command=logger.command,
                status=item_status,
                error=str(item.get("error") or ""),
                details={
                    "title": item.get("title"),
                    "url": item.get("url"),
                    "output_file": item.get("output_file", ""),
                    "duplicate_of": item.get("duplicate_of"),
                },
            )
    logger.finish(
        "ok" if result.get("ok") else "failed",
        processed=result.get("processed", 0),
        succeeded=result.get("succeeded", 0),
        failed=result.get("failed", 0),
        duplicate_resolved=result.get("duplicate_resolved", 0),
        skipped_duplicates=result.get("skipped_duplicates", 0),
    )

    print("=== Video Process ===")
    print(f"ok={result.get('ok')}")
    print(f"processed={result.get('processed', 0)}")
    print(f"succeeded={result.get('succeeded', 0)}")
    print(f"failed={result.get('failed', 0)}")
    print(f"skipped_duplicates={result.get('skipped_duplicates', 0)}")
    print(f"process_log_path={logger.path}")
    if result.get("error"):
        print(f"error={result['error']}")
    return 0 if result.get("ok") else 1


def run_ingest_video(
    config_path: str,
    raw_urls: list[str],
    file_path: str | None,
    run_ai: bool,
    analyze: bool,
    limit: int,
    dry_run: bool,
) -> int:
    from .manual_video import collect_manual_video_urls
    from .pipeline import CameraPulsePipeline

    if not file_path and not raw_urls:
        file_path = DEFAULT_VIDEO_LINKS_FILE

    urls = collect_manual_video_urls(raw_urls, file_path=file_path)
    if not urls:
        print("ingest_video_error=no_valid_urls")
        if file_path:
            print(f"hint=put_urls_into:{file_path}")
        return 1

    config = load_config(config_path)
    pipeline = CameraPulsePipeline(config)
    stats = pipeline.ingest_manual_video_urls(
        urls=urls,
        run_ai=run_ai,
        dry_run=dry_run,
    )

    print("=== Manual Video Ingest ===")
    print(f"urls_input={len(raw_urls)}")
    print(f"urls_valid={len(urls)}")
    print(f"scanned={stats['scanned']}")
    print(f"inserted={stats['inserted']}")
    print(f"skipped_duplicates={stats['skipped_duplicates']}")
    print(f"ai_enriched={stats['ai_enriched']}")
    print(f"ai_failed={stats['ai_failed']}")
    print(f"dry_run={1 if dry_run else 0}")
    if stats["errors"]:
        print("errors:")
        for line in stats["errors"][:20]:
            print(f"- {line}")

    if analyze and not dry_run and stats["inserted"] > 0:
        analyze_date = date.today()
        return run_video_process(
            config_path=config_path,
            report_date=analyze_date,
            row_id=None,
            limit=max(1, int(limit)),
            include_processed=False,
        )
    return 0


def run_competitor_video(
    config_path: str,
    targets: list[str],
    compare_to: list[str],
    direct_queries: list[str],
    platforms: list[str],
    lookback_days: int,
    limit_per_query: int,
    max_total: int,
    campaign_name: str | None,
    skip_ai: bool,
    analyze: bool,
    sync_lark: bool,
    dry_run: bool,
) -> int:
    from .competitor_video import CompetitorVideoRequest, run_competitor_video_task
    from .process_log import ProcessingRunLogger

    config = load_config(config_path)
    logger = ProcessingRunLogger(config.report_dir, command="competitor-video")
    logger.start(
        config_path=config_path,
        targets=list(targets),
        compare_to=list(compare_to),
        queries=list(direct_queries),
        platforms=list(platforms),
        lookback_days=max(1, int(lookback_days)),
        limit_per_query=max(1, int(limit_per_query)),
        max_total=max(1, int(max_total)),
        campaign_name=campaign_name or "",
        skip_ai=bool(skip_ai),
        analyze=bool(analyze),
        sync_lark=bool(sync_lark),
        dry_run=bool(dry_run),
    )
    try:
        result = run_competitor_video_task(
            config,
            CompetitorVideoRequest(
                targets=list(targets),
                compare_to=list(compare_to),
                direct_queries=list(direct_queries),
                platforms=list(platforms),
                lookback_days=max(1, int(lookback_days)),
                limit_per_query=max(1, int(limit_per_query)),
                max_total=max(1, int(max_total)),
                run_ai=not bool(skip_ai),
                analyze_video=bool(analyze),
                sync_lark=bool(sync_lark),
                dry_run=bool(dry_run),
                campaign_name=campaign_name or "",
            ),
        )
    except Exception as exc:  # noqa: BLE001
        logger.finish("failed", error=str(exc))
        print("=== Competitor Video ===")
        print("ok=False")
        print(f"error={exc}")
        print(f"process_log_path={logger.path}")
        return 1

    logger.finish(
        "ok_with_errors" if result["errors"] else "ok",
        targets=result["targets"],
        compare_to=result["compare_to"],
        platforms=result["platforms"],
        fetched=result["fetched"],
        inserted=result["inserted"],
        ai_enriched=result["ai_enriched"],
        ai_failed=result["ai_failed"],
        analyzed=result["analyzed"],
        skipped_duplicates=result["skipped_duplicates"],
        lark_synced=result["lark_synced"],
        errors=result["errors"],
    )

    print("=== Competitor Video ===")
    print(f"targets={','.join(result['targets'])}")
    print(f"compare_to={','.join(result['compare_to'])}")
    print(f"platforms={','.join(result['platforms'])}")
    print(f"fetched={result['fetched']}")
    print(f"inserted={result['inserted']}")
    print(f"ai_enriched={result['ai_enriched']}")
    print(f"ai_failed={result['ai_failed']}")
    print(f"analyzed={result['analyzed']}")
    print(f"skipped_duplicates={result['skipped_duplicates']}")
    print(f"lark_synced={result['lark_synced']}")
    print(f"process_log_path={logger.path}")
    if result["errors"]:
        print("errors:")
        for line in result["errors"][:20]:
            print(f"- {line}")
    return 0


def run_lark_sync_loop(
    config_path: str,
    report_date: date | None,
    limit: int,
    interval: int,
    max_rounds: int,
    force_all_updates: bool,
) -> int:
    from .pipeline import CameraPulsePipeline
    from .process_log import ProcessingRunLogger

    config = load_config(config_path)
    pipeline = CameraPulsePipeline(config)
    logger = ProcessingRunLogger(config.report_dir, command="sync-lark-loop")
    logger.start(
        config_path=config_path,
        report_date=report_date.isoformat() if report_date else None,
        limit=max(1, int(limit)),
        interval=max(3, int(interval)),
        max_rounds=max(0, int(max_rounds)),
        force_all_updates=bool(force_all_updates),
    )

    batch_size = max(1, int(limit))
    sleep_seconds = max(3, int(interval))
    rounds = max(0, int(max_rounds))

    print("=== Lark Sync Loop ===")
    print(f"date={report_date.isoformat() if report_date else 'ALL'}")
    print(f"limit={batch_size}")
    print(f"interval={sleep_seconds}s")
    print(f"max_rounds={rounds if rounds else 'infinite'}")
    print(f"force_all_updates={1 if force_all_updates else 0}")

    round_no = 0
    try:
        while True:
            round_no += 1
            pending_before = pipeline.repository.count_lark_pending(report_date)
            row_events: list[dict[str, object]] = []
            synced = pipeline.sync_lark(
                target_date=report_date,
                limit=batch_size,
                force_all_updates=bool(force_all_updates),
                on_row_result=row_events.append,
            )
            pending_after = pipeline.repository.count_lark_pending(report_date)
            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for payload in row_events:
                logger.item(step="sync-lark", **payload)
                row_id = int(payload.get("row_id") or 0)
                if row_id > 0:
                    pipeline.repository.upsert_processing_checkpoint(
                        feedback_item_id=row_id,
                        step="sync-lark",
                        run_id=logger.run_id,
                        command=logger.command,
                        status=str(payload.get("status") or "unknown"),
                        error=str(payload.get("error") or ""),
                        details={
                            "title": str(payload.get("title") or ""),
                            "url": str(payload.get("url") or ""),
                            "point_count": int(payload.get("point_count") or 0),
                            "record_id": str(payload.get("record_id") or ""),
                        },
                    )
            logger.note(
                step="sync-lark-loop-round",
                round=round_no,
                pending_before=pending_before,
                synced=synced,
                pending_after=pending_after,
            )
            print(
                f"[{now_text}] round={round_no} pending_before={pending_before} "
                f"synced={synced} pending_after={pending_after}"
            )
            if rounds > 0 and round_no >= rounds:
                break
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        print("sync_loop_stopped=keyboard_interrupt")
        logger.finish("stopped", rounds=round_no)
        print(f"process_log_path={logger.path}")
        return 0
    logger.finish("ok", rounds=round_no)
    print(f"process_log_path={logger.path}")
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "run":
        return run_once(
            config_path=args.config,
            report_date=parse_date(args.date),
            skip_lark=bool(args.skip_lark),
            dry_run=bool(args.dry_run),
        )

    if args.command == "report":
        from .pipeline import CameraPulsePipeline

        config = load_config(args.config)
        pipeline = CameraPulsePipeline(config)
        report_path = pipeline.generate_report_only(parse_date(args.date) or date.today())
        print(f"report_path={report_path}")
        return 0

    if args.command == "send-email-summary":
        from .email_summary import send_daily_summary_email
        from .pipeline import CameraPulsePipeline

        config = load_config(args.config)
        pipeline = CameraPulsePipeline(config)
        summary = send_daily_summary_email(
            config=config,
            repository=pipeline.repository,
            target_date=parse_date(args.date) or date.today(),
            run_log_path=args.run_log,
        )
        print("email_summary_sent=1")
        print(f"email_summary_subject={summary.subject}")
        if summary.run_log_path:
            print(f"email_summary_run_log={summary.run_log_path}")
        return 0

    if args.command == "email-check":
        from .email_summary import check_email_connection

        config = load_config(args.config)
        try:
            status = check_email_connection(config, allow_disabled=True)
        except Exception as exc:  # noqa: BLE001
            print("email_check_ok=0")
            print(f"email_check_error={exc}")
            return 1
        print("email_check_ok=1")
        print(f"smtp_host={status.smtp_host}")
        print(f"smtp_port={status.smtp_port}")
        print(f"use_tls={int(status.use_tls)}")
        print(f"use_ssl={int(status.use_ssl)}")
        print(f"smtp_username_configured={int(status.smtp_username_configured)}")
        print(f"from_addr_configured={int(bool(status.from_addr))}")
        print(f"recipient_count={status.recipient_count}")
        return 0

    if args.command == "send-test-email":
        from pathlib import Path

        from .email_summary import send_email_message

        config = load_config(args.config)
        subject_prefix = str(config.email_summary.subject_prefix or "[Media Pulse]").strip()
        subject = args.subject or f"{subject_prefix} SMTP 测试邮件"
        body = args.body or "这是一封测试邮件，用来验证 SMTP 连接、登录和 HTML 邮件发送链路。"
        html_body = None
        if args.html_file:
            html_body = Path(args.html_file).expanduser().read_text(encoding="utf-8")
        elif body:
            html_body = (
                "<!doctype html><html><body>"
                f"<p>{body}</p>"
                "<p style=\"color:#666;font-size:12px;\">Media Pulse test email</p>"
                "</body></html>"
            )
        try:
            send_email_message(
                config,
                subject,
                body,
                html_body=html_body,
                to_addrs=list(args.to or []),
                allow_disabled=True,
            )
        except Exception as exc:  # noqa: BLE001
            print("test_email_sent=0")
            print(f"test_email_error={exc}")
            return 1
        print("test_email_sent=1")
        print(f"test_email_subject={subject}")
        print(f"test_email_html={int(bool(html_body))}")
        print(f"test_email_to_count={len(list(args.to or [])) or len(config.email_summary.to_addrs)}")
        return 0

    if args.command == "weekly-email":
        from .pipeline import CameraPulsePipeline
        from .weekly_email import export_weekly_media_email_html, send_weekly_media_email

        config = load_config(args.config)
        pipeline = CameraPulsePipeline(config)
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)

        preview = export_weekly_media_email_html(
            config=config,
            repository=pipeline.repository,
            start_date=start_date,
            end_date=end_date,
            scope=args.scope,
            top_limit=max(1, int(args.top_limit)),
            output_path=args.output,
        )
        print(f"weekly_email_html={preview.output_path}")
        print(f"weekly_email_subject={preview.subject}")
        print(f"weekly_email_scope={preview.metrics['scope']}")
        print(f"weekly_email_range={preview.metrics['start_date']}~{preview.metrics['end_date']}")
        print(f"weekly_email_featured_total={preview.metrics['featured_total']}")
        if args.send:
            sent = send_weekly_media_email(
                config=config,
                repository=pipeline.repository,
                start_date=start_date,
                end_date=end_date,
                scope=args.scope,
                top_limit=max(1, int(args.top_limit)),
            )
            print("weekly_email_sent=1")
            print(f"weekly_email_subject_sent={sent.subject}")
        return 0

    if args.command == "sync-lark":
        from .pipeline import CameraPulsePipeline
        from .process_log import ProcessingRunLogger

        config = load_config(args.config)
        pipeline = CameraPulsePipeline(config)
        logger = ProcessingRunLogger(config.report_dir, command="sync-lark")
        target_date = parse_date(args.date)
        batch_limit = max(1, int(args.limit))
        logger.start(
            config_path=args.config,
            report_date=target_date.isoformat() if target_date else None,
            limit=batch_limit,
            force_all_updates=bool(args.force_all_updates),
        )
        row_events: list[dict[str, object]] = []
        try:
            synced = pipeline.sync_lark(
                target_date=target_date,
                limit=batch_limit,
                force_all_updates=bool(args.force_all_updates),
                on_row_result=row_events.append,
            )
        except Exception as exc:  # noqa: BLE001
            logger.finish("failed", error=str(exc))
            print(f"process_log_path={logger.path}")
            raise
        for payload in row_events:
            logger.item(step="sync-lark", **payload)
            row_id = int(payload.get("row_id") or 0)
            if row_id > 0:
                pipeline.repository.upsert_processing_checkpoint(
                    feedback_item_id=row_id,
                    step="sync-lark",
                    run_id=logger.run_id,
                    command=logger.command,
                    status=str(payload.get("status") or "unknown"),
                    error=str(payload.get("error") or ""),
                    details={
                        "title": str(payload.get("title") or ""),
                        "url": str(payload.get("url") or ""),
                        "point_count": int(payload.get("point_count") or 0),
                        "record_id": str(payload.get("record_id") or ""),
                    },
                )
        logger.finish(
            "ok",
            synced=synced,
            pending_after=pipeline.repository.count_lark_pending(target_date),
        )
        print(f"synced_to_lark={synced}")
        print(f"process_log_path={logger.path}")
        return 0

    if args.command == "mark-lark-dirty":
        from .storage import FeedbackRepository

        config = load_config(args.config)
        repository = FeedbackRepository(config.database_path)
        updated = repository.mark_lark_dirty(target_date=parse_date(args.date))
        print(f"lark_dirty_marked={updated}")
        return 0

    if args.command == "sync-lark-loop":
        return run_lark_sync_loop(
            config_path=args.config,
            report_date=parse_date(args.date),
            limit=max(1, int(args.limit)),
            interval=max(1, int(args.interval)),
            max_rounds=max(0, int(args.max_rounds)),
            force_all_updates=bool(args.force_all_updates),
        )

    if args.command == "prepare-lark-dashboard":
        from .lark import LarkBitableClient

        config = load_config(args.config)
        client = LarkBitableClient(config.lark)
        if not client.is_available():
            print("lark_dashboard_ready=0")
            print("error=Lark config is missing or disabled")
            return 1
        if not args.skip_views:
            result = client.prepare_dashboard_views()
            print(f"views_created={len(result['created'])}")
            print(f"views_updated={len(result['updated'])}")
            print(f"views_skipped={len(result['skipped'])}")
            if result["created"]:
                print(f"views_created_names={','.join(result['created'])}")
            if result["updated"]:
                print(f"views_updated_names={','.join(result['updated'])}")
            if result["warnings"]:
                print(f"view_warnings={' | '.join(result['warnings'])}")
        field_rows = client.inspect_dashboard_field_types()
        mismatches = [row for row in field_rows if row["ok"] != "1"]
        print(f"dashboard_field_type_mismatches={len(mismatches)}")
        for row in mismatches:
            print(
                "field_type_todo="
                f"{row['field_name']}:{row['current_type']}=>{row['expected_type']}"
            )
        print("lark_dashboard_ready=1")
        return 0

    if args.command == "backfill":
        from .pipeline import CameraPulsePipeline

        config = load_config(args.config)
        pipeline = CameraPulsePipeline(config)
        stats = pipeline.backfill_analysis(target_date=parse_date(args.date), limit=max(1, int(args.limit)))
        print(f"scanned={stats['scanned']}")
        print(f"updated={stats['updated']}")
        print(f"ai_enriched={stats['ai_enriched']}")
        print(f"ai_failed={stats['ai_failed']}")
        return 0

    if args.command == "retag":
        from .pipeline import CameraPulsePipeline

        config = load_config(args.config)
        pipeline = CameraPulsePipeline(config)
        limit = max(1, int(args.limit))
        if bool(args.all):
            total = int(
                pipeline.repository.connection.execute("SELECT COUNT(1) FROM feedback_items").fetchone()[0] or 0
            )
            limit = max(1, total)
        stats = pipeline.retag_with_ai(
            target_date=parse_date(args.date),
            limit=limit,
            sync_lark=bool(args.sync_lark),
            sync_batch_limit=max(1, int(args.sync_batch_limit)),
        )
        print(f"scanned={stats['scanned']}")
        print(f"updated={stats['updated']}")
        print(f"ai_enriched={stats['ai_enriched']}")
        print(f"ai_failed={stats['ai_failed']}")
        print(f"lark_synced={stats['lark_synced']}")
        print(f"lark_pending={stats['lark_pending']}")
        return 0

    if args.command == "dashboard":
        return run_dashboard(
            config_path=args.config,
            host=args.host,
            port=args.port,
            report_date=parse_date(args.date),
        )

    if args.command == "backend":
        from .backend import run_backend_server

        return run_backend_server(
            config_path=args.config,
            host=args.host,
            port=args.port,
            report_date=parse_date(args.date),
        )

    if args.command == "frontend":
        from .frontend import run_frontend_server

        return run_frontend_server(
            host=args.host,
            port=args.port,
            api_base_url=args.api_base_url,
        )

    if args.command == "video-tasks":
        from .storage import FeedbackRepository
        from .video_tasks import export_video_tasks

        config = load_config(args.config)
        repository = FeedbackRepository(config.database_path)
        target_date = parse_date(args.date) or date.today()
        path = export_video_tasks(repository, target_date, args.output_dir)
        print(f"video_tasks_path={path}")
        return 0

    if args.command == "video-process":
        return run_video_process(
            config_path=args.config,
            report_date=parse_date(args.date),
            row_id=args.id,
            limit=max(1, int(args.limit)),
            include_processed=bool(args.include_processed),
        )

    if args.command == "ingest-video":
        return run_ingest_video(
            config_path=args.config,
            raw_urls=list(args.url or []),
            file_path=args.file,
            run_ai=bool(args.run_ai),
            analyze=bool(args.analyze),
            limit=max(1, int(args.limit)),
            dry_run=bool(args.dry_run),
        )

    if args.command == "competitor-video":
        return run_competitor_video(
            config_path=args.config,
            targets=list(args.target or []),
            compare_to=list(args.compare_to or []),
            direct_queries=list(args.query or []),
            platforms=list(args.platform or []),
            lookback_days=max(1, int(args.lookback_days)),
            limit_per_query=max(1, int(args.limit_per_query)),
            max_total=max(1, int(args.max_total)),
            campaign_name=args.campaign_name,
            skip_ai=bool(args.skip_ai),
            analyze=bool(args.analyze),
            sync_lark=bool(args.sync_lark),
            dry_run=bool(args.dry_run),
        )

    if args.command == "schedule":
        return run_schedule(args.config)

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
