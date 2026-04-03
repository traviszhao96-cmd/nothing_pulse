from __future__ import annotations

import argparse
import time
from datetime import date, datetime

from .config import load_config
from .dashboard import run_dashboard

DEFAULT_VIDEO_LINKS_FILE = "reports/video-links.txt"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Nothing Camera Pulse")
    parser.add_argument("--config", default="config.yaml", help="Path to config yaml")

    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Fetch + filter + classify + report + optional Lark sync")
    run_parser.add_argument("--date", help="Report date (YYYY-MM-DD), default today")
    run_parser.add_argument("--skip-lark", action="store_true", help="Skip Lark sync")
    run_parser.add_argument("--dry-run", action="store_true", help="Fetch and classify only, do not persist")

    report_parser = subparsers.add_parser("report", help="Generate report only")
    report_parser.add_argument("--date", required=True, help="Report date (YYYY-MM-DD)")

    sync_parser = subparsers.add_parser("sync-lark", help="Sync unsynced rows to Lark")
    sync_parser.add_argument("--date", help="Only sync rows from report date YYYY-MM-DD")
    sync_parser.add_argument("--limit", type=int, default=500, help="Max rows per sync")
    sync_parser.add_argument(
        "--force-all-updates",
        action="store_true",
        help="Ignore only_sync_new_records and sync dirty rows even when record_id exists",
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
    from .pipeline import CameraPulsePipeline

    config = load_config(config_path)
    pipeline = CameraPulsePipeline(config)
    result = pipeline.run(target_date=report_date, skip_lark=skip_lark, dry_run=dry_run)

    print("=== Nothing Camera Pulse ===")
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
    if result.errors:
        print("errors:")
        for line in result.errors:
            print(f"- {line}")
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
        id="nothing_camera_pulse_daily",
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
            id="nothing_camera_video_nightly",
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
    from .video_analysis import VideoAnalysisService

    config = load_config(config_path)
    pipeline = CameraPulsePipeline(config)
    service = VideoAnalysisService(config, pipeline.repository)
    result = service.process(
        target_date=report_date,
        row_id=row_id,
        limit=max(1, int(limit)),
        only_unprocessed=not include_processed,
    )

    print("=== Video Process ===")
    print(f"ok={result.get('ok')}")
    print(f"processed={result.get('processed', 0)}")
    print(f"succeeded={result.get('succeeded', 0)}")
    print(f"failed={result.get('failed', 0)}")
    print(f"skipped_duplicates={result.get('skipped_duplicates', 0)}")
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


def run_lark_sync_loop(
    config_path: str,
    report_date: date | None,
    limit: int,
    interval: int,
    max_rounds: int,
    force_all_updates: bool,
) -> int:
    from .pipeline import CameraPulsePipeline

    config = load_config(config_path)
    pipeline = CameraPulsePipeline(config)

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
            synced = pipeline.sync_lark(
                target_date=report_date,
                limit=batch_size,
                force_all_updates=bool(force_all_updates),
            )
            pending_after = pipeline.repository.count_lark_pending(report_date)
            now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"[{now_text}] round={round_no} pending_before={pending_before} "
                f"synced={synced} pending_after={pending_after}"
            )
            if rounds > 0 and round_no >= rounds:
                break
            time.sleep(sleep_seconds)
    except KeyboardInterrupt:
        print("sync_loop_stopped=keyboard_interrupt")
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

    if args.command == "sync-lark":
        from .pipeline import CameraPulsePipeline

        config = load_config(args.config)
        pipeline = CameraPulsePipeline(config)
        synced = pipeline.sync_lark(
            target_date=parse_date(args.date),
            limit=max(1, int(args.limit)),
            force_all_updates=bool(args.force_all_updates),
        )
        print(f"synced_to_lark={synced}")
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

    if args.command == "schedule":
        return run_schedule(args.config)

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
