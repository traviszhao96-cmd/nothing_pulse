from __future__ import annotations

import sqlite3
import time
from datetime import date
from typing import Any

from .ai_enricher import LocalAIEnricher
from .classifier import CameraClassifier
from .config import AppConfig
from .fetchers import (
    BilibiliSearchCollector,
    CustomRSSCollector,
    GoogleNewsCollector,
    InstagramInstaloaderCollector,
    MockFileCollector,
    NothingCommunityCollector,
    RedditOAuthCollector,
    RedditSNScrapeCollector,
    XAPICollector,
    YouTubeSearchCollector,
    YouTubeYtDlpCollector,
    XSnscrapeCollector,
    XTWScrapeCollector,
)
from .fetchers.article_body import ArticleBodyExtractor
from .fetchers.base import BaseCollector
from .filtering import CameraScopeFilter, SimilarityDeduper
from .lark import LarkBitableClient
from .manual_video import build_manual_video_item, fetch_video_page_meta
from .models import FeedbackItem, PipelineResult
from .report import generate_daily_report
from .source_profile import SourceProfiler
from .storage import FeedbackRepository
from .utils import clean_content_text, is_summary_redundant, is_video_url, load_json, parse_datetime, since_hours, truncate
from .video_identity import extract_video_signatures, parse_video_signatures

_EXPLICIT_VIDEO_FLAG_SOURCES = {"x_api", "x_twscrape", "x_snscrape"}


class CameraPulsePipeline:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.repository = FeedbackRepository(config.database_path)
        self.classifier = CameraClassifier(config.camera_categories)
        self.source_profiler = SourceProfiler()
        self.ai_enricher = LocalAIEnricher(config.local_ai)
        self.scope_filter = CameraScopeFilter(config.camera_keywords)
        self.collectors = self._build_collectors(config)
        lark_cfg = dict(config.lark)
        lark_cfg["_camera_categories"] = dict(config.camera_categories)
        self.lark_client = LarkBitableClient(lark_cfg)
        self._article_extractors: dict[str, ArticleBodyExtractor] = {}

    def _lark_only_new(self) -> bool:
        return bool(self.config.lark.get("only_sync_new_records", False))

    def run(
        self,
        target_date: date | None = None,
        skip_lark: bool = False,
        dry_run: bool = False,
        run_logger: Any | None = None,
    ) -> PipelineResult:
        result = PipelineResult()
        since = since_hours(self.config.lookback_hours)
        filter_mode = self.config.camera_filter_mode
        deduper = SimilarityDeduper.from_repository(
            self.repository,
            threshold=self.config.dedupe.jaccard_threshold,
            lookback_days=self.config.dedupe.lookback_days,
        )
        if run_logger:
            run_logger.note(
                step="run-config",
                since=since.isoformat(),
                filter_mode=filter_mode,
                collector_names=[collector.name for collector in self.collectors],
            )

        for collector in self.collectors:
            collector_name = collector.name
            collector_started = time.monotonic()
            collector_processed = 0
            collector_inserted = 0
            collector_duplicates = 0
            self._emit_progress(
                f"[run] collector_start name={collector_name} since={since.isoformat()} dry_run={1 if dry_run else 0}"
            )
            try:
                items = collector.fetch(since)
            except Exception as exc:  # noqa: BLE001
                result.errors.append(f"collector={collector.name} error={exc}")
                if run_logger:
                    run_logger.note(
                        step="collector",
                        collector=collector_name,
                        status="failed",
                        error=str(exc),
                    )
                self._emit_progress(f"[run] collector_error name={collector_name} error={exc}")
                continue

            result.fetched += len(items)
            if run_logger:
                run_logger.note(
                    step="collector",
                    collector=collector_name,
                    status="fetched",
                    fetched=len(items),
                )
            self._emit_progress(
                f"[run] collector_fetched name={collector_name} fetched={len(items)} "
                f"elapsed={time.monotonic() - collector_started:.2f}s"
            )
            for item in items:
                collector_processed += 1
                item_status = "pending"
                item_reason = ""
                is_camera_related, hits = self.scope_filter.is_camera_related(item)
                item.camera_related = is_camera_related
                item.camera_keyword_hits = hits[:20]
                if item.source not in _EXPLICIT_VIDEO_FLAG_SOURCES:
                    item.video_candidate = bool(item.video_candidate or is_video_url(item.url))
                self._ensure_video_signatures(item)

                if filter_mode == "strict":
                    if not is_camera_related:
                        result.skipped_non_camera += 1
                        item_status = "skipped_non_camera"
                        item_reason = "filter_strict_non_camera"
                        self._emit_progress(
                            f"[run] item_skip name={collector_name} reason=non_camera title={truncate(item.title, 80)}"
                        )
                        if run_logger:
                            self._log_run_item(
                                run_logger,
                                collector_name=collector_name,
                                item=item,
                                status=item_status,
                                reason=item_reason,
                            )
                        continue
                    result.kept_camera_only += 1
                elif filter_mode == "review":
                    if is_camera_related:
                        result.kept_camera_only += 1
                    else:
                        result.retained_non_camera += 1
                else:  # off
                    result.kept_camera_only += 1

                duplicated, reason = deduper.is_duplicate(item)
                if duplicated:
                    result.skipped_duplicates += 1
                    collector_duplicates += 1
                    item.extra["dedupe_reason"] = reason
                    item_status = "skipped_duplicate"
                    item_reason = reason
                    self._emit_progress(
                        f"[run] item_skip name={collector_name} reason=duplicate({reason}) title={truncate(item.title, 80)}"
                    )
                    if run_logger:
                        self._log_run_item(
                            run_logger,
                            collector_name=collector_name,
                            item=item,
                            status=item_status,
                            reason=item_reason,
                        )
                    continue

                self.classifier.classify(item)
                self.source_profiler.classify(item)
                enrich_result = self.ai_enricher.enrich(item)
                if enrich_result.ok:
                    result.ai_enriched += 1
                    item.extra.pop("local_ai_error", None)
                elif enrich_result.error and enrich_result.error != "local_ai_disabled":
                    result.ai_failed += 1
                    item.extra["local_ai_error"] = enrich_result.error
                    self._emit_progress(
                        f"[run] item_ai_failed name={collector_name} error={enrich_result.error} title={truncate(item.title, 80)}"
                    )
                if dry_run:
                    result.inserted += 1
                    collector_inserted += 1
                    item_status = "dry_run_ready"
                    self._emit_progress(
                        f"[run] item_ready name={collector_name} video={1 if item.video_candidate else 0} "
                        f"title={truncate(item.title, 80)}"
                    )
                    if run_logger:
                        self._log_run_item(
                            run_logger,
                            collector_name=collector_name,
                            item=item,
                            status=item_status,
                        )
                    continue

                try:
                    inserted_row_id = self.repository.insert(item)
                except sqlite3.IntegrityError:
                    result.skipped_duplicates += 1
                    collector_duplicates += 1
                    item_status = "skipped_duplicate"
                    item_reason = "sqlite_integrity_error"
                    self._emit_progress(
                        f"[run] item_skip name={collector_name} reason=sqlite_integrity_error title={truncate(item.title, 80)}"
                    )
                    if run_logger:
                        self._log_run_item(
                            run_logger,
                            collector_name=collector_name,
                            item=item,
                            status=item_status,
                            reason=item_reason,
                        )
                    continue
                if inserted_row_id:
                    result.inserted += 1
                    collector_inserted += 1
                    item_status = "inserted"
                    self._emit_progress(
                        f"[run] item_inserted name={collector_name} row_id={inserted_row_id} "
                        f"video={1 if item.video_candidate else 0} title={truncate(item.title, 80)}"
                    )
                    if run_logger:
                        self.repository.upsert_processing_checkpoint(
                            feedback_item_id=int(inserted_row_id),
                            step="run",
                            run_id=run_logger.run_id,
                            command=run_logger.command,
                            status=item_status,
                            error=str(item.extra.get("local_ai_error", "") or ""),
                            details={
                                "collector": collector_name,
                                "source": item.source,
                                "source_item_id": item.source_item_id or "",
                                "url": item.url,
                                "camera_category": item.camera_category,
                                "sentiment": item.sentiment,
                                "severity": item.severity,
                            },
                        )
                else:
                    item_status = "skipped_insert"
                    item_reason = "insert_returned_false"
                if run_logger:
                    self._log_run_item(
                        run_logger,
                        collector_name=collector_name,
                        item=item,
                        status=item_status,
                        reason=item_reason,
                        row_id=int(inserted_row_id) if inserted_row_id else None,
                    )
            self._emit_progress(
                f"[run] collector_done name={collector_name} fetched={len(items)} "
                f"processed={collector_processed} inserted={collector_inserted} "
                f"duplicates={collector_duplicates} elapsed={time.monotonic() - collector_started:.2f}s"
            )

        if dry_run:
            if run_logger:
                run_logger.note(step="run-summary", phase="dry-run", **self._pipeline_result_payload(result))
            return result

        report_date = target_date or date.today()
        report_path = generate_daily_report(self.repository, report_date, self.config.report_dir)
        result.report_path = str(report_path)
        if run_logger:
            run_logger.note(
                step="report",
                status="generated",
                report_date=report_date.isoformat(),
                report_path=str(report_path),
            )

        if not skip_lark and self.lark_client.is_available():
            pending_rows = self.repository.fetch_lark_pending(
                report_date,
                limit=500,
                only_new=self._lark_only_new(),
            )
            if run_logger:
                run_logger.note(
                    step="sync-lark",
                    status="start",
                    pending_before=len(pending_rows),
                    only_new=self._lark_only_new(),
                )
            row_events: list[dict[str, Any]] = []
            synced = self.lark_client.sync_rows(
                pending_rows,
                mark_synced=self.repository.mark_synced,
                mark_failed=self.repository.mark_lark_sync_failed,
                list_point_links=self.repository.list_lark_point_links,
                get_point_record_id=self.repository.get_lark_point_record_id,
                upsert_point_link=self.repository.upsert_lark_point_link,
                delete_point_link=self.repository.delete_lark_point_link,
                mark_point_failed=self.repository.mark_lark_point_failed,
                on_row_result=row_events.append,
            )
            result.synced_to_lark = synced
            if run_logger:
                for payload in row_events:
                    run_logger.item(step="sync-lark", **payload)
                    row_id = int(payload.get("row_id") or 0)
                    if row_id > 0:
                        self.repository.upsert_processing_checkpoint(
                            feedback_item_id=row_id,
                            step="sync-lark",
                            run_id=run_logger.run_id,
                            command=run_logger.command,
                            status=str(payload.get("status") or "unknown"),
                            error=str(payload.get("error") or ""),
                            details={
                                "title": str(payload.get("title") or ""),
                                "url": str(payload.get("url") or ""),
                                "point_count": int(payload.get("point_count") or 0),
                                "record_id": str(payload.get("record_id") or ""),
                            },
                        )
                run_logger.note(
                    step="sync-lark",
                    status="finish",
                    pending_before=len(pending_rows),
                    synced=synced,
                    pending_after=self.repository.count_lark_pending(report_date),
                )
        elif run_logger:
            run_logger.note(
                step="sync-lark",
                status="skipped",
                reason="skip_lark_flag" if skip_lark else "lark_unavailable",
            )

        if run_logger:
            run_logger.note(step="run-summary", phase="complete", **self._pipeline_result_payload(result))

        return result

    @staticmethod
    def _emit_progress(message: str) -> None:
        print(message, flush=True)

    def generate_report_only(self, target_date: date) -> str:
        path = generate_daily_report(self.repository, target_date, self.config.report_dir)
        return str(path)

    def sync_lark_only(self, target_date: date | None = None, limit: int = 500) -> int:
        return self.sync_lark(
            target_date=target_date,
            limit=limit,
            force_all_updates=False,
        )

    def sync_lark(
        self,
        target_date: date | None = None,
        limit: int = 500,
        force_all_updates: bool = False,
        on_row_result: Any | None = None,
    ) -> int:
        rows = self.repository.fetch_lark_pending(
            target_date=target_date,
            limit=max(1, int(limit)),
            only_new=(False if force_all_updates else self._lark_only_new()),
        )
        return self.lark_client.sync_rows(
            rows,
            mark_synced=self.repository.mark_synced,
            mark_failed=self.repository.mark_lark_sync_failed,
            list_point_links=self.repository.list_lark_point_links,
            get_point_record_id=self.repository.get_lark_point_record_id,
            upsert_point_link=self.repository.upsert_lark_point_link,
            delete_point_link=self.repository.delete_lark_point_link,
            mark_point_failed=self.repository.mark_lark_point_failed,
            on_row_result=on_row_result,
        )

    def retag_with_ai(
        self,
        target_date: date | None = None,
        limit: int = 500,
        sync_lark: bool = False,
        sync_batch_limit: int = 200,
    ) -> dict[str, int]:
        stats = self.backfill_analysis(target_date=target_date, limit=max(1, int(limit)))
        stats["lark_synced"] = 0
        stats["lark_pending"] = 0

        if not sync_lark or not self.lark_client.is_available():
            stats["lark_pending"] = self.repository.count_lark_pending(target_date=target_date, only_new=False)
            return stats

        total_synced = 0
        rounds = 0
        while True:
            rounds += 1
            synced = self.sync_lark(
                target_date=target_date,
                limit=max(1, int(sync_batch_limit)),
                force_all_updates=True,
            )
            total_synced += synced
            pending = self.repository.count_lark_pending(target_date=target_date, only_new=False)
            if pending <= 0:
                break
            if synced <= 0:
                break
            if rounds >= 50:
                break

        stats["lark_synced"] = total_synced
        stats["lark_pending"] = self.repository.count_lark_pending(target_date=target_date, only_new=False)
        return stats

    def backfill_analysis(self, target_date: date | None = None, limit: int = 500) -> dict[str, int]:
        rows = self.repository.fetch_rows_for_backfill(target_date=target_date, limit=limit)
        updated = 0
        ai_enriched = 0
        ai_failed = 0

        for row in rows:
            item = FeedbackItem(
                source=row["source"],
                source_item_id=row["source_item_id"],
                title=clean_content_text(row["title"]),
                url=row["url"],
                content=clean_content_text(row["content"] or row["title"]),
                summary=clean_content_text(row["summary"] or ""),
                published_at=parse_datetime(row["published_at"]) or since_hours(0),
                author=row["author"],
                source_section=row["source_section"],
                camera_category=row["camera_category"] or "未分类",
                sentiment=row["sentiment"] or "neutral",
                severity=row["severity"] or "low",
                source_actor_type=row["source_actor_type"] or "unknown",
                source_actor_reason=row["source_actor_reason"] or "",
                domain_tag=row["domain_tag"] or "未分类",
                domain_subtags=load_json(row["domain_subtags_json"], []),
                sentiment_reason=row["sentiment_reason"] or "",
                ai_positive_points=load_json(row["ai_positive_points_json"], []),
                ai_neutral_points=load_json(row["ai_neutral_points_json"], []),
                ai_negative_points=load_json(row["ai_negative_points_json"], []),
                product_tags=load_json(row["product_tags"], []),
                camera_keyword_hits=load_json(row["camera_keyword_hits"], []),
                camera_related=bool(int(row["camera_related"] or 0)),
                video_candidate=bool(int(row["video_candidate"] or 0)),
                token_set=load_json(row["token_set_json"], []),
                language=row["language"] or "unknown",
                extra=load_json(row["extra_json"], {}),
            )
            if item.source not in _EXPLICIT_VIDEO_FLAG_SOURCES and is_video_url(item.url):
                item.video_candidate = True
            self._ensure_video_signatures(item)
            if (not clean_content_text(item.author or "")) and item.video_candidate:
                meta = fetch_video_page_meta(item.url, timeout_seconds=8)
                author = clean_content_text(meta.author or "")
                if author:
                    item.author = author
            if _is_google_news_stub(item.source, item.content):
                if item.summary and not _is_google_news_stub(item.source, item.summary):
                    item.content = item.summary
                else:
                    item.content = item.title
            if _is_google_news_stub(item.source, item.summary):
                item.summary = ""
            if is_summary_redundant(item.title, item.summary):
                item.summary = ""
            self._backfill_article_body(item)

            self.classifier.classify(item)
            self.source_profiler.classify(item)
            enrich_result = self.ai_enricher.enrich(item)
            if enrich_result.ok:
                ai_enriched += 1
                item.extra.pop("local_ai_error", None)
            elif enrich_result.error and enrich_result.error != "local_ai_disabled":
                ai_failed += 1
                item.extra["local_ai_error"] = enrich_result.error

            self.repository.update_analysis_fields(int(row["id"]), item)
            updated += 1

        return {
            "scanned": len(rows),
            "updated": updated,
            "ai_enriched": ai_enriched,
            "ai_failed": ai_failed,
        }

    def ingest_manual_video_urls(
        self,
        urls: list[str],
        run_ai: bool = False,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        deduper = SimilarityDeduper.from_repository(
            self.repository,
            threshold=self.config.dedupe.jaccard_threshold,
            lookback_days=self.config.dedupe.lookback_days,
        )
        scanned = 0
        inserted = 0
        skipped_duplicates = 0
        ai_enriched = 0
        ai_failed = 0
        errors: list[str] = []
        known_video_signatures = self._load_known_video_signatures(limit=6000)

        for raw_url in urls:
            scanned += 1
            try:
                item = build_manual_video_item(raw_url)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"url={raw_url} error={exc}")
                continue

            is_camera_related, hits = self.scope_filter.is_camera_related(item)
            item.camera_related = is_camera_related
            item.camera_keyword_hits = hits[:20]
            item.video_candidate = True
            self._ensure_video_signatures(item)
            item_signatures = set(parse_video_signatures(item.extra.get("video_signatures")))
            if item_signatures and item_signatures & known_video_signatures:
                skipped_duplicates += 1
                item.extra["dedupe_reason"] = "video_signature_known"
                continue

            duplicated, reason = deduper.is_duplicate(item)
            if duplicated:
                skipped_duplicates += 1
                item.extra["dedupe_reason"] = reason
                continue

            self.classifier.classify(item)
            self.source_profiler.classify(item)

            if run_ai:
                enrich_result = self.ai_enricher.enrich(item)
                if enrich_result.ok:
                    ai_enriched += 1
                    item.extra.pop("local_ai_error", None)
                elif enrich_result.error and enrich_result.error != "local_ai_disabled":
                    ai_failed += 1
                    item.extra["local_ai_error"] = enrich_result.error

            if dry_run:
                inserted += 1
                known_video_signatures.update(item_signatures)
                continue

            try:
                inserted_row_id = self.repository.insert(item)
            except sqlite3.IntegrityError:
                skipped_duplicates += 1
                continue
            if inserted_row_id:
                inserted += 1
                known_video_signatures.update(item_signatures)

        return {
            "scanned": scanned,
            "inserted": inserted,
            "skipped_duplicates": skipped_duplicates,
            "ai_enriched": ai_enriched,
            "ai_failed": ai_failed,
            "errors": errors,
        }

    def _backfill_article_body(self, item: FeedbackItem) -> None:
        if item.source not in {"google_news", "custom_rss"}:
            return
        source_cfg = dict(self.config.sources.get(item.source, {}))
        if not source_cfg.get("fetch_article_body", True):
            return
        extractor = self._get_article_extractor(item.source, source_cfg)
        if not extractor:
            return
        body_result = extractor.fetch(item.url)
        article_body = body_result.text
        if not article_body:
            return
        item.content = article_body
        item.extra["article_body_fetched"] = True
        if (not item.summary) or is_summary_redundant(item.title, item.summary):
            if not is_summary_redundant(item.title, article_body):
                item.summary = truncate(article_body, 240)

    def _ensure_video_signatures(self, item: FeedbackItem) -> None:
        if not item.video_candidate:
            if item.source in _EXPLICIT_VIDEO_FLAG_SOURCES or not is_video_url(item.url):
                return
        signatures = extract_video_signatures(
            url=item.url,
            title=item.title,
            author=item.author or "",
            source_item_id=item.source_item_id or "",
        )
        if signatures:
            item.extra["video_signatures"] = signatures

    def _load_known_video_signatures(self, limit: int = 4000) -> set[str]:
        rows = self.repository.fetch_rows_for_backfill(target_date=None, limit=max(100, int(limit)))
        pool: set[str] = set()
        for row in rows:
            url = str(row["url"] or "")
            if not url:
                continue
            extra = load_json(row["extra_json"], {})
            source = str(row["source"] or "").strip().lower()
            if source in _EXPLICIT_VIDEO_FLAG_SOURCES:
                is_tracked_video = int(row["video_candidate"] or 0) == 1
            else:
                is_tracked_video = int(row["video_candidate"] or 0) == 1 or is_video_url(url)
            if not is_tracked_video:
                continue
            existing = parse_video_signatures(extra.get("video_signatures")) if isinstance(extra, dict) else []
            signatures = existing or extract_video_signatures(
                url=url,
                title=str(row["title"] or ""),
                author=str(row["author"] or ""),
                source_item_id=str(row["source_item_id"] or ""),
            )
            pool.update(signatures)
        return pool

    @staticmethod
    def _pipeline_result_payload(result: PipelineResult) -> dict[str, Any]:
        return {
            "fetched": result.fetched,
            "kept_camera_only": result.kept_camera_only,
            "retained_non_camera": result.retained_non_camera,
            "skipped_non_camera": result.skipped_non_camera,
            "skipped_duplicates": result.skipped_duplicates,
            "inserted": result.inserted,
            "ai_enriched": result.ai_enriched,
            "ai_failed": result.ai_failed,
            "synced_to_lark": result.synced_to_lark,
            "report_path": result.report_path,
            "error_count": len(result.errors),
        }

    @staticmethod
    def _log_run_item(
        run_logger: Any,
        *,
        collector_name: str,
        item: FeedbackItem,
        status: str,
        reason: str = "",
        row_id: int | None = None,
    ) -> None:
        run_logger.item(
            step="run-item",
            status=status,
            row_id=row_id,
            collector=collector_name,
            source=item.source,
            source_item_id=item.source_item_id or "",
            title=truncate(clean_content_text(item.title), 240),
            url=item.url,
            camera_related=bool(item.camera_related),
            camera_hits=item.camera_keyword_hits[:10],
            reason=reason,
            camera_category=item.camera_category,
            sentiment=item.sentiment,
            severity=item.severity,
            video_candidate=bool(item.video_candidate),
            ai_error=item.extra.get("local_ai_error", ""),
        )

    def _get_article_extractor(self, source: str, source_cfg: dict[str, Any]) -> ArticleBodyExtractor | None:
        extractor = self._article_extractors.get(source)
        if extractor:
            return extractor
        timeout = int(source_cfg.get("article_timeout_seconds", 10))
        max_chars = int(source_cfg.get("article_max_chars", 6000))
        extractor = ArticleBodyExtractor(timeout=timeout, max_chars=max_chars)
        self._article_extractors[source] = extractor
        return extractor

    @staticmethod
    def _build_collectors(config: AppConfig) -> list[BaseCollector]:
        collectors: list[BaseCollector] = []
        for name, source_cfg in config.sources.items():
            if not source_cfg.get("enabled", False):
                continue
            collectors.extend(_instantiate_collector(name, source_cfg, config.product_keywords))
        return collectors


def _instantiate_collector(name: str, source_cfg: dict[str, Any], product_keywords: list[str]) -> list[BaseCollector]:
    if name == "bilibili":
        return [BilibiliSearchCollector(name, source_cfg, product_keywords)]
    if name == "nothing_community":
        return [NothingCommunityCollector(name, source_cfg, product_keywords)]
    if name == "google_news":
        return [GoogleNewsCollector(name, source_cfg, product_keywords)]
    if name == "custom_rss":
        return [CustomRSSCollector(name, source_cfg, product_keywords)]
    if name == "mock_file":
        return [MockFileCollector(name, source_cfg, product_keywords)]
    if name == "reddit":
        return [RedditOAuthCollector(name, source_cfg, product_keywords)]
    if name == "youtube":
        return [YouTubeSearchCollector(name, source_cfg, product_keywords)]
    if name == "youtube_yt_dlp":
        return [YouTubeYtDlpCollector(name, source_cfg, product_keywords)]
    if name == "x_api":
        return [XAPICollector(name, source_cfg, product_keywords)]
    if name == "x_twscrape":
        return [XTWScrapeCollector(name, source_cfg, product_keywords)]
    if name == "x_snscrape":
        return [XSnscrapeCollector(name, source_cfg, product_keywords)]
    if name == "instagram_instaloader":
        return [InstagramInstaloaderCollector(name, source_cfg, product_keywords)]
    if name == "reddit_snscrape":
        return [RedditSNScrapeCollector(name, source_cfg, product_keywords)]
    return []


def _is_google_news_stub(source: str, content: str) -> bool:
    if source != "google_news":
        return False
    low = (content or "").strip().lower()
    return low.startswith("comprehensive up-to-date news coverage")
