from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from .ai_enricher import apply_structured_analysis, extract_json_object
from .classifier import CameraClassifier
from .config import AppConfig
from .models import FeedbackItem
from .source_profile import SourceProfiler
from .storage import FeedbackRepository
from .utils import clean_content_text, is_video_url, load_json, parse_datetime, since_hours, truncate
from .video_identity import extract_video_signatures, parse_video_signatures
from .youtube_comments import YouTubeCommentMiner


@dataclass(slots=True)
class VideoProcessResult:
    ok: bool
    error: str = ""
    output_file: str = ""


class VideoAnalysisService:
    def __init__(self, config: AppConfig, repository: FeedbackRepository) -> None:
        self.config = config
        self.repository = repository
        self.classifier = CameraClassifier(config.camera_categories)
        self.source_profiler = SourceProfiler()
        self.comment_miner = YouTubeCommentMiner(config)

    def is_enabled(self) -> bool:
        cfg = self.config.video_processing
        if not cfg.enabled:
            return False
        script_path = Path(cfg.videosummary_script).expanduser()
        python_path = Path(cfg.videosummary_python).expanduser()
        return script_path.exists() and python_path.exists()

    def process(
        self,
        target_date: date | None = None,
        row_id: int | None = None,
        limit: int | None = None,
        only_unprocessed: bool = True,
    ) -> dict[str, Any]:
        if not self.is_enabled():
            return {
                "ok": False,
                "error": "video_processing_disabled_or_missing_runtime",
                "processed": 0,
                "succeeded": 0,
                "failed": 0,
                "items": [],
            }

        rows: list[Any]
        if row_id:
            row = self.repository.fetch_by_id(int(row_id))
            rows = [row] if row else []
        else:
            if target_date is None:
                dates = self.repository.list_report_dates(limit=1)
                target_date = date.fromisoformat(dates[0]) if dates else date.today()
            rows = self.repository.fetch_by_report_date(target_date, camera_only=None)

        force_ids = {int(row_id)} if row_id else set()
        processed_signature_map = self._load_processed_video_signature_map(exclude_ids=force_ids) if only_unprocessed else {}
        candidates, duplicate_rows, skipped_duplicates = self._pick_candidates(
            rows,
            limit=limit,
            only_unprocessed=only_unprocessed,
            force_ids=force_ids,
            processed_signature_map=processed_signature_map,
        )
        succeeded = 0
        failed = 0
        duplicate_resolved = 0
        item_results: list[dict[str, Any]] = []

        for row, source_row in duplicate_rows:
            self._apply_duplicate_analysis(row=row, source_row=source_row)
            duplicate_resolved += 1
            item_results.append(
                {
                    "id": int(row["id"]),
                    "title": row["title"],
                    "url": row["url"],
                    "ok": True,
                    "error": "",
                    "output_file": load_json(source_row["extra_json"], {}).get("video_analysis", {}).get("output_file", ""),
                    "duplicate_of": int(source_row["id"]),
                }
            )

        for row in candidates:
            item = self._row_to_item(row)
            self._ensure_video_signatures(item)
            comment_result = self._analyze_youtube_comments(item)
            comment_points = comment_result.get("points", [])
            result = self._process_single(item.url)
            extra = dict(item.extra)
            extra["video_analysis"] = {
                "processed_at": datetime.now(tz=timezone.utc).isoformat(),
                "status": "ok" if result.ok else "failed",
                "output_file": result.output_file,
                "error": result.error,
            }
            if comment_result:
                extra["youtube_comment_mining"] = comment_result.get("meta", {})
                if comment_result.get("error"):
                    extra["youtube_comment_mining"]["error"] = comment_result.get("error")

            if result.ok:
                try:
                    data = self._parse_structured_output(result.output_file)
                    apply_structured_analysis(item, data)
                    item.video_candidate = False
                    succeeded += 1
                except Exception as exc:  # noqa: BLE001
                    failed += 1
                    extra["video_analysis"]["status"] = "failed"
                    extra["video_analysis"]["error"] = f"parse_output_failed: {exc}"
            else:
                failed += 1

            item.extra = extra
            if comment_points:
                merged_points = self._merge_structured_points(
                    existing=item.extra.get("ai_structured_points"),
                    incoming=comment_points,
                )
                if merged_points:
                    item.extra["ai_structured_points"] = merged_points
                    self._sync_item_from_structured_points(item, merged_points)
            self.classifier.classify(item)
            self.source_profiler.classify(item)
            if comment_points:
                merged_points = item.extra.get("ai_structured_points")
                if isinstance(merged_points, list) and merged_points:
                    # Keep comment-mining priorities after classifier rule refresh.
                    self._sync_item_from_structured_points(item, merged_points)
            self.repository.update_analysis_fields(int(row["id"]), item)
            item_results.append(
                {
                    "id": int(row["id"]),
                    "title": row["title"],
                    "url": row["url"],
                    "ok": result.ok,
                    "error": extra["video_analysis"].get("error", ""),
                    "output_file": result.output_file,
                }
            )

        return {
            "ok": True,
            "processed": len(candidates) + duplicate_resolved,
            "succeeded": succeeded + duplicate_resolved,
            "failed": failed,
            "duplicate_resolved": duplicate_resolved,
            "skipped_duplicates": skipped_duplicates,
            "items": item_results,
        }

    def _analyze_youtube_comments(self, item: FeedbackItem) -> dict[str, Any]:
        if not self.comment_miner.is_enabled_for_url(item.url):
            return {}
        context = "\n".join(
            [
                item.title or "",
                item.summary or "",
                truncate(clean_content_text(item.content or ""), 600),
            ]
        )
        return self.comment_miner.analyze_video(item.url, context_text=context)

    def _merge_structured_points(self, existing: Any, incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
        merged: list[dict[str, Any]] = []
        seen: set[str] = set()

        def append_point(point: dict[str, Any]) -> None:
            text = clean_content_text(str(point.get("text", "")))
            original = clean_content_text(str(point.get("original_text", "")))
            if not text:
                return
            key = f"{text.lower()}||{original.lower()}"
            if key in seen:
                return
            seen.add(key)
            merged.append(point)

        if isinstance(existing, list):
            for point in existing:
                if isinstance(point, dict):
                    append_point(point)
        for point in incoming:
            if isinstance(point, dict):
                append_point(point)

        merged.sort(key=self._structured_point_rank, reverse=True)
        return merged[: self.config.video_processing.comment_max_points]

    @staticmethod
    def _structured_point_rank(point: dict[str, Any]) -> tuple[int, int, int]:
        secondary_tags = [clean_content_text(str(tag)) for tag in (point.get("secondary_tags") or [])]
        priority = 2
        for tag in secondary_tags:
            matched = re.search(r"priority\s*:\s*p?([1-4])", tag, re.I)
            if matched:
                priority = int(matched.group(1))
                break
        severity = clean_content_text(str(point.get("severity", ""))).lower()
        severity_rank = {"high": 3, "medium": 2, "low": 1}.get(severity, 1)
        sentiment = clean_content_text(str(point.get("sentiment", ""))).lower()
        sentiment_rank = {"negative": 3, "neutral": 2, "positive": 1}.get(sentiment, 2)
        return priority, severity_rank, sentiment_rank

    def _sync_item_from_structured_points(self, item: FeedbackItem, points: list[dict[str, Any]]) -> None:
        if not points:
            return
        positives: list[str] = []
        neutrals: list[str] = []
        negatives: list[str] = []
        secondary_tags: list[str] = []
        product_tags: list[str] = list(item.product_tags)
        primary_counts: dict[str, int] = {}
        sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
        severity_best = "low"

        for point in points:
            text = truncate(clean_content_text(str(point.get("text", ""))), 120)
            if not text:
                continue
            sentiment = clean_content_text(str(point.get("sentiment", ""))).lower()
            if sentiment not in {"positive", "neutral", "negative"}:
                sentiment = "neutral"
            sentiment_counts[sentiment] += 1
            if sentiment == "positive":
                positives.append(text)
            elif sentiment == "negative":
                negatives.append(text)
            else:
                neutrals.append(text)

            primary_tag = clean_content_text(str(point.get("primary_tag", "")))
            if primary_tag:
                primary_counts[primary_tag] = primary_counts.get(primary_tag, 0) + 1

            for tag in point.get("secondary_tags", []) or []:
                clean = clean_content_text(str(tag))
                if clean and clean not in secondary_tags:
                    secondary_tags.append(clean)

            for tag in point.get("product_tags", []) or []:
                clean = clean_content_text(str(tag))
                if clean and clean not in product_tags:
                    product_tags.append(clean)

            severity = clean_content_text(str(point.get("severity", ""))).lower()
            if severity == "high":
                severity_best = "high"
            elif severity == "medium" and severity_best != "high":
                severity_best = "medium"

        item.ai_positive_points = positives[:6]
        item.ai_neutral_points = neutrals[:6]
        item.ai_negative_points = negatives[:6]

        if secondary_tags:
            item.domain_subtags = secondary_tags[:8]
        if product_tags:
            item.product_tags = product_tags[:6]

        if primary_counts:
            best_primary = max(primary_counts.items(), key=lambda pair: pair[1])[0]
            if best_primary:
                item.domain_tag = best_primary
                item.camera_category = best_primary

        if sentiment_counts["negative"] >= max(sentiment_counts["positive"], sentiment_counts["neutral"]) and sentiment_counts["negative"] > 0:
            item.sentiment = "negative"
        elif sentiment_counts["positive"] > max(sentiment_counts["negative"], sentiment_counts["neutral"]):
            item.sentiment = "positive"
        else:
            item.sentiment = "neutral"

        item.severity = severity_best

    def _pick_candidates(
        self,
        rows: list[Any],
        limit: int | None,
        only_unprocessed: bool,
        force_ids: set[int] | None = None,
        processed_signature_map: dict[str, Any] | None = None,
    ) -> tuple[list[Any], list[tuple[Any, Any]], int]:
        force_ids = force_ids or set()
        processed_signature_map = processed_signature_map or {}
        max_items = max(1, int(limit or self.config.video_processing.max_items_per_run))
        candidates: list[Any] = []
        duplicate_rows: list[tuple[Any, Any]] = []
        skipped_duplicates = 0
        processed_signatures = set(processed_signature_map)
        selected_signatures: set[str] = set()
        ordered_rows = sorted(rows, key=lambda item: str(item["published_at"] or ""), reverse=True)
        for row in ordered_rows:
            if row is None:
                continue
            row_id = int(row["id"])
            url = str(row["url"] or "").strip()
            if not url:
                continue
            extra = load_json(row["extra_json"], {})
            video_analysis = extra.get("video_analysis", {}) if isinstance(extra, dict) else {}
            analysis_status = str(video_analysis.get("status", "")).strip().lower() if isinstance(video_analysis, dict) else ""
            is_tracked_video = int(row["video_candidate"] or 0) == 1 or is_video_url(url)
            if not is_tracked_video:
                continue

            if only_unprocessed and row_id not in force_ids:
                if analysis_status == "ok":
                    continue

            signatures = self._row_signatures(row, extra=extra)
            if row_id not in force_ids and signatures:
                matching_processed = signatures & processed_signatures
                if matching_processed:
                    skipped_duplicates += 1
                    source_signature = next(iter(matching_processed))
                    source_row = processed_signature_map.get(source_signature)
                    if source_row is not None:
                        duplicate_rows.append((row, source_row))
                    continue
                if signatures & selected_signatures:
                    skipped_duplicates += 1
                    continue

            candidates.append(row)
            selected_signatures.update(signatures)
            if len(candidates) >= max_items:
                break
        return candidates, duplicate_rows, skipped_duplicates

    def _process_single(self, url: str) -> VideoProcessResult:
        cfg = self.config.video_processing
        python_path = Path(cfg.videosummary_python).expanduser()
        script_path = Path(cfg.videosummary_script).expanduser()
        output_dir = script_path.parent / "output"
        before_files = {str(path) for path in output_dir.glob(f"*_{cfg.prompt_name}.md")}

        command = [
            str(python_path),
            str(script_path),
            "--url",
            url,
            "--prompts",
            cfg.prompt_name,
            "--model-size",
            cfg.model_size,
        ]

        try:
            completed = subprocess.run(
                command,
                cwd=str(script_path.parent),
                capture_output=True,
                text=True,
                timeout=max(120, int(cfg.timeout_seconds)),
                check=False,
            )
        except Exception as exc:  # noqa: BLE001
            return VideoProcessResult(ok=False, error=f"run_failed: {exc}")

        output_text = "\n".join([completed.stdout or "", completed.stderr or ""])
        if completed.returncode != 0:
            return VideoProcessResult(
                ok=False,
                error=f"transcribe_exit_{completed.returncode}: {self._short_error(output_text)}",
            )

        output_file = self._extract_output_file(output_text, cfg.prompt_name, script_path.parent, before_files)
        if not output_file:
            return VideoProcessResult(ok=False, error="output_file_not_found")
        return VideoProcessResult(ok=True, output_file=output_file)

    @staticmethod
    def _short_error(text: str) -> str:
        cleaned = clean_content_text(text)
        return cleaned[:200] if cleaned else "unknown_error"

    @staticmethod
    def _extract_output_file(text: str, prompt_name: str, workdir: Path, before_files: set[str]) -> str:
        pattern = re.compile(rf"优化版本 \({re.escape(prompt_name)}\):\s*(.+)")
        for raw_line in text.splitlines():
            line = raw_line.strip()
            matched = pattern.search(line)
            if not matched:
                continue
            candidate = Path(matched.group(1).strip()).expanduser()
            if not candidate.is_absolute():
                candidate = (workdir / candidate).resolve()
            if candidate.exists():
                return str(candidate)

        output_dir = workdir / "output"
        if output_dir.exists():
            candidates = sorted(output_dir.glob(f"*_{prompt_name}.md"), key=lambda p: p.stat().st_mtime, reverse=True)
            for path in candidates:
                if str(path) not in before_files:
                    return str(path.resolve())
            if candidates:
                return str(candidates[0].resolve())
        return ""

    @staticmethod
    def _parse_structured_output(path: str) -> dict[str, Any]:
        text = Path(path).read_text(encoding="utf-8")
        return extract_json_object(text)

    @staticmethod
    def _row_to_item(row: Any) -> FeedbackItem:
        return FeedbackItem(
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

    def _load_processed_video_signature_map(self, exclude_ids: set[int]) -> dict[str, Any]:
        rows = self.repository.fetch_rows_for_backfill(target_date=None, limit=6000)
        pool: dict[str, Any] = {}
        for row in rows:
            row_id = int(row["id"])
            if row_id in exclude_ids:
                continue
            extra = load_json(row["extra_json"], {})
            video_analysis = extra.get("video_analysis", {}) if isinstance(extra, dict) else {}
            status = str(video_analysis.get("status", "")).strip().lower() if isinstance(video_analysis, dict) else ""
            if status != "ok":
                continue
            for signature in self._row_signatures(row, extra=extra):
                pool.setdefault(signature, row)
        return pool

    def _apply_duplicate_analysis(self, row: Any, source_row: Any) -> None:
        item = self._row_to_item(row)
        source_item = self._row_to_item(source_row)
        self._ensure_video_signatures(item)
        self._ensure_video_signatures(source_item)

        item.author = source_item.author or item.author
        item.content = source_item.content
        item.summary = source_item.summary
        item.camera_category = source_item.camera_category
        item.sentiment = source_item.sentiment
        item.severity = source_item.severity
        item.source_actor_type = source_item.source_actor_type
        item.source_actor_reason = source_item.source_actor_reason
        item.domain_tag = source_item.domain_tag
        item.domain_subtags = list(source_item.domain_subtags)
        item.sentiment_reason = source_item.sentiment_reason
        item.ai_positive_points = list(source_item.ai_positive_points)
        item.ai_neutral_points = list(source_item.ai_neutral_points)
        item.ai_negative_points = list(source_item.ai_negative_points)
        item.product_tags = list(source_item.product_tags)
        item.language = source_item.language or item.language
        item.video_candidate = False

        source_extra = dict(source_item.extra)
        target_extra = dict(item.extra)
        source_video_analysis = source_extra.get("video_analysis", {}) if isinstance(source_extra.get("video_analysis"), dict) else {}
        target_extra["video_analysis"] = {
            "processed_at": datetime.now(tz=timezone.utc).isoformat(),
            "status": "duplicate",
            "output_file": str(source_video_analysis.get("output_file", "")).strip(),
            "error": "",
            "duplicate_of_row_id": int(source_row["id"]),
        }
        if "ai_structured_points" in source_extra:
            target_extra["ai_structured_points"] = source_extra["ai_structured_points"]
        if "youtube_comment_mining" in source_extra:
            target_extra["youtube_comment_mining"] = source_extra["youtube_comment_mining"]
        if source_item.extra.get("video_signatures"):
            target_extra["video_signatures"] = source_item.extra["video_signatures"]
        item.extra = target_extra

        structured_points = item.extra.get("ai_structured_points")
        if isinstance(structured_points, list) and structured_points:
            self._sync_item_from_structured_points(item, structured_points)
        self.classifier.classify(item)
        self.source_profiler.classify(item)
        if isinstance(structured_points, list) and structured_points:
            self._sync_item_from_structured_points(item, structured_points)
        self.repository.update_analysis_fields(int(row["id"]), item)

    def _row_signatures(self, row: Any, extra: dict | None = None) -> set[str]:
        parsed_extra = extra if isinstance(extra, dict) else load_json(row["extra_json"], {})
        existing = parse_video_signatures(parsed_extra.get("video_signatures")) if isinstance(parsed_extra, dict) else []
        if existing:
            return set(existing)
        computed = extract_video_signatures(
            url=str(row["url"] or ""),
            title=str(row["title"] or ""),
            author=str(row["author"] or ""),
            source_item_id=str(row["source_item_id"] or ""),
        )
        return set(computed)

    @staticmethod
    def _ensure_video_signatures(item: FeedbackItem) -> None:
        if not item.video_candidate and not is_video_url(item.url):
            return
        signatures = extract_video_signatures(
            url=item.url,
            title=item.title,
            author=item.author or "",
            source_item_id=item.source_item_id or "",
        )
        if signatures:
            item.extra["video_signatures"] = signatures
