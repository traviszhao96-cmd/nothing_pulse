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
from .utils import clean_content_text, is_video_url, load_json, parse_datetime, since_hours
from .video_identity import extract_video_signatures, parse_video_signatures


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
        candidates, skipped_duplicates = self._pick_candidates(
            rows,
            limit=limit,
            only_unprocessed=only_unprocessed,
            force_ids=force_ids,
        )
        succeeded = 0
        failed = 0
        item_results: list[dict[str, Any]] = []

        for row in candidates:
            item = self._row_to_item(row)
            self._ensure_video_signatures(item)
            result = self._process_single(item.url)
            extra = dict(item.extra)
            extra["video_analysis"] = {
                "processed_at": datetime.now(tz=timezone.utc).isoformat(),
                "status": "ok" if result.ok else "failed",
                "output_file": result.output_file,
                "error": result.error,
            }

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
            self.classifier.classify(item)
            self.source_profiler.classify(item)
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
            "processed": len(candidates),
            "succeeded": succeeded,
            "failed": failed,
            "skipped_duplicates": skipped_duplicates,
            "items": item_results,
        }

    def _pick_candidates(
        self,
        rows: list[Any],
        limit: int | None,
        only_unprocessed: bool,
        force_ids: set[int] | None = None,
    ) -> tuple[list[Any], int]:
        force_ids = force_ids or set()
        max_items = max(1, int(limit or self.config.video_processing.max_items_per_run))
        candidates: list[Any] = []
        skipped_duplicates = 0
        processed_signatures = self._load_processed_video_signatures(exclude_ids=force_ids) if only_unprocessed else set()
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
                if signatures & processed_signatures:
                    skipped_duplicates += 1
                    continue
                if signatures & selected_signatures:
                    skipped_duplicates += 1
                    continue

            candidates.append(row)
            selected_signatures.update(signatures)
            if len(candidates) >= max_items:
                break
        return candidates, skipped_duplicates

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

    def _load_processed_video_signatures(self, exclude_ids: set[int]) -> set[str]:
        rows = self.repository.fetch_rows_for_backfill(target_date=None, limit=6000)
        pool: set[str] = set()
        for row in rows:
            row_id = int(row["id"])
            if row_id in exclude_ids:
                continue
            extra = load_json(row["extra_json"], {})
            video_analysis = extra.get("video_analysis", {}) if isinstance(extra, dict) else {}
            status = str(video_analysis.get("status", "")).strip().lower() if isinstance(video_analysis, dict) else ""
            if status != "ok":
                continue
            pool.update(self._row_signatures(row, extra=extra))
        return pool

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
