from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any

from .utils import dump_json, ensure_dir, isoformat


class ProcessingRunLogger:
    def __init__(self, report_dir: str, command: str) -> None:
        now = datetime.now(tz=timezone.utc)
        safe_command = self._slugify(command)
        self.command = command
        self.run_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}-{safe_command}-pid{os.getpid()}"
        self.started_at = now
        self.finished_at: datetime | None = None
        base_dir = ensure_dir(Path(report_dir).expanduser() / "process-logs")
        self.path = (base_dir / f"{self.run_id}.jsonl").resolve()
        self.summary_path = (base_dir / f"{self.run_id}.summary.json").resolve()
        self._disabled = False
        self._event_count = 0

    @staticmethod
    def _slugify(value: str) -> str:
        cleaned = "".join(char.lower() if char.isalnum() else "-" for char in str(value or "").strip())
        cleaned = "-".join(part for part in cleaned.split("-") if part)
        return cleaned or "run"

    def start(self, **payload: Any) -> None:
        self.write("start", **payload)

    def item(self, step: str, status: str, **payload: Any) -> None:
        self.write("item", step=step, status=status, **payload)

    def note(self, step: str, **payload: Any) -> None:
        self.write("note", step=step, **payload)

    def finish(self, status: str, **payload: Any) -> None:
        self.finished_at = datetime.now(tz=timezone.utc)
        duration_seconds = round((self.finished_at - self.started_at).total_seconds(), 3)
        summary = {
            "run_id": self.run_id,
            "command": self.command,
            "status": status,
            "started_at": isoformat(self.started_at),
            "finished_at": isoformat(self.finished_at),
            "duration_seconds": duration_seconds,
            "event_count": self._event_count + 1,
        }
        summary.update(self._sanitize(payload))
        self.write("finish", **summary)
        self._write_summary(summary)

    def write(self, event: str, **payload: Any) -> None:
        if self._disabled:
            return
        record = {
            "ts": isoformat(datetime.now(tz=timezone.utc)),
            "run_id": self.run_id,
            "command": self.command,
            "event": str(event or "").strip() or "note",
        }
        record.update(self._sanitize(payload))
        try:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(dump_json(record))
                handle.write("\n")
            self._event_count += 1
        except OSError:
            self._disabled = True

    def _write_summary(self, payload: dict[str, Any]) -> None:
        if self._disabled:
            return
        try:
            self.summary_path.write_text(dump_json(payload), encoding="utf-8")
        except OSError:
            self._disabled = True

    @classmethod
    def no_op(cls) -> "ProcessingRunLogger":
        logger = cls.__new__(cls)
        logger.command = "noop"
        logger.run_id = "noop"
        logger.started_at = datetime.now(tz=timezone.utc)
        logger.finished_at = None
        logger.path = Path("/dev/null")
        logger.summary_path = Path("/dev/null")
        logger._disabled = True
        logger._event_count = 0
        return logger

    @staticmethod
    def _sanitize(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): ProcessingRunLogger._sanitize(item) for key, item in value.items()}
        if isinstance(value, list):
            return [ProcessingRunLogger._sanitize(item) for item in value]
        if isinstance(value, tuple):
            return [ProcessingRunLogger._sanitize(item) for item in value]
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, datetime):
            return isoformat(value)
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
