from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from .utils import expand_env

DEFAULT_CAMERA_CATEGORIES: dict[str, list[str]] = {
    "画质": ["画质", "photo quality", "blurry", "模糊", "锐度", "sharpness", "细节", "噪点", "noise"],
    "对焦": ["对焦", "focus", "autofocus", "af", "跑焦", "无法对焦", "focus hunting"],
    "曝光": ["曝光", "exposure", "过曝", "欠曝", "曝光补偿", "highlights", "shadow"],
    "夜景": ["夜景", "night mode", "low light", "暗光", "夜拍"],
    "人像": ["人像", "portrait", "bokeh", "背景虚化", "肤色"],
    "视频": ["视频", "video", "录像", "fps", "frame drop", "录制"],
    "防抖": ["防抖", "stabilization", "eis", "ois", "抖动"],
    "性能发热": ["发热", "heating", "卡顿", "lag", "stutter", "性能", "crash", "闪退"],
    "功能建议": ["建议", "希望", "feature", "wishlist", "please add", "should support"],
}


@dataclass(slots=True)
class DedupeConfig:
    jaccard_threshold: float = 0.9
    lookback_days: int = 7


@dataclass(slots=True)
class LocalAIConfig:
    enabled: bool = False
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    temperature: float = 0.1
    max_tokens: int = 1800
    timeout_seconds: int = 90
    retries: int = 1
    prompt_path: str = ""


@dataclass(slots=True)
class VideoProcessingConfig:
    enabled: bool = False
    videosummary_python: str = "/path/to/videosummary/venv/bin/python"
    videosummary_script: str = "/path/to/videosummary/transcribe.py"
    prompt_name: str = "camera_feedback"
    model_size: str = "tiny"
    timeout_seconds: int = 1800
    max_items_per_run: int = 8
    nightly_enabled: bool = False
    nightly_hour: int = 2
    nightly_minute: int = 30
    nightly_timezone: str = "Asia/Shanghai"
    comment_mining_enabled: bool = True
    comment_newest_limit: int = 500
    comment_top_limit: int = 120
    comment_timeout_seconds: int = 180
    comment_ai_batch_size: int = 20
    comment_ai_max_candidates: int = 180
    comment_ai_max_p3: int = 100
    comment_ai_max_p2_negative: int = 50
    comment_max_points: int = 40


@dataclass(slots=True)
class EmailSummaryConfig:
    enabled: bool = False
    auto_send_after_run: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    from_addr: str = ""
    to_addrs: list[str] = field(default_factory=list)
    use_tls: bool = True
    use_ssl: bool = False
    subject_prefix: str = "[Media Pulse]"


@dataclass(slots=True)
class AppConfig:
    database_path: str
    report_dir: str
    lookback_hours: int
    camera_filter_mode: str = "strict"
    product_keywords: list[str] = field(default_factory=list)
    camera_keywords: list[str] = field(default_factory=list)
    camera_categories: dict[str, list[str]] = field(default_factory=lambda: dict(DEFAULT_CAMERA_CATEGORIES))
    schedule: dict[str, Any] = field(default_factory=dict)
    sources: dict[str, Any] = field(default_factory=dict)
    competitor_video: dict[str, Any] = field(default_factory=dict)
    lark: dict[str, Any] = field(default_factory=dict)
    dedupe: DedupeConfig = field(default_factory=DedupeConfig)
    local_ai: LocalAIConfig = field(default_factory=LocalAIConfig)
    video_processing: VideoProcessingConfig = field(default_factory=VideoProcessingConfig)
    email_summary: EmailSummaryConfig = field(default_factory=EmailSummaryConfig)

    @property
    def lark_enabled(self) -> bool:
        return bool(self.lark.get("enabled"))



def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    _load_env_candidates(config_path)
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    data = expand_env(raw)
    dedupe_raw = dict(data.get("dedupe", {}))
    local_ai_raw = dict(data.get("local_ai", {}))
    video_processing_raw = dict(data.get("video_processing", {}))
    email_summary_raw = dict(data.get("email_summary", {}))
    camera_filter_mode = str(data.get("camera_filter_mode", "strict")).lower()
    if camera_filter_mode not in {"strict", "review", "off"}:
        camera_filter_mode = "strict"

    return AppConfig(
        database_path=str(data.get("database_path", "./data/feedback.db")),
        report_dir=str(data.get("report_dir", "./reports")),
        lookback_hours=int(data.get("lookback_hours", 48)),
        camera_filter_mode=camera_filter_mode,
        product_keywords=list(data.get("product_keywords", [])),
        camera_keywords=list(data.get("camera_keywords", [])),
        camera_categories=dict(data.get("camera_categories", DEFAULT_CAMERA_CATEGORIES)),
        schedule=dict(data.get("schedule", {})),
        sources=dict(data.get("sources", {})),
        competitor_video=dict(data.get("competitor_video", {})),
        lark=dict(data.get("lark", {})),
        dedupe=DedupeConfig(
            jaccard_threshold=float(dedupe_raw.get("jaccard_threshold", 0.9)),
            lookback_days=int(dedupe_raw.get("lookback_days", 7)),
        ),
        local_ai=LocalAIConfig(
            enabled=bool(local_ai_raw.get("enabled", False)),
            base_url=str(local_ai_raw.get("base_url", "")).strip(),
            api_key=str(local_ai_raw.get("api_key", "")).strip(),
            model=str(local_ai_raw.get("model", "")).strip(),
            temperature=float(local_ai_raw.get("temperature", 0.1)),
            max_tokens=int(local_ai_raw.get("max_tokens", 1800)),
            timeout_seconds=int(local_ai_raw.get("timeout_seconds", 90)),
            retries=int(local_ai_raw.get("retries", 1)),
            prompt_path=str(local_ai_raw.get("prompt_path", "")).strip(),
        ),
        video_processing=VideoProcessingConfig(
            enabled=bool(video_processing_raw.get("enabled", False)),
            videosummary_python=str(
                video_processing_raw.get(
                    "videosummary_python",
                    "/path/to/videosummary/venv/bin/python",
                )
            ).strip(),
            videosummary_script=str(
                video_processing_raw.get(
                    "videosummary_script",
                    "/path/to/videosummary/transcribe.py",
                )
            ).strip(),
            prompt_name=str(video_processing_raw.get("prompt_name", "camera_feedback")).strip() or "camera_feedback",
            model_size=str(video_processing_raw.get("model_size", "tiny")).strip() or "tiny",
            timeout_seconds=int(video_processing_raw.get("timeout_seconds", 1800)),
            max_items_per_run=max(1, int(video_processing_raw.get("max_items_per_run", 8))),
            nightly_enabled=bool(video_processing_raw.get("nightly_enabled", False)),
            nightly_hour=max(0, min(23, int(video_processing_raw.get("nightly_hour", 2)))),
            nightly_minute=max(0, min(59, int(video_processing_raw.get("nightly_minute", 30)))),
            nightly_timezone=str(video_processing_raw.get("nightly_timezone", "Asia/Shanghai")).strip()
            or "Asia/Shanghai",
            comment_mining_enabled=bool(video_processing_raw.get("comment_mining_enabled", True)),
            comment_newest_limit=max(20, min(3000, int(video_processing_raw.get("comment_newest_limit", 500)))),
            comment_top_limit=max(20, min(1000, int(video_processing_raw.get("comment_top_limit", 120)))),
            comment_timeout_seconds=max(20, int(video_processing_raw.get("comment_timeout_seconds", 180))),
            comment_ai_batch_size=max(5, min(40, int(video_processing_raw.get("comment_ai_batch_size", 20)))),
            comment_ai_max_candidates=max(20, min(500, int(video_processing_raw.get("comment_ai_max_candidates", 180)))),
            comment_ai_max_p3=max(0, min(300, int(video_processing_raw.get("comment_ai_max_p3", 100)))),
            comment_ai_max_p2_negative=max(0, min(300, int(video_processing_raw.get("comment_ai_max_p2_negative", 50)))),
            comment_max_points=max(5, min(80, int(video_processing_raw.get("comment_max_points", 40)))),
        ),
        email_summary=EmailSummaryConfig(
            enabled=bool(email_summary_raw.get("enabled", False)),
            auto_send_after_run=bool(email_summary_raw.get("auto_send_after_run", False)),
            smtp_host=str(email_summary_raw.get("smtp_host", "")).strip(),
            smtp_port=max(1, int(email_summary_raw.get("smtp_port", 587))),
            smtp_username=str(email_summary_raw.get("smtp_username", "")).strip(),
            smtp_password=str(email_summary_raw.get("smtp_password", "")).strip(),
            from_addr=str(email_summary_raw.get("from_addr", "")).strip(),
            to_addrs=[str(value).strip() for value in list(email_summary_raw.get("to_addrs", [])) if str(value).strip()],
            use_tls=bool(email_summary_raw.get("use_tls", True)),
            use_ssl=bool(email_summary_raw.get("use_ssl", False)),
            subject_prefix=str(email_summary_raw.get("subject_prefix", "[Media Pulse]")).strip()
            or "[Media Pulse]",
        ),
    )


def _load_env_candidates(config_path: Path) -> None:
    candidates = [config_path.parent / ".env", Path.cwd() / ".env"]
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate.expanduser().resolve())
        if normalized in seen:
            continue
        seen.add(normalized)
        _load_env_file(Path(normalized))


def _load_env_file(path: Path) -> None:
    if not path.exists() or not path.is_file():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return

    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and ((value[0] == value[-1] == '"') or (value[0] == value[-1] == "'")):
            value = value[1:-1]
        os.environ.setdefault(key, value)
