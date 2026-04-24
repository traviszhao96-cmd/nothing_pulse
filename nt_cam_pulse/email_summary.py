from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from email.message import EmailMessage
import json
from pathlib import Path
import smtplib
import time
from typing import Any

from .config import AppConfig
from .storage import FeedbackRepository
from .utils import is_video_url, load_json


@dataclass(slots=True)
class DailyEmailSummary:
    subject: str
    body: str
    metrics: dict[str, Any]
    run_log_path: str


@dataclass(slots=True)
class EmailConnectionStatus:
    smtp_host: str
    smtp_port: int
    use_tls: bool
    use_ssl: bool
    smtp_username_configured: bool
    from_addr: str
    recipient_count: int


def send_daily_summary_email(
    config: AppConfig,
    repository: FeedbackRepository,
    target_date: date,
    run_log_path: str | None = None,
) -> DailyEmailSummary:
    summary = build_daily_summary_email(
        config=config,
        repository=repository,
        target_date=target_date,
        run_log_path=run_log_path,
    )
    send_email_message(config, summary.subject, summary.body)
    return summary


def check_email_connection(config: AppConfig, *, allow_disabled: bool = True) -> EmailConnectionStatus:
    email_cfg = _resolve_email_config(config, allow_disabled=allow_disabled)
    from_addr = email_cfg.from_addr or email_cfg.smtp_username
    with _open_smtp_connection(email_cfg):
        pass
    return EmailConnectionStatus(
        smtp_host=email_cfg.smtp_host,
        smtp_port=email_cfg.smtp_port,
        use_tls=bool(email_cfg.use_tls),
        use_ssl=bool(email_cfg.use_ssl),
        smtp_username_configured=bool(email_cfg.smtp_username),
        from_addr=from_addr,
        recipient_count=len(email_cfg.to_addrs),
    )


def build_daily_summary_email(
    config: AppConfig,
    repository: FeedbackRepository,
    target_date: date,
    run_log_path: str | None = None,
) -> DailyEmailSummary:
    rows = repository.fetch_by_report_date(target_date, camera_only=True)
    report_path = Path(config.report_dir).expanduser() / f"camera-pulse-{target_date.isoformat()}.md"

    video_rows = [row for row in rows if _is_video_row(row)]
    post_rows = [row for row in rows if not _is_video_row(row)]
    positive_total = sum(1 for row in rows if str(row["sentiment"] or "").lower() == "positive")
    neutral_total = sum(1 for row in rows if str(row["sentiment"] or "").lower() == "neutral")
    negative_total = sum(1 for row in rows if str(row["sentiment"] or "").lower() == "negative")

    video_ok = 0
    video_duplicate = 0
    comment_rows = 0
    comment_merged_total = 0
    comment_points_total = 0
    source_counts: Counter[str] = Counter(str(row["source"] or "") for row in rows)

    for row in video_rows:
        extra = load_json(row["extra_json"], {})
        video_analysis = extra.get("video_analysis", {}) if isinstance(extra, dict) else {}
        video_status = str(video_analysis.get("status", "")).strip().lower() if isinstance(video_analysis, dict) else ""
        if video_status == "ok":
            video_ok += 1
        elif video_status == "duplicate":
            video_duplicate += 1

        comment_meta = extra.get("youtube_comment_mining", {}) if isinstance(extra, dict) else {}
        comment_status = str(comment_meta.get("status", "")).strip().lower() if isinstance(comment_meta, dict) else ""
        if video_status == "ok" and comment_status == "ok":
            comment_rows += 1
            comment_merged_total += _safe_int(comment_meta.get("merged_total"))
            comment_points_total += _safe_int(comment_meta.get("points"))

    lark_pending = repository.count_lark_pending(target_date=target_date, only_new=False)
    collector_stats, run_log_resolved = _resolve_run_collectors(
        report_dir=config.report_dir,
        target_date=target_date,
        run_log_path=run_log_path,
    )
    if not collector_stats:
        collector_stats = [
            {"collector": name, "status": "configured", "fetched": 0}
            for name, source_cfg in sorted(config.sources.items())
            if isinstance(source_cfg, dict) and source_cfg.get("enabled")
        ]

    subject_prefix = str(config.email_summary.subject_prefix or "[Media Pulse]").strip()
    subject = f"{subject_prefix} 每日处理汇报 {target_date.isoformat()}"
    collector_lines = [
        _format_collector_line(entry)
        for entry in collector_stats
    ]
    source_lines = [f"- {name}: {count}" for name, count in source_counts.most_common()]
    lark_status = "成功" if lark_pending == 0 else f"未完成（待同步 {lark_pending} 条）"
    report_path_text = str(report_path.resolve()) if report_path.exists() else "未生成日报文件"
    run_log_text = run_log_resolved or "未找到对应 run 日志"

    body_lines = [
        f"Media Pulse 每日处理汇报 - {target_date.isoformat()}",
        "",
        "1. 搜索平台",
        *(collector_lines or ["- 未找到本次 run 的平台搜索记录"]),
        "",
        "2. 今日数据概览",
        f"- 反馈总数（camera 相关）: {len(rows)}",
        f"- 视频数: {len(video_rows)}",
        f"- 帖子数: {len(post_rows)}",
        f"- 评论区抓取内容数: {comment_merged_total}",
        f"- 评论区提炼观点数: {comment_points_total}",
        "",
        "3. 视频处理",
        f"- 成功分析视频: {video_ok}",
        f"- 复用重复视频结果: {video_duplicate}",
        f"- 含评论区挖掘的视频数: {comment_rows}",
        "",
        "4. 反馈情绪",
        f"- 正向: {positive_total}",
        f"- 中性: {neutral_total}",
        f"- 负向: {negative_total}",
        "",
        "5. 数据来源分布",
        *(source_lines or ["- 无"]),
        "",
        "6. Lark 同步",
        f"- 状态: {lark_status}",
        "",
        "7. 文件",
        f"- 日报: {report_path_text}",
        f"- 本次 run 日志: {run_log_text}",
    ]

    metrics = {
        "target_date": target_date.isoformat(),
        "collector_stats": collector_stats,
        "feedback_total": len(rows),
        "video_total": len(video_rows),
        "post_total": len(post_rows),
        "comment_merged_total": comment_merged_total,
        "comment_points_total": comment_points_total,
        "video_ok": video_ok,
        "video_duplicate": video_duplicate,
        "comment_rows": comment_rows,
        "positive_total": positive_total,
        "neutral_total": neutral_total,
        "negative_total": negative_total,
        "source_counts": dict(source_counts),
        "lark_pending": lark_pending,
        "report_path": report_path_text,
        "run_log_path": run_log_resolved,
    }
    return DailyEmailSummary(
        subject=subject,
        body="\n".join(body_lines),
        metrics=metrics,
        run_log_path=run_log_resolved or "",
    )


def send_email_message(
    config: AppConfig,
    subject: str,
    body: str,
    html_body: str | None = None,
    to_addrs: list[str] | None = None,
    allow_disabled: bool = False,
) -> None:
    email_cfg = _resolve_email_config(config, allow_disabled=allow_disabled)
    resolved_to_addrs = [str(value).strip() for value in (to_addrs or email_cfg.to_addrs) if str(value).strip()]
    if not resolved_to_addrs:
        raise RuntimeError("email_summary_missing_to_addrs")
    from_addr = email_cfg.from_addr or email_cfg.smtp_username
    if not from_addr:
        raise RuntimeError("email_summary_missing_from_addr")

    last_error: Exception | None = None
    total = len(resolved_to_addrs)
    for index, recipient in enumerate(resolved_to_addrs, start=1):
        last_error = None
        for attempt in range(1, 4):
            try:
                with _open_smtp_connection(email_cfg) as server:
                    message = _build_email_message(
                        subject=subject,
                        body=body,
                        html_body=html_body,
                        from_addr=from_addr,
                        to_addr=recipient,
                    )
                    server.send_message(message)
                last_error = None
                break
            except (smtplib.SMTPServerDisconnected, smtplib.SMTPDataError, OSError) as exc:
                last_error = exc
                if attempt >= 3:
                    break
                time.sleep(min(8.0, float(attempt * 2)))
        if last_error:
            raise last_error
        if index < total:
            time.sleep(2.0)


def _resolve_email_config(config: AppConfig, *, allow_disabled: bool) -> Any:
    email_cfg = config.email_summary
    if (not allow_disabled) and (not email_cfg.enabled):
        raise RuntimeError("email_summary_disabled")
    if not email_cfg.smtp_host:
        raise RuntimeError("email_summary_missing_smtp_host")
    return email_cfg


def _build_email_message(
    *,
    subject: str,
    body: str,
    html_body: str | None,
    from_addr: str,
    to_addr: str,
) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = from_addr
    message["To"] = to_addr
    message.set_content(body)
    if html_body:
        message.add_alternative(html_body, subtype="html")
    return message


@contextmanager
def _open_smtp_connection(email_cfg: Any):
    if email_cfg.use_ssl:
        with smtplib.SMTP_SSL(email_cfg.smtp_host, email_cfg.smtp_port, timeout=30) as server:
            server.ehlo()
            if email_cfg.smtp_username:
                server.login(email_cfg.smtp_username, email_cfg.smtp_password)
            yield server
        return

    with smtplib.SMTP(email_cfg.smtp_host, email_cfg.smtp_port, timeout=30) as server:
        server.ehlo()
        if email_cfg.use_tls:
            server.starttls()
            server.ehlo()
        if email_cfg.smtp_username:
            server.login(email_cfg.smtp_username, email_cfg.smtp_password)
        yield server


def _resolve_run_collectors(
    report_dir: str,
    target_date: date,
    run_log_path: str | None,
) -> tuple[list[dict[str, Any]], str]:
    candidate = Path(run_log_path).expanduser().resolve() if run_log_path else _find_latest_run_log(report_dir, target_date)
    if not candidate or not candidate.exists():
        return [], ""

    collectors: list[dict[str, Any]] = []
    try:
        for raw_line in candidate.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if payload.get("event") != "note":
                continue
            if payload.get("step") != "collector":
                continue
            collectors.append(
                {
                    "collector": str(payload.get("collector") or ""),
                    "status": str(payload.get("status") or ""),
                    "fetched": _safe_int(payload.get("fetched")),
                    "error": str(payload.get("error") or ""),
                }
            )
    except (OSError, json.JSONDecodeError):
        return [], str(candidate)
    return collectors, str(candidate)


def _find_latest_run_log(report_dir: str, target_date: date) -> Path | None:
    log_dir = Path(report_dir).expanduser() / "process-logs"
    if not log_dir.exists():
        return None
    candidates = sorted(log_dir.glob("*-run-*.jsonl"), key=lambda path: path.stat().st_mtime, reverse=True)
    fallback: Path | None = None
    for path in candidates:
        if fallback is None:
            fallback = path
        matched_date = _extract_log_report_date(path)
        if matched_date == target_date.isoformat():
            return path
    return fallback


def _extract_log_report_date(path: Path) -> str:
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if payload.get("step") == "report" and payload.get("report_date"):
                return str(payload.get("report_date"))
            if payload.get("event") == "start" and payload.get("report_date"):
                return str(payload.get("report_date"))
        return ""
    except (OSError, json.JSONDecodeError):
        return ""


def _format_collector_line(entry: dict[str, Any]) -> str:
    collector = str(entry.get("collector") or "unknown")
    status = str(entry.get("status") or "unknown")
    fetched = _safe_int(entry.get("fetched"))
    error = str(entry.get("error") or "").strip()
    if error:
        return f"- {collector}: {status}, fetched={fetched}, error={error}"
    return f"- {collector}: {status}, fetched={fetched}"


def _is_video_row(row: Any) -> bool:
    source = str(row["source"] or "").strip().lower()
    return source in {"youtube_yt_dlp", "youtube_manual", "youtube"} or is_video_url(str(row["url"] or ""))


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0
