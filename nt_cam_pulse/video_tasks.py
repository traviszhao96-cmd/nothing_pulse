from __future__ import annotations

from datetime import date
from pathlib import Path

from .storage import FeedbackRepository
from .utils import ensure_dir


def export_video_tasks(repository: FeedbackRepository, target_date: date, output_dir: str) -> Path:
    rows = repository.fetch_video_candidates(target_date)
    lines: list[str] = []
    lines.append(f"# Video Tasks - {target_date.isoformat()}")
    lines.append("")
    lines.append(
        "建议使用 videosummary 执行：`python /path/to/videosummary/transcribe.py --url <视频链接> --prompts camera_feedback`"
    )
    lines.append("")

    if not rows:
        lines.append("- 当日未识别到待补视频转写链接。")
    else:
        seen: set[str] = set()
        idx = 1
        for row in rows:
            url = str(row["url"] or "").strip()
            if not url or url in seen:
                continue
            seen.add(url)
            lines.append(
                f"{idx}. [{row['title']}]({url}) | 来源: {row['source_section'] or row['source']} | 领域: {row['domain_tag'] or '未分类'}"
            )
            idx += 1

    directory = ensure_dir(output_dir)
    output_path = Path(directory) / f"video-tasks-{target_date.isoformat()}.md"
    output_path.write_text("\n".join(lines), encoding="utf-8")
    return output_path
