from __future__ import annotations

from collections import Counter
from datetime import date, timedelta
from pathlib import Path

from .storage import FeedbackRepository
from .utils import ensure_dir, load_json


def generate_daily_report(repository: FeedbackRepository, target_date: date, report_dir: str) -> Path:
    rows = repository.fetch_by_report_date(target_date, camera_only=True)
    prev_rows = repository.fetch_by_report_date(target_date - timedelta(days=1), camera_only=True)

    category_counts = Counter(row["camera_category"] for row in rows)
    sentiment_counts = Counter(row["sentiment"] for row in rows)
    severity_counts = Counter(row["severity"] for row in rows)
    source_role_counts = Counter(row["source_actor_type"] or "unknown" for row in rows)
    domain_counts = Counter(row["domain_tag"] or "未分类" for row in rows)

    prev_category_counts = Counter(row["camera_category"] for row in prev_rows)

    keyword_counts: Counter[str] = Counter()
    positive_points: Counter[str] = Counter()
    neutral_points: Counter[str] = Counter()
    negative_points: Counter[str] = Counter()
    for row in rows:
        for keyword in load_json(row["camera_keyword_hits"], []):
            keyword_counts[keyword] += 1
        for point in load_json(row["ai_positive_points_json"], []):
            positive_points[str(point)] += 1
        for point in load_json(row["ai_neutral_points_json"], []):
            neutral_points[str(point)] += 1
        for point in load_json(row["ai_negative_points_json"], []):
            negative_points[str(point)] += 1

    high_risk_rows = [row for row in rows if row["severity"] == "high"]
    high_risk_rows = sorted(high_risk_rows, key=lambda item: item["published_at"], reverse=True)[:10]

    report_lines: list[str] = []
    report_lines.append(f"# Media Pulse 日报 - {target_date.isoformat()}")
    report_lines.append("")

    report_lines.append("## 1. 今日概览")
    report_lines.append(f"- 新增 camera 相关反馈: **{len(rows)}**")
    report_lines.append(f"- 高风险问题: **{severity_counts.get('high', 0)}**")
    report_lines.append(f"- 中风险问题: **{severity_counts.get('medium', 0)}**")
    report_lines.append(f"- 低风险问题: **{severity_counts.get('low', 0)}**")
    report_lines.append("")

    report_lines.append("## 2. 问题类型 Top")
    if category_counts:
        for name, count in category_counts.most_common(10):
            previous = prev_category_counts.get(name, 0)
            delta = count - previous
            trend = f"{delta:+d}"
            report_lines.append(f"- {name}: {count} (较昨日 {trend})")
    else:
        report_lines.append("- 今日无新增问题")
    report_lines.append("")

    report_lines.append("## 3. 情绪分布")
    if sentiment_counts:
        for name, count in sentiment_counts.most_common():
            report_lines.append(f"- {name}: {count}")
    else:
        report_lines.append("- 无数据")
    report_lines.append("")

    report_lines.append("## 4. 高频关键词")
    if keyword_counts:
        for keyword, count in keyword_counts.most_common(12):
            report_lines.append(f"- {keyword}: {count}")
    else:
        report_lines.append("- 无")
    report_lines.append("")

    report_lines.append("## 5. 来源身份分布")
    if source_role_counts:
        labels = {
            "real_user": "真实购买用户",
            "official_kol": "官方KOL/媒体",
            "core_koc": "核心KOC/自媒体",
            "unknown": "待确认",
        }
        for name, count in source_role_counts.most_common():
            report_lines.append(f"- {labels.get(name, name)}: {count}")
    else:
        report_lines.append("- 无")
    report_lines.append("")

    report_lines.append("## 6. 领域分布")
    if domain_counts:
        for name, count in domain_counts.most_common(10):
            report_lines.append(f"- {name}: {count}")
    else:
        report_lines.append("- 无")
    report_lines.append("")

    report_lines.append("## 7. AI 观点提炼")
    report_lines.append("- 好评要点:")
    if positive_points:
        for point, count in positive_points.most_common(5):
            report_lines.append(f"  - {point} ({count})")
    else:
        report_lines.append("  - 无")
    report_lines.append("- 中性要点:")
    if neutral_points:
        for point, count in neutral_points.most_common(5):
            report_lines.append(f"  - {point} ({count})")
    else:
        report_lines.append("  - 无")
    report_lines.append("- 差评要点:")
    if negative_points:
        for point, count in negative_points.most_common(8):
            report_lines.append(f"  - {point} ({count})")
    else:
        report_lines.append("  - 无")
    report_lines.append("")

    report_lines.append("## 8. 高风险案例")
    if high_risk_rows:
        for idx, row in enumerate(high_risk_rows, start=1):
            report_lines.append(
                f"{idx}. [{row['title']}]({row['url']}) | 类型: {row['camera_category']} | 平台: {row['source']}"
            )
    else:
        report_lines.append("- 今日无高风险案例")
    report_lines.append("")

    report_lines.append("## 9. 跟进建议")
    report_lines.append("- 先处理高风险且集中出现的问题类型，优先验证可复现路径。")
    report_lines.append("- 对 Top 问题类型追加机型和版本维度分析，确认是否集中在单机型。")
    report_lines.append("- 将高频关键词同步给客服和测试，统一回复口径并追踪闭环。")
    report_lines.append("")

    output_dir = ensure_dir(report_dir)
    output_path = output_dir / f"media-pulse-{target_date.isoformat()}.md"
    output_path.write_text("\n".join(report_lines), encoding="utf-8")
    return output_path
