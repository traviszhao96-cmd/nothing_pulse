from __future__ import annotations

import sqlite3
import threading
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .models import DailyStats, FeedbackItem
from .utils import build_fingerprint, dump_json, ensure_parent, isoformat, load_json


class FeedbackRepository:
    def __init__(self, database_path: str) -> None:
        self.database_path = str(Path(database_path).expanduser().resolve())
        ensure_parent(self.database_path)
        self._lock = threading.RLock()
        self.connection = sqlite3.connect(
            self.database_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self.connection.row_factory = sqlite3.Row
        # Improve concurrent read/write behavior for dashboard + sync jobs.
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute("PRAGMA busy_timeout=30000")
        self._create_tables()

    def _create_tables(self) -> None:
        with self._lock:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS feedback_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    source_item_id TEXT,
                    title TEXT NOT NULL,
                    url TEXT NOT NULL,
                    author TEXT,
                    source_section TEXT,
                    published_at TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    report_date TEXT NOT NULL,
                    content TEXT NOT NULL,
                    summary TEXT,
                    camera_category TEXT NOT NULL,
                    sentiment TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    source_actor_type TEXT NOT NULL DEFAULT 'unknown',
                    source_actor_reason TEXT NOT NULL DEFAULT '',
                    domain_tag TEXT NOT NULL DEFAULT '未分类',
                    domain_subtags_json TEXT NOT NULL DEFAULT '[]',
                    sentiment_reason TEXT NOT NULL DEFAULT '',
                    ai_positive_points_json TEXT NOT NULL DEFAULT '[]',
                    ai_neutral_points_json TEXT NOT NULL DEFAULT '[]',
                    ai_negative_points_json TEXT NOT NULL DEFAULT '[]',
                    product_tags TEXT NOT NULL,
                    camera_keyword_hits TEXT NOT NULL,
                    camera_related INTEGER NOT NULL DEFAULT 1,
                    video_candidate INTEGER NOT NULL DEFAULT 0,
                    token_set_json TEXT NOT NULL,
                    language TEXT NOT NULL,
                    dedupe_exact_key TEXT NOT NULL UNIQUE,
                    fingerprint TEXT NOT NULL UNIQUE,
                    extra_json TEXT NOT NULL,
                    lark_record_id TEXT,
                    lark_dirty INTEGER NOT NULL DEFAULT 1,
                    lark_synced_at TEXT,
                    lark_last_sync_error TEXT NOT NULL DEFAULT ''
                )
                """
            )
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_feedback_report_date ON feedback_items(report_date)"
            )
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_feedback_published_at ON feedback_items(published_at)"
            )
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS lark_point_record_links (
                    point_uid TEXT PRIMARY KEY,
                    feedback_item_id INTEGER NOT NULL,
                    lark_record_id TEXT,
                    synced_at TEXT,
                    updated_at TEXT NOT NULL,
                    last_error TEXT NOT NULL DEFAULT ''
                )
                """
            )
            self.connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_lark_point_feedback_id ON lark_point_record_links(feedback_item_id)"
            )
            self._migrate_schema()
            self.connection.commit()

    def _migrate_schema(self) -> None:
        cursor = self.connection.execute("PRAGMA table_info(feedback_items)")
        columns = {row["name"] for row in cursor.fetchall()}
        if "camera_related" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN camera_related INTEGER NOT NULL DEFAULT 1"
            )
        if "source_actor_type" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN source_actor_type TEXT NOT NULL DEFAULT 'unknown'"
            )
        if "source_actor_reason" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN source_actor_reason TEXT NOT NULL DEFAULT ''"
            )
        if "domain_tag" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN domain_tag TEXT NOT NULL DEFAULT '未分类'"
            )
        if "domain_subtags_json" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN domain_subtags_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "sentiment_reason" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN sentiment_reason TEXT NOT NULL DEFAULT ''"
            )
        if "ai_positive_points_json" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN ai_positive_points_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "ai_neutral_points_json" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN ai_neutral_points_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "ai_negative_points_json" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN ai_negative_points_json TEXT NOT NULL DEFAULT '[]'"
            )
        if "video_candidate" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN video_candidate INTEGER NOT NULL DEFAULT 0"
            )
        if "lark_dirty" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN lark_dirty INTEGER NOT NULL DEFAULT 1"
            )
        if "lark_synced_at" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN lark_synced_at TEXT"
            )
        if "lark_last_sync_error" not in columns:
            self.connection.execute(
                "ALTER TABLE feedback_items ADD COLUMN lark_last_sync_error TEXT NOT NULL DEFAULT ''"
            )

    def insert(self, item: FeedbackItem) -> bool:
        dedupe_exact_key = str(item.extra.get("dedupe_exact_key", ""))
        if not dedupe_exact_key:
            raise ValueError("missing dedupe_exact_key before insert")

        fingerprint = build_fingerprint(item.source, item.source_item_id, item.title, item.url)
        now = datetime.now(tz=timezone.utc)
        report_date = now.astimezone().date().isoformat()

        with self._lock:
            cursor = self.connection.execute(
                """
                INSERT INTO feedback_items (
                    source,
                    source_item_id,
                    title,
                    url,
                    author,
                    source_section,
                    published_at,
                    collected_at,
                    report_date,
                    content,
                    summary,
                    camera_category,
                    sentiment,
                    severity,
                    source_actor_type,
                    source_actor_reason,
                    domain_tag,
                    domain_subtags_json,
                    sentiment_reason,
                    ai_positive_points_json,
                    ai_neutral_points_json,
                    ai_negative_points_json,
                    product_tags,
                    camera_keyword_hits,
                    camera_related,
                    video_candidate,
                    token_set_json,
                    language,
                    dedupe_exact_key,
                    fingerprint,
                    extra_json,
                    lark_dirty,
                    lark_last_sync_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.source,
                    item.source_item_id,
                    item.title,
                    item.url,
                    item.author,
                    item.source_section,
                    isoformat(item.published_at),
                    isoformat(now),
                    report_date,
                    item.content,
                    item.summary,
                    item.camera_category,
                    item.sentiment,
                    item.severity,
                    item.source_actor_type,
                    item.source_actor_reason,
                    item.domain_tag,
                    dump_json(item.domain_subtags),
                    item.sentiment_reason,
                    dump_json(item.ai_positive_points),
                    dump_json(item.ai_neutral_points),
                    dump_json(item.ai_negative_points),
                    dump_json(item.product_tags),
                    dump_json(item.camera_keyword_hits),
                    1 if item.camera_related else 0,
                    1 if item.video_candidate else 0,
                    dump_json(item.token_set),
                    item.language,
                    dedupe_exact_key,
                    fingerprint,
                    dump_json(item.extra),
                    1,
                    "",
                ),
            )
            self.connection.commit()
        return cursor.rowcount > 0

    def fetch_recent_dedupe_candidates(self, since: datetime, limit: int = 2000) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT dedupe_exact_key, token_set_json
                FROM feedback_items
                WHERE published_at >= ?
                ORDER BY published_at DESC
                LIMIT ?
                """,
                (isoformat(since), limit),
            )
            fetched = cursor.fetchall()
        rows = []
        for row in fetched:
            rows.append(
                {
                    "dedupe_exact_key": row["dedupe_exact_key"],
                    "token_set": load_json(row["token_set_json"], []),
                }
            )
        return rows

    def fetch_by_report_date(
        self,
        target_date: date,
        camera_only: bool | None = None,
        limit: int | None = None,
    ) -> list[sqlite3.Row]:
        sql = """
            SELECT *
            FROM feedback_items
            WHERE report_date = ?
        """
        params: list[Any] = [target_date.isoformat()]
        if camera_only is True:
            sql += " AND camera_related = 1"
        elif camera_only is False:
            sql += " AND camera_related = 0"
        sql += " ORDER BY severity DESC, published_at DESC"
        if limit is not None and limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            cursor = self.connection.execute(sql, tuple(params))
            return list(cursor.fetchall())

    def fetch_by_published_date_range(
        self,
        start_date: date,
        end_date: date,
        camera_only: bool | None = None,
        limit: int | None = None,
    ) -> list[sqlite3.Row]:
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()
        sql = """
            SELECT *
            FROM feedback_items
            WHERE substr(published_at, 1, 10) >= ? AND substr(published_at, 1, 10) <= ?
        """
        params: list[Any] = [start_iso, end_iso]
        if camera_only is True:
            sql += " AND camera_related = 1"
        elif camera_only is False:
            sql += " AND camera_related = 0"
        sql += " ORDER BY severity DESC, published_at DESC"
        if limit is not None and limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            cursor = self.connection.execute(sql, tuple(params))
            return list(cursor.fetchall())

    def fetch_lark_pending(
        self,
        target_date: date | None = None,
        limit: int = 500,
        only_new: bool = False,
    ) -> list[sqlite3.Row]:
        base_sql = """
            SELECT *
            FROM feedback_items
            WHERE
        """
        if only_new:
            base_sql += " lark_record_id IS NULL "
        else:
            base_sql += " (lark_dirty = 1 OR lark_record_id IS NULL) "
        params: list[Any] = []
        if target_date:
            base_sql += " AND report_date = ?"
            params.append(target_date.isoformat())
        base_sql += " ORDER BY published_at DESC"
        if limit > 0:
            base_sql += " LIMIT ?"
            params.append(max(1, int(limit)))
        with self._lock:
            cursor = self.connection.execute(base_sql, tuple(params))
            return list(cursor.fetchall())

    def count_lark_pending(self, target_date: date | None = None, only_new: bool = False) -> int:
        sql = """
            SELECT COUNT(1) AS total
            FROM feedback_items
            WHERE
        """
        if only_new:
            sql += " lark_record_id IS NULL "
        else:
            sql += " (lark_dirty = 1 OR lark_record_id IS NULL) "
        params: list[Any] = []
        if target_date:
            sql += " AND report_date = ?"
            params.append(target_date.isoformat())
        with self._lock:
            cursor = self.connection.execute(sql, tuple(params))
            row = cursor.fetchone()
            return int(row["total"] or 0) if row else 0

    def fetch_unsynced(self, target_date: date | None = None) -> list[sqlite3.Row]:
        return self.fetch_lark_pending(target_date=target_date, limit=500)

    def fetch_by_id(self, row_id: int) -> sqlite3.Row | None:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM feedback_items
                WHERE id = ?
                LIMIT 1
                """,
                (int(row_id),),
            )
            return cursor.fetchone()

    def mark_synced(self, row_id: int, record_id: str) -> None:
        with self._lock:
            self.connection.execute(
                """
                UPDATE feedback_items
                SET
                    lark_record_id = ?,
                    lark_dirty = 0,
                    lark_synced_at = ?,
                    lark_last_sync_error = ''
                WHERE id = ?
                """,
                (record_id, isoformat(datetime.now(tz=timezone.utc)), row_id),
            )
            self.connection.commit()

    def mark_lark_sync_failed(self, row_id: int, error: str) -> None:
        message = (str(error or "").strip() or "unknown_error")[:480]
        with self._lock:
            self.connection.execute(
                """
                UPDATE feedback_items
                SET
                    lark_dirty = 1,
                    lark_last_sync_error = ?
                WHERE id = ?
                """,
                (message, int(row_id)),
            )
            self.connection.commit()

    def list_lark_point_links(self, feedback_item_id: int) -> list[sqlite3.Row]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT point_uid, lark_record_id
                FROM lark_point_record_links
                WHERE feedback_item_id = ?
                """,
                (int(feedback_item_id),),
            )
            return list(cursor.fetchall())

    def get_lark_point_record_id(self, point_uid: str) -> str:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT lark_record_id
                FROM lark_point_record_links
                WHERE point_uid = ?
                LIMIT 1
                """,
                (str(point_uid),),
            )
            row = cursor.fetchone()
        if not row:
            return ""
        return str(row["lark_record_id"] or "").strip()

    def upsert_lark_point_link(self, feedback_item_id: int, point_uid: str, record_id: str) -> None:
        now_text = isoformat(datetime.now(tz=timezone.utc)) or ""
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO lark_point_record_links (
                    point_uid,
                    feedback_item_id,
                    lark_record_id,
                    synced_at,
                    updated_at,
                    last_error
                ) VALUES (?, ?, ?, ?, ?, '')
                ON CONFLICT(point_uid) DO UPDATE SET
                    feedback_item_id = excluded.feedback_item_id,
                    lark_record_id = excluded.lark_record_id,
                    synced_at = excluded.synced_at,
                    updated_at = excluded.updated_at,
                    last_error = ''
                """,
                (
                    str(point_uid),
                    int(feedback_item_id),
                    str(record_id or ""),
                    now_text,
                    now_text,
                ),
            )
            self.connection.commit()

    def mark_lark_point_failed(self, feedback_item_id: int, point_uid: str, error: str) -> None:
        now_text = isoformat(datetime.now(tz=timezone.utc)) or ""
        message = (str(error or "").strip() or "unknown_error")[:480]
        with self._lock:
            self.connection.execute(
                """
                INSERT INTO lark_point_record_links (
                    point_uid,
                    feedback_item_id,
                    lark_record_id,
                    synced_at,
                    updated_at,
                    last_error
                ) VALUES (?, ?, '', NULL, ?, ?)
                ON CONFLICT(point_uid) DO UPDATE SET
                    feedback_item_id = excluded.feedback_item_id,
                    updated_at = excluded.updated_at,
                    last_error = excluded.last_error
                """,
                (
                    str(point_uid),
                    int(feedback_item_id),
                    now_text,
                    message,
                ),
            )
            self.connection.commit()

    def delete_lark_point_link(self, point_uid: str) -> None:
        with self._lock:
            self.connection.execute(
                """
                DELETE FROM lark_point_record_links
                WHERE point_uid = ?
                """,
                (str(point_uid),),
            )
            self.connection.commit()

    def daily_stats(self, target_date: date) -> DailyStats:
        rows = self.fetch_by_report_date(target_date)
        categories = Counter(row["camera_category"] for row in rows)
        sentiments = Counter(row["sentiment"] for row in rows)
        high_risk = sum(1 for row in rows if row["severity"] == "high")
        return DailyStats(
            report_date=target_date,
            total=len(rows),
            high_risk=high_risk,
            categories=dict(categories),
            sentiments=dict(sentiments),
        )

    def list_report_dates(self, limit: int = 30) -> list[str]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT report_date, COUNT(1) AS total
                FROM feedback_items
                GROUP BY report_date
                ORDER BY report_date DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [row["report_date"] for row in cursor.fetchall()]

    def list_published_dates(self, limit: int = 120) -> list[str]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT substr(published_at, 1, 10) AS published_date, COUNT(1) AS total
                FROM feedback_items
                GROUP BY published_date
                ORDER BY published_date DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [str(row["published_date"]) for row in cursor.fetchall() if row["published_date"]]

    def published_date_bounds(self) -> tuple[str | None, str | None]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT
                    MIN(substr(published_at, 1, 10)) AS min_date,
                    MAX(substr(published_at, 1, 10)) AS max_date
                FROM feedback_items
                """
            )
            row = cursor.fetchone()
        if not row:
            return None, None
        return (str(row["min_date"]) if row["min_date"] else None, str(row["max_date"]) if row["max_date"] else None)

    def trend_by_report_date(self, days: int = 14) -> list[dict[str, Any]]:
        since_day = (datetime.now(tz=timezone.utc).astimezone().date() - timedelta(days=max(1, days - 1))).isoformat()
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT
                    report_date,
                    COUNT(1) AS total,
                    SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_risk,
                    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_total,
                    SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_total,
                    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_total
                FROM feedback_items
                WHERE report_date >= ?
                GROUP BY report_date
                ORDER BY report_date ASC
                """,
                (since_day,),
            )
            fetched = cursor.fetchall()
        return [
            {
                "report_date": row["report_date"],
                "total": int(row["total"] or 0),
                "high_risk": int(row["high_risk"] or 0),
                "positive_total": int(row["positive_total"] or 0),
                "neutral_total": int(row["neutral_total"] or 0),
                "negative_total": int(row["negative_total"] or 0),
            }
            for row in fetched
        ]

    def trend_by_published_date(self, start_date: date, end_date: date) -> list[dict[str, Any]]:
        start_iso = start_date.isoformat()
        end_iso = end_date.isoformat()
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT
                    substr(published_at, 1, 10) AS published_date,
                    COUNT(1) AS total,
                    SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END) AS positive_total,
                    SUM(CASE WHEN sentiment = 'neutral' THEN 1 ELSE 0 END) AS neutral_total,
                    SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END) AS negative_total
                FROM feedback_items
                WHERE substr(published_at, 1, 10) >= ? AND substr(published_at, 1, 10) <= ?
                GROUP BY published_date
                ORDER BY published_date ASC
                """,
                (start_iso, end_iso),
            )
            fetched = cursor.fetchall()

        bucket = {
            str(row["published_date"]): {
                "total": int(row["total"] or 0),
                "positive_total": int(row["positive_total"] or 0),
                "neutral_total": int(row["neutral_total"] or 0),
                "negative_total": int(row["negative_total"] or 0),
            }
            for row in fetched
            if row["published_date"]
        }

        trend: list[dict[str, Any]] = []
        current = start_date
        while current <= end_date:
            day_key = current.isoformat()
            day_data = bucket.get(
                day_key,
                {"total": 0, "positive_total": 0, "neutral_total": 0, "negative_total": 0},
            )
            trend.append(
                {
                    "report_date": day_key,
                    "total": int(day_data["total"]),
                    "positive_total": int(day_data["positive_total"]),
                    "neutral_total": int(day_data["neutral_total"]),
                    "negative_total": int(day_data["negative_total"]),
                }
            )
            current += timedelta(days=1)
        return trend

    def fetch_video_candidates(self, target_date: date, limit: int = 80) -> list[sqlite3.Row]:
        with self._lock:
            cursor = self.connection.execute(
                """
                SELECT *
                FROM feedback_items
                WHERE report_date = ? AND video_candidate = 1
                ORDER BY published_at DESC
                LIMIT ?
                """,
                (target_date.isoformat(), max(1, limit)),
            )
            return list(cursor.fetchall())

    def fetch_rows_for_backfill(self, target_date: date | None = None, limit: int = 500) -> list[sqlite3.Row]:
        if target_date:
            with self._lock:
                cursor = self.connection.execute(
                    """
                    SELECT *
                    FROM feedback_items
                    WHERE report_date = ?
                    ORDER BY published_at DESC
                    LIMIT ?
                    """,
                    (target_date.isoformat(), max(1, limit)),
                )
                return list(cursor.fetchall())
        else:
            with self._lock:
                cursor = self.connection.execute(
                    """
                    SELECT *
                    FROM feedback_items
                    ORDER BY published_at DESC
                    LIMIT ?
                    """,
                    (max(1, limit),),
                )
                return list(cursor.fetchall())

    def update_analysis_fields(self, row_id: int, item: FeedbackItem) -> None:
        with self._lock:
            self.connection.execute(
                """
                UPDATE feedback_items
                SET
                    author = ?,
                    content = ?,
                    summary = ?,
                    camera_category = ?,
                    sentiment = ?,
                    severity = ?,
                    source_actor_type = ?,
                    source_actor_reason = ?,
                    domain_tag = ?,
                    domain_subtags_json = ?,
                    sentiment_reason = ?,
                    ai_positive_points_json = ?,
                    ai_neutral_points_json = ?,
                    ai_negative_points_json = ?,
                    product_tags = ?,
                    camera_keyword_hits = ?,
                    camera_related = ?,
                    video_candidate = ?,
                    token_set_json = ?,
                    language = ?,
                    extra_json = ?,
                    lark_dirty = 1
                WHERE id = ?
                """,
                (
                    item.author or "",
                    item.content,
                    item.summary or "",
                    item.camera_category,
                    item.sentiment,
                    item.severity,
                    item.source_actor_type,
                    item.source_actor_reason,
                    item.domain_tag,
                    dump_json(item.domain_subtags),
                    item.sentiment_reason,
                    dump_json(item.ai_positive_points),
                    dump_json(item.ai_neutral_points),
                    dump_json(item.ai_negative_points),
                    dump_json(item.product_tags),
                    dump_json(item.camera_keyword_hits),
                    1 if item.camera_related else 0,
                    1 if item.video_candidate else 0,
                    dump_json(item.token_set),
                    item.language,
                    dump_json(item.extra),
                    int(row_id),
                ),
            )
            self.connection.commit()
