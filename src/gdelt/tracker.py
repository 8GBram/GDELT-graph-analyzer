import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

STATUSES = ("ok", "missing", "failed")

class IngestTracker:
    """Records the outcome of every export timestamp in a small SQLite database."""

    def __init__(self, db_path: Path) -> None:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS exports (
                timestamp  TEXT PRIMARY KEY,
                status     TEXT NOT NULL,
                rows       INTEGER,
                fetched_at TEXT NOT NULL
            )
            """
        )
        self.conn.commit()

    def record(self, timestamp: str, status: str, rows: int | None = None) -> None:
        """Save the outcome for `timestamp`, replacing any earlier one (e.g. 'failed' -> 'ok')."""
        if status not in STATUSES:
            raise ValueError(f"status must be one of {STATUSES}, got {status!r}")

        self.conn.execute(
            """
            INSERT INTO exports (timestamp, status, rows, fetched_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(timestamp) DO UPDATE SET
                status = excluded.status,
                rows = excluded.rows,
                fetched_at = excluded.fetched_at
            """,
            (timestamp, status, rows, datetime.now(timezone.utc).isoformat()),
        )
        self.conn.commit()

    def status(self, timestamp: str) -> str | None:
        row = self.conn.execute(
            "SELECT status FROM exports WHERE timestamp = ?", (timestamp,)
        ).fetchone()
        return row[0] if row else None

    def rows(self, timestamp: str) -> int | None:
        row = self.conn.execute(
            "SELECT rows FROM exports WHERE timestamp = ?", (timestamp,)
        ).fetchone()
        return row[0] if row else None

    def summary(self, day: date) -> dict[str, int]:
        """Count of each status for one day, e.g. {'ok': 95, 'missing': 1}."""
        cursor = self.conn.execute(
            "SELECT status, COUNT(*) FROM exports WHERE timestamp LIKE ? GROUP BY status",
            (f"{day.strftime('%Y%m%d')}%",),
        )
        return dict(cursor.fetchall())

    def close(self) -> None:
        self.conn.close()
