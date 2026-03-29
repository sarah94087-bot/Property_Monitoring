import sqlite3
import os
from datetime import datetime
from typing import List, Optional
import pandas as pd

from models import Case

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "inspections.db")


def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cases (
                case_number      TEXT PRIMARY KEY,
                apn              TEXT NOT NULL,
                case_type        TEXT,
                status           TEXT,
                current_status   TEXT,
                open_date        TEXT,
                close_date       TEXT,
                address          TEXT,
                inspector        TEXT,
                council_district TEXT,
                activity_count   INTEGER DEFAULT 0,
                priority         TEXT,
                is_new           INTEGER,
                scraped_at       TEXT,
                first_seen_at    TEXT DEFAULT (datetime('now')),
                previous_status  TEXT,
                previous_priority TEXT,
                changed_at       TEXT
            )
        """)
        existing = {row[1] for row in conn.execute("PRAGMA table_info(cases)")}
        for col, definition in [
            ("current_status",    "TEXT DEFAULT ''"),
            ("inspector",         "TEXT DEFAULT ''"),
            ("council_district",  "TEXT DEFAULT ''"),
            ("activity_count",    "INTEGER DEFAULT 0"),
            ("previous_status",   "TEXT"),
            ("previous_priority", "TEXT"),
            ("changed_at",        "TEXT"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE cases ADD COLUMN {col} {definition}")
        conn.commit()


def upsert_cases(cases: List[Case]) -> int:
    if not cases:
        return 0
    now = datetime.now().isoformat()
    rows = [
        (
            c.case_number, c.apn, c.case_type, c.status, c.current_status,
            c.open_date.isoformat() if c.open_date else None,
            c.close_date.isoformat() if c.close_date else None,
            c.address, c.inspector, c.council_district, c.activity_count,
            c.priority, int(c.is_new), now,
        )
        for c in cases
    ]
    with _connect() as conn:
        conn.executemany(
            """
            INSERT INTO cases
                (case_number, apn, case_type, status, current_status, open_date,
                 close_date, address, inspector, council_district, activity_count,
                 priority, is_new, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(case_number) DO UPDATE SET
                -- Track changes: save old values before overwriting
                previous_status   = CASE
                    WHEN excluded.status != cases.status
                    THEN cases.status ELSE cases.previous_status END,
                previous_priority = CASE
                    WHEN excluded.priority != cases.priority
                    THEN cases.priority ELSE cases.previous_priority END,
                changed_at        = CASE
                    WHEN excluded.status != cases.status
                      OR excluded.priority != cases.priority
                      OR excluded.current_status != cases.current_status
                    THEN datetime('now') ELSE cases.changed_at END,
                -- Regular fields
                status           = excluded.status,
                current_status   = excluded.current_status,
                open_date        = excluded.open_date,
                close_date       = excluded.close_date,
                address          = excluded.address,
                inspector        = excluded.inspector,
                council_district = excluded.council_district,
                activity_count   = excluded.activity_count,
                priority         = excluded.priority,
                is_new           = excluded.is_new,
                scraped_at       = excluded.scraped_at
            """,
            rows,
        )
        conn.commit()
    return len(rows)


def get_all_cases() -> pd.DataFrame:
    with _connect() as conn:
        df = pd.read_sql_query("SELECT * FROM cases ORDER BY open_date DESC NULLS LAST", conn)
    if df.empty:
        return df
    for col in ("open_date", "close_date"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    for col in ("scraped_at", "changed_at"):
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["is_new"] = df["is_new"].astype(bool)
    df["activity_count"] = df["activity_count"].fillna(0).astype(int)
    return df


def get_last_scraped() -> Optional[datetime]:
    with _connect() as conn:
        row = conn.execute("SELECT MAX(scraped_at) AS last FROM cases").fetchone()
    if row and row["last"]:
        try:
            return datetime.fromisoformat(row["last"])
        except ValueError:
            return None
    return None
