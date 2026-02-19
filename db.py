"""
Database layer — SQLite via aiosqlite.
All public methods raise sqlite3.IntegrityError on constraint violations;
callers that need idempotency must catch it explicitly.
"""

import sqlite3
import logging
from typing import Optional, List

import aiosqlite

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS groups (
    group_id  INTEGER PRIMARY KEY,
    title     TEXT,
    added_at  TEXT DEFAULT (datetime('now')),
    active    INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS settings (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id         INTEGER UNIQUE NOT NULL REFERENCES groups(group_id),
    poll_time        TEXT    DEFAULT '20:00',
    timezone         TEXT    DEFAULT 'Asia/Almaty',
    challenge_active INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS participants (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id     INTEGER NOT NULL REFERENCES groups(group_id),
    user_id      INTEGER,
    username     TEXT,
    display_name TEXT    NOT NULL,
    joined_at    TEXT    DEFAULT (datetime('now')),
    active       INTEGER DEFAULT 1,
    pending      INTEGER DEFAULT 0   -- 1 while user_id is unknown
);

-- Partial-unique: each (group, user_id) pair is unique when user_id IS NOT NULL.
-- SQLite enforces NULLs as distinct, so multiple NULLs are allowed.
CREATE UNIQUE INDEX IF NOT EXISTS ux_participants_group_user
    ON participants(group_id, user_id)
    WHERE user_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS polls (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id    INTEGER NOT NULL REFERENCES groups(group_id),
    poll_date   TEXT    NOT NULL,          -- ISO date, local Almaty day
    tg_poll_id  TEXT,                      -- Telegram's poll ID string
    message_id  INTEGER,                   -- Telegram message ID
    posted_at   TEXT,
    UNIQUE(group_id, poll_date)
);

CREATE TABLE IF NOT EXISTS votes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    poll_id    INTEGER NOT NULL REFERENCES polls(id),
    user_id    INTEGER NOT NULL,
    option_idx INTEGER,                    -- 0=Yes  1=No  NULL=retracted
    voted_at   TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(poll_id, user_id)
);

CREATE TABLE IF NOT EXISTS daily_results (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id       INTEGER NOT NULL REFERENCES groups(group_id),
    participant_id INTEGER NOT NULL REFERENCES participants(id),
    result_date    TEXT    NOT NULL,
    status         TEXT    CHECK(status IN ('yes','no','missed')),
    UNIQUE(group_id, participant_id, result_date)
);

CREATE TABLE IF NOT EXISTS weekly_results (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id        INTEGER NOT NULL REFERENCES groups(group_id),
    participant_id  INTEGER NOT NULL REFERENCES participants(id),
    week_start      TEXT    NOT NULL,
    total_yes       INTEGER DEFAULT 0,
    total_no        INTEGER DEFAULT 0,
    total_missed    INTEGER DEFAULT 0,
    completion_rate REAL,
    rank_pos        INTEGER,
    UNIQUE(group_id, participant_id, week_start)
);
"""


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------

class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()
        logger.info("Database connected: %s", self.path)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()

    # -----------------------------------------------------------------------
    # Low-level helpers
    # -----------------------------------------------------------------------

    async def execute(self, query: str, *args) -> int:
        """Run a write query, commit, return lastrowid."""
        async with self._conn.execute(query, args) as cur:
            await self._conn.commit()
            return cur.lastrowid or 0

    async def fetchone(self, query: str, *args) -> Optional[aiosqlite.Row]:
        async with self._conn.execute(query, args) as cur:
            return await cur.fetchone()

    async def fetchall(self, query: str, *args) -> List[aiosqlite.Row]:
        async with self._conn.execute(query, args) as cur:
            return await cur.fetchall()

    # -----------------------------------------------------------------------
    # Groups & Settings
    # -----------------------------------------------------------------------

    async def get_or_create_group(self, group_id: int, title: str) -> None:
        await self.execute(
            "INSERT OR IGNORE INTO groups (group_id, title) VALUES (?, ?)",
            group_id, title,
        )
        await self.execute(
            "INSERT OR IGNORE INTO settings (group_id) VALUES (?)",
            group_id,
        )

    async def deactivate_group(self, group_id: int) -> None:
        await self.execute(
            "UPDATE groups SET active = 0 WHERE group_id = ?", group_id
        )
        await self.execute(
            "UPDATE settings SET challenge_active = 0 WHERE group_id = ?", group_id
        )

    async def get_settings(self, group_id: int) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT * FROM settings WHERE group_id = ?", group_id
        )

    async def set_challenge_active(self, group_id: int, active: bool) -> None:
        await self.execute(
            "UPDATE settings SET challenge_active = ? WHERE group_id = ?",
            1 if active else 0, group_id,
        )

    async def set_poll_time(self, group_id: int, poll_time: str) -> None:
        await self.execute(
            "UPDATE settings SET poll_time = ? WHERE group_id = ?",
            poll_time, group_id,
        )

    async def get_all_active_challenges(self) -> List[aiosqlite.Row]:
        return await self.fetchall(
            "SELECT g.group_id, s.poll_time "
            "FROM groups g JOIN settings s ON s.group_id = g.group_id "
            "WHERE s.challenge_active = 1 AND g.active = 1"
        )

    # -----------------------------------------------------------------------
    # Participants
    # -----------------------------------------------------------------------

    async def get_participant_by_user_id(
        self, group_id: int, user_id: int
    ) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT * FROM participants WHERE group_id = ? AND user_id = ?",
            group_id, user_id,
        )

    async def get_participant_by_username(
        self, group_id: int, username: str
    ) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT * FROM participants "
            "WHERE group_id = ? AND lower(username) = lower(?)",
            group_id, username,
        )

    async def upsert_participant(
        self,
        group_id: int,
        user_id: int,
        username: Optional[str],
        display_name: str,
    ) -> int:
        """Add or reactivate a known participant. Returns participant id."""
        existing = await self.get_participant_by_user_id(group_id, user_id)
        if existing:
            await self.execute(
                "UPDATE participants "
                "SET username=?, display_name=?, active=1, pending=0 "
                "WHERE id=?",
                username, display_name, existing["id"],
            )
            return existing["id"]

        # Resolve a pending record that was added by @username
        if username:
            pending = await self.get_participant_by_username(group_id, username)
            if pending and pending["user_id"] is None:
                await self.execute(
                    "UPDATE participants "
                    "SET user_id=?, display_name=?, active=1, pending=0 "
                    "WHERE id=?",
                    user_id, display_name, pending["id"],
                )
                return pending["id"]

        return await self.execute(
            "INSERT INTO participants "
            "(group_id, user_id, username, display_name, active, pending) "
            "VALUES (?, ?, ?, ?, 1, 0)",
            group_id, user_id, username, display_name,
        )

    async def add_pending_participant(self, group_id: int, username: str) -> int:
        """Add a participant by username only; user_id to be resolved later."""
        existing = await self.get_participant_by_username(group_id, username)
        if existing:
            await self.execute(
                "UPDATE participants SET active=1 WHERE id=?", existing["id"]
            )
            return existing["id"]
        return await self.execute(
            "INSERT INTO participants "
            "(group_id, username, display_name, active, pending) "
            "VALUES (?, ?, ?, 1, 1)",
            group_id, username, username,
        )

    async def resolve_pending_by_username(
        self,
        group_id: int,
        username: str,
        user_id: int,
        display_name: str,
    ) -> bool:
        """Fill in user_id for a pending participant. Returns True if resolved."""
        row = await self.fetchone(
            "SELECT id FROM participants "
            "WHERE group_id=? AND lower(username)=lower(?) "
            "AND user_id IS NULL AND pending=1",
            group_id, username,
        )
        if row:
            await self.execute(
                "UPDATE participants "
                "SET user_id=?, display_name=?, pending=0 "
                "WHERE id=?",
                user_id, display_name, row["id"],
            )
            return True
        return False

    async def deactivate_participant_by_user_id(
        self, group_id: int, user_id: int
    ) -> bool:
        p = await self.get_participant_by_user_id(group_id, user_id)
        if not p or not p["active"]:
            return False
        await self.execute(
            "UPDATE participants SET active=0 WHERE id=?", p["id"]
        )
        return True

    async def deactivate_participant_by_username(
        self, group_id: int, username: str
    ) -> bool:
        p = await self.get_participant_by_username(group_id, username)
        if not p or not p["active"]:
            return False
        await self.execute(
            "UPDATE participants SET active=0 WHERE id=?", p["id"]
        )
        return True

    async def get_active_participants(self, group_id: int) -> List[aiosqlite.Row]:
        return await self.fetchall(
            "SELECT * FROM participants "
            "WHERE group_id=? AND active=1 "
            "ORDER BY display_name COLLATE NOCASE",
            group_id,
        )

    # -----------------------------------------------------------------------
    # Polls
    # -----------------------------------------------------------------------

    async def try_create_poll_slot(
        self, group_id: int, poll_date: str
    ) -> Optional[int]:
        """Reserve today's poll slot. Returns poll id on success, None if already exists."""
        try:
            return await self.execute(
                "INSERT INTO polls (group_id, poll_date, posted_at) "
                "VALUES (?, ?, datetime('now'))",
                group_id, poll_date,
            )
        except sqlite3.IntegrityError:
            return None

    async def update_poll_telegram_ids(
        self,
        group_id: int,
        poll_date: str,
        tg_poll_id: str,
        message_id: int,
    ) -> None:
        await self.execute(
            "UPDATE polls SET tg_poll_id=?, message_id=? "
            "WHERE group_id=? AND poll_date=?",
            tg_poll_id, message_id, group_id, poll_date,
        )

    async def get_poll_by_tg_id(self, tg_poll_id: str) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT * FROM polls WHERE tg_poll_id=?", tg_poll_id
        )

    async def get_poll_by_date(
        self, group_id: int, poll_date: str
    ) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT * FROM polls WHERE group_id=? AND poll_date=?",
            group_id, poll_date,
        )

    # -----------------------------------------------------------------------
    # Votes
    # -----------------------------------------------------------------------

    async def upsert_vote(
        self, poll_id: int, user_id: int, option_idx: Optional[int]
    ) -> None:
        await self.execute(
            "INSERT INTO votes (poll_id, user_id, option_idx, voted_at, updated_at) "
            "VALUES (?, ?, ?, datetime('now'), datetime('now')) "
            "ON CONFLICT(poll_id, user_id) DO UPDATE SET "
            "  option_idx=excluded.option_idx, updated_at=datetime('now')",
            poll_id, user_id, option_idx,
        )

    async def get_vote(self, poll_id: int, user_id: int) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT * FROM votes WHERE poll_id=? AND user_id=?",
            poll_id, user_id,
        )

    # -----------------------------------------------------------------------
    # Daily Results
    # -----------------------------------------------------------------------

    async def upsert_daily_result(
        self,
        group_id: int,
        participant_id: int,
        result_date: str,
        status: str,
    ) -> None:
        await self.execute(
            "INSERT INTO daily_results "
            "(group_id, participant_id, result_date, status) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(group_id, participant_id, result_date) "
            "DO UPDATE SET status=excluded.status",
            group_id, participant_id, result_date, status,
        )

    async def get_today_vote_for_user(
        self, group_id: int, user_id: int, today: str
    ) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT v.option_idx, v.voted_at, p.message_id "
            "FROM votes v "
            "JOIN polls p ON p.id = v.poll_id "
            "WHERE p.group_id=? AND p.poll_date=? AND v.user_id=?",
            group_id, today, user_id,
        )

    # -----------------------------------------------------------------------
    # Stats (computed from daily_results)
    # -----------------------------------------------------------------------

    async def get_participant_stats_alltime(
        self, participant_id: int
    ) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT "
            "  COALESCE(SUM(CASE WHEN status='yes'    THEN 1 ELSE 0 END), 0) AS total_yes, "
            "  COALESCE(SUM(CASE WHEN status='no'     THEN 1 ELSE 0 END), 0) AS total_no, "
            "  COALESCE(SUM(CASE WHEN status='missed' THEN 1 ELSE 0 END), 0) AS total_missed "
            "FROM daily_results WHERE participant_id=?",
            participant_id,
        )

    async def get_participant_stats_weekly(
        self, participant_id: int, week_start: str, week_end: str
    ) -> Optional[aiosqlite.Row]:
        return await self.fetchone(
            "SELECT "
            "  COALESCE(SUM(CASE WHEN status='yes'    THEN 1 ELSE 0 END), 0) AS total_yes, "
            "  COALESCE(SUM(CASE WHEN status='no'     THEN 1 ELSE 0 END), 0) AS total_no, "
            "  COALESCE(SUM(CASE WHEN status='missed' THEN 1 ELSE 0 END), 0) AS total_missed "
            "FROM daily_results "
            "WHERE participant_id=? AND result_date BETWEEN ? AND ?",
            participant_id, week_start, week_end,
        )

    async def get_weekly_leaderboard(
        self, group_id: int, week_start: str, week_end: str
    ) -> List[aiosqlite.Row]:
        return await self.fetchall(
            "SELECT p.id, p.display_name, p.user_id, p.username, p.joined_at, "
            "  COALESCE(SUM(CASE WHEN dr.status='yes'    THEN 1 ELSE 0 END), 0) AS yes_count, "
            "  COALESCE(SUM(CASE WHEN dr.status='no'     THEN 1 ELSE 0 END), 0) AS no_count, "
            "  COALESCE(SUM(CASE WHEN dr.status='missed' THEN 1 ELSE 0 END), 0) AS missed_count "
            "FROM participants p "
            "LEFT JOIN daily_results dr "
            "  ON dr.participant_id = p.id "
            "  AND dr.result_date BETWEEN ? AND ? "
            "WHERE p.group_id=? AND p.active=1 "
            "GROUP BY p.id "
            "ORDER BY yes_count DESC, p.display_name COLLATE NOCASE ASC",
            week_start, week_end, group_id,
        )

    # -----------------------------------------------------------------------
    # Weekly Results
    # -----------------------------------------------------------------------

    async def check_weekly_result_exists(
        self, group_id: int, week_start: str
    ) -> bool:
        row = await self.fetchone(
            "SELECT id FROM weekly_results "
            "WHERE group_id=? AND week_start=? LIMIT 1",
            group_id, week_start,
        )
        return row is not None

    async def insert_weekly_result(
        self,
        group_id: int,
        participant_id: int,
        week_start: str,
        total_yes: int,
        total_no: int,
        total_missed: int,
        completion_rate: float,
        rank_pos: int,
    ) -> None:
        await self.execute(
            "INSERT OR IGNORE INTO weekly_results "
            "(group_id, participant_id, week_start, "
            " total_yes, total_no, total_missed, completion_rate, rank_pos) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            group_id, participant_id, week_start,
            total_yes, total_no, total_missed, completion_rate, rank_pos,
        )
