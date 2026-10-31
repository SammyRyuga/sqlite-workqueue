"""SQLite-backed claims with lease tokens that fence out stale workers."""

from contextlib import contextmanager
from dataclasses import dataclass
import json
import math
from pathlib import Path
import sqlite3
import time
from typing import Any, Callable
from uuid import uuid4


class LeaseLost(RuntimeError):
    """The job is no longer owned by this claim."""


@dataclass(frozen=True)
class Job:
    id: str
    payload: Any
    state: str
    attempts: int
    max_attempts: int
    available_at: float
    lease_until: float | None
    lease_token: str | None
    last_error: str | None


def _seconds(value: float, name: str, *, positive: bool = False) -> float:
    value = float(value)
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(f"{name} must be finite and {'positive' if positive else 'nonnegative'}")
    return value


def _enable_wal(db) -> None:
    # Concurrent first-open WAL transitions can return BUSY without waiting.
    deadline = time.monotonic() + 5
    while True:
        try:
            db.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as exc:
            remaining = deadline - time.monotonic()
            code = getattr(exc, "sqlite_errorcode", 0)
            if code & 255 != sqlite3.SQLITE_BUSY or remaining <= 0:
                raise
            time.sleep(min(0.01, remaining))


class Queue:
    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time):
        if str(path) == ":memory:":
            raise ValueError("Use a file path; this queue opens one connection per operation")
        self.path = str(path)
        self.clock = clock
        with self._connect() as db:
            _enable_wal(db)
            db.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('pending','running','succeeded','failed')),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_attempts INTEGER NOT NULL,
                    retry_delay REAL NOT NULL,
                    available_at REAL NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    lease_until REAL,
                    lease_token TEXT,
                    last_error TEXT
                );
                CREATE INDEX IF NOT EXISTS pending_jobs ON jobs(state, available_at, created_at);
                CREATE INDEX IF NOT EXISTS leased_jobs ON jobs(state, lease_until);
                CREATE TABLE IF NOT EXISTS enqueue_keys (
                    key TEXT PRIMARY KEY,
                    request TEXT NOT NULL,
                    job_id TEXT NOT NULL UNIQUE
                );
            """)

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self):
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.commit()
            except BaseException:
                db.rollback()
                raise

    @staticmethod
    def _job(row) -> Job:
        return Job(**{name: json.loads(row[name]) if name == "payload" else row[name]
                      for name in Job.__dataclass_fields__})

    def enqueue(self, payload: Any, *, max_attempts: int = 3,
                retry_delay: float = 1, delay: float = 0,
                idempotency_key: str | None = None) -> str:
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        retry_delay = _seconds(retry_delay, "retry_delay")
        delay = _seconds(delay, "delay")
        if idempotency_key is not None and (
                not isinstance(idempotency_key, str) or not idempotency_key):
            raise ValueError("idempotency_key must be a nonempty string")
        encoded = json.dumps(payload, allow_nan=False)
        request = None
        if idempotency_key is not None:
            request = json.dumps([json.loads(encoded), max_attempts, retry_delay, delay],
                                 sort_keys=True, separators=(",", ":"))
        job_id, now = uuid4().hex, self.clock()
        with self._transaction() as db:
            if idempotency_key is not None:
                previous = db.execute("SELECT request, job_id FROM enqueue_keys WHERE key=?",
                                      (idempotency_key,)).fetchone()
                if previous is not None:
                    if previous["request"] != request:
                        raise ValueError("idempotency_key already used for a different request")
                    return previous["job_id"]
            db.execute("""INSERT INTO jobs
                (id,payload,state,max_attempts,retry_delay,available_at,created_at,updated_at)
                VALUES (?,?,'pending',?,?,?,?,?)""",
                (job_id, encoded, max_attempts, retry_delay, now + delay, now, now))
            if idempotency_key is not None:
                db.execute("INSERT INTO enqueue_keys (key,request,job_id) VALUES (?,?,?)",
                           (idempotency_key, request, job_id))
        return job_id

    def get(self, job_id: str) -> Job:
        with self._connect() as db:
            row = db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise KeyError(job_id)
        return self._job(row)

    def stats(self) -> dict[str, int]:
        """Count queue states in one snapshot without recovering expired claims."""
        now = self.clock()
        with self._connect() as db:
            row = db.execute("""SELECT COUNT(*) AS total,
                COUNT(CASE WHEN state='pending' THEN 1 END) AS pending,
                COUNT(CASE WHEN state='running' THEN 1 END) AS running,
                COUNT(CASE WHEN state='succeeded' THEN 1 END) AS succeeded,
                COUNT(CASE WHEN state='failed' THEN 1 END) AS failed,
                COUNT(CASE WHEN state='pending' AND available_at <= ? THEN 1 END) AS ready,
                COUNT(CASE WHEN state='pending' AND available_at > ? THEN 1 END) AS delayed,
                COUNT(CASE WHEN state='running' AND lease_until <= ? THEN 1 END) AS expired
                FROM jobs""", (now, now, now)).fetchone()
        return dict(row)

    def claim(self, *, lease_seconds: float = 60) -> Job | None:
        lease_seconds = _seconds(lease_seconds, "lease_seconds", positive=True)
        with self._transaction() as db:
            now = self.clock()
            # Expired attempts count toward the same budget as handler failures.
            db.execute("""UPDATE jobs SET
                state=CASE WHEN attempts >= max_attempts THEN 'failed' ELSE 'pending' END,
                available_at=?, updated_at=?, lease_until=NULL, lease_token=NULL,
                last_error='lease expired'
                WHERE state='running' AND lease_until <= ?""", (now, now, now))
            row = db.execute("""SELECT * FROM jobs WHERE state='pending' AND available_at <= ?
                ORDER BY available_at, created_at, id LIMIT 1""", (now,)).fetchone()
            if row is None:
                return None
            db.execute("""UPDATE jobs SET state='running', attempts=attempts+1,
                lease_until=?, lease_token=?, updated_at=? WHERE id=?""",
                (now + lease_seconds, uuid4().hex, now, row["id"]))
            return self._job(db.execute("SELECT * FROM jobs WHERE id=?", (row["id"],)).fetchone())

    def _owned(self, db, job: Job, now: float):
        row = db.execute("""SELECT * FROM jobs WHERE id=? AND state='running'
            AND lease_token=? AND lease_until > ?""", (job.id, job.lease_token, now)).fetchone()
        if row is None:
            raise LeaseLost(f"Lease expired or replaced for job {job.id}")
        return row

    def complete(self, job: Job) -> None:
        with self._transaction() as db:
            now = self.clock()
            self._owned(db, job, now)
            db.execute("""UPDATE jobs SET state='succeeded', lease_until=NULL,
                lease_token=NULL, updated_at=? WHERE id=?""", (now, job.id))

    def fail(self, job: Job, error: str) -> None:
        with self._transaction() as db:
            now = self.clock()
            row = self._owned(db, job, now)
            state = "failed" if row["attempts"] >= row["max_attempts"] else "pending"
            try:
                delay = min(math.ldexp(row["retry_delay"], row["attempts"] - 1), 3600)
            except OverflowError:
                delay = 3600 if row["retry_delay"] else 0
            db.execute("""UPDATE jobs SET state=?, available_at=?, last_error=?,
                lease_until=NULL, lease_token=NULL, updated_at=? WHERE id=?""",
                (state, now + delay, str(error)[:2000], now, job.id))

    def renew(self, job: Job, *, lease_seconds: float = 60) -> None:
        lease_seconds = _seconds(lease_seconds, "lease_seconds", positive=True)
        with self._transaction() as db:
            now = self.clock()
            self._owned(db, job, now)
            db.execute("UPDATE jobs SET lease_until=?, updated_at=? WHERE id=?",
                       (now + lease_seconds, now, job.id))
