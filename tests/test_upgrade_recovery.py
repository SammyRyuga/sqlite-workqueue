from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
import multiprocessing
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from workqueue import Queue


def start_together(barrier):
    global ready
    ready = barrier


def open_and_enqueue(path):
    ready.wait(timeout=20)
    return Queue(path, clock=lambda: 1000).enqueue("shared", idempotency_key="shared")


class UpgradeRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "jobs.sqlite3"
        self.queue = Queue(self.path, clock=lambda: 1000)
        self.active = self.queue.enqueue("active", idempotency_key="active")
        self.claim = self.queue.claim(lease_seconds=1)
        with closing(sqlite3.connect(self.path)) as db:
            db.executemany("""INSERT INTO jobs
                (id,payload,state,attempts,max_attempts,retry_delay,available_at,created_at,updated_at)
                VALUES (?,'{}','failed',1,1,1,0,0,1000)""",
                ((f"failure-{i:05d}",) for i in range(4000)))
            db.execute("DROP INDEX failed_jobs_order")
            db.commit()

    def assert_preserved(self):
        self.assertEqual(self.queue.get(self.active), self.claim)
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0], "ok")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM jobs WHERE state='failed'").fetchone()[0], 4000)
            self.assertEqual(db.execute("SELECT job_id FROM enqueue_keys WHERE key='active'").fetchone()[0],
                             self.active)

    def test_competing_initializers_preserve_existing_jobs_and_deduplicate(self):
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=4, mp_context=context, initializer=start_together,
                                 initargs=(context.Barrier(4),)) as pool:
            ids = list(pool.map(open_and_enqueue, [str(self.path)] * 4))
        self.assertEqual(len(set(ids)), 1)
        self.assert_preserved()
        self.assertEqual(self.queue.stats()["total"], 4002)

    def test_lock_timeout_preserves_jobs_and_later_open_recovers(self):
        connect = sqlite3.connect

        def short_wait(*args, **kwargs):
            # Exercise real SQLite locking without a five-second wait in every CI run.
            kwargs["timeout"] = 0.05
            return connect(*args, **kwargs)

        with closing(connect(self.path, isolation_level=None)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            try:
                with patch("workqueue.queue.sqlite3.connect", side_effect=short_wait):
                    with self.assertRaises(sqlite3.OperationalError) as caught:
                        Queue(self.path)
                self.assertEqual(caught.exception.sqlite_errorcode & 255, sqlite3.SQLITE_BUSY)
                self.assert_preserved()
                self.assertEqual(writer.execute(
                    "SELECT COUNT(*) FROM sqlite_master WHERE name='failed_jobs_order'").fetchone()[0], 0)
            finally:
                writer.rollback()
        reopened = Queue(self.path, clock=lambda: 1002)
        self.assert_preserved()
        self.assertEqual(reopened.enqueue("active", idempotency_key="active"), self.active)
        recovered = reopened.claim()
        self.assertEqual((recovered.id, recovered.attempts), (self.active, 2))
        reopened.complete(recovered)
        self.assertEqual(reopened.get(self.active).state, "succeeded")

    def test_process_exit_during_index_build_rolls_back_and_reopens(self):
        code = '''
from contextlib import contextmanager
import os
import sys
from workqueue import Queue

class InterruptedQueue(Queue):
    @contextmanager
    def _connect(self):
        with super()._connect() as db:
            building = [False]
            def trace(sql):
                if "CREATE INDEX IF NOT EXISTS failed_jobs_order" in sql:
                    building[0] = True
            def progress():
                if building[0]:
                    os._exit(23)
                return 0
            db.set_trace_callback(trace)
            db.set_progress_handler(progress, 1000)
            yield db

InterruptedQueue(sys.argv[1])
'''
        result = subprocess.run([sys.executable, "-c", code, str(self.path)],
                                capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assert_preserved()
        with closing(sqlite3.connect(self.path)) as db:
            self.assertEqual(db.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE name='failed_jobs_order'").fetchone()[0], 0)
        reopened = Queue(self.path, clock=lambda: 1000)
        self.assertEqual(len(reopened.failed_jobs()), 20)
        reopened.complete(self.claim)
        self.assertEqual(reopened.get(self.active).state, "succeeded")

    def test_initialized_queue_can_open_during_an_active_writer(self):
        Queue(self.path)
        with closing(sqlite3.connect(self.path, isolation_level=None)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            try:
                reopened = Queue(self.path)
                self.assertEqual(reopened.get(self.active), self.claim)
            finally:
                writer.rollback()
