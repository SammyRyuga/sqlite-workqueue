from contextlib import closing, redirect_stdout
from io import StringIO
import json
import sqlite3
from pathlib import Path
import tempfile
import unittest

from workqueue import Queue
from workqueue.cli import main


class FailedJobsTests(unittest.TestCase):
    def test_reopening_an_existing_queue_adds_failure_index_without_changing_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "jobs.sqlite3"
            queue = Queue(path, clock=lambda: 1000)
            failed = queue.enqueue("failed", max_attempts=1, idempotency_key="request")
            queue.fail(queue.claim(), "permanent failure")
            running = queue.enqueue("running")
            claim = queue.claim()
            before = [queue.get(failed), queue.get(running)]
            with closing(sqlite3.connect(path)) as db:
                db.execute("DROP INDEX failed_jobs_order")
            reopened = Queue(path, clock=lambda: 1000)
            self.assertEqual([reopened.get(failed), reopened.get(running)], before)
            self.assertEqual(reopened.failed_jobs(), [before[0]])
            self.assertEqual(reopened.enqueue("failed", max_attempts=1,
                                              idempotency_key="request"), failed)
            reopened.complete(claim)
            self.assertEqual(reopened.get(running).state, "succeeded")
            with closing(sqlite3.connect(path)) as db:
                self.assertIn("failed_jobs_order",
                              [row[1] for row in db.execute("PRAGMA index_list(jobs)")])

    def test_pages_cover_mixed_states_and_timestamp_ties_without_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            queue = Queue(Path(directory) / "jobs.sqlite3")
            expected = []
            # Explicit IDs and tied timestamps make the independent ordering oracle stable.
            with closing(sqlite3.connect(queue.path)) as db:
                for i in range(120):
                    state = ("failed", "pending", "running", "succeeded")[i % 4]
                    stamp = i // 12
                    job_id = f"{i:04d}"
                    db.execute("""INSERT INTO jobs
                        (id,payload,state,max_attempts,retry_delay,available_at,created_at,updated_at)
                        VALUES (?,'{}',?,1,1,0,0,?)""", (job_id, state, stamp))
                    if state == "failed":
                        expected.append((stamp, job_id))
                db.commit()
            expected_ids = [job_id for _, job_id in sorted(expected, key=lambda x: (-x[0], x[1]))]
            for limit in (1, 2, 7, 20, 1000):
                with self.subTest(limit=limit):
                    actual = []
                    after = None
                    for _ in range(len(expected_ids) + 1):
                        page = queue.failed_jobs(limit=limit, after=after)
                        if not page:
                            break
                        actual.extend(job.id for job in page)
                        after = page[-1].id
                    self.assertEqual(actual, expected_ids)

    def test_paging_with_tied_failure_times_and_a_new_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "jobs.sqlite3"
            now = 1000
            queue = Queue(path, clock=lambda: now)
            older = queue.enqueue("older failure", max_attempts=1)
            queue.fail(queue.claim(), "failed")
            now += 1
            ids = []
            for i in range(3):
                ids.append(queue.enqueue(i, max_attempts=1))
                queue.fail(queue.claim(), "failed")
            first = queue.failed_jobs(limit=1)[0]
            self.assertEqual(first.id, min(ids))

            now += 1
            queue.enqueue("new failure", max_attempts=1)
            queue.fail(queue.claim(), "failed")
            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(main(["--db", str(path), "failed", "--after", first.id]), 0)
            self.assertEqual([job["id"] for job in json.loads(output.getvalue())],
                             sorted(ids)[1:] + [older])
            self.assertEqual(queue.failed_jobs(after=older), [])
            with self.assertRaisesRegex(ValueError, "failed job"):
                queue.failed_jobs(after=queue.enqueue("pending"))

    def test_recent_terminal_failures_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "jobs.sqlite3"
            now = 1000
            queue = Queue(path, clock=lambda: now)
            self.assertEqual(queue.failed_jobs(), [])
            for reason in ("first failure", "second failure"):
                queue.enqueue({"reason": reason}, max_attempts=1)
                queue.fail(queue.claim(), reason)
                now += 1
            retry_id = queue.enqueue({"retry": True})
            queue.fail(queue.claim(), "try later")
            expired_id = queue.enqueue({"expired": True}, max_attempts=1)
            queue.claim(lease_seconds=1)
            now += 1

            self.assertEqual([job.last_error for job in queue.failed_jobs()],
                             ["second failure", "first failure"])
            self.assertEqual(queue.get(retry_id).state, "pending")
            self.assertEqual(queue.get(expired_id).state, "running")
            with self.assertRaises(ValueError):
                queue.failed_jobs(limit=-1)

            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(main(["--db", str(path), "failed", "--limit", "1"]), 0)
            self.assertEqual([row["last_error"] for row in json.loads(output.getvalue())],
                             ["second failure"])
