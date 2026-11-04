from contextlib import redirect_stdout
from io import StringIO
import json
from pathlib import Path
import tempfile
import unittest

from workqueue import Queue
from workqueue.cli import main


class FailedJobsTests(unittest.TestCase):
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
