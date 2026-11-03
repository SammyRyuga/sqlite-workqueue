from hashlib import sha256
from pathlib import Path
import tempfile
import unittest

from workqueue import Queue, work_once
from workqueue.examples import write_snapshot


class SnapshotExampleTests(unittest.TestCase):
    def test_retry_after_publication_before_acknowledgement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            now = 1000
            queue = Queue(root / "jobs.sqlite3", clock=lambda: now)
            output = root / "snapshots"
            payload = {"text": "snapshot café", "directory": str(output)}
            data = payload["text"].encode("utf-8")
            target = output / (sha256(data).hexdigest() + ".txt")
            job_id = queue.enqueue(payload, idempotency_key="snapshot:example")
            claimed = queue.claim(lease_seconds=5)
            write_snapshot(claimed.payload)
            self.assertEqual(target.read_bytes(), data)

            now += 5
            self.assertEqual(queue.enqueue(payload, idempotency_key="snapshot:example"), job_id)
            result = work_once(queue, write_snapshot)
            self.assertEqual((result.id, result.state, result.attempts), (job_id, "succeeded", 2))
            self.assertEqual(list(output.iterdir()), [target])
            self.assertEqual(target.read_bytes(), data)
