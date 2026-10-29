from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import sysconfig
import tempfile
import threading
import unittest
from unittest.mock import Mock, patch

from workqueue import LeaseLost, Queue, work_once
from workqueue.queue import _enable_wal


def startup_barrier(barrier):
    global startup
    startup = barrier


def enqueue_on_startup(path):
    startup.wait(timeout=20)
    return Queue(path).enqueue({"startup": True})


class InitializationTests(unittest.TestCase):
    def test_simultaneous_first_open_preserves_every_enqueue(self):
        context = multiprocessing.get_context("spawn")
        with tempfile.TemporaryDirectory() as directory:
            with ProcessPoolExecutor(max_workers=8, mp_context=context,
                                     initializer=startup_barrier,
                                     initargs=(context.Barrier(8),)) as pool:
                for round_number in range(8):
                    path = str(Path(directory) / f"startup-{round_number}.sqlite3")
                    ids = list(pool.map(enqueue_on_startup, [path] * 8))
                    queue = Queue(path)
                    self.assertEqual(len(set(ids)), 8)
                    self.assertEqual(queue.stats()["pending"], 8)
                    self.assertTrue(all(queue.get(job_id).attempts == 0 for job_id in ids))

    def test_wal_setup_does_not_retry_non_busy_errors(self):
        error = sqlite3.OperationalError("disk I/O error")
        error.sqlite_errorcode = sqlite3.SQLITE_IOERR
        db = Mock()
        db.execute.side_effect = error
        with patch("workqueue.queue.time.sleep") as sleep:
            with self.assertRaises(sqlite3.OperationalError) as caught:
                _enable_wal(db)
        self.assertIs(caught.exception, error)
        sleep.assert_not_called()

    def test_wal_setup_stops_retrying_at_deadline(self):
        error = sqlite3.OperationalError("database is locked")
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        db = Mock()
        db.execute.side_effect = error
        with patch("workqueue.queue.time.monotonic", side_effect=[100, 105]):
            with patch("workqueue.queue.time.sleep") as sleep:
                with self.assertRaises(sqlite3.OperationalError) as caught:
                    _enable_wal(db)
        self.assertIs(caught.exception, error)
        sleep.assert_not_called()


def drain(path):
    queue = Queue(path)
    claimed = []
    while (job := queue.claim()) is not None:
        claimed.append(job.id)
        queue.complete(job)
    return claimed


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "queue.sqlite3"
        self.now = 1000.0
        self.queue = Queue(self.path, clock=lambda: self.now)

    def test_payload_persists_across_connections(self):
        payload = {"unicode": "hello, 世界", "values": [True, None, 42]}
        job_id = self.queue.enqueue(payload)
        self.assertEqual(Queue(self.path).get(job_id).payload, payload)
        self.assertEqual(self.queue.get(job_id).attempts, 0)

    def test_delayed_job_does_not_block_ready_work(self):
        later = self.queue.enqueue("later", delay=10)
        ready = self.queue.enqueue("ready")
        job = self.queue.claim()
        self.assertEqual(job.id, ready)
        self.queue.complete(job)
        self.assertIsNone(self.queue.claim())
        self.now += 10
        self.assertEqual(self.queue.claim().id, later)

    def test_completion_is_terminal(self):
        job_id = self.queue.enqueue({})
        job = self.queue.claim()
        self.queue.complete(job)
        self.assertEqual(self.queue.get(job_id).state, "succeeded")
        self.assertIsNone(self.queue.claim())
        with self.assertRaises(LeaseLost):
            self.queue.complete(job)

    def test_retry_delay_grows_and_attempt_budget_is_bounded(self):
        job_id = self.queue.enqueue({}, max_attempts=3, retry_delay=2)
        first = self.queue.claim()
        self.queue.fail(first, "temporary failure")
        self.assertIsNone(self.queue.claim())
        self.assertEqual(self.queue.get(job_id).available_at, 1002)
        self.now = 1002
        second = self.queue.claim()
        self.queue.fail(second, "again")
        self.assertEqual(self.queue.get(job_id).available_at, 1006)
        self.now = 1006
        third = self.queue.claim()
        self.queue.fail(third, "permanent failure")
        self.assertEqual(self.queue.get(job_id).state, "failed")
        self.assertEqual(self.queue.get(job_id).last_error, "permanent failure")
        self.now += 100
        self.assertIsNone(self.queue.claim())

    def test_reclaimed_job_rejects_all_stale_worker_operations(self):
        self.queue.enqueue({})
        old = self.queue.claim(lease_seconds=5)
        self.now += 5
        new = self.queue.claim(lease_seconds=5)
        self.assertEqual(old.id, new.id)
        self.assertNotEqual(old.lease_token, new.lease_token)
        self.assertEqual(new.attempts, 2)
        for operation in (self.queue.complete, self.queue.renew,
                          lambda job: self.queue.fail(job, "stale")):
            with self.subTest(operation=operation), self.assertRaises(LeaseLost):
                operation(old)
        self.queue.complete(new)

    def test_retry_delay_is_capped_at_one_hour(self):
        job_id = self.queue.enqueue({}, retry_delay=3000)
        job = self.queue.claim()
        self.queue.fail(job, "retry")
        self.now += 3000
        job = self.queue.claim()
        self.queue.fail(job, "retry again")
        self.assertEqual(self.queue.get(job_id).available_at, self.now + 3600)

    def test_expired_lease_cannot_complete_before_reclaim(self):
        self.queue.enqueue({})
        job = self.queue.claim(lease_seconds=5)
        self.now += 5
        with self.assertRaises(LeaseLost):
            self.queue.complete(job)

    def test_last_expired_attempt_becomes_failed(self):
        job_id = self.queue.enqueue({}, max_attempts=1)
        self.queue.claim(lease_seconds=5)
        self.now += 5
        self.assertIsNone(self.queue.claim())
        job = self.queue.get(job_id)
        self.assertEqual((job.state, job.attempts, job.last_error),
                         ("failed", 1, "lease expired"))

    def test_renewal_keeps_current_claim_alive(self):
        self.queue.enqueue({})
        job = self.queue.claim(lease_seconds=5)
        self.now += 4
        self.queue.renew(job, lease_seconds=10)
        self.now += 2
        self.assertIsNone(self.queue.claim())
        self.queue.complete(job)

    def test_worker_calls_handler_and_completes(self):
        self.queue.enqueue({"value": 7})
        seen = []
        result = work_once(self.queue, seen.append)
        self.assertEqual(seen, [{"value": 7}])
        self.assertEqual(result.state, "succeeded")
        self.assertIsNone(work_once(self.queue, seen.append))

    def test_worker_records_handler_failure(self):
        self.queue.enqueue({}, max_attempts=1)

        def broken(payload):
            raise RuntimeError("offline")

        result = work_once(self.queue, broken)
        self.assertEqual(result.state, "failed")
        self.assertEqual(result.last_error, "RuntimeError: offline")

    def test_handler_running_past_lease_cannot_acknowledge(self):
        self.queue.enqueue({})

        def slow(payload):
            self.now += 61

        with self.assertRaises(LeaseLost):
            work_once(self.queue, slow)

    def test_heartbeat_keeps_a_long_running_handler_owned(self):
        job_id = self.queue.enqueue({})
        renewed = threading.Event()
        renew = self.queue.renew

        def signal_renewal(job, **kwargs):
            renew(job, **kwargs)
            renewed.set()

        self.queue.renew = signal_renewal

        def slow(payload):
            self.now += 2
            self.assertTrue(renewed.wait(2), "heartbeat did not renew the lease")
            self.now += 2
            self.assertIsNone(self.queue.claim())

        result = work_once(self.queue, slow, lease_seconds=3, heartbeat_interval=0.01)
        self.assertEqual(result.state, "succeeded")
        self.assertFalse(any(t.name == f"workqueue-lease-{job_id}"
                             for t in threading.enumerate()))

    def test_heartbeat_failure_prevents_acknowledgment(self):
        job_id = self.queue.enqueue({})
        attempted = threading.Event()

        def failed_renewal(job, **kwargs):
            attempted.set()
            raise LeaseLost("lease replaced")

        self.queue.renew = failed_renewal

        def slow(payload):
            self.assertTrue(attempted.wait(2), "heartbeat was not attempted")

        with self.assertRaisesRegex(LeaseLost, "lease replaced"):
            work_once(self.queue, slow, heartbeat_interval=0.01)
        self.assertEqual(self.queue.get(job_id).state, "running")
        self.assertFalse(any(t.name == f"workqueue-lease-{job_id}"
                             for t in threading.enumerate()))

    def test_invalid_heartbeat_does_not_consume_an_attempt(self):
        job_id = self.queue.enqueue({})
        for interval in (0, -1, 5, float("nan"), float("inf")):
            with self.subTest(interval=interval), self.assertRaises(ValueError):
                work_once(self.queue, lambda payload: None, lease_seconds=5,
                          heartbeat_interval=interval)
        self.assertEqual(self.queue.get(job_id).attempts, 0)

    def test_handler_failure_stops_heartbeat_and_schedules_retry(self):
        job_id = self.queue.enqueue({})

        def broken(payload):
            raise RuntimeError("offline")

        result = work_once(self.queue, broken, heartbeat_interval=1)
        self.assertEqual(result.state, "pending")
        self.assertEqual(result.last_error, "RuntimeError: offline")
        self.assertFalse(any(t.name == f"workqueue-lease-{job_id}"
                             for t in threading.enumerate()))

    def test_interruption_stops_heartbeat_without_acknowledgment(self):
        job_id = self.queue.enqueue({})

        def interrupted(payload):
            raise KeyboardInterrupt

        with self.assertRaises(KeyboardInterrupt):
            work_once(self.queue, interrupted, heartbeat_interval=1)
        self.assertEqual(self.queue.get(job_id).state, "running")
        self.assertFalse(any(t.name == f"workqueue-lease-{job_id}"
                             for t in threading.enumerate()))

    def test_invalid_inputs_do_not_create_jobs(self):
        for kwargs in ({"max_attempts": 0}, {"max_attempts": True}, {"max_attempts": 1.5},
                       {"delay": -1}, {"delay": float("nan")},
                       {"retry_delay": float("inf")}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.queue.enqueue({}, **kwargs)
        with self.assertRaises(ValueError):
            self.queue.enqueue({"x": float("nan")})
        with self.assertRaises(ValueError):
            self.queue.claim(lease_seconds=0)
        self.assertIsNone(self.queue.claim())

    def test_missing_job_has_clear_error(self):
        with self.assertRaises(KeyError):
            self.queue.get("missing")

    def test_empty_queue_stats_are_zero(self):
        self.assertEqual(self.queue.stats(), {
            "total": 0, "pending": 0, "running": 0, "succeeded": 0,
            "failed": 0, "ready": 0, "delayed": 0, "expired": 0,
        })

    def test_stats_distinguish_ready_delayed_and_expired_jobs_without_recovery(self):
        self.queue.enqueue({})
        self.queue.complete(self.queue.claim())
        self.queue.enqueue({}, max_attempts=1)
        self.queue.fail(self.queue.claim(), "permanent error")
        self.queue.enqueue({})
        self.queue.claim(lease_seconds=20)
        expired_id = self.queue.enqueue({})
        self.queue.claim(lease_seconds=5)
        self.queue.enqueue({})
        self.queue.enqueue({}, delay=50)
        self.now += 5
        before = self.queue.get(expired_id)
        self.assertEqual(self.queue.stats(), {
            "total": 6, "pending": 2, "running": 2, "succeeded": 1,
            "failed": 1, "ready": 1, "delayed": 1, "expired": 1,
        })
        self.assertEqual(self.queue.get(expired_id), before)

    def test_stats_move_delayed_jobs_to_ready_at_availability_time(self):
        self.queue.enqueue({}, delay=5)
        self.assertEqual(self.queue.stats()["delayed"], 1)
        self.now += 5
        counts = self.queue.stats()
        self.assertEqual((counts["pending"], counts["ready"], counts["delayed"]), (1, 1, 0))

    def test_stats_read_committed_snapshot_while_another_connection_writes(self):
        job_id = self.queue.enqueue({})
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("BEGIN IMMEDIATE")
            writer.execute("UPDATE jobs SET state='succeeded' WHERE id=?", (job_id,))
            counts = self.queue.stats()
            self.assertEqual((counts["pending"], counts["succeeded"]), (1, 0))
            writer.commit()
        self.assertEqual(self.queue.stats()["succeeded"], 1)

    def test_multiple_processes_claim_each_job_once(self):
        job_ids = {self.queue.enqueue(i) for i in range(40)}
        with ProcessPoolExecutor(max_workers=4,
                                 mp_context=multiprocessing.get_context("spawn")) as pool:
            batches = list(pool.map(drain, [str(self.path)] * 4))
        claimed = [job_id for batch in batches for job_id in batch]
        self.assertEqual(len(claimed), len(set(claimed)))
        self.assertEqual(set(claimed), job_ids)
        self.assertTrue(all(self.queue.get(job_id).state == "succeeded" for job_id in job_ids))

    def test_abrupt_process_exit_leaves_recoverable_claim(self):
        job_id = self.queue.enqueue({})
        code = ("import os,sys; from workqueue import Queue; "
                "q=Queue(sys.argv[1],clock=lambda:1000); "
                "q.claim(lease_seconds=5); os._exit(7)")
        result = subprocess.run([sys.executable, "-c", code, str(self.path)], timeout=15)
        self.assertEqual(result.returncode, 7)
        self.assertEqual(self.queue.get(job_id).state, "running")
        self.now += 6
        recovered = self.queue.claim()
        self.assertEqual((recovered.id, recovered.attempts), (job_id, 2))
        self.queue.complete(recovered)


class CLITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "cli.sqlite3")

    def run_cli(self, *args, cwd=None):
        return subprocess.run([sys.executable, "-m", "workqueue", "--db", self.path, *args],
                              cwd=cwd, capture_output=True, text=True, timeout=15)

    def test_module_entrypoint_imports_handler_from_current_directory(self):
        directory = Path(self.temp.name)
        (directory / "my_handlers.py").write_text(
            "from pathlib import Path\ndef handle(payload):\n"
            "    Path('handled.txt').write_text(payload['message'])\n",
            encoding="utf-8")
        result = self.run_cli("enqueue", '{"message":"local handler ran"}')
        self.assertEqual(result.returncode, 0, result.stderr)
        job_id = result.stdout.strip()
        result = self.run_cli("worker", "my_handlers:handle", "--once", cwd=directory)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((directory / "handled.txt").read_text(), "local handler ran")
        self.assertEqual(Queue(self.path).get(job_id).state, "succeeded")

    def test_installed_console_entrypoint_runs_packaged_handler(self):
        executable = Path(sysconfig.get_path("scripts")) / (
            "workqueue.exe" if os.name == "nt" else "workqueue")
        result = self.run_cli("enqueue", '{"message":"packaged handler ran"}')
        self.assertEqual(result.returncode, 0, result.stderr)
        job_id = result.stdout.strip()
        result = subprocess.run(
            [str(executable), "--db", self.path, "worker", "workqueue.examples:echo", "--once"],
            cwd=self.temp.name, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("packaged handler ran", result.stdout)
        self.assertEqual(Queue(self.path).get(job_id).state, "succeeded")

    def test_enqueue_worker_show_round_trip(self):
        result = self.run_cli("enqueue", '{"message":"hello"}')
        self.assertEqual(result.returncode, 0, result.stderr)
        job_id = result.stdout.strip()
        result = self.run_cli("worker", "workqueue.examples:echo", "--once",
                              "--heartbeat-interval", "1")
        self.assertEqual(result.returncode, 0, result.stderr)
        result = self.run_cli("show", job_id)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["state"], "succeeded")

    def test_invalid_json_returns_actionable_error(self):
        result = self.run_cli("enqueue", "not-json")
        self.assertEqual(result.returncode, 2)
        self.assertIn("workqueue:", result.stderr)
        self.assertNotIn("Traceback", result.stderr)

    def test_stats_command_reports_pending_jobs_as_json(self):
        self.assertEqual(self.run_cli("enqueue", "{}").returncode, 0)
        self.assertEqual(self.run_cli("enqueue", "{}", "--delay", "3600").returncode, 0)
        result = self.run_cli("stats")
        self.assertEqual(result.returncode, 0, result.stderr)
        counts = json.loads(result.stdout)
        self.assertEqual((counts["total"], counts["pending"], counts["ready"], counts["delayed"]),
                         (2, 2, 1, 1))

    def test_invalid_handler_does_not_claim_job(self):
        result = self.run_cli("enqueue", "{}")
        job_id = result.stdout.strip()
        result = self.run_cli("worker", "no_such_module:handler", "--once")
        self.assertEqual(result.returncode, 2)
        self.assertEqual(Queue(self.path).get(job_id).attempts, 0)

    def test_invalid_heartbeat_does_not_claim_job(self):
        result = self.run_cli("enqueue", "{}")
        job_id = result.stdout.strip()
        result = self.run_cli("worker", "workqueue.examples:echo", "--once",
                              "--heartbeat-interval", "60")
        self.assertEqual(result.returncode, 2)
        self.assertIn("shorter than lease_seconds", result.stderr)
        self.assertEqual(Queue(self.path).get(job_id).attempts, 0)


if __name__ == "__main__":
    unittest.main()
