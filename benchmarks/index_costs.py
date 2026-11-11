"""Measure failure index costs and concurrent upgrades on disposable databases.

Run after installing the package: python benchmarks/index_costs.py
This can take several minutes and needs space for a million-row local database.
"""

from concurrent.futures import ProcessPoolExecutor
from contextlib import closing
import json
import multiprocessing
from pathlib import Path
import platform
import sqlite3
from statistics import median
import tempfile
from time import perf_counter

from workqueue import Queue


def initialize(barrier):
    global ready
    ready = barrier


def open_queue(path):
    ready.wait(timeout=30)
    start = perf_counter()
    queue = Queue(path, clock=lambda: 1000)
    job_id = queue.enqueue({"producer": True}, idempotency_key="same-request")
    return (perf_counter() - start) * 1000, job_id


def populate(queue, count):
    with closing(sqlite3.connect(queue.path)) as db:
        db.executemany(
            """INSERT INTO jobs
            (id,payload,state,attempts,max_attempts,retry_delay,available_at,created_at,updated_at)
            VALUES (?,'{}','failed',1,1,1,0,0,1000)""",
            ((f"history-{i:08d}",) for i in range(count)),
        )
        db.commit()


def writes():
    results = []
    for round_number in range(4):
        for indexed in (False, True) if round_number % 2 == 0 else (True, False):
            with tempfile.TemporaryDirectory() as directory:
                q = Queue(Path(directory) / "queue.db", clock=lambda: 1000)
                if not indexed:
                    with closing(sqlite3.connect(q.path)) as db:
                        db.execute("DROP INDEX failed_jobs_order")
                populate(q, 100000)
                times = []
                for i in range(250):
                    start = perf_counter()
                    q.enqueue({"n": i}, max_attempts=1)
                    times.append((perf_counter() - start) * 1000)
                enqueue = median(times)
                claims = [q.claim() for _ in range(250)]
                times = []
                start_all = perf_counter()
                for job in claims:
                    start = perf_counter()
                    q.fail(job, "terminal error")
                    times.append((perf_counter() - start) * 1000)
                elapsed = perf_counter() - start_all
                assert q.stats()["failed"] == 100250
                results.append(
                    dict(
                        round=round_number,
                        indexed=indexed,
                        enqueue_median_ms=round(enqueue, 3),
                        failure_median_ms=round(median(times), 3),
                        terminal_failures_per_second=round(250 / elapsed, 1),
                    )
                )
    return results


def bounded_page(queue, after):
    """PR #1's continuation query, retained only for the benchmark comparison."""
    with queue._connect() as db:
        stamp = db.execute(
            "SELECT updated_at FROM jobs WHERE id=? AND state='failed'", (after,)
        ).fetchone()[0]
        rows = db.execute(
            """SELECT * FROM jobs WHERE state='failed'
            AND updated_at <= ? AND (updated_at < ? OR id > ?)
            ORDER BY updated_at DESC, id LIMIT 20""",
            (stamp, stamp, after),
        ).fetchall()
    return [queue._job(row) for row in rows]


def upgrade():
    context = multiprocessing.get_context("spawn")
    with tempfile.TemporaryDirectory() as directory:
        q = Queue(Path(directory) / "queue.db", clock=lambda: 1000)
        populate(q, 1000000)
        active = q.enqueue({"active": True}, max_attempts=2, idempotency_key="active")
        job = q.claim(lease_seconds=1)
        with closing(sqlite3.connect(q.path)) as db:
            db.execute("DROP INDEX failed_jobs_order")
        start = perf_counter()
        with ProcessPoolExecutor(
            max_workers=8,
            mp_context=context,
            initializer=initialize,
            initargs=(context.Barrier(8),),
        ) as pool:
            results = list(pool.map(open_queue, [q.path] * 8))
        assert len({r[1] for r in results}) == 1
        assert q.get(active) == job
        recovery = Queue(q.path, clock=lambda: 1002)
        claims = [recovery.claim(), recovery.claim()]
        recovered = next(j for j in claims if j.id == active)
        assert recovered.attempts == 2
        recovery.fail(recovered, "after crash")
        with closing(sqlite3.connect(q.path)) as db:
            integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
            count = db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0]
        assert integrity == "ok" and count == 1000002
        measurements = {}
        for label, read_page in (
            ("timestamp_bound", bounded_page),
            ("two_ranges", lambda queue, after: queue.failed_jobs(after=after)),
        ):
            durations = []
            for _ in range(7):
                start_page = perf_counter()
                page = read_page(q, "history-00999970")
                durations.append((perf_counter() - start_page) * 1000)
                assert [j.id for j in page] == [
                    f"history-{i:08d}" for i in range(999971, 999991)
                ]
            measurements[label] = round(median(durations), 3)
        return dict(
            rows=1000000,
            processes=8,
            open_and_enqueue_ms=[round(r[0], 3) for r in results],
            total_seconds=round(perf_counter() - start, 3),
            integrity=integrity,
            job_count=count,
            active_lease_preserved=True,
            expired_lease_recovered=True,
            deep_tied_page_median_ms=measurements,
        )


if __name__ == "__main__":
    result = dict(
        python=platform.python_version(),
        sqlite=sqlite3.sqlite_version,
        platform=platform.system(),
        writes=writes(),
    )
    result["upgrade"] = upgrade()
    print(json.dumps(result, indent=2))
