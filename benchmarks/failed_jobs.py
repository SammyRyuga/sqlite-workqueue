"""Compare failure pagination before/after indexing on disposable synthetic data.

Install the project, then run: python benchmarks/failed_jobs.py
Timings include a connection, cursor lookup, query, and Job decoding; not Queue setup.
"""
from contextlib import closing
import json
from pathlib import Path
import platform
import sqlite3
from statistics import median
import tempfile
from time import perf_counter

from workqueue import Queue


def legacy_page(queue, after):
    query = "SELECT * FROM jobs WHERE state='failed'"
    parameters = []
    with queue._connect() as db:
        if after is not None:
            stamp = db.execute("SELECT updated_at FROM jobs WHERE id=? AND state='failed'",
                               (after,)).fetchone()[0]
            query += " AND (updated_at < ? OR (updated_at = ? AND id > ?))"
            parameters = [stamp, stamp, after]
        rows = db.execute(query + " ORDER BY updated_at DESC, id LIMIT 20", parameters).fetchall()
    return [queue._job(row) for row in rows]


def measure():
    results = {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
               "platform": platform.system(), "rows": 100000, "repetitions": 7, "workloads": []}
    for failure_stride in (1, 10):
        with tempfile.TemporaryDirectory(prefix="workqueue-benchmark-") as directory:
            path = Path(directory) / "jobs.sqlite3"
            queue = Queue(path)
            with closing(sqlite3.connect(path)) as db:
                # Simulate the schema before the ordering index, only in this temporary DB.
                db.execute("DROP INDEX failed_jobs_order")
                db.executemany("""INSERT INTO jobs
                    (id,payload,state,attempts,max_attempts,retry_delay,available_at,created_at,updated_at)
                    VALUES (?,'{}',?,1,1,1,0,0,?)""",
                    ((f"{i:08d}", "failed" if i % failure_stride == 0 else "succeeded", i // 4)
                     for i in range(results["rows"])))
                db.commit()
                size_before = db.execute("PRAGMA page_count").fetchone()[0]
                page_size = db.execute("PRAGMA page_size").fetchone()[0]
            expected = sorted(range(0, results["rows"], failure_stride),
                              key=lambda i: (-(i // 4), i))
            ordered_ids = [f"{i:08d}" for i in expected]
            positions = (None, 20, len(expected) // 2, len(expected) - 40)
            workload = {"failed_rows": len(expected), "measurements": []}
            for variant in ("baseline", "indexed"):
                if variant == "indexed":
                    started = perf_counter()
                    queue = Queue(path)
                    workload["index_build_ms"] = round((perf_counter() - started) * 1000, 3)
                    with closing(sqlite3.connect(path)) as db:
                        workload["index_bytes"] = (db.execute("PRAGMA page_count").fetchone()[0]
                                                    - size_before) * page_size
                for position in positions:
                    after = None if position is None else ordered_ids[position]
                    start = 0 if position is None else position + 1
                    durations = []
                    for _ in range(results["repetitions"]):
                        started = perf_counter()
                        jobs = legacy_page(queue, after) if variant == "baseline" else queue.failed_jobs(after=after)
                        durations.append((perf_counter() - started) * 1000)
                        assert [job.id for job in jobs] == ordered_ids[start:start + 20]
                    query = "SELECT * FROM jobs WHERE state='failed'"
                    parameters = []
                    if after is not None:
                        stamp = expected[position] // 4
                        query += (" AND (updated_at < ? OR (updated_at = ? AND id > ?))"
                                  if variant == "baseline" else
                                  " AND updated_at <= ? AND (updated_at < ? OR id > ?)")
                        parameters = [stamp, stamp, after]
                    with closing(sqlite3.connect(path)) as db:
                        plan = [row[3] for row in db.execute(
                            "EXPLAIN QUERY PLAN " + query + " ORDER BY updated_at DESC,id LIMIT 20",
                            parameters)]
                    workload["measurements"].append({"variant": variant, "cursor_position": position,
                        "median_ms": round(median(durations), 3), "plan": plan})
            results["workloads"].append(workload)
    return results


if __name__ == "__main__":
    print(json.dumps(measure(), indent=2))
