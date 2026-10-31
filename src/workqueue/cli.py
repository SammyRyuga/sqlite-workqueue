import argparse
from dataclasses import asdict
import importlib
import json
import sqlite3
import sys
import time

from .queue import LeaseLost, Queue, _seconds
from .worker import work_once


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Local SQLite job queue")
    parser.add_argument("--db", default="jobs.sqlite3")
    commands = parser.add_subparsers(dest="command", required=True)
    enqueue = commands.add_parser("enqueue", help="Queue a JSON payload")
    enqueue.add_argument("payload")
    enqueue.add_argument("--max-attempts", type=int, default=3)
    enqueue.add_argument("--retry-delay", type=float, default=1)
    enqueue.add_argument("--delay", type=float, default=0)
    enqueue.add_argument("--idempotency-key", help="Reuse a job ID when retrying the same enqueue")
    show = commands.add_parser("show", help="Inspect a job")
    show.add_argument("job_id")
    commands.add_parser("stats", help="Count queue states without claiming or recovering jobs")
    worker = commands.add_parser("worker", help="Run a trusted Python handler")
    worker.add_argument("handler", help="module:function")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--lease-seconds", type=float, default=60)
    worker.add_argument("--heartbeat-interval", type=float,
                        help="Renew leases at this interval in seconds (disabled by default)")
    worker.add_argument("--poll-interval", type=float, default=1)
    args = parser.parse_args(argv)
    try:
        queue = Queue(args.db)
        if args.command == "enqueue":
            print(queue.enqueue(json.loads(args.payload), max_attempts=args.max_attempts,
                                retry_delay=args.retry_delay, delay=args.delay,
                                idempotency_key=args.idempotency_key))
        elif args.command == "show":
            print(json.dumps(asdict(queue.get(args.job_id))))
        elif args.command == "stats":
            print(json.dumps(queue.stats()))
        else:
            module, name = args.handler.split(":", 1)
            handler = getattr(importlib.import_module(module), name)
            if not callable(handler):
                raise ValueError("handler must be callable")
            _seconds(args.lease_seconds, "lease_seconds", positive=True)
            _seconds(args.poll_interval, "poll_interval", positive=True)
            while True:
                job = work_once(queue, handler, lease_seconds=args.lease_seconds,
                                heartbeat_interval=args.heartbeat_interval)
                if job:
                    print(json.dumps({"id": job.id, "state": job.state,
                                      "attempts": job.attempts}), flush=True)
                if args.once:
                    return 1 if job and job.state != "succeeded" else 0
                if job is None:
                    time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        return 130
    except (ValueError, KeyError, ImportError, AttributeError, LeaseLost, OSError, sqlite3.Error) as exc:
        print(f"workqueue: {exc}", file=sys.stderr)
        return 2
    return 0
