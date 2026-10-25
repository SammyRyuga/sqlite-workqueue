# sqlite-workqueue

A small persistent job queue for Python programs that run on one machine.
Producers enqueue JSON payloads; separate worker processes claim jobs, run a
trusted handler, and acknowledge completion. It uses SQLite and has no runtime
dependencies outside Python's standard library.

The first version focuses on queue correctness: atomic claims, bounded retries,
and recovery when a worker exits before acknowledging its job. It is useful for
local batch processing and learning how durable worker coordination works.

## Try it

Requires Python 3.11 or newer. From a checkout:

```sh
python -m pip install .
workqueue --db jobs.sqlite3 enqueue '{"message":"hello"}'
workqueue --db jobs.sqlite3 worker workqueue.examples:echo --once
workqueue --db jobs.sqlite3 show JOB_ID
```

Use the ID printed by `enqueue` in place of `JOB_ID`. The worker prints the
payload, then a JSON summary of the job's state. On PowerShell, use the Python
example below to avoid shell-specific JSON quoting.

```python
from workqueue import Queue, work_once

queue = Queue("jobs.sqlite3")
job_id = queue.enqueue({"message": "hello"}, max_attempts=3, retry_delay=2)
work_once(queue, lambda payload: print(payload["message"]))
print(queue.get(job_id).state)  # succeeded
```

For your own worker, define a callable that accepts a decoded JSON payload:

```python
# my_handlers.py, in your current directory or installed Python package
def handle(payload):
    print(payload["message"].upper())
```

```sh
workqueue --db jobs.sqlite3 worker my_handlers:handle
```

Start that command in more than one terminal to run separate workers. Without
`--once`, workers poll until interrupted. Handlers that raise an exception cause
the job to retry; error text is kept on the job. A one-shot worker exits with 0
after success or when no job is available, 1 after a handler failure, and 2 for
an invalid command input or a lost lease. Ctrl+C exits with 130.

For long-running handlers, enable background lease renewal:

```sh
workqueue --db jobs.sqlite3 worker my_handlers:handle --lease-seconds 60 --heartbeat-interval 15
```

The interval must be positive and shorter than the lease duration. The Python
equivalent is `work_once(queue, handler, lease_seconds=60, heartbeat_interval=15)`.
The renewal thread stops and is joined when the handler exits, including on
interruption. If renewal fails, the worker propagates that error without
acknowledging or retrying the job under uncertain ownership. Recovery happens
through the normal lease-expiry mechanism.

## Claims and recovery

Jobs move from `pending` to `running`, then to `succeeded` or back to `pending`.
A failed attempt at its retry limit becomes `failed`. `max_attempts` includes the
first attempt. Retries after handler exceptions wait `retry_delay * 2^(attempt-1)`
seconds, capped at one hour. `enqueue(..., delay=10)` defers the first attempt.

Claiming uses a short `BEGIN IMMEDIATE` transaction. Selecting a pending job and
recording its ownership happen under the same write lock. Handler code runs
outside that transaction, so a slow handler does not hold the database lock.

Each claim receives a new random lease token. Completion, failure and renewal
check both that token and the lease's expiry. An old worker cannot acknowledge a
claim after another worker has reclaimed it. The next `claim()` requeues expired
jobs immediately, or marks them failed if their attempt budget is exhausted.
`get()` is a snapshot; it does not perform lease recovery itself.

For direct API use, `queue.claim(lease_seconds=60)` returns a `Job` or `None`.
Use `queue.complete(job)`, `queue.fail(job, "reason")` or
`queue.renew(job, lease_seconds=60)` with that claim. These raise `LeaseLost` when
ownership has expired or changed. Renewal sets expiry relative to the renewal
time; choose an interval that keeps the lease alive.

## Limits

- Delivery is **at least once**, not exactly once. A handler can finish a side
  effect and crash before acknowledging it. Make handlers idempotent. Lease
  tokens protect queue state, not side effects in another system.
- All workers must use a local database on the same host. Do not put the SQLite
  WAL database on a network filesystem or file-sync folder. SQLite serializes
  writers; this is not a replacement for a multi-host broker.
- Heartbeats are opt-in. Without them, set a lease longer than your handler's
  expected runtime, or use the direct API to renew it. A heartbeat cannot
  interrupt a handler that has already started or guarantee renewal during a
  process pause. An expired handler may overlap a new attempt, even though it
  can no longer acknowledge the job.
- Scheduling uses the host wall clock. Significant clock changes can affect
  delays and lease expiry.
- Handlers are trusted local Python code, not a sandbox. Database payloads and
  error messages are stored in plaintext. Do not enqueue credentials or private
  records into a shared database.
- There is no cancellation, periodic scheduling, schema migration system,
  result storage or HTTP service yet. Database lock waits time out after five
  seconds. This is an early implementation, not a production reliability claim.

## Development

```sh
python -m pip install -e .
python -m unittest discover -s tests -v
```

Tests cover persistence, delayed availability, retries, lease fencing, lease
renewal, handler failures, an abrupt worker process exit, competing worker
processes and the CLI. The CI workflow runs these on Linux and Windows with
Python 3.11 and 3.13. Workflow presence does not imply a successful hosted run.

The design follows SQLite's [transaction documentation](https://www.sqlite.org/lang_transaction.html)
and [WAL constraints](https://www.sqlite.org/wal.html).

## Provenance and license

Developed with OpenAI Codex assistance for design, implementation, documentation
and automated testing. This does not imply the account owner manually authored
or tested every line. MIT licensed; see [LICENSE](LICENSE).
