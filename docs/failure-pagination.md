# Failure pagination performance

Failure inspection previously used the state/lease index and a temporary sort,
even when only 20 rows were requested. Adding a partial index on
`(updated_at DESC, id)` alone did not change that plan in our experiment.

The partial `failed_jobs_order` index includes `(state, updated_at DESC, id)`
for failed rows only. Including the state lets SQLite search the index and return
rows in the requested order. Continuation queries also use `updated_at <= ?`:
SQLite can seek to that timestamp before checking the ID tie-breaker. Without
this range bound, deep pages still scanned newer failures.

This follows SQLite's guidance on [searching and sorting together](https://www.sqlite.org/queryplanner.html)
and [partial indexes](https://www.sqlite.org/partialindex.html). API ordering and
cursor semantics are unchanged.

## Reproduce

After installing the project, run:

```sh
python benchmarks/failed_jobs.py
```

The script creates and removes only temporary databases. It compares the old
query/schema against the current API on 100,000 jobs, with either all jobs failed
or one in ten failed. It includes tied timestamps, verifies every returned batch
against independently sorted fixture IDs, and prints timings and query plans.
Seven calls are measured at each cursor position. Times include connection setup,
cursor lookup, query execution and Job decoding, but exclude fixture creation and
Queue initialization. Baseline measurements precede indexed measurements; this is
a local diagnostic, not a controlled throughput study.

## Observed results

Windows, Python 3.13.9, SQLite 3.51.0; measured 11 November 2026.
Median milliseconds for a batch of 20:

| Failed / total jobs | Page | Previous | Indexed |
|---|---|---:|---:|
| 100,000 / 100,000 | First | 59.398 | 1.096 |
| 100,000 / 100,000 | After midpoint | 38.043 | 1.180 |
| 100,000 / 100,000 | Near oldest | 17.111 | 1.165 |
| 10,000 / 100,000 | First | 21.696 | 1.231 |
| 10,000 / 100,000 | After midpoint | 15.792 | 1.463 |
| 10,000 / 100,000 | Near oldest | 12.210 | 1.336 |

Both indexed workloads used `failed_jobs_order` with no temporary sort.
Continuation plans included a state and timestamp range search. Index creation
took approximately 69.5 ms / 30.1 ms and added 2,686,976 / 270,336 bytes,
respectively, on these fixtures.

## Costs and limits

The index is created automatically on first open after upgrade. Building it needs
a write lock and storage; this can delay other writers or encounter the existing
five-second lock timeout. Each new terminal failure also updates the index.
Write throughput and very large database upgrades have not been benchmarked.
Existing jobs, leases, payloads and persistent enqueue keys are preserved.

The timestamp bound avoids scanning newer timestamp groups; a very large group
with identical timestamps can still require scanning many IDs within that group.
This is not a constant-time pagination guarantee. Timing varies with data,
SQLite version, storage and contention. No multi-host or production reliability
claim follows from this experiment.
