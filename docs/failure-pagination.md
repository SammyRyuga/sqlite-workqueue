# Failure pagination performance

Failure inspection previously used the state/lease index and a temporary sort,
even when only 20 rows were requested. Adding a partial index on
`(updated_at DESC, id)` alone did not change that plan in our experiment.

The partial `failed_jobs_order` index includes `(state, updated_at DESC, id)`
for failed rows only. Including the state lets SQLite search the index and return
rows in the requested order. Continuation queries merge two disjoint ranges:
the cursor timestamp with larger IDs, and strictly older timestamps. `UNION ALL`
with a single outer ordering and limit lets SQLite seek into each range, including
the ID within a large timestamp tie. A timestamp bound alone still scanned the
preceding IDs in that group. API ordering and cursor validation are unchanged.

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

## Original index experiment

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
Existing jobs, leases, payloads and persistent enqueue keys are preserved.

This is not a constant-time pagination guarantee. Timing varies with data,
SQLite version, storage and contention. No multi-host or production reliability
claim follows from this experiment.

## Write cost, large upgrades and timestamp ties

Run `python benchmarks/index_costs.py` after installing the package. It creates
only temporary databases, needs space for a million-row database, and may take
several minutes. It alternates indexed/unindexed order across four pairs, each
with 100,000 historical failures and 250 enqueue/terminal-failure operations.
Failures are preclaimed: this measures `Queue.fail`, not the full worker lifecycle.
Timings include normal connection setup, SQLite locking, commit and checkpoint
costs. Fixture creation and claiming are outside the write measurements.

On the same Windows/Python/SQLite versions above, measured 11 November 2026:

| Round | Index | Enqueue median (ms) | Terminal failure median (ms) | Failures/s |
|---|---|---:|---:|---:|
| 1 | No | 4.455 | 7.379 | 136.5 |
| 1 | Yes | 6.695 | 6.914 | 143.9 |
| 2 | No | 5.552 | 6.452 | 156.6 |
| 2 | Yes | 6.727 | 6.270 | 155.1 |
| 3 | No | 6.087 | 6.583 | 143.6 |
| 3 | Yes | 6.853 | 6.534 | 154.7 |
| 4 | No | 3.988 | 3.910 | 245.2 |
| 4 | Yes | 6.823 | 4.383 | 218.1 |

The index requires additional writes when jobs enter the failed state. These
end-to-end measurements show substantial run-to-run storage variation; they do
not isolate a stable percentage overhead or establish a throughput guarantee.
Even enqueue, which does not insert a failed-index entry, varied substantially.
No write-path change was justified by these measurements.

Eight spawned processes simultaneously reopened an existing million-failure
database after removing the index to simulate an upgrade. Each opened and
enqueued the same idempotency key in 1.27–1.45 seconds. All returned one shared
job ID. Existing leases and keys survived, an expired lease was recovered, the
final job count was 1,000,002, and `PRAGMA integrity_check` returned `ok`.

On this fixture, a 20-row page near the end of one million equal-timestamp
failures took **124.116 ms** with the previous timestamp-bound query and
**1.284 ms** with the two-range query (seven calls each, median). Both paths
include cursor lookup, connection setup and decoding; expected IDs are asserted.
The previous query is measured first, so this is a local diagnostic. The
regression test uses a generous SQLite VM-instruction ceiling instead of a
wall-clock threshold and also verifies a page crossing into an older timestamp.

Upgrade regression tests additionally cover four competing processes, a real
write-lock timeout (shortened to 50 ms in the test), and an abrupt child-process
exit during index construction. They verify rollback, integrity, key/lease
preservation and successful reopening. An already-initialized database can open
while another connection holds a write transaction. No locking defect requiring
a production change was found. If an upgrade times out, let the existing writer
finish and retry opening; do not delete the database or its WAL files. These
process-exit tests do not simulate power loss or storage hardware failure.
