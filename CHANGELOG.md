# Changelog

## 0.1.0

First public release of the persistent job queue for trusted Python programs
running on one host. Includes atomic job claims, lease renewal and recovery,
bounded retries, and producer idempotency. The Python API and CLI support
submitting work, running synchronous handlers, inspecting queue state, and
paging through terminal failures. There are no runtime dependencies.
