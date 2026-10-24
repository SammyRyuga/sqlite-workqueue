from typing import Any, Callable

from .queue import Job, Queue


def work_once(queue: Queue, handler: Callable[[Any], None], *,
              lease_seconds: float = 60) -> Job | None:
    """Run one available job. Exceptions schedule a retry; lost leases propagate."""
    job = queue.claim(lease_seconds=lease_seconds)
    if job is None:
        return None
    try:
        handler(job.payload)
    except Exception as exc:
        queue.fail(job, f"{type(exc).__name__}: {exc}")
    else:
        queue.complete(job)
    return queue.get(job.id)
