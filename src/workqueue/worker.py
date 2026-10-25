import threading
from typing import Any, Callable

from .queue import Job, Queue, _seconds


def work_once(queue: Queue, handler: Callable[[Any], None], *,
              lease_seconds: float = 60,
              heartbeat_interval: float | None = None) -> Job | None:
    """Run one job, optionally renewing its lease until the handler exits."""
    lease_seconds = _seconds(lease_seconds, "lease_seconds", positive=True)
    if heartbeat_interval is not None:
        heartbeat_interval = _seconds(heartbeat_interval, "heartbeat_interval", positive=True)
        if heartbeat_interval >= lease_seconds:
            raise ValueError("heartbeat_interval must be shorter than lease_seconds")
    job = queue.claim(lease_seconds=lease_seconds)
    if job is None:
        return None
    stopped = threading.Event()
    renewal_errors = []

    def heartbeat():
        while not stopped.wait(heartbeat_interval):
            try:
                queue.renew(job, lease_seconds=lease_seconds)
            except Exception as exc:
                renewal_errors.append(exc)
                return

    thread = None
    if heartbeat_interval is not None:
        thread = threading.Thread(target=heartbeat, name=f"workqueue-lease-{job.id}", daemon=True)
        thread.start()
    handler_error = None
    try:
        handler(job.payload)
    except Exception as exc:
        handler_error = exc
    finally:
        stopped.set()
        if thread is not None:
            thread.join()
    if renewal_errors:
        raise renewal_errors[0]
    if handler_error is not None:
        queue.fail(job, f"{type(handler_error).__name__}: {handler_error}")
    else:
        queue.complete(job)
    return queue.get(job.id)
