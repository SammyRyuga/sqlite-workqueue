"""Check the package and its command-line example from a clean installation."""

from importlib.metadata import distribution
import json
from pathlib import Path
import shutil
import subprocess
import sys
import sysconfig
from tempfile import TemporaryDirectory

import workqueue
from workqueue import Queue, work_once


def run(*command, cwd):
    return subprocess.check_output(command, cwd=cwd, text=True).strip()


def main(version):
    package = distribution("sqlite-workqueue")
    assert package.version == version, package.version
    assert package.metadata["Description-Content-Type"] == "text/markdown"
    assert package.metadata["License-Expression"] == "MIT"
    assert package.metadata["License-File"] == "LICENSE"
    assert package.metadata["Requires-Python"] == ">=3.11"
    assert not package.requires, package.requires
    source = "https://github.com/SammyRyuga/sqlite-workqueue"
    assert set(package.metadata.get_all("Project-URL")) == {
        f"Source, {source}",
        f"Issues, {source}/issues",
        f"Documentation, {source}#readme",
    }
    installed_path = Path(workqueue.__file__).resolve()
    assert installed_path.is_relative_to(Path(sys.prefix).resolve()), installed_path
    console = shutil.which("workqueue", path=sysconfig.get_path("scripts"))
    assert console, "The installed console command is missing"

    with TemporaryDirectory() as directory:
        queue = Queue(Path(directory) / "python.sqlite3")
        job_id = queue.enqueue({"message": "hello"}, max_attempts=3, retry_delay=2)
        messages = []
        work_once(queue, lambda payload: messages.append(payload["message"]))
        assert messages == ["hello"]
        assert queue.get(job_id).state == "succeeded"

        payload = {"message": "hello"}
        db = ("--db", "jobs.sqlite3")
        job_id = run(console, *db, "enqueue", json.dumps(payload), cwd=directory)
        worker = run(
            console, *db, "worker", "workqueue.examples:echo", "--once", cwd=directory
        )
        echo, summary = worker.splitlines()
        assert json.loads(echo) == payload
        assert json.loads(summary) == {"id": job_id, "state": "succeeded", "attempts": 1}
        module = (sys.executable, "-I", "-m", "workqueue")
        for entry_point in ((console,), module):
            job = json.loads(run(*entry_point, *db, "show", job_id, cwd=directory))
            assert job["id"] == job_id and job["state"] == "succeeded"
            assert job["payload"] == payload and job["attempts"] == 1

    print(f"sqlite-workqueue {version} passed installed checks ({installed_path})")


if __name__ == "__main__":
    main(sys.argv[1])
