"""Small handlers for trying the command-line worker."""

from hashlib import sha256
import json
import os
from pathlib import Path
from tempfile import NamedTemporaryFile


def echo(payload):
    print(json.dumps(payload), flush=True)


def write_snapshot(payload):
    """Publish a UTF-8 snapshot under its content hash."""
    data = payload["text"].encode("utf-8")
    directory = Path(payload["directory"])
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / (sha256(data).hexdigest() + ".txt")
    with NamedTemporaryFile(dir=directory, delete=False) as temporary:
        pending = Path(temporary.name)
        try:
            temporary.write(data)
            temporary.close()
            os.replace(pending, target)
        finally:
            temporary.close()
            pending.unlink(missing_ok=True)
