"""Small handler for trying the command-line worker."""

import json


def echo(payload):
    print(json.dumps(payload), flush=True)
