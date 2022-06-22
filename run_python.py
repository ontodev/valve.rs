import json
import sys
import time
import valve
from pprint import pformat


def log(message, suppress_time=True):
    if not suppress_time:
        print(f"{time.asctime()} {message}", file=sys.stderr)
    else:
        print(f"{message}", file=sys.stderr)


def warn(message, suppress_time=True):
    log(f"WARNING: {message}", suppress_time)


result = valve.py_configure_and_or_load("test/src/table.tsv", "build", True)
log(pformat(json.loads(result)))
