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


config = valve.py_configure_and_or_load("test/src/table.tsv", "build", True)
config = json.loads(config)

matching_values = valve.py_get_matching_values(json.dumps(config), "build", "foobar", "child")
matching_values = json.loads(matching_values)
log(pformat(matching_values))

log("-----")

matching_values = valve.py_get_matching_values(json.dumps(config), "build", "foobar", "child", "b")
matching_values = json.loads(matching_values)
log(pformat(matching_values))
