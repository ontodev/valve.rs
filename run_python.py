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

log("-----")

row = {
    "child": {"messages": [], "valid": True, "value": "b"},
    "parent": {"messages": [], "valid": True, "value": "f"},
    "xyzzy": {"messages": [], "valid": True, "value": "w"},
    "foo": {"messages": [], "valid": True, "value": "A"},
    "bar": {
        "messages": [
            {"level": "error", "message": "An unrelated error", "rule": "custom:unrelated"}
        ],
        "valid": False,
        "value": "B",
    },
}

result_row = valve.py_validate_row(json.dumps(config), "build", "foobar", json.dumps(row), True, 1)
result_row = json.loads(result_row)
log(pformat(result_row))

valve.py_update_row("build", "foobar", json.dumps(result_row), 1)

log("-----")
