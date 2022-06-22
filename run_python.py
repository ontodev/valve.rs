import json
import valve
from pprint import pformat

result = valve.py_configure("test/src/table.tsv", "build")
print(pformat(json.loads(result)))
