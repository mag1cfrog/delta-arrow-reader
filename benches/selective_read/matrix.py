"""Extract expressions through the pinned Polars SDK without table I/O."""

import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "runners"))
from run import load_adapter


if __name__ == "__main__":
    adapter = load_adapter(Path(sys.argv[1]))
    print(json.dumps({"runtime": adapter["runtime_metadata"](adapter["engine"]()),
                      "expressions": {case: adapter["expression_identity"](sql)
                                      for case, sql in json.load(sys.stdin).items()}}))
