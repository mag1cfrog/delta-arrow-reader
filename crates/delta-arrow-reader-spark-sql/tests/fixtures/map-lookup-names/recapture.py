"""Run the unchanged #823 capture using its archived checkout-relative dependencies."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import sys
from tempfile import TemporaryDirectory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("map-lookup-names-spark.jsonl"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    original_cases = (root / "823-preparation-v2-cases.json").read_bytes()
    cases = json.loads(original_cases)
    for dependency in cases["source_dependencies"]:
        archived = root / "sources" / Path(dependency["path"]).name
        assert hashlib.sha256(archived.read_bytes()).hexdigest() == dependency["sha256"]
        dependency["path"] = str(archived)
    cases["leaf_encoder_path"] = str(root / "sources/capture_floating_group_keys_spark.py")
    sys.dont_write_bytecode = True
    os.environ["PYTHONDONTWRITEBYTECODE"] = "1"
    with TemporaryDirectory(prefix="map-lookup-names-") as temporary:
        manifest = Path(temporary) / "cases.json"
        manifest.write_text(json.dumps(cases, indent=2, ensure_ascii=True) + "\n")
        sys.argv = [str(root / "capture.py"), "--cases", str(manifest),
                    "--output", str(args.output)]
        runpy.run_path(str(root / "capture.py"), run_name="__main__")
        print(json.dumps({
            "portable_recipe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "original_cases_sha256": hashlib.sha256(original_cases).hexdigest(),
            "rebased_cases_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            "external_metadata_differences": ["leaf_encoder_path", "source_dependencies.path",
                                              "cases_sha256"],
        }))


if __name__ == "__main__":
    main()
