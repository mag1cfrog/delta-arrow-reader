"""Regenerate the preservation corpus from the frozen, accepted runtime captures."""

import gzip
import hashlib
import json
from pathlib import Path
import subprocess


FROZEN_COMMIT = "c426ef86e4da43eb57f270f1a4945c9dcc6559fe"
ARCHIVE_PATH = "experiments/spark-sql/local-decimal-cast-runs.json.gz"
OUTCOME_FIELDS = {
    "status", "types", "rows", "error", "output_names",
    "logical_nullable", "physical_nullable",
}


def main():
    packed = subprocess.check_output(["git", "show", f"{FROZEN_COMMIT}:{ARCHIVE_PATH}"])
    archive = json.loads(gzip.decompress(packed))
    files = archive["files"]
    for name, value in files.items():
        assert hashlib.sha256(value.encode()).hexdigest() == archive["sha256"][name], name

    groups = [
        (name, name.replace("-cases.jsonl", "-after.json"))
        for name in sorted(files)
        if name.startswith("replay/") and name.endswith("-cases.jsonl")
    ]
    groups += [("cases.jsonl", "after.json"), ("prior-first/cases.jsonl", "prior-first/after.json")]
    unique = {}
    observations = 0
    for cases_path, results_path in groups:
        results = json.loads(files[results_path])["results"]
        cases = [json.loads(line) for line in files[cases_path].splitlines()]
        assert len(results) == 2 * len(cases), results_path
        for case_index, case in enumerate(cases):
            for mode_index, ansi in enumerate((True, False)):
                observation_id = case["id"] + "_" + str(ansi).lower()
                # Some historical IDs repeat. Pair by recorded position as well
                # as ID so distinct captures cannot overwrite one another.
                result = results[2 * case_index + mode_index]
                assert result["id"] == observation_id, (results_path, case_index)
                actual = result["actual"]
                expected = {key: value for key, value in actual.items() if key in OUTCOME_FIELDS}
                if "rows" in expected:
                    expected["rows"] = sorted(expected["rows"], key=json.dumps)
                # The archived probe defaults to batch size 1 when the case omits it.
                key = (case["sql"], case.get("batch_size", 1), ansi)
                row = {
                    "id": f"{cases_path.removesuffix('.jsonl')}/{case_index}/{observation_id}",
                    "sql": key[0], "batch_size": key[1], "ansi": ansi,
                    "expected": expected,
                }
                if key in unique:
                    assert unique[key]["expected"] == expected, (unique[key]["id"], row["id"])
                else:
                    unique[key] = row
                observations += 1

    folder = Path(__file__).resolve().parent
    payload = "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in unique.values())
    compressed = gzip.compress(payload.encode(), compresslevel=9, mtime=0)
    (folder / "accepted-runtime.jsonl.gz").write_bytes(compressed)
    manifest = {
        "frozen_commit": FROZEN_COMMIT,
        "archive": ARCHIVE_PATH,
        "archive_sha256": hashlib.sha256(packed).hexdigest(),
        "source_groups": [results for _, results in groups],
        "source_observations": observations,
        "unique_observations": len(unique),
        "fixture_sha256": hashlib.sha256(compressed).hexdigest(),
        "comparison": "Preserve the accepted runtime, including unresolved Spark differences. Rows are compared as multisets; output order has separate integration tests.",
    }
    (folder / "accepted-runtime.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Imported {observations} captures as {len(unique)} distinct SQL/ANSI/batch observations")


if __name__ == "__main__":
    main()
