"""Check the Spark pilot against the saved Spark-written Delta/DV corpus."""

import argparse
import json
from pathlib import Path
import resource
import sys

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import run
from capabilities import CORPUS, delta_capabilities, probe


def check(binary, fixtures, output):
    output.mkdir()
    # Reproduce the production validation cap: the AWS JAR exceeds this size.
    soft, hard = resource.getrlimit(resource.RLIMIT_FSIZE)
    resource.setrlimit(resource.RLIMIT_FSIZE, (min(256 * 1024**2, soft) if soft != resource.RLIM_INFINITY else 256 * 1024**2, hard))
    checks = delta_capabilities(binary, fixtures, output)
    base = run.request(fixtures, "li.clustered.eq2-in20", "open", "diagnostic", "invalid")
    for name, changes in (("unknown-field", {"surprise": True}),
                          ("negative-version", {"snapshot_version": -1}),
                          ("bad-budget", {"resource_budget": {}}),
                          ("credential-uri", {"table_uri": "s3://user:secret@bucket/table"}),
                          ("timing-rejected", {"purpose": "timing"}),
                          ("campaign-rejected", {"campaign_id": "formal"}),
                          ("sql-rejected", {"canonical_sql": "SELECT * FROM bench; DROP TABLE bench"})):
        record = probe(binary, dict(base, **changes), output / name)
        assert record["status"] == "invalid_input" and record["failure_reason"], record
        checks.append({"path": name, "status": "passed", "feature": "invalid/formal request rejected"})
    summary = {"status": "passed", "reader": "spark", "pilot_only": True, "publication_ready": False,
               "build_sha256": run.digest(binary.with_name("build.json")),
               "corpus_manifest_sha256": run.digest(CORPUS / "manifest.json"),
               "scope": "bounded exact-value capability checks; not a performance campaign",
               "max_file_size_bytes": resource.getrlimit(resource.RLIMIT_FSIZE)[0],
               "observations": checks}
    run.save(output / "capabilities.json", summary)
    print(json.dumps({"status": "passed", "invocations": len(checks), "pilot_only": True}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    check(args.binary, args.fixtures, args.output)
