"""Replay the published evidence archive: audit_evidence.py BUNDLE NEW_OUTPUT.

Run with the archive's pinned oracle Python environment on Linux with bubblewrap.
The archived harness, records and source paths remain unchanged.
"""

import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def replay():
    bundle, output = Path("/tmp/evidence"), Path("/tmp/output")
    package = json.loads((bundle / "package.json").read_text())
    source = Path(package["report_source"]) / "benches/selective_read"
    sys.path[:0] = [str(source), str(source / "runners")]
    import production_report
    import oracle
    import pyarrow as pa

    started = time.monotonic()
    original = Path(package["source_report"])
    assert digest(original) == package["source_report_sha256"]
    report = json.loads(original.read_text())
    campaigns = [Path(c["path"]) for c in report["campaigns"]]
    production_report.report(Path(report["definition"]), campaigns, output / "report")
    assert (output / "report/production-report.json").read_bytes() == original.read_bytes()

    # Recheck retained exports against retained references. This does not rebuild
    # the references from the excluded full-size Delta/source tables.
    exports = 0
    for row in report["rows"]:
        gate = row["gate"]
        reference = Path(gate["reference"])
        metadata = json.loads((reference / "reference.json").read_text())
        saved = output / "references" / row["job"]["case_id"] / "reference.parquet"
        saved.parent.mkdir(parents=True, exist_ok=True)
        if not saved.exists():
            shutil.copyfile(reference / "reference.parquet", saved)
        assert digest(saved) == metadata["reference_sha256"]
        certificate = Path(gate["correctness_file"])
        checks = json.loads(certificate.read_text())["checks"]
        assert len(checks) == (1 if row["job"]["execution_mode"] == "open" else 2)
        for index, check in enumerate(checks):
            result = certificate.parent / "reader" / f"query-{index}.arrow"
            identity = result.with_suffix(".identity.json")
            assert check["status"] == "passed" and check["reader_id"] == row["reader_id"]
            assert check["case_id"] == row["job"]["case_id"]
            assert digest(identity) == check["identity_sha256"]
            assert digest(result) == check["result_sha256"]
            assert digest(saved) == check["reference_sha256"]
            with result.open("rb") as stream, pa.ipc.open_stream(stream) as batches:
                oracle.compare(batches, saved, metadata["projection"], metadata["output_rows"])
                assert not stream.read(1), "trailing Arrow bytes"
            exports += 1

    # Run the original exporter unchanged, using its expected directory layout.
    extraction = output / "extract-replay"
    exporter = extraction / "results-review/export.py"
    exporter.parent.mkdir(parents=True)
    shutil.copyfile(bundle / "original/selective-read-formal-323-rerun/results-review/export.py", exporter)
    source_report = extraction / "q4-final-stage6/audited-8-case-report"
    source_report.parent.mkdir()
    source_report.symlink_to(output / "report", target_is_directory=True)
    csv_dir = extraction / "report-worktree/docs/content/benchmarks"
    csv_dir.mkdir(parents=True)
    subprocess.run([sys.executable, "-I", "-B", str(exporter)], check=True)
    for path in (bundle / "published").iterdir():
        assert (csv_dir / path.name).read_bytes() == path.read_bytes(), path.name
    receipt = {"status": "passed", "source_report_sha256": digest(original),
               "report_byte_identical": True, "published_extracts_byte_identical": True,
               "cases": len(report["complete_cases"]), "reader_profile_entries": len(report["rows"]),
               "timing_invocations": 400, "retained_exact_exports_rechecked": exports,
               "new_performance_samples": 0, "full_tables_regenerated": False,
               "network_enabled": False, "original_benchmark_directories_visible": False,
               "python": sys.version, "elapsed_seconds": time.monotonic() - started}
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    if sys.argv[1:] == ["--inside"]:
        replay()
    else:
        if len(sys.argv) != 3 or sys.prefix == sys.base_prefix:
            sys.exit(__doc__)
        bundle, output = (Path(arg).resolve() for arg in sys.argv[1:])
        subprocess.run(["sha256sum", "--check", "--quiet", "SHA256SUMS"], cwd=bundle, check=True)
        output.mkdir(parents=True)
        subprocess.run([
            "bwrap", "--unshare-all", "--die-with-parent", "--ro-bind", "/", "/",
            "--tmpfs", "/home", "--tmpfs", "/tmp",
            "--ro-bind", str(bundle), "/tmp/evidence",
            "--ro-bind", str(bundle / "original"), "/home/hanbo/repo",
            "--ro-bind", sys.base_prefix, sys.base_prefix,
            "--ro-bind", sys.prefix, "/tmp/audit-venv",
            "--bind", str(output), "/tmp/output", "--proc", "/proc", "--dev", "/dev",
            "--chdir", "/tmp/evidence", "--clearenv", "--setenv", "PATH", "/usr/bin:/bin",
            "/tmp/audit-venv/bin/python", "-I", "-B", "/tmp/evidence/audit.py", "--inside",
        ], check=True)
