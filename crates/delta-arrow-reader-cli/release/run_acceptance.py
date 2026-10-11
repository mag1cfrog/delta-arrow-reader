"""Check the extracted archive bytes and run the existing CLI acceptance tests."""

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa

REPOSITORY = Path(__file__).resolve().parents[3]
MEMBER_MODES = {"dar": 0o755, "LICENSE": 0o644, "NOTICE": 0o644}


def file_sha256(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def verify_platform(xtask, directory, identity, architecture):
    manifest = json.loads(
        subprocess.check_output(
            [
                str(xtask), "cli", "artifacts", "verify", str(directory),
                "--architecture", architecture, "--version", identity["version"],
                "--source-commit", identity["source_commit"],
                "--rust-toolchain", identity["rust_toolchain"],
            ],
            text=True,
        )
    )
    [target] = manifest["targets"]
    return target


def run_acceptance(directory, extracted, smoke_output, identity, architecture, xtask):
    target = verify_platform(xtask, directory, identity, architecture)
    if (
        extracted.is_symlink()
        or not extracted.is_dir()
        or sorted(path.name for path in extracted.iterdir()) != sorted(MEMBER_MODES)
    ):
        raise ValueError("expected only the extracted dar, LICENSE, and NOTICE files")
    if extracted.resolve().is_relative_to(REPOSITORY):
        raise ValueError("the archive must be extracted outside the source checkout")
    for name, mode in MEMBER_MODES.items():
        member = extracted / name
        if member.is_symlink() or not member.is_file():
            raise ValueError(f"expected a regular file: {member}")
        if (extracted / name).stat().st_mode & 0o7777 != mode:
            raise ValueError(f"unexpected extracted permissions: {name}")
    binary = (extracted / "dar").resolve()
    metadata = json.loads(subprocess.check_output(
        [str(xtask), "cli", "check-binary", str(binary),
         "--architecture", architecture, "--expected-version", identity["version"],
         "--expected-sha256", target["dar_sha256"]], text=True,
    ))
    if metadata["runtime_dependencies"] != target["runtime_dependencies"]:
        raise ValueError(
            "manifest runtime dependencies differ from the executed binary"
        )

    # These files were produced before the container installed test instrumentation.
    inspection = json.loads((smoke_output / "inspect.json").read_text())
    if inspection["format_version"] != 1 or inspection["table_version"] != "0":
        raise ValueError("minimal-runtime inspection returned an unexpected snapshot")
    corpus = REPOSITORY / "tests/reader/fixtures/external_writer/corpus"
    with pa.ipc.open_file(corpus / "partitioned/expected.arrow") as reader:
        expected = reader.read_all().select(["id"])
    payload = (smoke_output / "scan.arrow").read_bytes()
    actual = pa.ipc.open_stream(payload).read_all()
    if (
        not payload.endswith(b"\xff\xff\xff\xff\x00\x00\x00\x00")
        or not actual.schema.equals(expected.schema, check_metadata=False)
        or actual.num_rows != 2
        or any(row not in expected.to_pylist() for row in actual.to_pylist())
        or len(set(actual.column("id").to_pylist())) != 2
    ):
        raise ValueError(
            "minimal-runtime scan differs from the external-writer fixture"
        )

    tests = REPOSITORY / "crates/delta-arrow-reader-cli/tests"
    for script, arguments in (
        ("process.py", [str(binary), identity["version"]]),
        ("scan.py", [str(binary)]),
    ):
        subprocess.run(
            [sys.executable, str(tests / script), *arguments],
            cwd=smoke_output,
            check=True,
            timeout=600,
        )
    verify_platform(xtask, directory, identity, architecture)
    if file_sha256(binary) != target["dar_sha256"]:
        raise ValueError("binary changed during acceptance tests")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--extracted", type=Path, required=True)
    parser.add_argument("--smoke-output", type=Path, required=True)
    parser.add_argument("--xtask", type=Path, required=True)
    parser.add_argument("--architecture", choices=("amd64", "arm64"), required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--rust-toolchain", required=True)
    args = parser.parse_args()
    identity = {
        "version": args.version,
        "source_commit": args.source_commit,
        "rust_toolchain": args.rust_toolchain,
    }
    run_acceptance(
        args.directory, args.extracted, args.smoke_output, identity, args.architecture,
        args.xtask,
    )
    print(f"Archive acceptance passed: {args.architecture}")


if __name__ == "__main__":
    main()
