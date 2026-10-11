"""Check the extracted archive bytes and run the existing CLI acceptance tests."""

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
from artifacts import (
    MANIFEST,
    MEMBER_MODES,
    REPOSITORY,
    RUST_TARGETS,
    archive_name,
    build_identity,
    check_files,
    check_manifest,
    file_sha256,
)
from check_binary import check_binary


def run_acceptance(directory, extracted, smoke_output, identity, architecture):
    check_files(directory, [archive_name(identity["version"], architecture), MANIFEST])
    [target] = check_manifest(directory, identity, [architecture])
    check_files(extracted, MEMBER_MODES)
    if extracted.resolve().is_relative_to(REPOSITORY):
        raise ValueError("the archive must be extracted outside the source checkout")
    for name, mode in MEMBER_MODES.items():
        if (extracted / name).stat().st_mode & 0o7777 != mode:
            raise ValueError(f"unexpected extracted permissions: {name}")
    binary = (extracted / "dar").resolve()
    metadata = check_binary(
        binary, architecture, identity["version"], target["dar_sha256"]
    )
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
    check_manifest(directory, identity, [architecture])
    if file_sha256(binary) != target["dar_sha256"]:
        raise ValueError("binary changed during acceptance tests")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--extracted", type=Path, required=True)
    parser.add_argument("--smoke-output", type=Path, required=True)
    parser.add_argument("--architecture", choices=RUST_TARGETS, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--rust-toolchain", required=True)
    args = parser.parse_args()
    identity = build_identity(args.version, args.source_commit, args.rust_toolchain)
    run_acceptance(
        args.directory, args.extracted, args.smoke_output, identity, args.architecture
    )
    print(f"Archive acceptance passed: {args.architecture}")


if __name__ == "__main__":
    main()
