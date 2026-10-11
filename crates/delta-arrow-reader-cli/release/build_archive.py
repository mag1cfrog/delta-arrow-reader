"""Build and package one CLI architecture inside the pinned Debian environment."""

import argparse
import tempfile
from pathlib import Path

from artifacts import RUST_TARGETS, build_identity, check_manifest, package
from build import build, check_source
from notices import generate_notices


def build_archive(identity, architecture, target_dir, output):
    metadata = build(
        identity["source_commit"],
        architecture,
        target_dir,
        rust_toolchain=identity["rust_toolchain"],
    )
    if any(metadata[key] != value for key, value in identity.items()):
        raise ValueError("build version, source commit, or Rust toolchain differs")
    with tempfile.TemporaryDirectory(prefix="cli-notices-") as temporary:
        notices = Path(temporary) / "notices.txt"
        notices.write_text(
            generate_notices(architecture, rust_toolchain=identity["rust_toolchain"]),
            encoding="utf-8",
            newline="\n",
        )
        check_source(identity["source_commit"])
        binary = target_dir / RUST_TARGETS[architecture] / "release/dar"
        package(
            binary,
            notices,
            output,
            identity,
            architecture,
            metadata["runtime_dependencies"],
        )
    [target] = check_manifest(output, identity, [architecture])
    if target["dar_sha256"] != metadata["dar_sha256"]:
        raise ValueError("packaged binary differs from the validated build")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--architecture", choices=RUST_TARGETS, required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-rust-toolchain", required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    identity = build_identity(
        args.expected_version, args.source_commit, args.expected_rust_toolchain
    )
    build_archive(identity, args.architecture, args.target_dir, args.output)
    print(f"CLI archive built: {args.output}")


if __name__ == "__main__":
    main()
