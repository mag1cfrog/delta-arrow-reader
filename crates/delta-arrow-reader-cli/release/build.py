"""Build the CLI from a clean source commit inside the pinned Debian 12 environment."""

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

from check_binary import (
    RUNTIME_PACKAGES,
    RUST_TARGETS,
    check_binary,
    check_environment,
    file_sha256,
)

REPOSITORY = Path(__file__).resolve().parents[3]
DEBIAN_IMAGE = (
    "docker.io/library/debian@sha256:"
    "7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587"
)
BASELINE_FLAGS = {
    "amd64": ("x86-64", "-march=x86-64 -mtune=generic"),
    "arm64": ("generic", "-march=armv8-a -mtune=generic"),
}


def build_identity(version, source_commit, rust_toolchain):
    for name, value, pattern in (
        ("version", version, r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"),
        ("source commit", source_commit, r"[0-9a-f]{40}"),
        ("Rust toolchain", rust_toolchain, r"[0-9]+\.[0-9]+\.[0-9]+"),
    ):
        if not re.fullmatch(pattern, value):
            raise ValueError(f"invalid {name}: {value!r}")
    return {
        "schema_version": 1,
        "version": version,
        "source_commit": source_commit,
        "rust_toolchain": rust_toolchain,
    }


def build_environment(architecture, rust_toolchain):
    # Caller flags and compiler overrides must not raise the CPU baseline.
    keep = {
        "PATH",
        "HOME",
        "CARGO_HOME",
        "RUSTUP_HOME",
        "CARGO_BUILD_JOBS",
        "CARGO_NET_OFFLINE",
    }
    environment = {
        key: value
        for key, value in os.environ.items()
        if key in keep or key.lower() in {"http_proxy", "https_proxy", "no_proxy"}
    }
    cpu, cflags = BASELINE_FLAGS[architecture]
    environment.update(
        LC_ALL="C",
        RUSTUP_TOOLCHAIN=rust_toolchain,
        RUSTFLAGS=f"-Ctarget-cpu={cpu}",
        CFLAGS=cflags,
        CXXFLAGS=cflags,
    )
    return environment


def check_source(source_commit):
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if head != source_commit:
        raise ValueError(
            f"requested source commit {source_commit!r}, checked out {head}"
        )
    status = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=all"], text=True
    )
    if status:
        raise ValueError("the source checkout must have no modified or untracked files")


def build(source_commit, architecture, target_dir, *, rust_toolchain=None):
    check_environment(architecture)
    check_source(source_commit)
    manifest = tomllib.loads(Path("Cargo.toml").read_text())
    version = manifest["workspace"]["package"]["version"]
    declared_rust_version = manifest["package"]["rust-version"]
    rust_toolchain = rust_toolchain or declared_rust_version
    environment = build_environment(architecture, rust_toolchain)
    compiler_output = subprocess.check_output(
        ["rustup", "run", rust_toolchain, "rustc", "--version", "--verbose"],
        text=True,
        env=environment,
    )
    compiler_info = dict(
        line.split(": ", 1) for line in compiler_output.splitlines() if ": " in line
    )
    rust_target = RUST_TARGETS[architecture]
    if compiler_info.get("host") != rust_target:
        raise ValueError(f"expected a compiler running on {rust_target}")
    installed_rust_version = compiler_info.get("release", "")
    expected_components = declared_rust_version.split(".")
    if (
        installed_rust_version.split(".")[: len(expected_components)]
        != expected_components
    ):
        raise ValueError(
            f"expected Rust {declared_rust_version}, got {installed_rust_version}"
        )
    identity = build_identity(version, source_commit, installed_rust_version)
    target_dir = target_dir.absolute()
    metadata_file = target_dir / rust_target / "build-info.json"
    metadata_file.unlink(missing_ok=True)
    subprocess.run(
        [
            "rustup",
            "run",
            rust_toolchain,
            "cargo",
            "build",
            "--release",
            "--locked",
            "-p",
            "delta-arrow-reader-cli",
            "--target",
            rust_target,
            "--target-dir",
            str(target_dir),
        ],
        check=True,
        env=environment,
        stdout=sys.stderr,
    )
    check_source(source_commit)
    binary = target_dir / rust_target / "release" / "dar"
    metadata = check_binary(binary, architecture, version, file_sha256(binary))
    result = {**identity, **metadata, "runtime_packages": RUNTIME_PACKAGES}
    metadata_file.write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--architecture", choices=RUST_TARGETS, required=True)
    parser.add_argument("--target-dir", type=Path, default=Path("target/cli-build"))
    args = parser.parse_args()
    try:
        result = build(args.source_commit, args.architecture, args.target_dir)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        parser.exit(1, f"CLI build failed: {error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
