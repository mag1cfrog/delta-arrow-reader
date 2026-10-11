"""Inspect a CLI ELF binary and execute it on its supported Debian 12 platform."""

import argparse
import json
import os
import platform
import re
import struct
import subprocess
from pathlib import Path

from artifacts import (
    MIN_GLIBC,
    RUST_TARGETS,
    check_runtime_dependencies,
    file_sha256,
    require_regular_file,
)

ELF_PLATFORMS = {
    "amd64": {
        "elf_machine": 62,
        "uname_machine": "x86_64",
        "interpreter": "/lib64/ld-linux-x86-64.so.2",
    },
    "arm64": {
        "elf_machine": 183,
        "uname_machine": "aarch64",
        "interpreter": "/lib/ld-linux-aarch64.so.1",
    },
}
RUNTIME_PACKAGES = ["ca-certificates", "libc6", "libgcc-s1"]


def check_environment(architecture):
    release = platform.freedesktop_os_release()
    if release.get("ID") != "debian" or release.get("VERSION_ID") != "12":
        raise ValueError("build and runtime checks require Debian 12")
    if os.confstr("CS_GNU_LIBC_VERSION") != f"glibc {MIN_GLIBC}":
        raise ValueError(f"expected the Debian 12 glibc {MIN_GLIBC} runtime")
    if platform.machine() != ELF_PLATFORMS[architecture]["uname_machine"]:
        raise ValueError(f"{architecture} must execute on its matching architecture")


def check_elf_header(header, architecture):
    if len(header) != 20 or header[:7] != b"\x7fELF\x02\x01\x01":
        raise ValueError("expected a 64-bit little-endian ELF binary")
    file_type, machine = struct.unpack("<HH", header[16:20])
    if file_type not in (2, 3) or machine != ELF_PLATFORMS[architecture]["elf_machine"]:
        raise ValueError(f"expected an {architecture} ELF executable")


def check_readelf_output(output, architecture):
    interpreter = ELF_PLATFORMS[architecture]["interpreter"]
    if re.findall(r"\[Requesting program interpreter: (.+)\]", output) != [interpreter]:
        raise ValueError(f"expected ELF interpreter {interpreter}")
    if re.search(r"\((?:RPATH|RUNPATH)\)", output):
        raise ValueError("release binaries must not depend on an RPATH or RUNPATH")
    dependencies = sorted(
        re.findall(r"\(NEEDED\)\s+Shared library: \[([^\]]+)\]", output)
    )
    check_runtime_dependencies(dependencies)
    supported = {"libc.so.6", "libm.so.6", "libgcc_s.so.1", Path(interpreter).name}
    if "libc.so.6" not in dependencies or set(dependencies) - supported:
        raise ValueError(f"libraries outside the supported runtime: {dependencies!r}")
    versions = re.findall(r"Name: GLIBC_(\S+)", output)
    if not versions or any(
        not re.fullmatch(r"[0-9]+\.[0-9]+(?:\.[0-9]+)?", version)
        for version in versions
    ):
        raise ValueError("missing or unsupported GLIBC symbol requirements")
    maximum = max(tuple(map(int, version.split("."))) for version in versions)
    if maximum > (*map(int, MIN_GLIBC.split(".")), 0):
        raise ValueError(
            f"binary requires GLIBC_{'.'.join(map(str, maximum))}, above {MIN_GLIBC}"
        )
    if any(
        value.strip() != "x86-64-baseline"
        for value in re.findall(r"x86 ISA needed: ([^\n]+)", output)
    ):
        raise ValueError("binary requires an x86 instruction set above the baseline")
    return {
        "interpreter": interpreter,
        "runtime_dependencies": dependencies,
        "max_glibc": ".".join(map(str, maximum)),
    }


def inspect_elf(binary, architecture):
    require_regular_file(binary)
    if not binary.stat().st_mode & 0o111 or binary.stat().st_mode & 0o7000:
        raise ValueError("binary must be executable without special permission bits")
    with binary.open("rb") as source:
        check_elf_header(source.read(20), architecture)
    result = subprocess.run(
        [
            "readelf",
            "--program-headers",
            "--dynamic",
            "--version-info",
            "--notes",
            "--wide",
            str(binary),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=dict(os.environ, LC_ALL="C"),
        timeout=30,
    )
    if result.stderr:
        raise ValueError(f"readelf reported a problem: {result.stderr.strip()}")
    return {
        "architecture": architecture,
        "rust_target": RUST_TARGETS[architecture],
        **check_readelf_output(result.stdout, architecture),
        "dar_sha256": file_sha256(binary),
    }


def check_binary(binary, architecture, version, expected_sha256):
    check_environment(architecture)
    binary = binary.absolute()
    metadata = inspect_elf(binary, architecture)
    if metadata["dar_sha256"] != expected_sha256:
        raise ValueError("binary SHA-256 differs from the expected validated bytes")
    result = subprocess.run(
        [str(binary), "--version"],
        check=True,
        capture_output=True,
        timeout=30,
    )
    if result.stdout != f"dar {version}\n".encode() or result.stderr:
        raise ValueError(f"binary does not report exactly dar {version}")
    if file_sha256(binary) != expected_sha256:
        raise ValueError("binary changed during execution")
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("--architecture", choices=RUST_TARGETS, required=True)
    parser.add_argument("--expected-version", required=True)
    parser.add_argument("--expected-sha256", required=True)
    args = parser.parse_args()
    try:
        metadata = check_binary(
            args.binary, args.architecture, args.expected_version, args.expected_sha256
        )
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        parser.exit(1, f"binary check failed: {error}\n")
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
