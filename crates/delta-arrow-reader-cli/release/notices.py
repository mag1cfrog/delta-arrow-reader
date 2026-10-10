"""Collect dependency redistribution text for one CLI target using cargo-about."""

import argparse
import hashlib
import json
import re
import subprocess
import tomllib
import urllib.request
from pathlib import Path

from artifacts import REPOSITORY, RUST_TARGETS, require_regular_file
from build import build_environment

ABOUT_VERSION = "0.9.2"
RELEASE_DIRECTORY = Path(__file__).resolve().parent
LEGAL_FILENAME = re.compile(
    r"(?:licen[cs]es?|copying|copyrights?|notices?)(?:[-_.].*)?", re.I
)


def read_notice(path):
    require_regular_file(path)
    text = path.read_bytes().decode("utf-8")
    if not text.strip():
        raise ValueError(f"empty license or notice: {path}")
    return text


def dependency_license_files(package):
    root = Path(package["manifest_path"]).parent
    paths = set()
    directories = [root]
    # iterdir propagates errors; rglob can silently skip unreadable directories.
    while directories:
        for path in directories.pop().iterdir():
            relative = path.relative_to(root)
            if path.is_symlink():
                raise ValueError(f"symlink in dependency source: {path}")
            if path.is_dir():
                directories.append(path)
            elif LEGAL_FILENAME.fullmatch(path.name) or any(
                part.lower() in {"licenses", "licences"}
                for part in relative.parts[:-1]
            ):
                paths.add(path)
    if package["license_file"]:
        paths.add(root / package["license_file"])
    return [(str(path.relative_to(root)), read_notice(path)) for path in sorted(paths)]


def download_extra_notices(packages, files):
    notices = {}
    for entry in files:
        matching = [
            package for package in packages if package["name"] == entry["package"]
        ]
        if not matching:
            continue
        url = entry["url"]
        if not re.fullmatch(
            r"https://raw\.githubusercontent\.com/[^/]+/[^/]+/[0-9a-f]{40}/[^?#]+",
            url,
        ):
            raise ValueError(f"notice URL must name a full source commit: {url}")
        for package in matching:
            root = Path(package["manifest_path"]).parent
            source = json.loads((root / ".cargo_vcs_info.json").read_text())
            if (
                package["version"] != entry["version"]
                or source["git"]["sha1"] != url.split("/")[5]
            ):
                raise ValueError(
                    "update extra-notices.toml for "
                    f"{package['name']} {package['version']}"
                )
        with urllib.request.urlopen(url, timeout=30) as response:
            content = response.read()
        if hashlib.sha256(content).hexdigest() != entry["sha256"]:
            raise ValueError(f"downloaded notice SHA-256 differs: {url}")
        text = content.decode("utf-8")
        if not text.strip():
            raise ValueError(f"empty downloaded notice: {url}")
        for package in matching:
            notices.setdefault(package["id"], []).append((url, text))
    return notices


def render_notices(report, rust_docs, extra_notices):
    packages = {item["package"]["id"]: item["package"] for item in report["crates"]}
    if not packages or not report["licenses"]:
        raise ValueError("cargo-about returned no crates or licenses")
    sections = [
        f"Dependency license texts generated with cargo-about {ABOUT_VERSION}\n"
    ]
    covered = set()
    for license in report["licenses"]:
        text = license["text"]
        users = [item["crate"] for item in license["used_by"]]
        if not text.strip() or not users:
            raise ValueError("cargo-about returned a license without text or crates")
        covered.update(package["id"] for package in users)
        names = ", ".join(
            f"{package['name']} {package['version']}" for package in users
        )
        sections.append(f"\n{license['id']}\nUsed by: {names}\n\n{text}\n")
    if covered != set(packages):
        raise ValueError("cargo-about did not cover every resolved crate")

    # Original files retain notices and combined licenses the scanner may omit.
    for package in sorted(packages.values(), key=lambda p: (p["name"], p["version"])):
        if package["source"] is None:
            if package["name"] not in {"delta-arrow-reader", "delta-arrow-reader-cli"}:
                raise ValueError(f"unreviewed local dependency: {package['name']}")
            continue  # The archive also contains the repository LICENSE and NOTICE.
        files = dependency_license_files(package) + extra_notices.get(package["id"], [])
        if not files:
            raise ValueError(f"no original license files for {package['name']}")
        for name, text in files:
            sections.append(
                f"\n{package['name']} {package['version']}: {name}\n\n{text}\n"
            )

    # Rust's standard library is linked into dar but is absent from Cargo metadata.
    copyright_file = rust_docs / "COPYRIGHT-library.html"
    sections.append(f"\nRust standard library: {copyright_file.name}\n\n")
    sections.append(read_notice(copyright_file))
    license_files = sorted((rust_docs / "licenses").iterdir())
    if not {"Apache-2.0.txt", "MIT.txt"} <= {path.name for path in license_files}:
        raise ValueError("Rust toolchain license files are missing")
    for path in license_files:
        sections.append(
            f"\nRust toolchain: licenses/{path.name}\n\n{read_notice(path)}\n"
        )
    return "".join(sections)


def generate_notices(architecture):
    manifest = tomllib.loads((REPOSITORY / "Cargo.toml").read_text())
    rust_version = manifest["package"]["rust-version"]
    environment = build_environment(architecture, rust_version)
    command = ["rustup", "run", rust_version]
    version = subprocess.check_output(
        [*command, "cargo", "about", "--version"], text=True, env=environment
    ).strip()
    if version != f"cargo-about {ABOUT_VERSION}":
        raise ValueError(f"expected cargo-about {ABOUT_VERSION}, got {version!r}")
    report = json.loads(
        subprocess.check_output(
            [
                *command,
                "cargo",
                "about",
                "generate",
                "--locked",
                "--fail",
                "--manifest-path",
                str(RELEASE_DIRECTORY.parent / "Cargo.toml"),
                "--config",
                str(RELEASE_DIRECTORY / "about.toml"),
                "--target",
                RUST_TARGETS[architecture],
                "--format",
                "json",
            ],
            text=True,
            env=environment,
            cwd=REPOSITORY,
        )
    )
    packages = [item["package"] for item in report["crates"]]
    extra = tomllib.loads((RELEASE_DIRECTORY / "extra-notices.toml").read_text())
    extra_notices = download_extra_notices(packages, extra["files"])
    sysroot = subprocess.check_output(
        [*command, "rustc", "--print", "sysroot"], text=True, env=environment
    ).strip()
    return render_notices(report, Path(sysroot) / "share/doc/rust", extra_notices)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--architecture", choices=RUST_TARGETS, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        notices = generate_notices(args.architecture)
        with args.output.open("x", encoding="utf-8", newline="\n") as output:
            output.write(notices)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
        parser.exit(1, f"dependency notices failed: {error}\n")
    print(f"Dependency notices written to {args.output}")


if __name__ == "__main__":
    main()
