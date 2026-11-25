"""Record the packages and exact texts in the ordinary linked release NOTICE."""

import argparse
import gzip
from hashlib import sha256
import json
from pathlib import Path
import re
import tomllib


HEADER = re.compile(
    rb"^([0-9A-Za-z_-]+ [^:\s]+|Rust standard library|Rust toolchain): ([^\r\n]+)\n\n",
    re.MULTILINE,
)
RUST = {"Rust standard library", "Rust toolchain"}
PROJECT = {
    "delta-arrow-reader",
    "delta-arrow-reader-cli",
    "delta-arrow-reader-python",
    "delta-arrow-reader-spark-sql",
}


def describe_file(name, content):
    return {"name": name, "bytes": len(content), "sha256": sha256(content).hexdigest()}


def receipt(scan, notice, config):
    packages = {}
    by_id = {}
    for crate in scan["crates"]:
        package = crate["package"]
        name = package["name"]
        display = f"{name} {package['version']}"
        assert display not in packages, f"Ambiguous package in NOTICE: {display}"
        assert package["id"] not in by_id
        declared = package["license"]
        clarified = config.get(name, {}).get("clarify", {}).get("license")
        row = {
            "name": name,
            "version": package["version"],
            "declared_license": declared,
            "license_expression": clarified or declared,
            "selected_texts": [],
            "original_files": [],
        }
        packages[display] = row
        by_id[package["id"]] = row

    texts = {}
    for license in scan["licenses"]:
        text = license["text"].encode()
        assert text in notice
        digest = sha256(text).hexdigest()
        texts[digest] = license["text"]
        for usage in license["used_by"]:
            by_id[usage["crate"]["id"]]["selected_texts"].append(
                {"license_id": license["id"], "sha256": digest}
            )
    for row in packages.values():
        assert row["license_expression"] and row["selected_texts"], row["name"]

    toolchain = []
    sections = [
        (m, m[1].decode(), m[2].decode())
        for m in HEADER.finditer(notice)
        if m[1].decode() in packages or m[1].decode() in RUST
    ]
    assert sections
    for index, (match, owner, filename) in enumerate(sections):
        if index + 1 == len(sections):
            assert notice.endswith(b"\n")
            content = notice[match.end():-1]
        else:
            end = sections[index + 1][0].start()
            assert notice[end - 2:end] == b"\n\n"
            content = notice[match.end():end - 2]
        assert content.strip()
        if owner in RUST:
            toolchain.append(describe_file(f"{owner}: {filename}", content))
        elif filename == "Upstream source":
            packages[owner]["upstream_source"] = content.decode()
        else:
            files = packages[owner]["original_files"]
            assert all(file["name"] != filename for file in files), (owner, filename)
            files.append(describe_file(filename, content))
    assert toolchain
    for row in packages.values():
        if row["name"] not in PROJECT:
            assert row["original_files"], row["name"]
    return {
        "notice": describe_file("dependency-notices.txt", notice),
        "packages": [packages[display] for display in sorted(packages)],
        "selected_texts": texts,
        "rust_toolchain_files": toolchain,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scan", type=Path, required=True)
    parser.add_argument("--notice", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    notice = args.notice.read_bytes()
    data = receipt(
        json.loads(args.scan.read_bytes()),
        notice,
        tomllib.loads(args.config.read_text(encoding="utf-8")),
    )
    rendered = (json.dumps(data, indent=2, sort_keys=True) + "\n").encode()
    for text in (notice, rendered):
        assert str(Path.home()).encode() not in text
        assert b"/private/tmp/dar-" not in text
    with args.output.open("xb") as destination:
        with gzip.GzipFile(fileobj=destination, filename="", mtime=0) as compressed:
            compressed.write(rendered)
    print(f"Verified receipt for {len(data['packages'])} packages")


if __name__ == "__main__":
    main()
