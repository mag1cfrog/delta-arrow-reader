"""Measure the retained Sail source and the standalone frontend's dependency graph."""

import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def test_lines(lines):
    """Count test-only items in the retained, rustfmt-formatted Sail source."""
    selected = set()
    for start, line in enumerate(lines):
        if start in selected or line.strip() not in {
            "#[cfg(test)]", "#[test]", "#[tokio::test]"
        }:
            continue
        indent = line[:len(line) - len(line.lstrip())]
        end = start + 1
        while lines[end].lstrip().startswith("#["):
            end += 1
        if lines[end].lstrip().startswith(("mod ", "fn ", "async fn ")):
            # shortcut: relies on retained rustfmt layout; revisit if item layout changes.
            end = lines.index(indent + "}", end + 1)
        elif not (lines[end].lstrip().startswith("const ") and lines[end].endswith(";")):
            raise ValueError(f"unrecognized test-only item: {lines[end]}")
        selected.update(range(start, end + 1))
    return len(selected)


def rust_source_size(root):
    files = sorted(root.rglob("*.rs"))
    lines = [line for path in files for line in path.read_text().splitlines()]
    return {
        "rust_files": len(files),
        "gross_lines": len(lines),
        "nonblank_lines": sum(bool(line.strip()) for line in lines),
        "source_bytes": sum(path.stat().st_size for path in files),
    }


def inventory(build_messages):
    sail_crates = []
    for crate in sorted((ROOT / "vendor/sail/crates").iterdir()):
        counts = {"crate": crate.name, **rust_source_size(crate), "test_lines": 0,
                  "build_script_lines": 0}
        for path in sorted(crate.rglob("*.rs")):
            lines = path.read_text().splitlines()
            if path.name == "build.rs":
                counts["build_script_lines"] += len(lines)
            else:
                counts["test_lines"] += (
                    len(lines) if "tests" in path.relative_to(crate).parts else test_lines(lines)
                )
        counts["production_lines"] = (
            counts["gross_lines"] - counts["test_lines"] - counts["build_script_lines"]
        )
        sail_crates.append(counts)
    totals = {key: sum(crate[key] for crate in sail_crates)
              for key in sail_crates[0] if key != "crate"}
    digest = hashlib.sha256()
    for path in sorted((ROOT / "vendor").rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(ROOT).as_posix().encode() + b"\0")
            digest.update(path.read_bytes() + b"\0")
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--locked", "--format-version=1"
    ], cwd=ROOT))
    tree = subprocess.check_output([
        "cargo", "tree", "--locked", "-p", "delta-arrow-reader-spark-sql",
        "--target", "x86_64-unknown-linux-gnu", "--edges", "normal,build",
        "--prefix", "none", "--format", "{p}"
    ], cwd=ROOT, text=True)
    packages = sorted(set(line.removesuffix(" (*)").replace(str(ROOT), "$CRATE")
                          for line in tree.splitlines()))
    generated = None
    if build_messages:
        generated = {}
        for line in build_messages.read_text().splitlines():
            message = json.loads(line)
            if message["reason"] == "build-script-executed" and "/vendor/sail/" in message["package_id"]:
                name = message["package_id"].split("#")[0].split("/")[-1]
                generated[name] = rust_source_size(Path(message["out_dir"]))
    return {
        "vendor_sha256": digest.hexdigest(),
        "sail_crates": sail_crates,
        "sail_totals": totals,
        "dependency_forks": [{"crate": path.name, **rust_source_size(path)}
                             for path in sorted((ROOT / "vendor/dependencies").iterdir())],
        "resolved_packages_all_targets_including_dev": len(metadata["packages"]),
        "linux_normal_build_packages_including_frontend": len(packages),
        "python_packages_in_frontend": [p for p in packages if re.match(
            r"(pyo3|sail-pyarrow|sail-python-udf)\b", p)],
        "build_generated_sail_rust": generated,
        "counting_scope": "Sail test/production counts include comments and blank lines. "
            "Dependency forks are reported in full, without a test/production split. "
            "Generated source is additional; null means no build messages were supplied.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--build-messages", type=Path,
                        help="Cargo --message-format=json output for generated-source counts")
    args = parser.parse_args()
    text = json.dumps(inventory(args.build_messages), indent=2) + "\n"
    if args.out:
        args.out.write_text(text)
    else:
        print(text, end="")


if __name__ == "__main__":
    assert test_lines(["fn main() {}", "#[cfg(test)]", "mod tests {",
                       "    #[test]", "    fn example() {}", "}"]) == 5
    assert test_lines(["#[tokio::test]", "async fn example()", "-> Result<()> {",
                       "    Ok(())", "}", "fn normal() {}"]) == 5
    assert test_lines(["#[cfg(test)]", "#[tokio::test]", "async fn example() {", "}"]) == 4
    main()
