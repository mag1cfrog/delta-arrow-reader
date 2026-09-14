"""Count the retained Sail source and resolved dependencies without compiling it."""

import argparse
import hashlib
import json
import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENDOR = ROOT / "vendor/sail"


def source_hash():
    digest = hashlib.sha256()
    for path in sorted(VENDOR.rglob("*")):
        if path.is_file() and "target" not in path.relative_to(VENDOR).parts:
            digest.update(str(path.relative_to(VENDOR)).encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def test_lines(lines):
    """Count formatted cfg(test) modules/constants, failing on unknown shapes."""
    selected = set()
    for start, line in enumerate(lines):
        if start in selected or line.strip() != "#[cfg(test)]":
            continue
        indent = line[:len(line) - len(line.lstrip())]
        end = start + 1
        while lines[end].lstrip().startswith("#["):
            end += 1
        if lines[end].lstrip().startswith("mod ") and lines[end].endswith("{"):
            # ponytail: uses upstream rustfmt indentation; use a Rust AST if this source layout changes.
            end = lines.index(indent + "}", end + 1)
        elif not (lines[end].lstrip().startswith("const ") and lines[end].endswith(";")):
            raise ValueError(f"unrecognized cfg(test) item: {lines[end]}")
        selected.update(range(start, end + 1))
    return len(selected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--build-messages", type=Path, help="Cargo --message-format=json output for generated-source counts")
    args = parser.parse_args()
    crates = []
    for crate in sorted((VENDOR / "crates").iterdir()):
        counts = {"crate": crate.name, "rust_files": 0, "gross_lines": 0, "nonblank_lines": 0,
                  "test_lines": 0, "build_script_lines": 0}
        for path in sorted(crate.rglob("*.rs")):
            lines = path.read_text().splitlines()
            counts["rust_files"] += 1
            counts["gross_lines"] += len(lines)
            counts["nonblank_lines"] += sum(bool(line.strip()) for line in lines)
            counts["test_lines"] += len(lines) if "tests" in path.relative_to(crate).parts else test_lines(lines)
            if path.name == "build.rs":
                counts["build_script_lines"] += len(lines)
        counts["production_lines"] = counts["gross_lines"] - counts["test_lines"] - counts["build_script_lines"]
        crates.append(counts)
    lock = tomllib.loads((ROOT / "Cargo.lock").read_text())
    metadata = json.loads(subprocess.check_output([
        "cargo", "metadata", "--locked", "--format-version=1", "--manifest-path", str(ROOT / "Cargo.toml")]))
    tree = subprocess.check_output([
        "cargo", "tree", "--locked", "--manifest-path", str(ROOT / "Cargo.toml"),
        "--target", "x86_64-unknown-linux-gnu", "--edges", "normal,build", "--prefix", "none", "--format", "{p}"
    ], text=True)
    target_packages = sorted(set(line.removesuffix(" (*)").replace(str(ROOT), "$EXPERIMENT") for line in tree.splitlines()))
    totals = {key: sum(crate[key] for crate in crates) for key in crates[0] if key != "crate"}
    report = {"source_sha256": source_hash(), "crates": crates, "totals": totals,
              "lock_packages": len(lock["package"]), "resolved_packages_all_targets_including_dev": len(metadata["packages"]),
              "linux_normal_build_packages_including_runner": len(target_packages),
              "python_packages_in_linux_build": [p for p in target_packages if re.match(r"(pyo3|sail-pyarrow|sail-python-udf)\b", p)]}
    if args.build_messages:
        generated = {}
        for line in args.build_messages.read_text().splitlines():
            message = json.loads(line)
            if message["reason"] == "build-script-executed" and "/vendor/sail/" in message["package_id"]:
                files = list(Path(message["out_dir"]).rglob("*.rs"))
                name = message["package_id"].split("#")[0].split("/")[-1]
                generated[name] = {"files": len(files), "lines": sum(len(p.read_text().splitlines()) for p in files)}
        report["build_generated_rust"] = generated
    text = json.dumps(report, indent=2) + "\n"
    if args.out:
        args.out.write_text(text)
    else:
        print(text, end="")


if __name__ == "__main__":
    assert test_lines(["fn main() {}", "#[cfg(test)]", "mod tests {", "    #[test]", "    fn test() {}", "}"]) == 5
    main()
