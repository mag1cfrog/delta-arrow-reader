"""Capture native registrations and run the added-function probes."""

import argparse
import csv
import json
from pathlib import Path
import re
import subprocess


REGISTRY_FIELDS = ("category", "name", "canonical_name", "order")
FUNCTION_KINDS = {"scalar", "higher_order", "aggregate", "window", "table_function"}
TARGET = "aarch64-apple-darwin"


def write_rows(path, fields, rows):
    with path.open("x", encoding="utf-8", newline="") as out:
        writer = csv.DictWriter(out, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def features(root):
    command = [
        "cargo", "+1.96.0", "tree", "--locked", "-p", "delta-arrow-reader-cli",
        "--target", TARGET, "--edges", "normal,build", "--prefix", "none",
        "--format", "{p}\t{f}",
    ]
    tree = subprocess.run(command, cwd=root, capture_output=True, text=True, check=True)
    enabled = set()
    for line in tree.stdout.splitlines():
        package, _, flags = line.partition("\t")
        if package == "datafusion v54.1.0":
            enabled.update(flags.removesuffix(" (*)").split(","))
    assert "sql" in enabled
    return enabled


def inventory(binary, root):
    result = subprocess.run(
        [binary, "--proof-native-registry"], capture_output=True, text=True,
        check=True, timeout=30,
    )
    entries = json.loads(result.stdout)
    for feature in features(root):
        entries.append({
            "category": "cargo_feature", "name": feature,
            "canonical_name": feature, "order": None,
        })
    rows = {(row["category"], row["name"]): row for row in entries}
    assert len(rows) == len(entries)
    return rows


def compare(args):
    stock = inventory(args.stock, args.stock_root)
    linked = inventory(args.linked, args.linked_root)
    assert ("scalar", "map_entries") not in stock
    assert ("scalar", "map_entries") in linked
    for name, rows in [("stock", stock), ("linked", linked)]:
        write_rows(
            args.output / f"native-registry-1559-{name}.csv",
            REGISTRY_FIELDS, [rows[key] for key in sorted(rows)],
        )
    delta = []
    for key in sorted(stock.keys() | linked.keys()):
        before, after = stock.get(key), linked.get(key)
        if before == after:
            continue
        category, name = key
        delta.append({
            "category": category, "name": name,
            "change": "added" if before is None else "removed" if after is None else "changed",
            "stock_canonical": before["canonical_name"] if before else "",
            "linked_canonical": after["canonical_name"] if after else "",
            "stock_order": before["order"] if before else "",
            "linked_order": after["order"] if after else "",
        })
    write_rows(
        args.output / "native-registry-1559-difference.csv",
        ("category", "name", "change", "stock_canonical", "linked_canonical",
         "stock_order", "linked_order"), delta,
    )
    count = sum(
        r["change"] == "added" and r["category"] in FUNCTION_KINDS for r in delta
    )
    print(f"Stock {len(stock)} entries, linked {len(linked)} entries, {count} added function names")


def portable(text, root):
    text = text.replace(str(root.resolve()) + "/", "")
    text = re.sub(
        r"/(?:Users|home)/[^/\s]+/\.cargo/registry/src/[^/\s]+/",
        "<cargo-registry>/", text,
    )
    assert str(Path.home()) not in text
    assert "/private/tmp/dar-1559-" not in text
    return text


def run_statement(binary, root, statement):
    result = subprocess.run(
        [binary, "--proof-native-sql", statement],
        capture_output=True, text=True, timeout=30,
    )
    panics = []
    for line in result.stderr.splitlines():
        if not line.startswith("{"):
            continue
        record = json.loads(line)
        if record.get("status") == "panic":
            location = record.get("location")
            if location:
                location["file"] = portable(location["file"], root)
            record["message"] = portable(record["message"] or "", root)
            panics.append(record)
    if panics:
        return {"status": "panic", "rows": "", "details": json.dumps(panics, sort_keys=True)}
    observed = json.loads(result.stdout)
    if observed["status"] == "error":
        assert result.returncode != 0
        return {"status": "error", "rows": "", "details": portable(observed["message"], root)}
    assert observed["status"] == "rows" and result.returncode == 0
    return {"status": "rows", "rows": observed["row_count"], "details": ""}


def probe(args):
    with args.difference.open(encoding="utf-8", newline="") as source:
        added = [
            row for row in csv.DictReader(source)
            if row["change"] == "added" and row["category"] in FUNCTION_KINDS
        ]
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    seen = {(row["category"], row["linked_canonical"]) for row in added}
    assert seen == {tuple(key.split(":", 1)) for key in manifest}, seen
    results = []
    for row in added:
        category, name = row["category"], row["name"]
        canonical = row["linked_canonical"]
        statement = manifest[f"{category}:{canonical}"].replace("{function}", name)
        observed = run_statement(args.linked, args.linked_root, statement)
        results.append({
            "category": category, "name": name, "canonical_name": canonical,
            "statement": statement, **observed,
        })
    write_rows(
        args.output, ("category", "name", "canonical_name", "statement",
                      "status", "rows", "details"), results,
    )
    counts = {s: sum(r["status"] == s for r in results) for s in ("rows", "error", "panic")}
    print(f"Verified {len(results)} added function names", counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(required=True)
    registry = commands.add_parser("compare")
    registry.add_argument("--stock", type=Path, required=True)
    registry.add_argument("--stock-root", type=Path, required=True)
    registry.add_argument("--linked", type=Path, required=True)
    registry.add_argument("--linked-root", type=Path, required=True)
    registry.add_argument("--output", type=Path, required=True)
    registry.set_defaults(run=compare)
    statements = commands.add_parser("probe")
    statements.add_argument("--linked", type=Path, required=True)
    statements.add_argument("--linked-root", type=Path, required=True)
    statements.add_argument("--difference", type=Path, required=True)
    statements.add_argument("--manifest", type=Path, required=True)
    statements.add_argument("--output", type=Path, required=True)
    statements.set_defaults(run=probe)
    args = parser.parse_args()
    args.run(args)


if __name__ == "__main__":
    main()
