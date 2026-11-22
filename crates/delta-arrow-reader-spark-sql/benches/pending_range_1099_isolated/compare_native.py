"""Compare native RANGE instructions after normalizing linked addresses."""

from collections import defaultdict
import difflib
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

SYMBOLS = {
    "plain_evaluate_stateful": r"::PlainAggregateWindowExpr as .*::WindowExpr>::evaluate_stateful$",
    "sliding_evaluate_stateful": r"::SlidingAggregateWindowExpr as .*::WindowExpr>::evaluate_stateful$",
    "aggregate_get_result_column": r"::AggregateWindowExpr::get_result_column$",
    "range_calculate": r"::WindowFrameContext::calculate_range$",
}


def instructions(binary, address, size, name):
    raw = subprocess.check_output(
        ["objdump", "-d", "--no-show-raw-insn", "--demangle",
         f"--start-address={address}", f"--stop-address={address + size}", binary],
        text=True,
    )
    result = []
    for line in raw.splitlines():
        match = re.match(r"^\s*[0-9a-f]+:\s+(.+)$", line)
        if not match:
            continue
        assembly, _, comment = match[1].partition("#")
        assembly = re.sub(r"-?0x[0-9a-f]+\(%rip\)", "relocation(%rip)", assembly.strip())
        assembly = re.sub(r"\b[0-9a-f]+ (<.+>)$", r"\1", assembly)
        if "relocation" in assembly:
            target = re.search(r"<(.+)>", comment)
            if target and not target[1].startswith(("_DYNAMIC", ".rodata", "anon.")):
                assembly += f" <{target[1]}>"
        result.append(re.sub(r"\.llvm\.\d+", "", assembly.replace(name, "<self>")))
    assert result, name
    return result


def fingerprint(binary):
    table = defaultdict(list)
    raw = subprocess.check_output(["nm", "-S", "--defined-only", "--demangle", binary], text=True)
    for line in raw.splitlines():
        parts = line.split(maxsplit=3)
        if len(parts) != 4 or not all(re.fullmatch(r"[0-9a-f]+", part) for part in parts[:2]):
            continue
        address, size = (int(part, 16) for part in parts[:2])
        name = parts[3]
        for label, pattern in SYMBOLS.items():
            if re.search(pattern, name):
                code = instructions(binary, address, size, name)
                digest = hashlib.sha256("\n".join(code).encode()).hexdigest()
                table[label].append((digest, size, code))
                break
    assert all(len(table[name]) == (2 if name == "aggregate_get_result_column" else 1)
               for name in SYMBOLS)
    return {name: sorted(entries, key=lambda entry: (entry[1], entry[0]))
            for name, entries in table.items()}


def compare(before, after, output):
    old, new = fingerprint(before), fingerprint(after)
    comparison, changes = {}, []
    for name in SYMBOLS:
        comparison[name] = {
            "staging": [{"digest": digest, "bytes": size} for digest, size, _ in old[name]],
            "candidate": [{"digest": digest, "bytes": size} for digest, size, _ in new[name]],
            "identical": old[name] == new[name],
        }
        if not comparison[name]["identical"]:
            for index, (left, right) in enumerate(zip(old[name], new[name], strict=True)):
                changes.extend(difflib.unified_diff(left[2], right[2],
                                                   f"staging/{name}/{index}", f"candidate/{name}/{index}",
                                                   lineterm=""))
    output.mkdir()
    (output / "instructions.json").write_text(json.dumps(comparison, indent=2) + "\n")
    (output / "instructions.diff").write_text("\n".join(changes) + ("\n" if changes else ""))
    print(json.dumps({key: evidence["identical"] for key, evidence in comparison.items()}))


if __name__ == "__main__":
    compare(*(Path(arg).resolve() for arg in sys.argv[1:]))
