"""Compare jiter 0.15.0 and 0.16.0 over committed JSON captures and edge cases."""

import json
from pathlib import Path
import shutil
import subprocess
import tempfile


SPARK = Path(__file__).resolve().parents[1]
VERSIONS = ("0.15.0", "0.16.0")
CAPTURES = (
    "from-json-permissive",
    "get-json-object-extraction",
    "json-array-length-roots",
    "json-hash-results",
)


def inputs():
    for name in CAPTURES:
        path = SPARK / "tests" / "fixtures" / name / "requests.json"
        request = json.loads(path.read_text(encoding="utf-8"))
        for corpus in request["corpora"]:
            for table in corpus["metadata"]["inputs"]:
                for index, field in enumerate(table["schema"]["fields"]):
                    if field["name"] != "j":
                        continue
                    assert field["type"] == "string"
                    for row_number, row in enumerate(table["rows"], start=1):
                        if row[index] is not None:
                            yield f"{name}/{table['name']} row {row_number}", row[index].encode()
        for group in ("specs", "writer_specs", "root_admission_specs"):
            for index, case in enumerate(request.get(group, []), start=1):
                if case["document"] is not None:
                    yield f"{name}/{group} case {index}", case["document"].encode()

    yield "empty", b""
    yield "malformed string", br'"\uD800"'
    yield "invalid UTF-8", b'"\xff"'
    yield "malformed object", b'{"a" 1}'
    yield "truncated array", b"[0,"
    yield "truncated string", br'"\u12'
    yield "trailing value", b"true false"
    yield "long integer", b"9" * 128
    yield "nested array", b"[" * 170 + b"0" + b"]" * 170
    yield "overly nested array", b"[" * 1000 + b"0" + b"]" * 1000
    yield "long string", b'"' + b"a" * (1024 * 1024) + b'"'
    yield "truncated long string", b'"' + b"a" * (1024 * 1024)
    yield "long array", b"[" + b"0," * 32768 + b"1]"


def run(directory, version, documents):
    crate = directory / version
    (crate / "src").mkdir(parents=True)
    shutil.copyfile(SPARK / "ci" / "jiter-upgrade-probe.rs", crate / "src" / "main.rs")
    (crate / "Cargo.toml").write_text(
        '[package]\nname = "jiter-upgrade-probe"\nversion = "0.0.0"\nedition = "2024"\n'
        '[dependencies]\n'
        f'jiter = {{ version = "={version}", default-features = false, features = ["num-bigint"] }}\n',
        encoding="utf-8",
    )
    return subprocess.run(
        ["cargo", "run", "--quiet", "--manifest-path", str(crate / "Cargo.toml")],
        input=documents,
        stdout=subprocess.PIPE,
        check=True,
    ).stdout.splitlines()


def main():
    # Identical documents are parsed once. Keep their first label for diagnostics.
    names = {}
    for label, document in inputs():
        names.setdefault(document, label)
    documents = b"".join(len(document).to_bytes(4, "little") + document for document in names)
    # The releases have different optional PyO3 links and need independent lockfiles.
    with tempfile.TemporaryDirectory(prefix="jiter-upgrade-") as directory:
        old, new = [run(Path(directory), version, documents) for version in VERSIONS]
    if len(old) != len(names) or len(new) != len(names):
        raise SystemExit("Parser probe returned the wrong number of records")
    for label, before, after in zip(names.values(), old, new, strict=True):
        if before != after:
            raise SystemExit(f"jiter changed its result for {label}")
    print(f"jiter {VERSIONS[0]} and {VERSIONS[1]} agree on {len(names)} unique inputs")


if __name__ == "__main__":
    main()
