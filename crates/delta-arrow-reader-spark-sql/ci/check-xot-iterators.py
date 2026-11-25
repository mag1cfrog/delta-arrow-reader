"""Compare the upstream and vendored xot iterator results for the same XML."""

import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile


SPARK = Path(__file__).resolve().parents[1]


def documents():
    source = (SPARK / "tests/xpath_batch_reuse.rs").read_text(encoding="utf-8")
    for literal in re.findall(r'Some\(("(?:[^"\\]|\\.)*")\)', source):
        text = json.loads(literal)
        if text.startswith("<"):
            yield text
    yield '<root><left><item/><item/></left><right><item><leaf/></item></right></root>'
    yield "<?pi data?><a xmlns='urn:a' xmlns:p='urn:1'><p:b xmlns:p='urn:2' p:k='v'><c xmlns=''/>text</p:b><!--comment--></a>"
    yield "<a xmlns:p='urn:a' xmlns:q='urn:b'><b q:z='1' p:y='2' plain='3'/></a>"
    yield "<a>hello &amp; <![CDATA[<world>]]></a>"
    yield "<a broken='1' broken='2'/>"
    yield "<missing:prefix/>"
    yield ""


def run(work, dependency, inputs):
    (work / "src").mkdir(parents=True)
    shutil.copyfile(SPARK / "ci/xot-iterators-probe.rs", work / "src/main.rs")
    (work / "Cargo.toml").write_text(
        '[package]\nname = "xot-iterators-probe"\nversion = "0.0.0"\nedition = "2024"\n'
        f"[dependencies]\nxot = {dependency}\n",
        encoding="utf-8",
    )
    return subprocess.run(
        ["cargo", "run", "--quiet", "--manifest-path", str(work / "Cargo.toml")],
        input=inputs,
        stdout=subprocess.PIPE,
        check=True,
    ).stdout.splitlines()


def main():
    cases = list(dict.fromkeys(documents()))
    assert all("\0" not in document for document in cases)
    inputs = "".join(document + "\0" for document in cases).encode()
    vendor = json.dumps(str(SPARK / "vendor/dependencies/xot"))
    with tempfile.TemporaryDirectory(prefix="xot-iterators-") as directory:
        work = Path(directory)
        before = run(work / "before", '"=0.31.2"', inputs)
        after = run(work / "after", f'{{ path = {vendor} }}', inputs)
    if len(before) != len(cases) or len(after) != len(cases):
        raise SystemExit("Expected one iterator record for each input")
    for index, (old, new) in enumerate(zip(before, after, strict=True), start=1):
        if old != new:
            raise SystemExit(f"xot changed the result for input {index}")
    print(f"Upstream and vendored xot agree on {len(cases)} XML inputs")


if __name__ == "__main__":
    main()
