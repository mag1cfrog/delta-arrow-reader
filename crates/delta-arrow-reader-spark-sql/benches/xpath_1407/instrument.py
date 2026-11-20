"""Generate scratch-only XPath phase probes from the existing source."""

import re
from pathlib import Path

XML = Path("vendor/sail/crates/sail-function/src/scalar/xml")
PROBE = """
use std::cell::Cell;
use std::time::Instant;

thread_local! {
    static PHASES: Cell<[u128; 7]> = const { Cell::new([0; 7]) };
}

pub(super) fn begin() -> Instant {
    PHASES.set([0; 7]);
    Instant::now()
}

pub(super) fn record(phase: usize, start: Instant) {
    let elapsed = start.elapsed().as_nanos();
    let mut phases = PHASES.get();
    phases[phase] += elapsed;
    phases[5] += u128::from(phase == 0);
    PHASES.set(phases);
}

pub(super) fn compiled(start: Instant) {
    record(2, start);
    let mut phases = PHASES.get();
    phases[6] += 1;
    PHASES.set(phases);
}

pub(super) fn finish(start: Instant, rows: usize) {
    let wall = start.elapsed().as_nanos();
    eprintln!("xpath-phases {rows} {wall} {:?}", PHASES.get());
}
"""


def replace(source, before, after, count=1):
    assert source.count(before) == count, before
    return source.replace(before, after)


def driver(source, rows):
    source = replace(source, "const ROWS: usize = 1_000_000;", f"const ROWS: usize = {rows};")
    return replace(source, "        let comparison = native.as_ref()",
                   '        fs::write(work.join(format!("{configuration}.rows.json")), '
                   'serde_json::to_vec(&rows.0)?)?;\n        let comparison = native.as_ref()')


def kernel(source, typed):
    source = "use super::xpath_phases as probe;\n" + source if not typed else replace(
        source, "use std::sync::Arc;", "use super::xpath_phases as probe;\nuse std::sync::Arc;")
    evaluators = 3 if typed else 1
    source = replace(source, "    let mut documents = Documents::new();",
                     "    let phase = std::time::Instant::now();\n    let mut documents = Documents::new();",
                     evaluators)
    source = replace(source, "    let query = Queries::default()",
                     """    probe::record(0, phase);
    let phase = std::time::Instant::now();
    let queries = Queries::default();
    probe::record(1, phase);
    let phase = std::time::Instant::now();
    let query = queries""", evaluators)
    source, count = re.subn(r"(    \.map_err\([^\n]+Invalid XPath[^\n]+\?;\n)",
                            r"""\1    probe::compiled(phase);
    let phase = std::time::Instant::now();
    drop(queries);
    probe::record(1, phase);
    let phase = std::time::Instant::now();
""", source)
    assert count == evaluators
    if typed:
        source, count = re.subn(r"(    let wrapped = format!\([^\n]+;\n)",
                                r"    let phase = std::time::Instant::now();\n\1    probe::record(2, phase);\n",
                                source)
        assert count == 3
        source = replace(source, "    Ok(Some(result))", "    probe::record(3, phase);\n    Ok(Some(result))", 3)
        source, count = re.subn(
            r"        if xmls\.is_null\(row\) \|\| paths\.is_null\(row\) \{\n"
            r"            builder\.append_null\(\);\n        \} else \{\n"
            r"            match (evaluate_xpath_\w+\(xmls\.value\(row\), paths\.value\(row\)\)\?) \{\n"
            r"                Some\(v\) => builder\.append_value\(([^\n]+)\),\n"
            r"                None => builder\.append_null\(\),\n            \}\n        \}",
            r"""        let value = if xmls.is_null(row) || paths.is_null(row) {
            None
        } else {
            \1
        };
        let phase = std::time::Instant::now();
        match value {
            Some(v) => builder.append_value(\2),
            None => builder.append_null(),
        }
        probe::record(4, phase);""", source)
        assert count == 7
    else:
        source = replace(source, "    Ok(Some(\n        value",
                         """    probe::record(3, phase);
    let phase = std::time::Instant::now();
    let output = value""")
        source = replace(source, "            .collect::<Result<Vec<_>>>()?,\n    ))",
                         """            .collect::<Result<Vec<_>>>()?;
    probe::record(4, phase);
    Ok(Some(output))""")
        source = replace(source, "        match values {", "        let phase = std::time::Instant::now();\n        match values {")
        source = replace(source, "            None => builder.append(false),\n        }",
                         "            None => builder.append(false),\n        }\n        probe::record(4, phase);")
    batches = 7 if typed else 1
    source = replace(source, "    let mut builder = ", "    let batch = probe::begin();\n    let phase = std::time::Instant::now();\n    let mut builder = ", batches)
    source = replace(source, "    for row in 0..xmls.len() {",
                     "    probe::record(4, phase);\n    for row in 0..xmls.len() {", batches)
    return replace(source, "    Ok(Arc::new(builder.finish()))",
                   """    let phase = std::time::Instant::now();
    let output = Arc::new(builder.finish());
    probe::record(4, phase);
    probe::finish(batch, xmls.len());
    Ok(output)""", batches)


def changes(workspace, rows, phases):
    original = workspace / "benches/functions.rs"
    yield original, driver(original.read_text(), rows)
    if phases:
        xml = workspace / XML
        modules = xml / "mod.rs"
        yield modules, modules.read_text() + "mod xpath_phases;\n"
        for name in ("xpath", "xpath_typed"):
            path = xml / f"{name}.rs"
            yield path, kernel(path.read_text(), typed=name == "xpath_typed")
        yield xml / "xpath_phases.rs", PROBE.lstrip()
