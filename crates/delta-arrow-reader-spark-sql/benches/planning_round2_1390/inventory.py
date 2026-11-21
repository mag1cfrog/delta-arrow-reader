"""Trace expression visits without modifying the checkout or timing rules."""

import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
sys.dont_write_bytecode = True
sys.path.insert(0, str(HERE.parent / "planning_attribution"))
import assemble
import run

EXPR = "vendor/dependencies/datafusion-expr/src"
OPTIMIZER = "vendor/dependencies/datafusion-optimizer/src"


def patch(workspace, case):
    def replace(name, anchor, text):
        path = workspace / name
        original = path.read_text()
        assert original.count(anchor) == 1, (name, anchor)
        path.write_text(original.replace(anchor, text))
        changed.add(name)

    changed = set()
    module = f"{EXPR}/planning_visit_counts.rs"
    (workspace / module).write_bytes((HERE / "visit_counts.rs").read_bytes())
    changed.add(module)
    library = f"{EXPR}/lib.rs"
    path = workspace / library
    path.write_text(path.read_text() + '\n#[doc(hidden)]\npub mod planning_visit_counts;\n')
    changed.add(library)

    tree = f"{EXPR}/logical_plan/tree_node.rs"
    for function, expression, output, variable in [
        ("apply_expressions", "&Expr", "TreeNodeRecursion", "borrowed"),
        ("map_expressions", "Expr", "Transformed<Self>", "owned"),
    ]:
        signature = (f"pub fn {function}<F: FnMut({expression}) -> Result<"
                     f"{'Transformed<Expr>' if variable == 'owned' else output}>>(\n"
                     f"        {'self' if variable == 'owned' else '&self'},\n"
                     f"        mut f: F,\n    ) -> Result<{output}> {{")
        replace(tree, signature, signature + f"""
        let mut f = |expression: {expression}| {{
            crate::planning_visit_counts::record_root();
            f(expression)
        }};""")

    tree = f"{EXPR}/tree_node.rs"
    for signature, expression in [
        ("fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(\n"
         "        &'n self,\n        f: F,\n    ) -> Result<TreeNodeRecursion> {", "&'n Self"),
        ("fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(\n"
         "        self,\n        f: F,\n    ) -> Result<Transformed<Self>> {", "Self"),
    ]:
        replace(tree, signature, signature.replace("f: F", "mut f: F") + f"""
        let f = |expression: {expression}| {{
            crate::planning_visit_counts::record_child();
            f(expression)
        }};""")

    analyzer = f"{OPTIMIZER}/analyzer/mod.rs"
    replace(analyzer, "            new_plan = rule\n                .analyze(new_plan, config)",
            "            let input = datafusion_expr::planning_visit_counts::begin(&new_plan)?;\n"
            "            new_plan = rule\n                .analyze(new_plan, config)")
    replace(analyzer, "            log_plan(rule.name(), &new_plan);",
            '            datafusion_expr::planning_visit_counts::finish("analyzer", 0, rule.name(), input);\n'
            "            log_plan(rule.name(), &new_plan);")

    optimizer = f"{OPTIMIZER}/optimizer.rs"
    replace(optimizer, "                let result = match rule.apply_order() {",
            "                let input = datafusion_expr::planning_visit_counts::begin(&new_plan)?;\n"
            "                let result = match rule.apply_order() {")
    anchor = "                }\n                .and_then(|tnr| {\n                    // run checks optimizer invariant checks"
    replace(optimizer, anchor,
            '                };\n'
            '                datafusion_expr::planning_visit_counts::finish("optimizer", i as usize, rule.name(), input);\n'
            '                let result = result.and_then(|tnr| {\n'
            '                    // run checks optimizer invariant checks')

    driver = "benches/planning_attribution/driver.rs"
    replace(driver, '    let trace = args[0] == "trace";',
            '    let trace = args[0] == "trace";\n    if !trace { return Err("inventory only".into()); }')
    replace(driver, "    for query in &manifest.queries {",
            f'    for query in &manifest.queries {{\n        if query.id != "{case}" {{ continue; }}')
    replace(driver, '    let expected = if trace && configuration.starts_with("spark-") {\n'
            '        309\n    } else {\n        30\n    };', '    let expected = 1;')
    return changed


def build(protocol, source, target, destination):
    receipt = assemble.assemble(source, observed=False)
    workspace = source / assemble.WORKSPACE.relative_to(assemble.REPO)
    changed = patch(workspace, protocol["query_id"])
    receipt["assembly"].update({name: assemble.digest(workspace / name) for name in sorted(changed)})
    receipt["tree_sha256"] = run.tree_hash(source)
    run.write(source / "planning-1390-assembly.json", receipt)
    run.build(source, target, destination, refresh_paths=sorted(changed))


def trace(protocol, build_receipt, delta, destination):
    receipt = run.verify_build(build_receipt)
    expected = run.read(assemble.WORKSPACE / "benches/reports/planning-1379-metadata.json")["delta_files"]
    assert run.delta_hashes(delta) == expected
    assert len(run.CONFIGURATIONS) == protocol["maximum_trace_processes"]
    destination.mkdir()
    rows = []
    for configuration in run.CONFIGURATIONS:
        output = run.invoke(receipt["binary"], ["trace", str(delta), configuration, configuration, "-"],
                            destination, configuration)
        assert len(output) == 1 and output[0]["id"] == protocol["query_id"]
        for line in (destination / (configuration + ".stderr")).read_text().splitlines():
            tag, stage, index, rule, projections, expressions, roots, children = line.split("\t")
            assert tag == "DAR_WIDE" and stage in ("analyzer", "optimizer")
            rows.append(dict(configuration=configuration, stage=stage, rule=rule, index=int(index),
                             input_projections=int(projections), input_expressions=int(expressions),
                             root_visits=int(roots), child_visits=int(children)))
        assert {row["stage"] for row in rows if row["configuration"] == configuration} == {"analyzer", "optimizer"}
    assert run.delta_hashes(delta) == expected
    run.write(destination / "rule-visits.json", rows)


def main():
    if sys.flags.optimize:
        raise SystemExit("validation requires Python assertions")
    protocol = run.read(HERE / "wide-inventory.json")
    assemble.BASELINE = protocol["staging_source"]
    if len(sys.argv) != 5 or sys.argv[1] not in ("build", "trace"):
        raise SystemExit("usage: inventory.py build|trace ARGUMENTS")
    args = [pathlib.Path(argument).resolve() for argument in sys.argv[2:]]
    (build if sys.argv[1] == "build" else trace)(protocol, *args)


if __name__ == "__main__":
    main()
