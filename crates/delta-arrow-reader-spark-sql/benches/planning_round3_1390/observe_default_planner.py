"""Attach release-only timing spans to a disposable DataFusion package."""

from pathlib import Path


def observe(package: Path, driver: Path, events: Path, sail_planner: Path):
    def once(text, old, new):
        assert text.count(old) == 1, old
        return text.replace(old, new, 1)

    def span(text, start, end, name, variable):
        begin = f'let {variable} = crate::planning_attribution::Span::new({name});'
        text = once(text, start, f"        {begin}\n{start}")
        return once(text, end, f"{end}\n        drop({variable});")

    planner = package / "src/physical_planner.rs"
    source = planner.read_text()
    lowering = """        let plan = self
            .create_initial_plan(logical_plan, session_state)
            .await?;"""
    source = span(source, lowering, lowering, '"default.lowering"', "lowering")
    optimize = "        self.optimize_physical_plan(plan, session_state, |_, _| {})"
    source = once(source, optimize, f'        let _optimize = crate::planning_attribution::Span::new("default.optimization");\n{optimize}')
    boundary = source.index("    fn create_project_physical_exec_with_props(")
    before, projection = source[:boundary], source[boundary:]
    boundary = projection.index('\n    ///')
    projection, after = projection[:boundary], projection[boundary:]
    projection = span(projection, "        let physical_exprs = expr",
                      "            .collect::<Result<Vec<_>>>()?;",
                      '"default.projection.expressions"', "expressions")
    sync = "                Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input_exec)?))"
    projection = once(projection, sync, '                let _build = crate::planning_attribution::Span::new("default.projection.build");\n' + sync)
    source = before + projection + after

    boundary = source.index("    pub fn optimize_physical_plan<F>(")
    before, optimizer = source[:boundary], source[boundary:]
    boundary = optimizer.index("    // return an record_batch")
    optimizer, after = optimizer[:boundary], optimizer[boundary:]
    for phase, level in [("start", "Always"), ("finish", "Executable")]:
        target = "plan" if phase == "start" else "new_plan"
        expression = f"        InvariantChecker(InvariantLevel::{level}).check(&{target})?;"
        optimizer = span(optimizer, expression, expression, f'"default.invariants.{phase}"', phase)
    call = """            new_plan = optimizer
                .optimize_with_context(new_plan, session_state)
                .map_err(|e| {
                    DataFusionError::Context(optimizer.name().to_string(), Box::new(e))
                })?;"""
    optimizer = once(optimizer, call, '            let rule = crate::planning_attribution::Span::new(format!("default.rule/{}/body", optimizer.name()));\n' + call + "\n            drop(rule);")
    validation = """            OptimizationInvariantChecker::new(optimizer)
                .check(&new_plan, &before_schema)?;"""
    optimizer = once(optimizer, validation, '            let validation = crate::planning_attribution::Span::new(format!("default.rule/{}/invariants", optimizer.name()));\n' + validation + "\n            drop(validation);")
    planner.write_text(before + optimizer + after)
    library = package / "src/lib.rs"
    library.write_text(library.read_text() + '\n/// Disposable planning measurements.\npub mod planning_attribution;\n')
    (package / "src/planning_attribution.rs").write_bytes(events.read_bytes())
    source = driver.read_text()
    source = once(source, "    datafusion::optimizer::planning_attribution::reset();",
                 "    datafusion::optimizer::planning_attribution::reset();\n    datafusion::planning_attribution::reset();")
    source = once(source, "    events.extend(datafusion::optimizer::planning_attribution::take()?);",
                 "    events.extend(datafusion::optimizer::planning_attribution::take()?);\n    events.extend(datafusion::planning_attribution::take()?);")
    driver.write_text(source)

    source = sail_planner.read_text()
    boundary = source.index("fn align_result_metadata_for_execution(")
    before, align = source[:boundary], source[boundary:]
    boundary = align.index("fn remove_alias_result_metadata(")
    align, after = align[:boundary], align[boundary:]
    begin = "            let node = node\n                .map_expressions(|expression| {"
    finish = "                })?\n                .data;"
    align = once(align, begin, '            let map = crate::planning_attribution::Span::new("metadata.alias_walk");\n' + begin)
    align = once(align, finish, finish + "\n            drop(map);")
    rebuild = "                node.recompute_schema()?"
    align = once(align, rebuild, '                let _schema = crate::planning_attribution::Span::new("metadata.recompute_schema");\n' + rebuild)
    sail_planner.write_text(before + align + after)
