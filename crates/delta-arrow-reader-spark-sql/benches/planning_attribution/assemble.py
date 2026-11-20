#!/usr/bin/env python3
"""Assemble the same driver with untouched or observed production source."""

import hashlib
import json
import pathlib
import subprocess
import tarfile
import tempfile

HERE = pathlib.Path(__file__).resolve().parent
WORKSPACE = HERE.parents[1]
REPO = WORKSPACE.parents[1]
BASELINE = "9fa5b4cd98fecc7a2e95debc67128c394b25fb9a"
SAIL = "vendor/sail/crates/"
OPT = "vendor/dependencies/datafusion-optimizer/src/"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def patch_sources(workspace=WORKSPACE):
    """Fail closed if any reviewed insertion/replacement anchor has drifted."""
    changed = {}
    anchors = []

    def replace(path, old, new):
        source = changed.get(path, (workspace / path).read_text())
        assert source.count(old) == 1, (path, old, source.count(old))
        changed[path] = source.replace(old, new, 1)
        anchors.append({"path": path, "anchor_sha256": hashlib.sha256(old.encode()).hexdigest()})

    def before(path, anchor, insertion):
        replace(path, anchor, insertion + anchor)

    def after(path, anchor, insertion):
        replace(path, anchor, anchor + insertion)

    def span(path, first, last, name, variable):
        before(path, first, f'let {variable} = crate::planning_attribution::Span::new("{name}");\n        ')
        after(path, last, f'\n        drop({variable});')

    modules = ["src/lib.rs", SAIL + "sail-sql-analyzer/src/lib.rs",
               SAIL + "sail-plan/src/lib.rs", OPT + "lib.rs"]
    for path in modules:
        # Append after all inner attributes/documentation, without changing them.
        source = (workspace / path).read_text()
        changed[path] = source + '\n/// Disposable benchmark observations.\npub mod planning_attribution;\n'

    path = "src/lib.rs"
    span(path, "let sql = sail_sql_analyzer::substitution::substitute_sql_variables(",
         "&options.variable_substitution_values,\n    )\n    .map_err(|error| DataFusionError::External(Box::new(error)))?;",
         "logical.substitution", "__substitution")
    span(path, "let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)",
         "let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)\n        .map_err(|error| DataFusionError::External(Box::new(error)))?;",
         "logical.parsing", "__parsing")
    span(path, "let statement = sail_sql_analyzer::statement::from_ast_read_only_statement(ast)",
         "let statement = sail_sql_analyzer::statement::from_ast_read_only_statement(ast)\n        .map_err(|error| DataFusionError::External(Box::new(error)))?;",
         "logical.ast_conversion", "__ast")
    span(path, "let options = options\n        .with_normalized_timezone()",
         ".with_normalized_timezone()\n        .map_err(|error| DataFusionError::External(Box::new(error)))?;",
         "logical.timezone", "__timezone")
    span(path, "let mut state = session.state();", "let frame = DataFrame::new(state, self.logical_plan.clone());",
         "physical.preparation", "__preparation")
    span(path, "let physical = frame.create_physical_plan().await?;", "let physical = frame.create_physical_plan().await?;",
         "physical.frame", "__frame")
    before(path, "rename_physical_plan(physical, &self.column_names)",
           'let _rename = crate::planning_attribution::Span::new("physical.rename");\n        ')

    path = "src/statement_planning.rs"
    span(path, "let resolver = if matches!(&statement, ReadOnlyStatement::Query(_)) {",
         ".with_file_metadata_adapter(Arc::new(crate::file_metadata::adapt));",
         "logical.resolver_preparation", "__resolver")
    span(path, "let named = resolver\n            .resolve_named_plan(query)",
         ".resolve_named_plan(query)\n            .await\n            .map_err(|error| DataFusionError::External(Box::new(error)))?;",
         "logical.resolution", "__resolution")

    path = SAIL + "sail-sql-analyzer/src/parser.rs"
    first = "        let options = ParserOptions::default();\n        let length = $input.len();"
    replace(path, first, first + '\n        let __statement = stringify!($parser) == "create_parser";')
    macro_lexer = ('        let __statement = stringify!($parser) == "create_parser";\n'
                   "        let lexer = create_lexer::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);")
    replace(path, macro_lexer,
            macro_lexer.replace("        let lexer =", '        let __lexer = crate::planning_attribution::Span::when("logical.parsing.lexer_construction", __statement);\n        let lexer =')
            + '\n        drop(__lexer);\n        let __lex = crate::planning_attribution::Span::when("logical.parsing.lexing", __statement);')
    replace(path, "            .map_err(SqlError::parser)?;\n        let tokens = tokens",
            '            .map_err(SqlError::parser)?;\n        drop(__lex);\n        let __input = crate::planning_attribution::Span::when("logical.parsing.token_input", __statement);\n        let tokens = tokens')
    after(path, "            .map((length..length).into(), map_parser_input);",
          '\n        drop(__input);\n        let __construction = crate::planning_attribution::Span::when("logical.parsing.parser_construction", __statement);')
    after(path, "        let parser = $parser::<_, chumsky::extra::Err<chumsky::error::Rich<_, _>>>(&options);",
          '\n        drop(__construction);\n        let __parse = crate::planning_attribution::Span::when("logical.parsing.parse", __statement);')
    replace(path, "        parser.parse(tokens).into_result().map_err(SqlError::parser)",
            "        let __result = parser.parse(tokens).into_result().map_err(SqlError::parser);\n        drop(__parse);\n        __result")

    path = OPT + "analyzer/mod.rs"
    before(path, "        // verify the logical plan required invariants at the start, before analyzer",
           '        let _wall = crate::planning_attribution::Span::new("physical.analyzer");\n')
    span(path, "plan.check_invariants(InvariantLevel::Always)",
         '.map_err(|e| e.context("Invalid input plan passed to Analyzer"))?;',
         "physical.analyzer.input_validation", "__input")
    span(path, "let expr_to_function: Option<Arc<dyn AnalyzerRule + Send + Sync>> =",
         "let rules = expr_to_function.iter().chain(self.rules.iter());",
         "physical.analyzer.function_rewrite_preparation", "__rewrites")
    before(path, "            new_plan = rule\n                .analyze(new_plan, config)",
           '            let __rule = crate::planning_attribution::Span::new(format!("physical.analyzer.rule/{}", rule.name()));\n')
    after(path, "                .map_err(|e| e.context(rule.name()))?;", "\n            drop(__rule);")
    span(path, "new_plan\n            .check_invariants(InvariantLevel::Executable)",
         '.map_err(|e| e.context("Invalid (non-executable) plan after Analyzer"))?;',
         "physical.analyzer.final_validation", "__final")

    path = OPT + "optimizer.rs"
    before(path, "        // verify LP is valid, before the first LP optimizer pass.",
           '        let _wall = crate::planning_attribution::Span::new("physical.optimizer");\n')
    span(path, "plan.check_invariants(InvariantLevel::Executable)",
         '.map_err(|e| e.context("Invalid input plan before LP Optimizers"))?;',
         "physical.optimizer.input_validation", "__input")
    span(path, "let mut previous_plans = HashSet::with_capacity(16);",
         "let mut previous_plans = HashSet::with_capacity(16);\n        previous_plans.insert(LogicalPlanSignature::new(&new_plan));",
         "physical.optimizer.initial_signature", "__initial")
    before(path, "                // If skipping failed rules, copy plan before attempting to rewrite",
           '                let __prefix = format!("physical.optimizer.rule/{i}/{}", rule.name());\n                let __prepare = crate::planning_attribution::Span::new(format!("{__prefix}/preparation"));\n')
    before(path, "                let result = match rule.apply_order() {",
           '                drop(__prepare);\n                let __body = crate::planning_attribution::Span::new(format!("{__prefix}/body"));\n')
    replace(path, "                }\n                .and_then(|tnr| {\n                    // run checks optimizer invariant checks, per optimizer rule applied",
            '                };\n                drop(__body);\n                let result = result.and_then(|tnr| {\n                    let _check = crate::planning_attribution::Span::new(format!("{__prefix}/validation"));\n                    // run checks optimizer invariant checks, per optimizer rule applied')
    before(path, "                // Handle results",
           '                let _status = crate::planning_attribution::Span::new(format!("{__prefix}/transformed={}", result.as_ref().is_ok_and(|result| result.transformed)));\n                drop(_status);\n')
    span(path, "let plan_is_fresh =\n                previous_plans.insert(LogicalPlanSignature::new(&new_plan));",
         "let plan_is_fresh =\n                previous_plans.insert(LogicalPlanSignature::new(&new_plan));",
         "physical.optimizer.pass_signature", "__signature")
    span(path, "assert_valid_optimization(&new_plan, &starting_schema).map_err(|e| {",
         'e.context("Check optimizer-specific invariants after all passes")\n        })?;',
         "physical.optimizer.final_schema_validation", "__schema")
    span(path, "new_plan\n            .check_invariants(InvariantLevel::Executable)",
         'e.context("Invalid (non-executable) plan after LP Optimizers")\n            })?;',
         "physical.optimizer.final_validation", "__final")

    path = SAIL + "sail-plan/src/physical_plan/mod.rs"
    before(path, "        let mut state = Cow::Borrowed(session_state);",
           '        let _wall = crate::planning_attribution::Span::new("physical.planner");\n')
    span(path, "let mut state = Cow::Borrowed(session_state);",
         "        })?;\n        let spark_semantics = session_state",
         "physical.planner.extension_order_scan", "__order")
    # Move the inserted drop before the following let, keeping the original expression intact.
    replace(path, "        })?;\n        let spark_semantics = session_state\n        drop(__order);",
            "        })?;\n        drop(__order);\n        let spark_semantics = session_state")
    span(path, "let spark_semantics = session_state", "            Cow::Borrowed(logical_plan)\n        };",
         "physical.planner.spark_lowering", "__lower")
    span(path, "let runtime_plan = drop_declaration_inputs(runtime_plan.into_owned(), session_state)?;",
         "let runtime_plan = drop_declaration_inputs(runtime_plan.into_owned(), session_state)?;",
         "physical.planner.drop_declaration_inputs", "__declarations")
    span(path, "let runtime_plan = if spark_semantics {\n            align_result_metadata_for_execution(runtime_plan)?",
         "            runtime_plan\n        };",
         "physical.planner.align_result_metadata", "__metadata")
    span(path, "let physical =\n            DefaultPhysicalPlanner::with_extension_planners",
         ".create_physical_plan(&runtime_plan, &state)\n                .await?;",
         "physical.planner.default_physical_planner", "__default")
    before(path, "        seeded_rand::bind_seeded_rand_streams(physical)",
           '        let _rand = crate::planning_attribution::Span::new("physical.planner.seeded_rand_binding");\n')
    return changed, modules, anchors


def driver_source(observed):
    events = "fn reset_events() {}\nfn take_events() -> TestResult<Vec<(String, u64)>> { Ok(Vec::new()) }"
    if observed:
        paths = ["delta_arrow_reader_spark_sql", "sail_sql_analyzer", "sail_plan", "datafusion::optimizer"]
        events = "fn reset_events() {\n" + "\n".join(f"    {path}::planning_attribution::reset();" for path in paths) + "\n}\n"
        events += "fn take_events() -> TestResult<Vec<(String, u64)>> {\n    let mut events = Vec::new();\n"
        events += "\n".join(f"    events.extend({path}::planning_attribution::take()?);" for path in paths)
        events += "\n    Ok(events)\n}"
    source = (HERE / "driver.rs").read_text()
    assert source.count("// @event_functions@") == 1
    return source.replace("// @event_functions@", events)


def assemble(destination, observed):
    destination = pathlib.Path(destination).resolve()
    assert not destination.exists(), "assembly destination must be new"
    assert not destination.is_relative_to(REPO), "disposable source must be outside the checkout"
    assert not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all"], cwd=REPO, text=True).strip(), "commit the reviewed protocol and harness before assembly"
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip()
    subprocess.run(["git", "merge-base", "--is-ancestor", BASELINE, commit], cwd=REPO, check=True)
    changed, modules, anchors = patch_sources() if observed else ({}, [], [])
    destination.mkdir(parents=True)
    with tempfile.TemporaryFile() as archive:
        subprocess.run(["git", "archive", "--format=tar", commit], cwd=REPO, stdout=archive, check=True)
        archive.seek(0)
        with tarfile.open(fileobj=archive) as source:
            source.extractall(destination, filter="data")
    workspace = destination / WORKSPACE.relative_to(REPO)
    generated = {}
    if observed:
        generated.update(changed)
        for path in modules:
            generated[str(pathlib.Path(path).parent / "planning_attribution.rs")] = (HERE / "events.rs").read_text()
    generated["benches/planning_attribution/driver.rs"] = driver_source(observed)
    generated["benches/planning_attribution_fixture.rs"] = (WORKSPACE / "benches/planning.rs").read_text() + '\n#[path = "planning_attribution/driver.rs"]\npub mod attribution;\n'
    generated["benches/planning_attribution_entry.rs"] = '''//! Run the planning-only attribution benchmark.

#[allow(dead_code)]
#[path = "planning_attribution_fixture.rs"]
mod frozen;
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    frozen::attribution::run().await
}
'''
    generated["Cargo.toml"] = (WORKSPACE / "Cargo.toml").read_text() + '\n[[bench]]\nname = "planning_attribution"\npath = "benches/planning_attribution_entry.rs"\nharness = false\n'
    for path, source in generated.items():
        (workspace / path).write_text(source)
    receipt = {"baseline": BASELINE, "protocol_commit": commit, "observed": observed, "anchors": anchors if observed else [],
               "assembly": {path: digest(workspace / path) for path in sorted(generated)}}
    (destination / "planning-1390-assembly.json").write_text(json.dumps(receipt, indent=2) + "\n")
    return receipt
