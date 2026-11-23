"""Build the frozen function harness with restricted in-memory inputs."""

import argparse
from pathlib import Path
import sys
from unittest.mock import patch

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "time_conversions_1409_round3"))
import run_pair
from instrument import replace


def restricted_changes(workspace, instrumented):
    parts = dict(run_pair_original_changes(workspace, instrumented))
    path = workspace / "benches" / "functions.rs"
    source = parts[path]
    source = replace(source, "    projection_sql: Option<String>,",
                     "    projection_sql: Option<String>,\n    restricted_input_sql: Option<String>,")
    source = replace(source, "struct Sessions {", "struct Sessions {\n    input_rows: usize,")
    source = replace(source, "fn sessions() -> Result<Sessions> {",
                     "async fn sessions(case: &Case) -> Result<Sessions> {")
    source = replace(source, """    let table = Arc::new(MemTable::try_new(batches[0].schema(), vec![batches])?);
    native.register_table("bench", table.clone())?;
    spark.register_table("bench", table)?;
    Ok(Sessions { native, spark })""", """    let table = Arc::new(MemTable::try_new(batches[0].schema(), vec![batches])?);
    native.register_table("bench", table)?;
    if let Some(sql) = &case.restricted_input_sql {
        let restricted = native.sql(sql).await?.collect().await?;
        let schema = restricted.first().ok_or("restricted fixture is empty")?.schema();
        native.deregister_table("bench")?;
        native.register_table("bench", Arc::new(MemTable::try_new(schema, vec![restricted])?))?;
    }
    let table = native.table_provider("bench").await?;
    spark.register_table("bench", table)?;
    let input_rows = native.sql("SELECT COUNT(*) FROM bench").await?.collect().await?;
    let input_rows = input_rows[0].column(0).as_any().downcast_ref::<Int64Array>()
        .ok_or("fixture count type")?.value(0);
    let input_rows = usize::try_from(input_rows)?;
    if input_rows == 0 || input_rows > ROWS {
        return Err("invalid restricted fixture row count".into());
    }
    Ok(Sessions { native, spark, input_rows })""")
    source = replace(source, """async fn complete_rows(
    context: &SessionContext,""", """async fn complete_rows(
    context: &SessionContext,
    input_rows: usize,""")
    source = replace(source, "consumed != ROWS", "consumed != input_rows")
    source = replace(source, "expected {ROWS}", "expected {input_rows}")
    start = source.index("async fn preflight(")
    end = source.index("\nasync fn drain(", start)
    preflight = source[start:end].replace("complete_rows(context, physical)",
                                          "complete_rows(context, sessions.input_rows, physical)")
    preflight = preflight.replace("ROWS", "sessions.input_rows")
    source = source[:start] + preflight + source[end:]
    start = source.index("async fn measure(")
    end = source.index("\nasync fn run(", start)
    measure = source[start:end].replace("configuration: &str)", "configuration: &str, input_rows: usize)")
    source = source[:start] + measure.replace("ROWS", "input_rows") + source[end:]
    source = replace(source, "        let contexts = sessions()?;\n        for id in ids {",
                     "        for id in ids {")
    source = replace(source, "            let measured = tokio::time::timeout(",
                     "            let contexts = sessions(case).await?;\n"
                     "            let measured = tokio::time::timeout(")
    source = replace(source, "    let contexts = sessions()?;", "    let contexts = sessions(case).await?;")
    source = replace(source, 'measure(contexts.get(&args[3]), case, &args[3])',
                     'measure(contexts.get(&args[3]), case, &args[3], contexts.input_rows)', 2)
    parts[path] = source
    return parts.items()


run_pair_original_changes = run_pair.changes

if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("source")
    for name in ("worktree", "target", "output"):
        cli.add_argument(name, type=lambda v: Path(v).resolve())
    args = cli.parse_args()
    with patch.object(run_pair, "changes", restricted_changes):
        run_pair.build(args.source, args.worktree, args.target, args.output)
