use super::*;
use arrow_array::{
    ArrayRef, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray, types::Date32Type,
};
use std::sync::Arc;

#[test]
#[ignore = "requires pinned PyArrow/DuckDB and the compiled controller"]
fn source_predicates_and_fractional_file_membership_use_the_real_duckdb_bridge() -> Result<()> {
    let python = std::env::var_os("SELECTIVE_READ_ORACLE_PYTHON")
        .ok_or("set SELECTIVE_READ_ORACLE_PYTHON")?;
    let harness = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("benches/selective_read");
    let day = Date32Type::from_naive_date("1995-03-15".parse()?);
    let mut rows = Vec::new();
    for i in 0..10 {
        rows.push((
            2 * i + 2,
            if matches!(i, 4 | 6) { 2 } else { 1 },
            day + if i < 2 {
                -1
            } else if i < 7 {
                0
            } else {
                1
            },
            if matches!(i, 5 | 6) { "RAIL" } else { "AIR" },
        ));
        rows.push((
            2 * i + 1,
            if i == 4 { 2 } else { 1 },
            day + if i < 3 {
                -1
            } else if i < 5 {
                0
            } else {
                1
            },
            "AIR",
        ));
    }
    rows.reverse();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "l_orderkey",
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "l_linenumber",
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.1))),
        ),
        (
            "l_shipdate",
            Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.2))),
        ),
        (
            "l_shipmode",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
        ),
    ])?;
    let temporary = tempfile::tempdir()?;
    let output = temporary.path();
    let path = output.join("source.parquet");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(fs::File::create(&path)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    let shape = json!({"files":5,"stripes":2});
    let too_many = json!({"files":21,"stripes":2});
    let result = output.join("statistics.json");
    let mut child = fixtures::spawn(
        Command::new(python)
            .args(["-I", "-B"])
            .arg(harness.join("production_shapes.py"))
            .arg("--analyze")
            .arg(output)
            .stdin(Stdio::piped())
            .stdout(fs::File::create_new(&result)?),
        16 * 1024u64.pow(3),
    )?;
    serde_json::to_writer(
        child.0.stdin.take().ok_or("missing planner input")?,
        &json!({
            "paths":[path],"queries":{"valid":layout_query(&shape)?,"too_many":layout_query(&too_many)?},
        }),
    )?;
    require(
        wait_with_timeout(&mut child.0, Duration::from_secs(60))?.success(),
        "DuckDB geometry probe failed",
    )?;
    let result = read_json(&result)?;
    assert_eq!(result["counts"], json!([20, 7, 5, 3]));
    // Fractional cuts are 4/3/3 and 5/5; overlapping bounds retain one nonmatching file.
    assert_eq!(
        layout_geometry(&shape, &result["statistics"]["valid"])?,
        json!([
            {"stripe":0,"file_index":0,"rows":4,"candidate":true,"matching_rows":2},
            {"stripe":0,"file_index":1,"rows":3,"candidate":true,"matching_rows":0},
            {"stripe":0,"file_index":2,"rows":3,"candidate":false,"matching_rows":0},
            {"stripe":1,"file_index":0,"rows":5,"candidate":true,"matching_rows":1},
            {"stripe":1,"file_index":1,"rows":5,"candidate":false,"matching_rows":0},
        ])
    );
    assert!(
        layout_geometry(&too_many, &result["statistics"]["too_many"])
            .unwrap_err()
            .to_string()
            .contains("populate")
    );
    Ok(())
}
