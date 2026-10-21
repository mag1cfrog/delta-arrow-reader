use super::*;
use crate::{selective_read::storage::read_json, selective_read_build::digest};
use rows::{Cell, Layout, Row, Rows};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
};
pub(super) mod fixtures;

#[test]
fn oracle_integer_and_date_overflow_returns_errors() -> Result<()> {
    let layout = Layout::new(&["row_id".into()])?;
    for value in [i64::from(i32::MIN) - 1, i64::from(i32::MAX) + 1] {
        let row = layout.row(vec![Cell::Int(value)])?;
        assert!(layout.batch(&[row]).is_err());
    }
    let layout = Layout::new(&[
        "l_orderkey".into(),
        "l_linenumber".into(),
        "l_shipdate".into(),
    ])?;
    let row = layout.row(vec![Cell::Int(1), Cell::Int(1), Cell::Int(i64::MAX)])?;
    assert!(layout.batch(&[row]).is_err());
    for value in [i64::from(i32::MAX), i64::MAX] {
        let condition = rules::Condition {
            name: "l_shipdate".into(),
            op: ">=",
            values: vec![Cell::Int(value)],
        };
        assert!(condition.describe().is_err());
    }
    Ok(())
}

pub(super) fn save(path: &Path, value: &Value) -> Result<()> {
    fs::write(path, serde_json::to_vec(value)?)?;
    Ok(())
}

pub(super) fn state() -> Result<State> {
    let python = std::env::var_os("SELECTIVE_READ_ORACLE_PYTHON")
        .ok_or("set SELECTIVE_READ_ORACLE_PYTHON to the pinned oracle interpreter")?;
    State::new(
        Path::new(&python),
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("benches/selective_read"),
    )
}
fn rows(state: &mut State, reference: &Path, projection: &[String]) -> Result<Vec<Row>> {
    let layout = Layout::new(projection)?;
    let mut parquet = state
        .decoder
        .open(&reference.join("reference.parquet"), &[])?;
    let rows = Rows::new(parquet.by_ref(), &layout).collect::<Result<Vec<_>>>()?;
    parquet.finish()?;
    Ok(rows)
}
fn export(path: &Path, layout: &Layout, rows: &[Row]) -> Result<()> {
    let mut writer =
        arrow_ipc::writer::StreamWriter::try_new(fs::File::create(path)?, &layout.schema)?;
    for part in rows.chunks(7) {
        writer.write(&layout.batch(part)?)?;
    }
    writer.finish()?;
    Ok(())
}
fn provenance(metadata: &Value, result: &Path) -> Result<Value> {
    let mut value = crate::selective_read::contract::comparison(metadata)?;
    for name in check::IDENTITY_FIELDS {
        value[name] = metadata[name].clone();
    }
    value.as_object_mut().unwrap().extend(json!({"reader_id":"delta-arrow-reader","reader_build_sha256":"1".repeat(64),"reader_config":{"threads":8},"reader_artifact":{"executable_sha256":"3".repeat(64)},"native_expression":null,"result_sha256":digest(result)?}).as_object().unwrap().clone());
    Ok(value)
}
fn check_request(root: &Path, reference: &Path, result: &Path, identity: &Path) -> Value {
    json!({"action":"check-export","fixtures":root,"reference":reference,"result":result,"identity":identity})
}

#[test]
#[ignore = "requires the pinned PyArrow environment"]
fn public_matrix_exact_values_corruption_identity_and_limit_membership() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let fixtures = root.join("fixtures");
    let manifest = fixtures::dataset(&fixtures)?;
    let mut state = state()?;
    for (family, is_wide) in [("li", false), ("wide", true)] {
        for organization in ["clustered", "shuffled"] {
            for query in fixtures::cases(is_wide) {
                let case = format!("{family}.{organization}.{query}");
                let reference = root.join(&case);
                let metadata=state.prepare(&json!({"fixtures":fixtures,"case_id":case,"reference":reference,"reuse":false}))?;
                for key in [
                    "protocol_sha256",
                    "oracle_sha256",
                    "oracle_io_sha256",
                    "oracle_dependencies_sha256",
                    "generator_protocol_sha256",
                ] {
                    assert!(metadata.get(key).is_none(), "{key}");
                }
                if case == "li.clustered.all-keys" {
                    let mut reuse = json!({"fixtures":fixtures,"case_id":case,"reference":reference,"reuse":true,"sql":metadata["canonical_sql"]});
                    assert_eq!(state.prepare(&reuse)?, metadata);
                    reuse["sql"] = json!("SELECT wrong FROM bench");
                    assert!(state.prepare(&reuse).is_err());
                }
                let projection = rules::strings(&metadata["projection"])?;
                let expected = rows(&mut state, &reference, &projection)?;
                assert_eq!(
                    expected.iter().map(|r| r.key[0]).collect::<Vec<_>>(),
                    fixtures::expected(query),
                    "{case}"
                );
                let mut actual = expected.clone();
                actual.reverse();
                if query == &"date7-limit" {
                    actual.truncate(100);
                }
                let layout = Layout::new(&projection)?;
                let result = root.join("result.arrow");
                let identity = root.join("identity.json");
                export(&result, &layout, &actual)?;
                save(&identity, &provenance(&metadata, &result)?)?;
                let request = check_request(&fixtures, &reference, &result, &identity);
                assert_eq!(state.check(&request)?["status"], "passed");
                if query == &"eq2-in20" {
                    let duplicate = root.join(format!("{case}.duplicate"));
                    let extra=state.prepare(&json!({"fixtures":fixtures,"case_id":case,"reference":duplicate,"duplicate_in_literal":true}))?;
                    assert!(extra["canonical_sql"].as_str().unwrap().ends_with(", 1)"));
                    assert_eq!(rows(&mut state, &duplicate, &projection)?, expected);
                }
                if actual.is_empty() {
                    continue;
                }
                for corrupt in ["key", "count", "duplicate", "value", "null", "trailer"] {
                    if corrupt == "duplicate" && actual.len() < 2 {
                        continue;
                    }
                    let mut bad = actual.clone();
                    match corrupt {
                        "key" => {
                            bad[0].key[0] = 0;
                            bad[0].values[0] = Cell::Int(0);
                        }
                        "count" => {
                            bad.pop();
                        }
                        "duplicate" => bad[0] = bad[1].clone(),
                        "value" => {
                            if let Some(i) = projection.iter().position(|n| n == "l_quantity") {
                                bad[0].values[i] = Cell::Decimal(1701);
                            } else {
                                bad[0].values[0] = Cell::Int(9999);
                            }
                        }
                        "null" => bad[0].values[0] = Cell::Null,
                        _ => {}
                    }
                    if corrupt == "null" {
                        let mut batch = layout.batch(&actual)?;
                        let fields = layout
                            .schema
                            .fields()
                            .iter()
                            .map(|f| f.as_ref().clone().with_nullable(true))
                            .collect::<Vec<_>>();
                        let mut columns = batch.columns().to_vec();
                        columns[0] = std::sync::Arc::new(arrow_array::Int64Array::from_iter(
                            (0..actual.len())
                                .map(|i| if i == 0 { None } else { Some(actual[i].key[0]) }),
                        ));
                        batch = arrow_array::RecordBatch::try_new(
                            std::sync::Arc::new(arrow_schema::Schema::new(fields)),
                            columns,
                        )?;
                        let mut writer = arrow_ipc::writer::StreamWriter::try_new(
                            fs::File::create(&result)?,
                            &batch.schema(),
                        )?;
                        writer.write(&batch)?;
                        writer.finish()?;
                    } else {
                        export(&result, &layout, &bad)?;
                    }
                    if corrupt == "trailer" {
                        use std::io::Write;
                        fs::OpenOptions::new()
                            .append(true)
                            .open(&result)?
                            .write_all(b"trailing")?;
                    }
                    save(&identity, &provenance(&metadata, &result)?)?;
                    assert!(state.check(&request).is_err(), "{case} accepted {corrupt}");
                }
                export(&result, &layout, &actual)?;
                let good = provenance(&metadata, &result)?;
                for (name, value) in [
                    ("canonical_sql", json!("SELECT wrong")),
                    ("reader_id", json!("unknown")),
                    ("snapshot_version", json!(99)),
                    ("reader_config", Value::Null),
                    ("native_expression", json!({})),
                    ("result_sha256", json!("0".repeat(64))),
                ] {
                    let mut bad = good.clone();
                    bad[name] = value;
                    save(&identity, &bad)?;
                    assert!(state.check(&request).is_err(), "accepted {name}");
                }
                let mut audit = good;
                audit["protocol_sha256"] = json!("edited documentation");
                audit["reader_build_sha256"] = json!("a".repeat(64));
                save(&identity, &audit)?;
                assert_eq!(state.check(&request)?["status"], "passed");
            }
        }
    }
    let bad = root.join("bad-reference");
    let mut changed = manifest.clone();
    changed["status"] = json!("preparing");
    save(&fixtures.join("manifest.json"), &changed)?;
    assert!(
        state
            .prepare(
                &json!({"fixtures":fixtures,"case_id":"li.clustered.all-full","reference":bad})
            )
            .is_err()
    );
    assert!(!bad.join("reference.json").exists());
    save(&fixtures.join("manifest.json"), &manifest)?;
    let mut changed = manifest;
    changed["tables"][0]["files"][0]["delta_stats"]["minValues"]["l_shipdate"] =
        json!("1990-01-01");
    changed["tables"][0]["files"][0]["delta_stats"]["maxValues"]["l_shipdate"] =
        json!("1990-01-01");
    fixtures::rewrite_log(&fixtures, &mut changed["tables"][0])?;
    save(&fixtures.join("manifest.json"), &changed)?;
    assert!(
        state
            .prepare(
                &json!({"fixtures":fixtures,"case_id":"li.clustered.date7-full","reference":bad})
            )
            .is_err()
    );
    assert!(!bad.join("reference.json").exists());
    Ok(())
}

#[test]
#[ignore = "requires the pinned PyArrow environment"]
fn independent_decoder_repacked_boundaries_schema_encodings_and_spill_limits() -> Result<()> {
    use arrow_array::{ArrayRef, LargeStringArray, StringViewArray};
    use std::sync::Arc;
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let mut state = state()?;
    let projection = vec![
        "l_orderkey".into(),
        "l_linenumber".into(),
        "l_comment".into(),
    ];
    let layout = Layout::new(&projection)?;
    let expected = (0..17000)
        .map(|n| Row {
            key: [n, 1],
            values: vec![Cell::Int(n), Cell::Int(1), Cell::Text("trailing  ".into())],
        })
        .collect::<Vec<_>>();
    let mut writer = io::Reference::new(&root.join("reference.parquet"), &layout, None)?;
    for row in &expected {
        writer.push(row.clone())?;
    }
    writer.finish()?;
    let metadata = json!({"projection":projection,"output_rows":expected.len()});
    let result = root.join("result.arrow");
    for kind in [
        arrow_schema::DataType::LargeUtf8,
        arrow_schema::DataType::Utf8View,
        arrow_schema::DataType::Dictionary(
            Box::new(arrow_schema::DataType::Int32),
            Box::new(arrow_schema::DataType::Utf8),
        ),
    ] {
        let batch = layout.batch(&expected)?;
        let strings: ArrayRef = match kind {
            arrow_schema::DataType::LargeUtf8 => {
                Arc::new(LargeStringArray::from(vec!["trailing  "; expected.len()]))
            }
            arrow_schema::DataType::Utf8View => {
                Arc::new(StringViewArray::from(vec!["trailing  "; expected.len()]))
            }
            _ => arrow_cast::cast(batch.column(2), &kind)?,
        };
        let mut columns = batch.columns().to_vec();
        columns[2] = strings;
        let schema = arrow_schema::Schema::new(vec![
            layout.schema.field(0).clone(),
            layout.schema.field(1).clone(),
            arrow_schema::Field::new("l_comment", kind, true),
        ]);
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(fs::File::create(&result)?, &schema)?;
        writer.write(&arrow_array::RecordBatch::try_new(
            Arc::new(schema),
            columns,
        )?)?;
        writer.finish()?;
        assert_eq!(
            state.compare_file(&result, root, &metadata, None, false)?.0,
            17000
        );
    }
    let left = fixtures::group(root, "left", &layout, &expected, false)?;
    let mut right = fixtures::group(root, "right", &layout, &expected, false)?;
    let one = root.join("right/all.parquet");
    fixtures::write(&one, &layout, &expected)?;
    right["files"] =
        json!([{"path":"all.parquet","bytes":fs::metadata(&one)?.len(),"sha256":digest(&one)?}]);
    assert_eq!(state.same_tables(root, &left, root, &right)?, 17000);
    let mut bad = expected.clone();
    bad[8000].values[2] = Cell::Text("wrong".into());
    fixtures::write(&one, &layout, &bad)?;
    right["files"][0]["bytes"] = json!(fs::metadata(&one)?.len());
    right["files"][0]["sha256"] = json!(digest(&one)?);
    assert!(state.same_tables(root, &left, root, &right).is_err());
    assert!(
        io::Reference::new(&root.join("limited.parquet"), &layout, Some(1))?
            .finish()
            .is_err()
    );
    Ok(())
}

#[test]
#[ignore = "requires the pinned PyArrow environment and SELECTIVE_READ_FIXTURES"]
fn generated_public_fixture_uses_independent_expected_values() -> Result<()> {
    let fixture = PathBuf::from(
        std::env::var_os("SELECTIVE_READ_FIXTURES").ok_or("set SELECTIVE_READ_FIXTURES")?,
    );
    let temporary = tempfile::tempdir()?;
    let mut state = state()?;
    for (family, wide) in [("li", false), ("wide", true)] {
        for organization in ["clustered", "shuffled"] {
            for query in fixtures::cases(wide) {
                let case = format!("{family}.{organization}.{query}");
                let reference = temporary.path().join(&case);
                let metadata = state
                    .prepare(&json!({"fixtures":fixture,"case_id":case,"reference":reference}))?;
                assert_eq!(metadata["status"], "complete");
                assert_eq!(
                    metadata["output_rows"].as_u64().unwrap() == 0,
                    *query == "empty"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn retained_catalog_matches_independent_sql_projections_and_literals() -> Result<()> {
    let catalog = read_json(
        &Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("benches/selective_read/query-matrix.json"),
    )?;
    for (scale, source) in catalog["scales"].as_object().unwrap() {
        let literals = source["in_literals"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_i64().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(literals.len(), if scale == "0.01" { 5 } else { 20 });
        assert!(literals.windows(2).all(|p| p[0] < p[1]));
        for (family, wide) in [("li", false), ("wide", true)] {
            assert_eq!(
                catalog["shapes"][family].as_object().unwrap().len(),
                fixtures::cases(wide).len()
            );
            for query in fixtures::cases(wide) {
                let (projection, predicate, limit) = fixtures::shape(query, wide);
                let shape = &catalog["shapes"][family][query];
                assert_eq!(shape["projection"], json!(projection));
                assert_eq!(shape["predicate"], predicate);
                assert_eq!(shape["limit"], json!(limit));
                assert_eq!(
                    source["queries"][family][query]["sql"],
                    rules::sql(&projection, predicate, limit, &literals)?
                );
            }
        }
    }
    Ok(())
}

#[test]
#[ignore = "requires the pinned PyArrow environment; run with --test-threads=1"]
fn bounded_workload_contracts_reject_changed_inputs_but_accept_audit_edits() -> Result<()> {
    use crate::selective_read::workload::{self, Request};
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let fixtures = root.join("fixtures");
    let manifest = fixtures::dataset(&fixtures)?;
    let mut state = state()?;
    let original_harness = state.harness.clone();
    let harness = root.join("harness/benches/selective_read");
    fs::create_dir_all(&harness)?;
    for name in ["oracle_io.py", "oracle-requirements.txt"] {
        fs::copy(original_harness.join(name), harness.join(name))?;
    }
    let protocols = harness.join("../../docs/content/benchmarks");
    fs::create_dir_all(&protocols)?;
    fs::copy(
        original_harness.join("../../docs/content/benchmarks/selective-read-protocol.md"),
        protocols.join("selective-read-protocol.md"),
    )?;
    let case = "large.wide.shuffled.date30-wide";
    let mut row = workload::run(Request::LargeQuery {
        case: case.into(),
        manifest: manifest.clone(),
    })?;
    row["fixtures"] = json!(fixtures);
    row["fixture_manifest_sha256"] = json!(digest(&fixtures.join("manifest.json"))?);
    let expression = json!({"canonical_sql":row["canonical_sql"],"native_expression":{"projection":row["projection"]}});
    let mut translations = json!({});
    for reader in ["polars", "daft"] {
        let path = harness.join("runners").join(reader).join("lock.json");
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(&path, "{}")?;
        translations[reader] =
            json!({"expressions":{case:expression},"lock_sha256":digest(&path)?});
    }
    let value = json!({"format":"selective-read-large-workload-v1","comparison_revision":4,"scope":"smoke","family":"pilot","publication_ready":false,
        "protocol_sha256":"old documentation","base_protocol_sha256":"old base documentation","sampling_sha256":"old sampling documentation","source_sha256":{"oracle.py":"historical"},
        "readers":["delta-arrow-reader","delta-rs","duckdb","polars","daft"],"sessions":{"reuse.large.date30":case},"scales":{"data":0.01},"cases":[row],"translations":translations,
        "oracle_limits":{"memory_bytes":16*1024u64.pow(3),"disk_bytes":64*1024*1024,"elapsed_seconds":30}});
    let workload = root.join("workload.json");
    save(&workload, &value)?;
    state = State::new(&state.python, &harness)?;
    let reference = root.join("reference");
    let request =
        json!({"fixtures":fixtures,"case_id":case,"reference":reference,"workload":workload});
    let metadata = state.prepare(&request)?;
    assert_eq!(metadata["output_rows"], 144);
    assert!(metadata["projected_logical_bytes"].as_u64().unwrap() > 144);
    let projection = rules::strings(&metadata["projection"])?;
    let actual = rows(&mut state, &reference, &projection)?;
    let layout = Layout::new(&projection)?;
    let result = root.join("result.arrow");
    let identity = root.join("identity.json");
    export(&result, &layout, &actual)?;
    save(&identity, &provenance(&metadata, &result)?)?;
    let check = check_request(&fixtures, &reference, &result, &identity);
    assert_eq!(state.check(&check)?["status"], "passed");
    let mut audit = value.clone();
    audit["source_sha256"] = json!({"oracle":"updated audit record"});
    save(&workload, &audit)?;
    assert_eq!(state.check(&check)?["status"], "passed");
    let mut identity_value = provenance(&metadata, &result)?;
    identity_value["workload_manifest_sha256"] = json!("0".repeat(64));
    save(&identity, &identity_value)?;
    assert_eq!(state.check(&check)?["status"], "passed");
    identity_value
        .as_object_mut()
        .unwrap()
        .remove("workload_contract");
    save(&identity, &identity_value)?;
    assert!(state.check(&check).is_err());
    save(&identity, &provenance(&metadata, &result)?)?;
    let mut changed = value.clone();
    changed["oracle_limits"]["elapsed_seconds"] = json!(31);
    save(&workload, &changed)?;
    assert!(state.check(&check).is_err());
    save(&workload, &value)?;
    let session = root.join("session");
    fs::create_dir(&session)?;
    fs::copy(&result, session.join("query-0.arrow"))?;
    fs::File::create(session.join("query-1.arrow"))?.set_len(32 * 1024 * 1024)?;
    let mut over = check.clone();
    over["result"] = json!(session.join("query-0.arrow"));
    assert!(
        state
            .check(&over)
            .unwrap_err()
            .to_string()
            .contains("session exports")
    );
    for label in ["limits", "manifest", "audit"] {
        let output = root.join(label);
        let mut request = request.clone();
        request["reference"] = json!(output);
        let mut changed = value.clone();
        let path = if label == "manifest" {
            fixtures.join("manifest.json")
        } else {
            workload.clone()
        };
        if label == "manifest" {
            changed = manifest.clone();
            changed["changed"] = json!(true);
        } else if label == "limits" {
            changed["oracle_limits"]["elapsed_seconds"] = json!(31);
        } else {
            changed["source_sha256"] = json!({"updated":"audit only"});
        }
        let watched = output.join("reference.parquet");
        let change = std::thread::spawn(move || -> std::result::Result<(), String> {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
            while !watched.exists() {
                if std::time::Instant::now() > deadline {
                    return Err("reference was not opened".into());
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            fs::write(&path, serde_json::to_vec(&changed).unwrap()).map_err(|e| e.to_string())
        });
        let mut fresh = State::new(&state.python, &harness)?;
        let prepared = fresh.prepare(&request);
        change.join().unwrap().map_err(std::io::Error::other)?;
        if label == "audit" {
            prepared?;
        } else {
            assert!(prepared.is_err(), "accepted concurrent {label} change");
            assert!(!output.join("reference.json").exists());
        }
        save(&workload, &value)?;
        save(&fixtures.join("manifest.json"), &manifest)?;
    }
    Ok(())
}
