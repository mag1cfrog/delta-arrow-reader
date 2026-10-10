use std::{
    fs::{self, File},
    io::Cursor,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{compute::concat_batches, ipc::reader::StreamReader};
use delta_arrow_reader::{
    DeltaComparison, DeltaPredicate, DeltaScalar, DeltaSnapshotSelection, DeltaTableBuilder,
    TryStreamExt, WarmupMode,
};
use serde_json::{Value, json};

use super::{run_with_timeout, sort_by_id};

struct PredicateFile {
    path: PathBuf,
}

impl PredicateFile {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let path = std::env::temp_dir().join(format!(
            "dar-predicate-{}-{}.json",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        File::create_new(&path)?;
        Ok(Self { path })
    }
}

impl Drop for PredicateFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn predicate_cases() -> Vec<(&'static str, Value, DeltaPredicate)> {
    let compare = |column: &str, op, value| DeltaPredicate::Compare {
        column: column.into(),
        op,
        value: DeltaScalar::Int32(value),
    };
    let mut cases = vec![
        (
            "constant_true",
            json!({"op": "constant", "value": true}),
            DeltaPredicate::Constant(true),
        ),
        (
            "constant_false",
            json!({"op": "constant", "value": false}),
            DeltaPredicate::Constant(false),
        ),
        (
            "empty_and",
            json!({"op": "and", "args": []}),
            DeltaPredicate::And(vec![]),
        ),
        (
            "empty_or",
            json!({"op": "or", "args": []}),
            DeltaPredicate::Or(vec![]),
        ),
        (
            "is_null",
            json!({"op": "is_null", "column": "value"}),
            DeltaPredicate::IsNull {
                column: "value".into(),
            },
        ),
        (
            "is_not_null",
            json!({"op": "is_not_null", "column": "value"}),
            DeltaPredicate::IsNotNull {
                column: "value".into(),
            },
        ),
        (
            "and",
            json!({"op": "and", "args": [
                {"op": "ge", "column": "value", "value": {"type": "int32", "value": "10"}},
                {"op": "lt", "column": "id", "value": {"type": "int32", "value": "10"}}
            ]}),
            DeltaPredicate::And(vec![
                compare("value", DeltaComparison::GtEq, 10),
                compare("id", DeltaComparison::Lt, 10),
            ]),
        ),
        (
            "or",
            json!({"op": "or", "args": [
                {"op": "lt", "column": "value", "value": {"type": "int32", "value": "10"}},
                {"op": "is_null", "column": "value"}
            ]}),
            DeltaPredicate::Or(vec![
                compare("value", DeltaComparison::Lt, 10),
                DeltaPredicate::IsNull {
                    column: "value".into(),
                },
            ]),
        ),
        (
            "not_nullable_comparison",
            json!({"op": "not", "arg": {
                "op": "eq", "column": "value", "value": {"type": "int32", "value": "30"}
            }}),
            DeltaPredicate::Not(Box::new(compare("value", DeltaComparison::Eq, 30))),
        ),
        (
            "deleted_row",
            json!({"op": "eq", "column": "id", "value": {"type": "int32", "value": "2"}}),
            compare("id", DeltaComparison::Eq, 2),
        ),
    ];
    for (name, op) in [
        ("eq", DeltaComparison::Eq),
        ("ne", DeltaComparison::NotEq),
        ("lt", DeltaComparison::Lt),
        ("le", DeltaComparison::LtEq),
        ("gt", DeltaComparison::Gt),
        ("ge", DeltaComparison::GtEq),
    ] {
        cases.push((
            name,
            json!({"op": name, "column": "value", "value": {"type": "int32", "value": "30"}}),
            compare("value", op, 30),
        ));
    }
    cases
}

#[test]
fn filtered_scans_match_native_predicates() -> Result<(), Box<dyn std::error::Error>> {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/reader/fixtures/external_writer/corpus");
    let predicate_file = PredicateFile::new()?;
    let cases = predicate_cases();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        for (fixture, version) in [
            ("partitioned", None),
            ("nested_mapping", None),
            ("deletion_vectors", None),
            ("deletion_vectors", Some(0)),
        ] {
            let path = corpus.join(fixture).join("table");
            let table = DeltaTableBuilder::new(path.to_str().unwrap())
                .with_warmup(WarmupMode::None)
                .with_snapshot_selection(version.map_or(
                    DeltaSnapshotSelection::Latest,
                    DeltaSnapshotSelection::Version,
                ))
                .load_table()
                .await?;
            for (name, predicate_json, native_predicate) in &cases {
                fs::write(&predicate_file.path, serde_json::to_vec(predicate_json)?)?;
                let projections: [Option<&[&str]>; 3] = [None, Some(&["id"]), Some(&[])];
                for columns in projections {
                    let mut builder = table.scan().with_predicate(native_predicate.clone());
                    if let Some(columns) = columns {
                        builder = builder.with_projection(columns.iter().copied());
                    }
                    let scan = builder.build().await?;
                    let schema = scan.schema();
                    let batches = scan.into_stream().try_collect::<Vec<_>>().await?;
                    let expected = concat_batches(&schema, &batches)?;
                    if fixture == "deletion_vectors" && *name == "deleted_row" {
                        assert_eq!(expected.num_rows(), usize::from(version == Some(0)));
                    }
                    for limit in [None, Some(0), Some(2), Some(1_000)] {
                        let context = format!(
                            "{fixture} version={version:?} predicate={name} columns={columns:?} limit={limit:?}"
                        );
                        let mut command = Command::new(env!("CARGO_BIN_EXE_dar"));
                        command
                            .arg("scan")
                            .arg(&path)
                            .arg("--predicate-file")
                            .arg(&predicate_file.path);
                        if let Some(version) = version {
                            command.args(["--table-version", &version.to_string()]);
                        }
                        if let Some(columns) = columns {
                            if columns.is_empty() {
                                command.arg("--no-columns");
                            }
                            for column in columns {
                                command.args(["--column", column]);
                            }
                        }
                        if let Some(limit) = limit {
                            command.args(["--limit", &limit.to_string()]);
                        }
                        let output = run_with_timeout(&mut command)?;
                        assert!(
                            output.status.success(),
                            "{context}: {}",
                            String::from_utf8_lossy(&output.stderr)
                        );
                        assert!(output.stderr.is_empty(), "{context}");
                        assert!(
                            output.stdout.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]),
                            "missing IPC end marker: {context}"
                        );
                        let reader = StreamReader::try_new(Cursor::new(output.stdout), None)?;
                        assert_eq!(reader.schema(), schema, "{context}");
                        let actual =
                            concat_batches(&schema, &reader.collect::<Result<Vec<_>, _>>()?)?;
                        assert_eq!(
                            actual.num_rows(),
                            expected.num_rows().min(limit.unwrap_or(usize::MAX)),
                            "{context}"
                        );
                        if actual.num_columns() == 0 || actual.num_rows() == 0 {
                            continue;
                        }
                        if actual.num_rows() == expected.num_rows() {
                            assert_eq!(
                                sort_by_id(&actual)?,
                                sort_by_id(&expected)?,
                                "{context}"
                            );
                        } else {
                            // Limited scans may return any subset; consume matches to reject duplicates.
                            let mut remaining = (0..expected.num_rows())
                                .map(|row| expected.slice(row, 1))
                                .collect::<Vec<_>>();
                            for row in 0..actual.num_rows() {
                                let row = actual.slice(row, 1);
                                let index = remaining
                                    .iter()
                                    .position(|expected| *expected == row)
                                    .unwrap_or_else(|| {
                                        panic!("unexpected or duplicate row: {context}: {row:?}")
                                    });
                                remaining.swap_remove(index);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    });
    runtime.shutdown_background();
    result
}
