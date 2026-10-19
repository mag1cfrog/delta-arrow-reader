use serde_json::json;
use snafu::ensure;

use super::{ProcessFixture, check_error, describe_output, run_cli};
use crate::error::{InvalidInputSnafu, Result};

impl ProcessFixture<'_> {
    pub(super) fn check_predicate_planning(&self) -> Result<()> {
        let fields: Vec<_> = [
            ("id", "integer"),
            ("amount", "decimal(12,2)"),
            ("created_at", "timestamp"),
            ("label", "string"),
            ("data", "binary"),
            ("day", "date"),
        ]
        .into_iter()
        .map(|(name, kind)| json!({"name": name, "type": kind, "nullable": true, "metadata": {}}))
        .collect();
        let table = self.create_metadata_table(json!(fields), 1)?;
        for (column, scalar) in [
            ("id", json!({"type": "int32", "value": "1"})),
            (
                "amount",
                json!({"type": "decimal128", "value": "12345", "precision": 12, "scale": 2}),
            ),
            (
                "created_at",
                json!({"type": "timestamp_us", "value": "-1", "timezone": "UTC"}),
            ),
            ("label", json!({"type": "utf8", "value": "secret-literal"})),
            ("data", json!({"type": "binary", "value": [0, 255]})),
            ("day", json!({"type": "date32", "value": "-1"})),
        ] {
            let predicate = json!({"op": "eq", "column": column, "value": scalar});
            let path = self.write_input_file(predicate.to_string().as_bytes())?;
            let output = run_cli(
                self.command()
                    .args(["scan", "--predicate-file"])
                    .arg(path)
                    .arg(&table),
            )?;
            ensure!(
                output.status.success() && output.stderr.is_empty(),
                InvalidInputSnafu {
                    message: format!(
                        "valid predicate {predicate} failed: {}",
                        describe_output(&output)
                    )
                }
            );
        }

        let mut cases = vec![
            (
                json!({"op": "is_null", "column": ""}),
                "invalid_column_reference",
            ),
            (
                json!({"op": "is_null", "column": "profile.secret"}),
                "invalid_column_reference",
            ),
            (
                json!({"op": "is_null", "column": "secret-missing"}),
                "column_not_found",
            ),
        ];
        for (column, scalar) in [
            ("id", json!({"type": "utf8", "value": "secret-literal"})),
            ("id", json!({"type": "int64", "value": "1"})),
            (
                "amount",
                json!({"type": "decimal128", "value": "1", "precision": 11, "scale": 2}),
            ),
            (
                "amount",
                json!({"type": "decimal128", "value": "1", "precision": 12, "scale": 3}),
            ),
            (
                "created_at",
                json!({"type": "timestamp_us", "value": "-1", "timezone": null}),
            ),
            (
                "created_at",
                json!({"type": "timestamp_us", "value": "-1", "timezone": "Etc/UTC"}),
            ),
            (
                "label",
                json!({"type": "large_utf8", "value": "secret-literal"}),
            ),
            ("data", json!({"type": "large_binary", "value": [0, 255]})),
            (
                "data",
                json!({"type": "fixed_size_binary", "value": [0, 255], "size": 2}),
            ),
            ("day", json!({"type": "int32", "value": "-1"})),
        ] {
            cases.push((
                json!({"op": "eq", "column": column, "value": scalar}),
                "scalar_type_mismatch",
            ));
        }
        for (value, precision, scale) in [
            ("1", 0, 0),
            ("1", 39, 2),
            ("1", 12, 13),
            ("1000000000000", 12, 2),
            ("1", 255, -128),
        ] {
            cases.push((
                json!({
                    "op": "eq", "column": "amount",
                    "value": {
                        "type": "decimal128", "value": value,
                        "precision": precision, "scale": scale,
                    },
                }),
                "invalid_decimal",
            ));
        }
        for (op, constant) in [("and", false), ("or", true)] {
            cases.push((
                json!({"op": op, "args": [
                    {"op": "constant", "value": constant},
                    {"op": "is_null", "column": "secret-missing"},
                ]}),
                "column_not_found",
            ));
        }
        for (predicate, reason) in cases {
            let path = self.write_input_file(predicate.to_string().as_bytes())?;
            for flags in [&[][..], &["--limit", "0"]] {
                let output = run_cli(
                    self.command()
                        .args(["scan", "--predicate-file"])
                        .arg(&path)
                        .args(flags)
                        .arg(&table),
                )?;
                let diagnostic = check_error(&output, 1, "scan_planning", "unsupported_predicate")?;
                ensure!(
                    diagnostic
                        .split_whitespace()
                        .any(|field| field == format!("error_reason=\"{reason}\"")),
                    InvalidInputSnafu {
                        message: format!(
                            "predicate {predicate} with flags {flags:?} expected reason={reason}: {diagnostic}"
                        )
                    }
                );
            }
        }
        Ok(())
    }

    pub(super) fn check_duplicate_columns(&self) -> Result<()> {
        // Kernel rejects duplicate schema names before predicate planning can run.
        let field =
            json!({"name": "secret-id", "type": "integer", "nullable": true, "metadata": {}});
        let table = self.create_metadata_table(json!([field, field]), 1)?;
        let path = self.write_input_file(br#"{"op":"is_null","column":"secret-id"}"#)?;
        check_error(
            &run_cli(
                self.command()
                    .args(["scan", "--predicate-file"])
                    .arg(path)
                    .arg(table),
            )?,
            1,
            "snapshot",
            "snapshot_load",
        )?;
        Ok(())
    }

    pub(super) fn check_projection_planning(&self) -> Result<()> {
        let table = self
            .repository
            .join("crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/partitioned/table");
        for columns in [
            vec!["secret-missing"],
            vec!["id", "id"],
            vec!["id,value"],
            vec!["profile.city"],
        ] {
            let mut command = self.command();
            command.arg("scan").arg(&table);
            for column in columns {
                command.args(["--column", column]);
            }
            check_error(
                &run_cli(&mut command)?,
                1,
                "scan_planning",
                "invalid_projection",
            )?;
        }
        Ok(())
    }
}
