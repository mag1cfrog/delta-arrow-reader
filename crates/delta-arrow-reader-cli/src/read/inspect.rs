//! Inspect a snapshot before choosing columns and types for a read.
//!
//! `inspect TABLE --view summary` reports the version and read checks. Add
//! `--column-prefix ord` to find names, or `--column order_id` for that column's
//! type. Pin follow-ups with `--table-version N`. Each lookup returns only the
//! requested information; metadata requires `--include-metadata`.
//!
//! The legacy full view remains the default for existing consumers.

use std::num::NonZeroUsize;

use arrow_schema::Schema;
use clap::{Args, ValueEnum, error::ErrorKind};
use delta_arrow_reader::DeltaTable;
use serde_json::{Value, json};

use crate::{
    Error,
    cli::parse_unsigned_decimal,
    diagnostics::{InputCorrection, build_completion_record},
    output::{OutputStats, field_descriptions::field_details, write_stdout},
    table_access::{TableArgs, unity_catalog::UnityCatalogTableTarget},
};

pub(crate) const DEFAULT_SUMMARY_BYTES: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
enum InspectionView {
    Full,
    Summary,
}

#[derive(Args)]
pub(crate) struct InspectionArgs {
    /// Full Arrow schema (legacy default), or summary checks and selected details.
    #[arg(long, value_enum, default_value = "full")]
    view: InspectionView,
    /// With --view summary: exact top-level column type; repeat to select columns.
    #[arg(long = "column", value_name = "NAME", conflicts_with = "column_prefix")]
    columns: Vec<String>,
    /// With --view summary: matching top-level names only (case-sensitive prefix).
    #[arg(long, value_name = "PREFIX")]
    column_prefix: Option<String>,
    /// Include nonempty metadata for every selected column and its nested fields; requires --column.
    /// Select only columns whose metadata you need; get other columns' types separately.
    #[arg(long, requires = "columns")]
    include_metadata: bool,
    /// Summary stdout ceiling in bytes, including newline; default: 4096.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    max_output_bytes: Option<NonZeroUsize>,
}

impl InspectionArgs {
    pub(crate) fn validate_view_options(&self) -> Result<(), Error> {
        if self.view == InspectionView::Full
            && (!self.columns.is_empty()
                || self.column_prefix.is_some()
                || self.include_metadata
                || self.max_output_bytes.is_some())
        {
            return Err(invalid_selection(
                "--view",
                "summary when selecting columns or an output budget",
                "Use --view summary with inspection selections and budgets.",
            ));
        }
        Ok(())
    }

    pub(super) async fn execute(
        &self,
        table: &TableArgs,
        uc_target: Option<UnityCatalogTableTarget>,
    ) -> Result<Value, Error> {
        let (table, _authorized_access) = table.load_table(uc_target).await?;
        let version = table.version();
        let output = self.encode_json(&table)?;
        let output_stats = OutputStats {
            bytes_written: output.len() as u64,
            ..Default::default()
        };
        write_stdout(&output)?;
        Ok(build_completion_record(
            "inspect",
            Some(version),
            output_stats,
            None,
        ))
    }

    pub(crate) fn encode_json(&self, table: &DeltaTable) -> Result<Vec<u8>, Error> {
        if self.view == InspectionView::Full {
            return Ok(encode_inspection_json(
                table.version(),
                table.schema().as_ref(),
            ));
        }
        let mut result = json!({
            "format_version": 1,
            "kind": "inspection_summary",
            "table_version": table.version().to_string(),
        });
        let schema = table.schema();
        if let Some(prefix) = &self.column_prefix {
            result["column_names"] = json!(
                schema
                    .fields()
                    .iter()
                    .filter(|field| field.name().starts_with(prefix))
                    .map(|field| field.name())
                    .collect::<Vec<_>>()
            );
        } else if !self.columns.is_empty() {
            result["fields"] = self.selected_field_details(schema.as_ref())?.into();
        } else {
            result["checks"] = json!({
                "scan_protocol": if table.validate_protocol().is_ok() { "passed" } else { "failed" },
                "data_files": "not_checked",
            });
        }
        let mut output = result.to_string().into_bytes();
        output.push(b'\n');
        if output.len()
            > self
                .max_output_bytes
                .map_or(DEFAULT_SUMMARY_BYTES, NonZeroUsize::get)
        {
            return Err(Error::OutputLimit {
                reason: "inspection_output_limit_exceeded",
            });
        }
        Ok(output)
    }

    fn selected_field_details(&self, schema: &Schema) -> Result<Vec<Value>, Error> {
        let mut fields = Vec::with_capacity(self.columns.len());
        for (index, name) in self.columns.iter().enumerate() {
            if self.columns[..index].contains(name) {
                return Err(invalid_selection(
                    "--column",
                    "each column selected once",
                    "Remove repeated --column values.",
                ));
            }
            let mut matches = schema.fields().iter().filter(|field| field.name() == name);
            let field = matches.next().ok_or_else(|| invalid_selection(
                "--column", "an exact top-level column name in this snapshot",
                "Use inspect TABLE --view summary --column-prefix PREFIX with the same authorization and --table-version N to find names.",
            ))?;
            if matches.next().is_some() {
                return Err(invalid_selection(
                    "--column",
                    "an unambiguous top-level column name",
                    "Choose a column whose name occurs once in this snapshot.",
                ));
            }
            fields.push(field_details(field, self.include_metadata));
        }
        Ok(fields)
    }
}

pub(crate) fn encode_inspection_json(version: u64, schema: &Schema) -> Vec<u8> {
    // Arrow Schema's Serde representation contains only JSON-compatible types.
    // Keep upstream serialization, including metadata and nested field order.
    let mut output = json!({
        "format_version": 1,
        "table_version": version.to_string(),
        "schema": schema,
    })
    .to_string()
    .into_bytes();
    output.push(b'\n');
    output
}

fn invalid_selection(argument: &str, expected: &str, hint: &'static str) -> Error {
    Error::Argument {
        kind: ErrorKind::InvalidValue,
        correction: Some(InputCorrection {
            argument: argument.to_owned(),
            expected: expected.to_owned(),
            hint,
            field_path: None,
            supplied: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, TimeUnit};

    use super::*;
    use crate::capabilities;

    #[test]
    fn inspection_format_preserves_nested_schema_and_all_metadata() {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "profile",
                    DataType::Struct(
                        vec![
                            Field::new("amount", DataType::Decimal128(20, 4), false).with_metadata(
                                HashMap::from([("comment".into(), "secret user metadata".into())]),
                            ),
                        ]
                        .into(),
                    ),
                    true,
                ),
                Field::new(
                    "times",
                    DataType::List(Arc::new(Field::new(
                        "element",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        false,
                    ))),
                    false,
                ),
            ],
            HashMap::from([("owner".into(), "table author".into())]),
        );
        let encoded = encode_inspection_json(u64::MAX, &schema);
        assert_eq!(encoded.last(), Some(&b'\n'));
        assert_eq!(encoded.iter().filter(|&&byte| byte == b'\n').count(), 1);
        let actual: Value = serde_json::from_slice(&encoded).unwrap();
        let schema_document = capabilities::DiscoverySchema::Inspection.schema_document();
        jsonschema::validate(&schema_document["schema"], &actual).unwrap();
        // This literal freezes the upstream Arrow encoding within format version 1.
        assert_eq!(
            actual,
            json!({
                "format_version": 1, "table_version": "18446744073709551615",
                "schema": {"fields": [
                    {"name": "profile", "nullable": true, "dict_id": 0, "dict_is_ordered": false, "metadata": {},
                     "data_type": {"Struct": [
                         {"name": "amount", "data_type": {"Decimal128": [20, 4]}, "nullable": false,
                          "dict_id": 0, "dict_is_ordered": false, "metadata": {"comment": "secret user metadata"}}
                     ]}},
                    {"name": "times", "nullable": false, "dict_id": 0, "dict_is_ordered": false, "metadata": {},
                     "data_type": {"List": {
                         "name": "element", "data_type": {"Timestamp": ["Microsecond", "UTC"]}, "nullable": false,
                         "dict_id": 0, "dict_is_ordered": false, "metadata": {}
                     }}}
                ], "metadata": {"owner": "table author"}}
            })
        );
        let decoded: Schema = serde_json::from_value(actual["schema"].clone()).unwrap();
        assert_eq!(decoded, schema);
    }
}
