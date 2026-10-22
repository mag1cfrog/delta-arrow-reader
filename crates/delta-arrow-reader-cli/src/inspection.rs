//! Inspect a snapshot before choosing columns and types for a read.
//!
//! `inspect TABLE --view summary` reports the version and read checks. Add
//! `--column-prefix ord` to find names, or `--column order_id` for that column's
//! type. Pin follow-ups with `--table-version N`. Each lookup returns only the
//! requested information; metadata requires `--include-metadata`.
//!
//! The legacy full view remains the default for existing consumers.

use std::num::NonZeroUsize;

use arrow_schema::{DataType, Field, Schema};
use clap::{Args, ValueEnum, error::ErrorKind};
use delta_arrow_reader::DeltaTable;
use serde_json::{Value, json};

use crate::{Error, diagnostics::InputCorrection, parse_unsigned_decimal};

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
    /// Include nonempty metadata on the selected columns and their nested fields.
    #[arg(long, requires = "columns")]
    include_metadata: bool,
    /// Summary stdout ceiling in bytes, including newline; default: 4096.
    #[arg(long, value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    max_output_bytes: Option<NonZeroUsize>,
}

impl InspectionArgs {
    pub(crate) fn validate(&self) -> Result<(), Error> {
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

    pub(crate) fn encode(&self, table: &DeltaTable) -> Result<Vec<u8>, Error> {
        if self.view == InspectionView::Full {
            return Ok(crate::encode_inspection_json(
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
            result["fields"] = self.selected_fields(schema.as_ref())?.into();
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

    fn selected_fields(&self, schema: &Schema) -> Result<Vec<Value>, Error> {
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

fn field_details(field: &Field, include_metadata: bool) -> Value {
    let mut result = json!({
        "name": field.name(),
        "data_type": type_details(field.data_type(), include_metadata),
        "nullable": field.is_nullable(),
    });
    if let Some(ordered) = field.dict_is_ordered() {
        result["dict_is_ordered"] = json!(ordered);
    }
    if include_metadata && !field.metadata().is_empty() {
        result["metadata"] = json!(field.metadata());
    }
    result
}

fn type_details(data_type: &DataType, include_metadata: bool) -> Value {
    // Keep Arrow's type encoding while applying the metadata policy to nested fields.
    match data_type {
        DataType::List(field) => json!({"List": field_details(field, include_metadata)}),
        DataType::LargeList(field) => json!({"LargeList": field_details(field, include_metadata)}),
        DataType::ListView(field) => json!({"ListView": field_details(field, include_metadata)}),
        DataType::LargeListView(field) => {
            json!({"LargeListView": field_details(field, include_metadata)})
        }
        DataType::FixedSizeList(field, size) => {
            json!({"FixedSizeList": [field_details(field, include_metadata), size]})
        }
        DataType::Struct(fields) => {
            json!({"Struct": fields.iter().map(|field| field_details(field, include_metadata)).collect::<Vec<_>>()})
        }
        DataType::Map(field, sorted) => {
            json!({"Map": [field_details(field, include_metadata), sorted]})
        }
        DataType::Union(fields, mode) => {
            json!({"Union": [fields.iter().map(|(id, field)| json!([id, field_details(field, include_metadata)])).collect::<Vec<_>>(), mode]})
        }
        DataType::Dictionary(keys, values) => {
            json!({"Dictionary": [type_details(keys, include_metadata), type_details(values, include_metadata)]})
        }
        DataType::RunEndEncoded(ends, values) => {
            json!({"RunEndEncoded": [field_details(ends, include_metadata), field_details(values, include_metadata)]})
        }
        _ => json!(data_type),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{TimeUnit, UnionFields, UnionMode};

    use super::*;

    #[test]
    fn nested_types_preserve_arrow_semantics_and_metadata_is_opt_in() {
        let metadata = HashMap::from([
            ("comment".to_owned(), "private nested metadata".to_owned()),
            (
                "dict_id".to_owned(),
                "a metadata key, not a field attribute".to_owned(),
            ),
        ]);
        let child = Arc::new(
            Field::new("子列", DataType::Decimal128(18, 4), false).with_metadata(metadata),
        );
        let types = [
            DataType::Timestamp(TimeUnit::Microsecond, Some("private/timezone".into())),
            DataType::Struct(vec![child.clone()].into()),
            DataType::List(child.clone()),
            DataType::LargeList(child.clone()),
            DataType::ListView(child.clone()),
            DataType::LargeListView(child.clone()),
            DataType::FixedSizeList(child.clone(), 3),
            DataType::Map(child.clone(), true),
            DataType::Union(
                UnionFields::try_new([2], [child.clone()]).unwrap(),
                UnionMode::Dense,
            ),
            DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Struct(vec![child.clone()].into())),
            ),
            DataType::RunEndEncoded(Arc::new(Field::new("ends", DataType::Int32, false)), child),
        ];
        for data_type in types {
            let field = Field::new("selected", data_type, true);
            for include_metadata in [false, true] {
                let detail = field_details(&field, include_metadata);
                let text = detail.to_string();
                if !include_metadata {
                    assert!(
                        !text.contains("metadata") && !text.contains("dict_id"),
                        "{text}"
                    );
                }
                let full = json!(field);
                assert_same_selected_values(&detail, &full, include_metadata);
            }
        }
    }

    fn assert_same_selected_values(selected: &Value, full: &Value, include_metadata: bool) {
        match full {
            Value::Object(properties) => {
                for (key, value) in properties {
                    if key == "dict_id" || (key == "dict_is_ordered" && selected.get(key).is_none())
                    {
                        continue;
                    }
                    if key == "metadata" {
                        if include_metadata && !value.as_object().unwrap().is_empty() {
                            assert_eq!(&selected[key], value);
                        } else {
                            assert!(selected.get(key).is_none());
                        }
                    } else {
                        assert_same_selected_values(&selected[key], value, include_metadata);
                    }
                }
            }
            Value::Array(values) => {
                assert_eq!(selected.as_array().unwrap().len(), values.len());
                for (index, value) in values.iter().enumerate() {
                    assert_same_selected_values(&selected[index], value, include_metadata);
                }
            }
            _ => assert_eq!(selected, full),
        }
    }
}
