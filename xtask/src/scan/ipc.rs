use std::{fs::File, io::Cursor, path::Path, process::Output, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch, StructArray};
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_schema::{DataType, Fields, SchemaRef};
use serde_json::{Value, json};
use snafu::{ResultExt, ensure};

use crate::error::{ArrowSnafu, FileSnafu, InvalidInputSnafu, Result};

pub(super) const IPC_END_MARKER: [u8; 8] = [255, 255, 255, 255, 0, 0, 0, 0];

pub(crate) struct IpcTable {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

pub(crate) fn check_arrow_output(output: &Output) -> Result<IpcTable> {
    ensure!(
        output.status.success() && output.stderr.is_empty(),
        InvalidInputSnafu {
            message: format!(
                "Arrow output command failed: {}; stderr={}",
                output.status,
                String::from_utf8_lossy(&output.stderr)
            )
        }
    );
    IpcTable::read_stream(&output.stdout, Path::new("Arrow stdout"))
}

/// Validate a JSON-mode scan against the requested snapshot, limit, and decoded output.
pub(crate) fn check_completed_scan(
    output: &Output,
    table_version: u64,
    limit: Option<usize>,
) -> Result<IpcTable> {
    ensure!(
        output.status.success(),
        InvalidInputSnafu {
            message: format!("Scan command failed: {}", output.status)
        }
    );
    ensure!(
        output.stderr.ends_with(b"\n")
            && output.stderr.iter().filter(|&&byte| byte == b'\n').count() == 1,
        InvalidInputSnafu {
            message: "Scan must emit exactly one newline-terminated completion record"
        }
    );
    // CLI output can contain private table data; do not include it in validation errors.
    let completion: Value = serde_json::from_slice(&output.stderr).map_err(|_| {
        InvalidInputSnafu {
            message: "Scan completion is not valid JSON",
        }
        .build()
    })?;
    let table = IpcTable::read_stream(&output.stdout, Path::new("Arrow stdout")).map_err(|_| {
        InvalidInputSnafu {
            message: "Scan stdout is not a complete Arrow IPC stream",
        }
        .build()
    })?;
    let rows = table.row_count();
    ensure!(
        limit.is_none_or(|limit| rows <= limit),
        InvalidInputSnafu {
            message: "Scan returned more rows than requested"
        }
    );
    let limit_reached = limit == Some(rows);
    for (field, expected) in [
        ("format_version", json!(1)),
        ("kind", json!("completion")),
        ("terminal", json!(true)),
        ("command", json!("scan")),
        ("completion", json!("complete")),
        (
            "scope",
            json!(if limit.is_some() {
                "limited_scan"
            } else {
                "scan"
            }),
        ),
        (
            "stop_reason",
            json!(if limit_reached {
                "row_limit"
            } else {
                "end_of_input"
            }),
        ),
        (
            "has_more",
            if limit_reached {
                Value::Null
            } else {
                json!(false)
            },
        ),
        ("output_bytes", json!(output.stdout.len().to_string())),
        ("rows", json!(rows.to_string())),
        ("batches", json!(table.batches.len().to_string())),
    ] {
        ensure!(
            completion.get(field) == Some(&expected),
            InvalidInputSnafu {
                message: format!("Scan completion has an unexpected {field}")
            }
        );
    }
    let expected_version = table_version.to_string();
    ensure!(
        completion["bindings"].as_array().is_some_and(|bindings| {
            bindings.len() == 1
                && bindings[0]["alias"] == "t"
                && bindings[0]["table_version"].as_str() == Some(expected_version.as_str())
        }),
        InvalidInputSnafu {
            message: "Scan completion does not match the requested snapshot binding"
        }
    );
    Ok(table)
}

impl IpcTable {
    pub fn read_file(path: &Path) -> Result<Self> {
        let reader = FileReader::try_new(File::open(path).context(FileSnafu { path })?, None)
            .context(ArrowSnafu { path })?;
        Ok(Self {
            schema: reader.schema(),
            batches: reader
                .collect::<std::result::Result<_, _>>()
                .context(ArrowSnafu { path })?,
        })
    }

    pub fn read_stream(payload: &[u8], path: &Path) -> Result<Self> {
        ensure!(
            payload.ends_with(&IPC_END_MARKER),
            InvalidInputSnafu {
                message: "Arrow stream is missing its IPC end marker"
            }
        );
        let mut cursor = Cursor::new(payload);
        let reader = StreamReader::try_new(&mut cursor, None).context(ArrowSnafu { path })?;
        let table = Self {
            schema: reader.schema(),
            batches: reader
                .collect::<std::result::Result<_, _>>()
                .context(ArrowSnafu { path })?,
        };
        ensure!(
            cursor.position() == payload.len() as u64,
            InvalidInputSnafu {
                message: "unexpected data after the Arrow IPC end marker"
            }
        );
        Ok(table)
    }

    pub fn project(&self, indices: &[usize], path: &Path) -> Result<Self> {
        Ok(Self {
            schema: Arc::new(self.schema.project(indices).context(ArrowSnafu { path })?),
            batches: self
                .batches
                .iter()
                .map(|batch| batch.project(indices))
                .collect::<std::result::Result<_, _>>()
                .context(ArrowSnafu { path })?,
        })
    }

    pub fn row_count(&self) -> usize {
        self.batches.iter().map(RecordBatch::num_rows).sum()
    }

    pub fn check_matches(&self, expected: &Self, limit: Option<usize>) -> Result<()> {
        ensure!(
            fields_match_without_metadata(self.schema.fields(), expected.schema.fields()),
            InvalidInputSnafu {
                message: format!("Arrow schema differs from the fixture: {:?}", self.schema)
            }
        );
        let expected_rows = limit.map_or(expected.row_count(), |limit| {
            limit.min(expected.row_count())
        });
        ensure!(
            self.row_count() == expected_rows,
            InvalidInputSnafu {
                message: format!("expected {expected_rows} rows, got {}", self.row_count())
            }
        );
        if self.schema.fields().is_empty() {
            return Ok(());
        }
        // Removing each match preserves duplicate counts without assuming batch or row order.
        let mut unmatched: Vec<_> = expected
            .batches
            .iter()
            .flat_map(|batch| (0..batch.num_rows()).map(move |index| (batch, index)))
            .collect();
        for batch in &self.batches {
            for index in 0..batch.num_rows() {
                let matched = unmatched
                    .iter()
                    .position(|(expected_batch, expected_index)| {
                        rows_match(
                            batch.columns(),
                            index,
                            expected_batch.columns(),
                            *expected_index,
                        )
                    });
                let Some(matched) = matched else {
                    return InvalidInputSnafu {
                        message: "Arrow output contains a row absent from the fixture or repeated it too often",
                    }.fail();
                };
                unmatched.swap_remove(matched);
            }
        }
        Ok(())
    }
}

fn fields_match_without_metadata(actual: &Fields, expected: &Fields) -> bool {
    // shortcut: only struct metadata is ignored recursively; extend for other nested fixture types.
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            actual.name() == expected.name()
                && actual.is_nullable() == expected.is_nullable()
                && match (actual.data_type(), expected.data_type()) {
                    // Spark's nested fixture omits Delta column-mapping metadata at every level.
                    (DataType::Struct(actual), DataType::Struct(expected)) => {
                        fields_match_without_metadata(actual, expected)
                    }
                    (actual, expected) => actual == expected,
                }
        })
}

fn rows_match(
    actual: &[ArrayRef],
    index: usize,
    expected: &[ArrayRef],
    expected_index: usize,
) -> bool {
    actual.len() == expected.len()
        && actual.iter().zip(expected).all(|(actual, expected)| {
            if actual.is_null(index) || expected.is_null(expected_index) {
                return actual.is_null(index) == expected.is_null(expected_index);
            }
            match (
                actual.as_any().downcast_ref::<StructArray>(),
                expected.as_any().downcast_ref::<StructArray>(),
            ) {
                (Some(actual), Some(expected)) => {
                    rows_match(actual.columns(), index, expected.columns(), expected_index)
                }
                _ => actual.slice(index, 1).as_ref() == expected.slice(expected_index, 1).as_ref(),
            }
        })
}

#[cfg(test)]
mod tests;
