//! Retain public DV descriptors while Kernel's scan callback exposes only opaque DvInfo.

use std::sync::LazyLock;

use delta_kernel::{
    DeltaResult, Error,
    actions::deletion_vector::DeletionVectorDescriptor,
    engine_data::{FilteredRowVisitor, GetData, RowIndexIterator, TypedGetData},
    expressions::ColumnName,
    scan::ScanMetadata,
    schema::DataType,
};

#[derive(Clone)]
pub(crate) struct KernelDeletionVectorHandle(pub(crate) DeletionVectorDescriptor);

// Kernel 0.25's DvInfo hides its descriptor and compact bitmap accessor. Retain
// the public descriptor from scan metadata, then delegate decoding to read().
// Use Kernel's filtered visitor so row selection and null-parent handling match
// visit_scan_files, including skipped non-Add rows and short selection vectors.
pub(super) fn scan_deletion_vectors(
    metadata: &ScanMetadata,
) -> DeltaResult<Vec<Option<KernelDeletionVectorHandle>>> {
    let mut visitor = DeletionVectors(Vec::new());
    visitor.visit_rows_of(&metadata.scan_files)?;
    Ok(visitor.0)
}

struct DeletionVectors(Vec<Option<KernelDeletionVectorHandle>>);

impl FilteredRowVisitor for DeletionVectors {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        static NAMES: LazyLock<[ColumnName; 6]> = LazyLock::new(|| {
            [
                ColumnName::new(["path"]),
                ColumnName::new(["deletionVector", "storageType"]),
                ColumnName::new(["deletionVector", "pathOrInlineDv"]),
                ColumnName::new(["deletionVector", "offset"]),
                ColumnName::new(["deletionVector", "sizeInBytes"]),
                ColumnName::new(["deletionVector", "cardinality"]),
            ]
        });
        static TYPES: [DataType; 6] = [
            DataType::STRING,
            DataType::STRING,
            DataType::STRING,
            DataType::INTEGER,
            DataType::INTEGER,
            DataType::LONG,
        ];
        (&*NAMES, &TYPES)
    }

    fn visit_filtered<'a>(
        &mut self,
        getters: &[&'a dyn GetData<'a>],
        rows: RowIndexIterator<'_>,
    ) -> DeltaResult<()> {
        let [path, storage, payload, offset, size, cardinality] = getters else {
            return Err(Error::internal_error(
                "unexpected deletion-vector getter count",
            ));
        };
        for row in rows {
            if path.get_str(row, "scanFile.path")?.is_none() {
                continue;
            }
            let descriptor = storage
                .get_str(row, "deletionVector.storageType")?
                .map(|storage| -> DeltaResult<_> {
                    Ok(KernelDeletionVectorHandle(DeletionVectorDescriptor {
                        storage_type: storage.parse()?,
                        path_or_inline_dv: payload.get(row, "deletionVector.pathOrInlineDv")?,
                        offset: offset.get_opt(row, "deletionVector.offset")?,
                        size_in_bytes: size.get(row, "deletionVector.sizeInBytes")?,
                        cardinality: cardinality.get(row, "deletionVector.cardinality")?,
                    }))
                })
                .transpose()?;
            self.0.push(descriptor);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, sync::Arc};

    use arrow::json::ReaderBuilder;
    use delta_kernel::{
        engine::{arrow_conversion::TryIntoArrow, arrow_data::ArrowEngineData},
        engine_data::FilteredEngineData,
    };
    use serde_json::{Value, json};

    use super::*;

    fn metadata(
        rows: &[Value],
        selection: Vec<bool>,
    ) -> Result<ScanMetadata, Box<dyn std::error::Error>> {
        let schema = delta_kernel::scan::scan_row_schema()
            .as_ref()
            .try_into_arrow()?;
        let json = rows
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        let batch = ReaderBuilder::new(Arc::new(schema))
            .build(Cursor::new(json))?
            .next()
            .ok_or("missing metadata batch")??;
        Ok(ScanMetadata {
            scan_files: FilteredEngineData::try_new(
                Box::new(ArrowEngineData::new(batch)),
                selection,
            )?,
            scan_file_transforms: Vec::new(),
        })
    }

    fn file(path: Option<&str>, dv: Value) -> Value {
        json!({"path":path,"size":123,"modificationTime":456,"stats":"{\"numRecords\":9}",
            "deletionVector":dv,"fileConstantValues":{"partitionValues":{"region":"west"}}})
    }

    fn descriptor(storage: &str, path: &str, offset: Option<i32>, cardinality: i64) -> Value {
        json!({"storageType":storage,"pathOrInlineDv":path,"offset":offset,"sizeInBytes":44,"cardinality":cardinality})
    }

    #[test]
    fn descriptors_follow_selected_add_rows_and_preserve_all_storage_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        let rows = [
            file(None, descriptor("invalid", "ignored-non-add", None, 0)),
            file(Some("plain"), Value::Null),
            file(Some("inline"), descriptor("i", "inline-payload", None, 6)),
            file(
                Some("relative"),
                descriptor("u", "prefixvBn[lx{q8@P<9BNH/isA", Some(17), 2),
            ),
            file(
                Some("absolute"),
                descriptor("p", "file:///tmp/deletions.bin", Some(81), 1),
            ),
        ];
        for selection in [
            vec![],
            vec![false, true, false, true, true],
            vec![true, false],
            vec![false; 5],
        ] {
            let expected: Vec<_> = rows
                .iter()
                .enumerate()
                .filter(|(i, row)| {
                    row["path"].is_string() && selection.get(*i).copied().unwrap_or(true)
                })
                .map(|(_, row)| row)
                .collect();
            let batch = metadata(&rows, selection)?;
            let files = super::super::collect_scan_files(std::iter::once(Ok(batch)))?.files;
            assert_eq!(files.len(), expected.len());
            for (file, expected) in files.iter().zip(expected) {
                assert_eq!(file.path, expected["path"].as_str().ok_or("path")?);
                assert_eq!(file.size, 123);
                assert_eq!(file.modification_time_ms, Some(456));
                assert_eq!(file.estimated_rows, Some(9));
                assert_eq!(
                    file.partition_values.get("region").map(String::as_str),
                    Some("west")
                );
                if expected["deletionVector"].is_null() {
                    assert!(file.deletion_vector.is_none());
                } else {
                    let actual = &file.deletion_vector.as_ref().ok_or("missing DV")?.0;
                    let expected = &expected["deletionVector"];
                    assert_eq!(
                        actual.storage_type,
                        expected["storageType"].as_str().ok_or("type")?.parse()?
                    );
                    assert_eq!(
                        actual.path_or_inline_dv,
                        expected["pathOrInlineDv"].as_str().ok_or("payload")?
                    );
                    assert_eq!(actual.offset, expected["offset"].as_i64().map(|v| v as i32));
                    assert_eq!(actual.size_in_bytes, 44);
                    assert_eq!(
                        actual.cardinality,
                        expected["cardinality"].as_i64().ok_or("cardinality")?
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn deselected_invalid_descriptors_are_ignored_but_selected_ones_fail()
    -> Result<(), Box<dyn std::error::Error>> {
        let rows = [
            file(Some("invalid"), descriptor("invalid", "bad", None, 1)),
            file(Some("valid"), descriptor("i", "good", None, 0)),
        ];
        let batch = metadata(&rows, vec![false, true])?;
        assert_eq!(
            super::super::collect_scan_files(std::iter::once(Ok(batch)))?
                .files
                .len(),
            1
        );
        let batch = metadata(&rows, vec![])?;
        assert!(super::super::collect_scan_files(std::iter::once(Ok(batch))).is_err());
        Ok(())
    }
}
