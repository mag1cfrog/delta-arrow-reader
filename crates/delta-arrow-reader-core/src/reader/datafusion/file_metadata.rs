//! Explicit source-file columns, attached before per-file streams are combined.

use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{DataFusionError, Result, ScalarValue},
    datasource::{TableProvider, TableType},
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};
use futures_util::StreamExt;
use url::Url;

use super::{DeltaTableProvider, execution::with_file_metadata};
use crate::{
    DeltaReaderError,
    reader::{
        backend::file_location::resolve_data_file_metadata,
        planning::DeltaScanFileTask,
        scheduling::{FileBatchStream, FileExecutor},
    },
};

/// Opt-in DataFusion provider with source-file URI and file-name STRING columns.
///
/// The caller chooses two nonempty, distinct names that do not match stored
/// columns. Both fields are non-null Arrow `Utf8View` strings. This provider's
/// `SELECT *` includes them; [`DeltaTableProvider`] keeps its original schema.
/// Data-only projections delegate to the wrapped provider without adding file
/// metadata. File size and modification time are not exposed.
///
/// Paths use the reader's canonical, percent-encoded file URI without URI
/// credentials, query strings or fragments. File names are decoded object-key
/// basenames. Qualified Azure URIs retain their structural container username.
#[derive(Debug, Clone)]
pub struct DeltaFileMetadataProvider {
    provider: DeltaTableProvider,
    schema: SchemaRef,
}

impl DeltaFileMetadataProvider {
    /// Wraps an existing provider and validates the additional column names.
    pub fn try_new(
        provider: DeltaTableProvider,
        file_path_column: impl Into<String>,
        file_name_column: impl Into<String>,
    ) -> std::result::Result<Self, DeltaReaderError> {
        let names = [file_path_column.into(), file_name_column.into()];
        if names.iter().any(|name| {
            name.is_empty()
                || provider
                    .schema
                    .fields()
                    .iter()
                    .any(|field| field.name() == name)
        }) || names[0] == names[1]
        {
            return Err(DeltaReaderError::InvalidConfiguration {
                reason: "file_metadata_column_names_must_be_nonempty_and_unique",
            });
        }
        let mut fields = provider.schema.fields().to_vec();
        fields.extend(names.map(|name| Arc::new(Field::new(name, DataType::Utf8View, false))));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            provider.schema.metadata().clone(),
        ));
        Ok(Self { provider, schema })
    }
}

#[async_trait]
impl TableProvider for DeltaFileMetadataProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        self.provider.table_type()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let data_columns = self.provider.schema.fields().len();
        if projection.is_some_and(|columns| columns.iter().all(|&i| i < data_columns)) {
            return self.provider.scan(state, projection, filters, limit).await;
        }
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..self.schema.fields().len()).collect());
        let schema = Arc::new(self.schema.project(&projection)?);
        let data_projection = projection
            .iter()
            .copied()
            .filter(|&i| i < data_columns)
            .collect();
        let plan = self
            .provider
            .scan(state, Some(&data_projection), filters, limit)
            .await?;
        let mut metadata = Vec::new();
        for (offset, kind) in [FileMetadataKind::Path, FileMetadataKind::Name]
            .into_iter()
            .enumerate()
        {
            if projection.contains(&(data_columns + offset)) {
                metadata.push((
                    Arc::clone(&self.schema.fields()[data_columns + offset]),
                    kind,
                ));
            }
        }
        let has_path = projection.contains(&data_columns);
        let mut next_data = 0;
        let output_order = projection
            .into_iter()
            .map(|i| {
                if i < data_columns {
                    let position = next_data;
                    next_data += 1;
                    position
                } else {
                    data_projection.len() + usize::from(i > data_columns && has_path)
                }
            })
            .collect();
        with_file_metadata(plan, schema, output_order, metadata)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.provider.supports_filters_pushdown(filters)
    }
}

#[derive(Clone, Copy)]
pub(super) enum FileMetadataKind {
    Path,
    Name,
}

pub(super) struct FileMetadata {
    pub(super) schema: SchemaRef,
    pub(super) columns: Vec<(FieldRef, FileMetadataKind)>,
}

impl FileMetadata {
    pub(super) fn decorate(
        self: &Arc<Self>,
        executor: FileExecutor<DeltaScanFileTask, FileBatchStream>,
        table_url: Url,
    ) -> FileExecutor<DeltaScanFileTask, FileBatchStream> {
        let metadata = Arc::clone(self);
        Arc::new(move |task, permit, cancellation| {
            let metadata = Arc::clone(&metadata);
            let executor = Arc::clone(&executor);
            let table_url = table_url.clone();
            Box::pin(async move {
                let (location, path) = resolve_data_file_metadata(&table_url, &task.path)?;
                let file_name = path.filename().unwrap_or_default();
                let location = uri_without_authentication(location)?;
                let values: Vec<_> = metadata
                    .columns
                    .iter()
                    .map(|(_, kind)| {
                        ScalarValue::Utf8View(Some(
                            match kind {
                                FileMetadataKind::Path => location.as_str(),
                                FileMetadataKind::Name => file_name,
                            }
                            .to_owned(),
                        ))
                    })
                    .collect();
                let stream = executor(task, permit, cancellation).await?;
                Ok(
                    Box::pin(stream.map(move |batch| metadata.append(batch?, &values)))
                        as FileBatchStream,
                )
            })
        })
    }

    fn append(
        &self,
        batch: RecordBatch,
        values: &[ScalarValue],
    ) -> std::result::Result<RecordBatch, DeltaReaderError> {
        let mut columns = batch.columns().to_vec();
        for value in values {
            columns.push(
                value
                    .to_array_of_size(batch.num_rows())
                    .map_err(adapter_error)?,
            );
        }
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(adapter_error)
    }
}

fn uri_without_authentication(mut location: Url) -> std::result::Result<Url, DeltaReaderError> {
    let structural_container = matches!(location.scheme(), "az" | "abfs" | "abfss")
        && matches!(
            object_store::ObjectStoreScheme::parse(&location),
            Ok((object_store::ObjectStoreScheme::MicrosoftAzure, _))
        );
    if !location.username().is_empty() && !structural_container {
        location
            .set_username("")
            .map_err(|()| adapter_error(std::io::Error::other("invalid metadata URI")))?;
    }
    if location.password().is_some() {
        location
            .set_password(None)
            .map_err(|()| adapter_error(std::io::Error::other("invalid metadata URI")))?;
    }
    location.set_query(None);
    location.set_fragment(None);
    Ok(location)
}

fn adapter_error(error: impl std::error::Error + Send + Sync + 'static) -> DeltaReaderError {
    DeltaReaderError::DataFusionAdapter {
        reason: "file_metadata_projection_failed",
        source: Box::new(DataFusionError::External(Box::new(error))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, StringViewArray},
        record_batch::RecordBatchOptions,
    };

    #[test]
    fn file_metadata_constants_store_path_bytes_once_per_batch()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let field = Arc::new(Field::new("path", DataType::Utf8View, false));
        let metadata = FileMetadata {
            schema: Arc::new(Schema::new(vec![Arc::clone(&field)])),
            columns: vec![(field, FileMetadataKind::Path)],
        };
        let path = "s3://bucket/a/long/partition/path/part-00001.parquet";
        for rows in [0, 1, 4096] {
            let input = RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )?;
            let output = metadata.append(input, &[ScalarValue::Utf8View(Some(path.to_owned()))])?;
            let array = output
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or("view array")?;
            assert_eq!(array.len(), rows);
            assert!(array.iter().all(|value| value == Some(path)));
            assert_eq!(
                array
                    .data_buffers()
                    .iter()
                    .map(|buffer| buffer.len())
                    .sum::<usize>(),
                if rows == 0 { 0 } else { path.len() }
            );
            assert!(array.views().iter().all(|view| *view == array.views()[0]));
        }
        Ok(())
    }

    #[test]
    fn file_metadata_uris_preserve_store_identity_without_authentication()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        for (base, relative, expected) in [
            (
                "s3://bucket/table/",
                "a%20b/%E9%9B%AA.parquet?secret-token#secret-fragment",
                "s3://bucket/table/a%20b/%E9%9B%AA.parquet",
            ),
            (
                "gs://bucket/table/",
                "part.parquet",
                "gs://bucket/table/part.parquet",
            ),
            (
                "https://s3.us-east-1.amazonaws.com/bucket/table/",
                "part.parquet",
                "https://s3.us-east-1.amazonaws.com/bucket/table/part.parquet",
            ),
            (
                "https://account.blob.core.windows.net/container/table/",
                "part.parquet?secret-sas",
                "https://account.blob.core.windows.net/container/table/part.parquet",
            ),
            (
                "abfss://container@account.dfs.core.windows.net/table/",
                "part.parquet?secret-sas",
                "abfss://container@account.dfs.core.windows.net/table/part.parquet",
            ),
            (
                "custom://secret-user:secret-password@store/table/",
                "part.parquet",
                "custom://store/table/part.parquet",
            ),
        ] {
            let (url, path) = resolve_data_file_metadata(&Url::parse(base)?, relative)?;
            let public = uri_without_authentication(url)?;
            assert_eq!(public.as_str(), expected);
            assert!(!public.as_str().contains("secret"));
            assert_eq!(
                path.filename(),
                Some(if relative.starts_with("a%20b") {
                    "\u{96ea}.parquet"
                } else {
                    "part.parquet"
                })
            );
        }
        for relative in [
            "s3://secret-other/table/part.parquet?secret-token",
            "https://secret-user:secret-password@s3.us-east-1.amazonaws.com/bucket/table/part.parquet",
        ] {
            let error = resolve_data_file_metadata(&Url::parse("s3://bucket/table/")?, relative)
                .err()
                .ok_or("foreign store accepted")?;
            assert!(error.to_string().contains("data_file_store_mismatch"));
            assert!(!format!("{error} {error:?}").contains("secret"));
        }
        Ok(())
    }
}
