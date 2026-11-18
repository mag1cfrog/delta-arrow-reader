//! Adapt direct Delta providers without changing their session registration.

use std::sync::Arc;

use datafusion::{
    common::{DataFusionError, Result},
    datasource::TableProvider,
};
use delta_arrow_reader::datafusion::{DeltaFileMetadataProvider, DeltaTableProvider};
use sail_plan::resolver::FileMetadataTable;

pub(crate) fn adapt(provider: Arc<dyn TableProvider>) -> Result<Option<FileMetadataTable>> {
    let Some(delta) = provider.downcast_ref::<DeltaTableProvider>() else {
        return Ok(None);
    };
    let schema = delta.schema();
    let mut path = "__dar_file_path".to_owned();
    let mut name = "__dar_file_name".to_owned();
    while schema.fields().iter().any(|field| {
        field.name().eq_ignore_ascii_case(&path) || field.name().eq_ignore_ascii_case(&name)
    }) {
        path.push('_');
        name.push('_');
    }
    let provider = DeltaFileMetadataProvider::try_new(delta.clone(), path, name)
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    Ok(Some(FileMetadataTable {
        provider: Arc::new(provider),
        file_path_column: schema.fields().len(),
        file_name_column: schema.fields().len() + 1,
    }))
}
