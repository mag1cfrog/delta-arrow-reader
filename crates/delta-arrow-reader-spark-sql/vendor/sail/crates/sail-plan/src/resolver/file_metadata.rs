//! File-origin slots are trusted resolver state, never user field metadata.

use datafusion_common::{Column, DFSchemaRef, ScalarValue};
use datafusion_expr::{Expr, lit};
use datafusion_functions::core::expr_fn::coalesce;

use super::state::PlanResolverState;
use crate::error::{PlanError, PlanResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FileMetadataKind {
    Path,
    Name,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct FileMetadataColumn {
    pub source_id: usize,
    pub kind: FileMetadataKind,
}

pub(super) fn input_file_name(schema: &DFSchemaRef, state: &PlanResolverState) -> PlanResult<Expr> {
    let mut paths = schema.iter().filter_map(|(qualifier, field)| {
        state
            .get_field_info(field.name())
            .ok()?
            .file_metadata()
            .filter(|metadata| metadata.kind == FileMetadataKind::Path)
            .map(|_| Expr::Column(Column::from((qualifier, field))))
    });
    let Some(path) = paths.next() else {
        return Ok(lit(""));
    };
    if paths.next().is_some() {
        return Err(PlanError::analysis(
            "[MULTI_SOURCES_UNSUPPORTED_FOR_EXPRESSION] The expression input_file_name() does not support more than one source.",
        ));
    }
    Ok(coalesce(vec![
        path,
        lit(ScalarValue::Utf8View(Some(String::new()))),
    ]))
}
