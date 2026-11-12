// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion_common::{Column, DFSchemaRef, TableReference};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

pub(super) fn quote_column_name(parts: impl IntoIterator<Item = impl AsRef<str>>) -> String {
    parts
        .into_iter()
        .map(|part| format!("`{}`", part.as_ref().replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

impl PlanResolver<'_> {
    pub(super) fn unknown_column_error(
        name: &spec::ObjectName,
        mut columns: Vec<String>,
    ) -> PlanError {
        columns.sort_unstable();
        columns.dedup();
        let condition = if columns.is_empty() {
            "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION"
        } else {
            "UNRESOLVED_COLUMN.WITH_SUGGESTION"
        };
        let display_name = quote_column_name(name.parts().iter().map(|part| part.as_ref()));
        PlanError::UnknownColumn {
            name: name.clone(),
            message: format!(
                "attribute {display_name} is missing from the schema: cannot resolve attribute [{condition}]; available columns: [{}]",
                columns.join(", ")
            ),
        }
    }

    pub(super) fn resolve_table_reference(
        &self,
        name: &spec::ObjectName,
    ) -> PlanResult<TableReference> {
        let names = name.parts();
        match names {
            [a] => Ok(TableReference::Bare {
                table: Arc::from(a.as_ref()),
            }),
            [a, b] => Ok(TableReference::Partial {
                schema: Arc::from(a.as_ref()),
                table: Arc::from(b.as_ref()),
            }),
            [a, b, c] => Ok(TableReference::Full {
                catalog: Arc::from(a.as_ref()),
                schema: Arc::from(b.as_ref()),
                table: Arc::from(c.as_ref()),
            }),
            _ => Err(PlanError::invalid(format!("table reference: {names:?}"))),
        }
    }

    pub(super) fn resolve_column_candidates(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        state: &PlanResolverState,
    ) -> Vec<Column> {
        schema
            .iter()
            .filter(|(_, field)| {
                state
                    .get_field_info(field.name())
                    .is_ok_and(|info| !info.is_hidden() && info.name().eq_ignore_ascii_case(name))
            })
            .map(|x| x.into())
            .collect()
    }

    pub(super) fn resolve_optional_column(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        state: &PlanResolverState,
    ) -> PlanResult<Option<Column>> {
        let columns = self.resolve_column_candidates(schema, name, state);
        if columns.len() > 1 {
            return Err(PlanError::AnalysisError(format!(
                "[AMBIGUOUS_REFERENCE] Reference {name} is ambiguous, found: {} matches",
                columns.len()
            )));
        }
        if columns.is_empty() {
            Ok(None)
        } else {
            Ok(Some(columns.one()?.clone()))
        }
    }

    pub(super) fn resolve_one_column(
        &self,
        schema: &DFSchemaRef,
        name: &str,
        state: &PlanResolverState,
    ) -> PlanResult<Column> {
        if let Some(column) = self.resolve_optional_column(schema, name, state)? {
            Ok(column)
        } else {
            let columns = Self::get_field_names(schema, state)?
                .iter()
                .map(|name| quote_column_name([name]))
                .collect();
            Err(Self::unknown_column_error(
                &spec::ObjectName::bare(name),
                columns,
            ))
        }
    }

    pub(super) fn resolve_columns<T: AsRef<str>>(
        &self,
        schema: &DFSchemaRef,
        names: &[T],
        state: &PlanResolverState,
    ) -> PlanResult<Vec<Column>> {
        names
            .iter()
            .map(|name| self.resolve_one_column(schema, name.as_ref(), state))
            .collect::<PlanResult<Vec<Column>>>()
    }

    /// Returns the user-visible field names for a resolved schema.
    pub(super) fn get_field_names(
        schema: &DFSchemaRef,
        state: &PlanResolverState,
    ) -> PlanResult<Vec<String>> {
        schema
            .fields()
            .iter()
            .filter_map(|field| {
                state
                    .get_field_info(field.name())
                    .map(|info| (!info.is_hidden()).then(|| info.name().to_string()))
                    .transpose()
            })
            .collect::<PlanResult<Vec<_>>>()
    }
}
