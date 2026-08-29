//! Optional DataFusion table-provider and registration surface.

mod dynamic_filters;
mod dynamic_partition_pruning;
mod execution;
mod planning;

pub use execution::{
    IntraFileRepartitioning, ScanMetrics, ScanMetricsSnapshot, collect_scan_metrics,
};

use std::{collections::HashSet, fmt, sync::Arc};

use arrow::datatypes::{DataType, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{DataFusionError, Result as DataFusionResult},
    datasource::{TableProvider, TableType, physical_plan::wrap_partition_type_in_dict},
    execution::context::SessionContext,
    logical_expr::{Expr, TableProviderFilterPushDown},
    physical_plan::ExecutionPlan,
};

use self::{
    execution::create_datafusion_execution_plan,
    planning::{FilterCapabilities, plan_datafusion_filters, plan_datafusion_scan},
};

use crate::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaTable, ParquetReaderBackend,
    delta::kernel::kernel_pruning_predicate,
    reader::{
        backend::direct_parquet::ParquetRangeReadEstimator,
        planning::{DeltaScanPartitionTargetOptions, build_physical_row_predicate, plan_scan},
        transform::schema_with_view_types,
    },
};

const TRACING_TARGET: &str = "delta_arrow_reader::datafusion";

/// DataFusion-specific scan settings for one provider.
#[must_use = "scan options do nothing unless passed to a provider"]
#[derive(Debug, Clone)]
pub struct ScanOptions {
    /// Reader execution settings used by each provider scan.
    pub execution_options: DeltaScanExecutionOptions,
    /// Optional explicit scan partition target.
    pub target_partitions: Option<usize>,
    /// Controls when DataFusion may split direct Parquet reads into ranged scan tasks.
    pub intra_file_repartitioning: IntraFileRepartitioning,
    /// Decode string and binary data-file columns into Arrow view arrays.
    pub use_arrow_view_types: bool,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            execution_options: DeltaScanExecutionOptions::default(),
            target_partitions: None,
            intra_file_repartitioning: IntraFileRepartitioning::default(),
            use_arrow_view_types: true,
        }
    }
}

/// Immutable DataFusion provider for one loaded Delta table snapshot.
///
/// ```no_run
/// use std::sync::Arc;
/// use datafusion::prelude::SessionContext;
/// use delta_arrow_reader::{
///     DeltaTableBuilder,
///     datafusion::{DeltaTableProvider, ScanOptions},
/// };
///
/// # async fn build_provider() -> Result<(), Box<dyn std::error::Error>> {
/// let table = DeltaTableBuilder::new("/tmp/example-delta-table")
///     .load_table()
///     .await?;
/// let provider = DeltaTableProvider::try_new(
///     table,
///     ScanOptions::default(),
/// )?;
/// SessionContext::new().register_table("orders", Arc::new(provider))?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct DeltaTableProvider {
    table: DeltaTable,
    schema: SchemaRef,
    options: ScanOptions,
    registration_name: Option<String>,
    range_read_estimator: Arc<ParquetRangeReadEstimator>,
}

impl DeltaTableProvider {
    /// Creates a provider after validating its options and table protocol.
    pub fn try_new(table: DeltaTable, options: ScanOptions) -> Result<Self, DeltaReaderError> {
        Self::try_new_with_registration_name(table, options, None)
    }

    fn try_new_with_registration_name(
        table: DeltaTable,
        options: ScanOptions,
        registration_name: Option<String>,
    ) -> Result<Self, DeltaReaderError> {
        if options.target_partitions == Some(0) {
            return Err(DeltaReaderError::InvalidConfiguration {
                reason: "scan_partition_target_must_be_positive",
            });
        }
        table.validate_protocol()?;
        let partition_columns = table.partition_columns().iter().cloned().collect();
        let schema = build_provider_schema(
            &table.schema(),
            &partition_columns,
            options.use_arrow_view_types,
        );
        Ok(Self {
            table,
            schema,
            options,
            registration_name,
            range_read_estimator: Arc::default(),
        })
    }

    fn plan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
    ) -> Result<(Arc<dyn ExecutionPlan>, usize), DeltaReaderError> {
        let _planning = tracing::debug_span!(
            target: "delta_arrow_reader::profile",
            "Delta scan planning"
        )
        .entered();
        let partition_columns = self
            .table
            .partition_columns()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let filter_refs = filters.iter().collect::<Vec<_>>();
        let mut datafusion_plan = plan_datafusion_scan(
            &self.table.schema(),
            &partition_columns,
            projection,
            &filter_refs,
            FilterCapabilities {
                supports_exact_row_filtering: self.options.execution_options.parquet_backend()
                    == ParquetReaderBackend::Direct,
            },
        )?;
        if datafusion_plan
            .filters
            .decisions
            .iter()
            .any(|decision| decision.pushdown == TableProviderFilterPushDown::Unsupported)
        {
            return Err(DeltaReaderError::UnsupportedPredicate {
                reason: "datafusion_scan_contains_unsupported_filter",
            });
        }
        let scan_projection = datafusion_plan.projection.scan_projection.clone();
        let hidden_columns = datafusion_plan.projection.hidden_columns.clone();
        let pruning_predicate = datafusion_plan
            .filters
            .pruning_predicate
            .as_ref()
            .map(|predicate| {
                kernel_pruning_predicate(predicate).ok_or(DeltaReaderError::UnsupportedPredicate {
                    reason: "datafusion_predicate_not_kernel_safe",
                })
            })
            .transpose()?;
        let exact_row_predicate = datafusion_plan
            .filters
            .exact_row_predicate
            .as_ref()
            .map(|predicate| {
                kernel_pruning_predicate(predicate).ok_or(DeltaReaderError::UnsupportedPredicate {
                    reason: "exact_row_predicate_not_kernel_safe",
                })
            })
            .transpose()?;
        let exact_row_predicate = build_physical_row_predicate(
            self.table.snapshot(),
            scan_projection.as_deref(),
            &hidden_columns,
            exact_row_predicate,
        )?;
        let mut reader_plan = plan_scan(
            self.table.snapshot(),
            scan_projection.as_deref(),
            &hidden_columns,
            pruning_predicate,
            datafusion_plan.filters.requires_statistics,
            self.options.execution_options,
            DeltaScanPartitionTargetOptions {
                explicit_target_partitions: self.options.target_partitions,
                datafusion_target_partitions: Some(state.config().target_partitions()),
            },
        )?;
        reader_plan.logical_schema = build_provider_schema(
            &reader_plan.logical_schema,
            &partition_columns,
            self.options.use_arrow_view_types,
        );
        reader_plan.physical_schema = build_provider_schema(
            &reader_plan.physical_schema,
            &partition_columns,
            self.options.use_arrow_view_types,
        );
        reader_plan.projected_schema = build_provider_schema(
            &reader_plan.projected_schema,
            &partition_columns,
            self.options.use_arrow_view_types,
        );
        datafusion_plan.projection.output_schema = build_provider_schema(
            &datafusion_plan.projection.output_schema,
            &partition_columns,
            self.options.use_arrow_view_types,
        );
        let partition_count = reader_plan.partitions.len();
        let plan = {
            let _setup = tracing::debug_span!(
                target: "delta_arrow_reader::profile",
                "Delta scan execution setup"
            )
            .entered();
            let metrics = ScanMetrics::new(
                self.registration_name.clone(),
                reader_plan.metrics.clone(),
                self.options.use_arrow_view_types,
            );
            create_datafusion_execution_plan(
                reader_plan,
                datafusion_plan,
                exact_row_predicate,
                Arc::clone(&self.range_read_estimator),
                self.table.prepared_parquet_metadata_cache(),
                metrics,
                self.options.intra_file_repartitioning,
            )
        };
        Ok((plan, partition_count))
    }
}

fn build_provider_schema(
    schema: &Schema,
    partition_columns: &HashSet<String>,
    use_arrow_view_types: bool,
) -> SchemaRef {
    let view_schema = schema_with_view_types(schema);
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .zip(view_schema.fields())
            .map(|(logical, view)| {
                if partition_columns.contains(logical.name())
                    && matches!(
                        logical.data_type(),
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Binary
                            | DataType::LargeBinary
                    )
                {
                    Arc::new(
                        logical
                            .as_ref()
                            .clone()
                            .with_data_type(wrap_partition_type_in_dict(
                                logical.data_type().clone(),
                            )),
                    )
                } else if use_arrow_view_types {
                    Arc::clone(view)
                } else {
                    Arc::clone(logical)
                }
            })
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ))
}

impl fmt::Debug for DeltaTableProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeltaTableProvider")
            .field("snapshot_version", &self.table.version())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for DeltaTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        match self.plan(state, projection.map(Vec::as_slice), filters) {
            Ok((plan, partition_count)) => {
                tracing::debug!(
                    target: TRACING_TARGET,
                    event = "provider_scan.planned",
                    snapshot_version = self.table.version(),
                    partition_count,
                    backend = ?self.options.execution_options.parquet_backend(),
                    outcome = "planned"
                );
                Ok(plan)
            }
            Err(error) => {
                trace_failure(
                    "provider_scan.failed",
                    self.table.version(),
                    self.options.execution_options.parquet_backend(),
                    &error,
                );
                Err(DataFusionError::External(Box::new(error)))
            }
        }
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        let partition_columns = self
            .table
            .partition_columns()
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let datafusion_plan = plan_datafusion_filters(
            &self.table.schema(),
            &partition_columns,
            filters,
            FilterCapabilities {
                supports_exact_row_filtering: self.options.execution_options.parquet_backend()
                    == ParquetReaderBackend::Direct,
            },
        );
        Ok(datafusion_plan
            .decisions
            .iter()
            .map(|decision| decision.pushdown.clone())
            .collect())
    }
}

/// Result of registering one loaded Delta table in a DataFusion context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRegistration {
    /// Caller-supplied DataFusion table name.
    pub name: String,
    /// Loaded Delta snapshot version.
    pub snapshot_version: u64,
}

/// Registers one loaded Delta table in a DataFusion session.
///
/// Registration performs no scan. Existing registrations are preserved and
/// reported through [`DeltaReaderError`].
///
/// ```no_run
/// use datafusion::prelude::SessionContext;
/// use delta_arrow_reader::{
///     DeltaTableBuilder,
///     datafusion::{ScanOptions, register_table},
/// };
///
/// # async fn register() -> Result<(), Box<dyn std::error::Error>> {
/// let context = SessionContext::new();
/// let table = DeltaTableBuilder::new("/tmp/example-delta-table")
///     .load_table()
///     .await?;
/// let registration = register_table(
///     &context,
///     "orders",
///     table,
///     ScanOptions::default(),
/// )?;
/// assert_eq!(registration.name, "orders");
/// # Ok(())
/// # }
/// ```
pub fn register_table(
    context: &SessionContext,
    name: impl Into<String>,
    table: DeltaTable,
    options: ScanOptions,
) -> Result<TableRegistration, DeltaReaderError> {
    let name = name.into();
    let snapshot_version = table.version();
    let backend = options.execution_options.parquet_backend();
    let result = (|| {
        validate_registration_name(&name)?;
        let provider =
            DeltaTableProvider::try_new_with_registration_name(table, options, Some(name.clone()))?;
        context
            .register_table(name.as_str(), Arc::new(provider))
            .map_err(|source| DeltaReaderError::DataFusionAdapter {
                reason: "table_registration_failed",
                source: Box::new(source),
            })?;
        Ok(TableRegistration {
            name,
            snapshot_version,
        })
    })();
    match result {
        Ok(registration) => {
            tracing::debug!(
                target: TRACING_TARGET,
                event = "provider_registration.registered",
                snapshot_version,
                partition_count = tracing::field::Empty,
                backend = ?backend,
                outcome = "registered"
            );
            Ok(registration)
        }
        Err(error) => {
            trace_failure(
                "provider_registration.failed",
                snapshot_version,
                backend,
                &error,
            );
            Err(error)
        }
    }
}

fn validate_registration_name(name: &str) -> Result<(), DeltaReaderError> {
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|first| first == '_' || first.is_ascii_alphabetic())
        && chars.all(|value| value == '_' || value.is_ascii_alphanumeric());
    if !valid || is_reserved_sql_keyword(name) {
        let reason = if name.is_empty() {
            "table_registration_name_empty"
        } else {
            "table_registration_name_invalid"
        };
        return Err(DeltaReaderError::DataFusionAdapter {
            reason,
            source: Box::new(DataFusionError::Plan(reason.to_owned())),
        });
    }
    Ok(())
}

fn is_reserved_sql_keyword(name: &str) -> bool {
    const KEYWORDS: &[&str] = &[
        "all",
        "alter",
        "analyze",
        "and",
        "anti",
        "as",
        "asof",
        "by",
        "case",
        "connect",
        "cross",
        "delete",
        "distinct",
        "distribute",
        "drop",
        "else",
        "end",
        "except",
        "exists",
        "explain",
        "false",
        "fetch",
        "for",
        "format",
        "from",
        "full",
        "global",
        "group",
        "having",
        "in",
        "inner",
        "insert",
        "intersect",
        "into",
        "is",
        "join",
        "lateral",
        "left",
        "like",
        "limit",
        "minus",
        "natural",
        "not",
        "null",
        "offset",
        "on",
        "open",
        "or",
        "order",
        "outer",
        "partition",
        "pivot",
        "prewhere",
        "qualify",
        "returning",
        "right",
        "sample",
        "select",
        "semi",
        "set",
        "settings",
        "sort",
        "start",
        "table",
        "tablesample",
        "then",
        "top",
        "true",
        "union",
        "unpivot",
        "update",
        "using",
        "values",
        "view",
        "when",
        "where",
        "window",
        "with",
    ];
    KEYWORDS
        .iter()
        .any(|keyword| name.eq_ignore_ascii_case(keyword))
}

fn trace_failure(
    event: &'static str,
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    error: &DeltaReaderError,
) {
    tracing::debug!(
        target: TRACING_TARGET,
        event,
        snapshot_version,
        partition_count = tracing::field::Empty,
        backend = ?backend,
        outcome = "failed",
        error_code = error.code(),
        error_phase = error.phase().as_str()
    );
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use arrow::datatypes::{DataType, Field, Schema};

    use super::{build_provider_schema, validate_registration_name};

    #[test]
    fn registration_names_preserve_the_frozen_unquoted_identifier_boundary() {
        for name in ["orders", "_customers", "Regions_2026", "line_items"] {
            assert!(validate_registration_name(name).is_ok(), "{name}");
        }

        for name in [
            "",
            "2026_orders",
            "orders.latest",
            "line-items",
            "line items",
            "\"orders\"",
            "orders$",
            "ordérs",
            "select",
            "FROM",
            "Join",
            "where",
            "table",
        ] {
            assert!(validate_registration_name(name).is_err(), "{name}");
        }
    }

    #[test]
    fn datafusion_schema_uses_views_except_for_dictionary_partitions() {
        let field_metadata = HashMap::from([("field-key".to_owned(), "field-value".to_owned())]);
        let schema_metadata = HashMap::from([("schema-key".to_owned(), "schema-value".to_owned())]);
        let schema = Schema::new_with_metadata(
            vec![
                Field::new("text", DataType::Utf8, true).with_metadata(field_metadata.clone()),
                Field::new("payload", DataType::Binary, true),
                Field::new("region", DataType::Utf8, true),
                Field::new("partition_payload", DataType::LargeBinary, true),
                Field::new("id", DataType::Int32, false),
            ],
            schema_metadata.clone(),
        );
        let partitions = HashSet::from(["region".to_owned(), "partition_payload".to_owned()]);

        let mapped = build_provider_schema(&schema, &partitions, true);

        assert_eq!(
            mapped.as_ref(),
            &Schema::new_with_metadata(
                vec![
                    Field::new("text", DataType::Utf8View, true).with_metadata(field_metadata),
                    Field::new("payload", DataType::BinaryView, true),
                    Field::new(
                        "region",
                        DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
                        true,
                    ),
                    Field::new(
                        "partition_payload",
                        DataType::Dictionary(
                            Box::new(DataType::UInt16),
                            Box::new(DataType::LargeBinary),
                        ),
                        true,
                    ),
                    Field::new("id", DataType::Int32, false),
                ],
                schema_metadata.clone(),
            )
        );

        let standard = build_provider_schema(&schema, &partitions, false);
        assert_eq!(standard.field(0).data_type(), &DataType::Utf8);
        assert_eq!(standard.field(1).data_type(), &DataType::Binary);
        assert_eq!(
            standard.field(2).data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8))
        );
        assert_eq!(
            standard.field(3).data_type(),
            &DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::LargeBinary),)
        );
        assert_eq!(standard.field(4).data_type(), &DataType::Int32);
        assert_eq!(standard.metadata(), &schema_metadata);
    }
}
