//! A projection transports real rows and the same owned failure exactly once.

use std::error::Error;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{ColumnarValue, EvaluatedRowPrefix, RowEvaluationFailure};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion::physical_plan::projection::{
    ProjectionExec, make_with_child, remove_unnecessary_projections,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::SessionContext;
use futures_util::{Stream, StreamExt, stream};

#[derive(Debug)]
struct OriginalFailure(Arc<()>);

impl Display for OriginalFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original owned failure")
    }
}
impl Error for OriginalFailure {}

fn owned_error(source: &Arc<()>) -> DataFusionError {
    DataFusionError::External(Box::new(OriginalFailure(Arc::clone(source))))
}

fn assert_original(error: &DataFusionError, source: &Arc<()>) {
    assert!(matches!(error.find_root(), DataFusionError::External(error)
        if error.downcast_ref::<OriginalFailure>()
            .is_some_and(|error| Arc::ptr_eq(&error.0, source))));
}

#[derive(Debug)]
struct ProbeExpression {
    id: usize,
    failure_row: Option<usize>,
    opaque: bool,
    source: Arc<()>,
    calls: Arc<Mutex<Vec<usize>>>,
}

impl PartialEq for ProbeExpression {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for ProbeExpression {}
impl Hash for ProbeExpression {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
impl Display for ProbeExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "probe_{}", self.id)
    }
}

impl PhysicalExpr for ProbeExpression {
    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
    fn return_field(&self, schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&schema.fields()[0]))
    }
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.evaluate_rows_until_error(batch)?.into_result()
    }
    fn evaluate_rows_until_error(&self, batch: &RecordBatch) -> Result<EvaluatedRowPrefix> {
        self.calls
            .lock()
            .map_err(|_| DataFusionError::Internal("probe lock poisoned".into()))?
            .push(batch.num_rows());
        if self.opaque {
            return Err(owned_error(&self.source));
        }
        let failure = self
            .failure_row
            .filter(|row| *row < batch.num_rows())
            .map(|row_index| RowEvaluationFailure {
                row_index,
                error: owned_error(&self.source),
            });
        let rows = failure
            .as_ref()
            .map_or(batch.num_rows(), |failure| failure.row_index);
        Ok(EvaluatedRowPrefix {
            values: ColumnarValue::Array(batch.column(0).slice(0, rows)),
            failure,
        })
    }
    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }
}

#[derive(Debug)]
struct InputBatches {
    batch: RecordBatch,
    polls: Arc<AtomicUsize>,
    first_error: Option<Arc<()>>,
}

impl PartitionStream for InputBatches {
    fn schema(&self) -> &SchemaRef {
        self.batch.schema_ref()
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let first = self
            .first_error
            .as_ref()
            .map_or_else(|| Ok(self.batch.clone()), |source| Err(owned_error(source)));
        let batches = stream::iter([first, Ok(self.batch.clone())]);
        let polls = Arc::clone(&self.polls);
        let mut batches = Box::pin(batches);
        let counted = stream::poll_fn(move |cx| {
            polls.fetch_add(1, Ordering::SeqCst);
            batches.as_mut().poll_next(cx)
        });
        Box::pin(RecordBatchStreamAdapter::new(self.batch.schema(), counted))
    }
}

fn input(
    batch: &RecordBatch,
    polls: &Arc<AtomicUsize>,
    first_error: Option<Arc<()>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(StreamingTableExec::try_new(
        batch.schema(),
        vec![Arc::new(InputBatches {
            batch: batch.clone(),
            polls: Arc::clone(polls),
            first_error,
        })],
        None,
        [],
        false,
        None,
    )?))
}

fn batch() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("value", DataType::Int32, true)
                .with_metadata([("field_key".into(), "field_value".into())].into()),
        ],
        [("schema_key".into(), "schema_value".into())].into(),
    ));
    // The visible batch has a storage offset and a semantic NULL in its prefix.
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![
            Some(99),
            None,
            Some(11),
            Some(12),
            Some(13),
        ])) as ArrayRef],
    )
    .map(|batch| batch.slice(1, 4))
    .map_err(Into::into)
}

fn probe(id: usize, failure_row: Option<usize>, opaque: bool) -> (Arc<ProbeExpression>, Arc<()>) {
    let source = Arc::new(());
    (
        Arc::new(ProbeExpression {
            id,
            failure_row,
            opaque,
            source: Arc::clone(&source),
            calls: Arc::new(Mutex::new(vec![])),
        }),
        source,
    )
}

fn assert_calls(expr: &ProbeExpression, expected: &[usize]) -> Result<()> {
    assert_eq!(
        &*expr
            .calls
            .lock()
            .map_err(|_| DataFusionError::Internal("probe lock poisoned".into()))?,
        expected
    );
    Ok(())
}

#[test]
fn projector_shrinks_prior_values_and_evaluates_later_outputs_only_on_the_prefix() -> Result<()> {
    let batch = batch()?;
    let (first, _) = probe(0, Some(3), false);
    let (second, source) = probe(1, Some(2), false);
    let (third, _) = probe(2, Some(2), false);
    let projection = ProjectionExprs::from_expressions(vec![
        ProjectionExpr::new(first.clone(), "first"),
        ProjectionExpr::new(
            Arc::new(Literal::new(ScalarValue::Int32(Some(7)))),
            "scalar",
        ),
        ProjectionExpr::new(second.clone(), "second"),
        ProjectionExpr::new(third.clone(), "third"),
    ]);
    let projector = projection.make_projector(batch.schema_ref())?;
    let output = projector.project_batch_until_error(&batch)?;
    assert_eq!(output.batch.num_rows(), 2);
    assert_eq!(output.batch.schema_ref(), projector.output_schema());
    assert_eq!(output.batch.schema().metadata(), batch.schema().metadata());
    assert_eq!(
        output.batch.schema().field(0).metadata(),
        batch.schema().field(0).metadata()
    );
    assert_eq!(
        output.batch.column(0).as_any().downcast_ref::<Int32Array>(),
        Some(&Int32Array::from(vec![None, Some(11)]))
    );
    assert_eq!(
        output.batch.column(1).as_any().downcast_ref::<Int32Array>(),
        Some(&Int32Array::from(vec![7, 7]))
    );
    for column in output.batch.columns() {
        column.to_data().validate_full()?;
    }
    let Some(failure) = output.failure else {
        return internal_err!("expected localized failure");
    };
    assert_eq!(failure.row_index, 2);
    assert_original(&failure.error, &source);
    assert_calls(&first, &[4])?;
    assert_calls(&second, &[3])?;
    assert_calls(&third, &[2])?;
    Ok(())
}

#[tokio::test]
async fn stream_emits_prefix_then_owned_error_and_never_polls_input_again() -> Result<()> {
    let batch = batch()?;
    let polls = Arc::new(AtomicUsize::new(0));
    let (expr, source) = probe(3, Some(2), false);
    let plan = ProjectionExec::try_new(
        [ProjectionExpr::new(expr.clone(), "r")],
        input(&batch, &polls, None)?,
    )?
    .with_row_error_prefix(true);
    let mut stream = plan.execute(0, SessionContext::new().task_ctx())?;
    assert_eq!(stream.size_hint().0, 0);
    let prefix = stream
        .next()
        .await
        .transpose()?
        .ok_or_else(|| DataFusionError::Internal("missing prefix".into()))?;
    assert_eq!(prefix.num_rows(), 2);
    assert_eq!(prefix.schema(), plan.schema());
    assert_eq!(
        prefix.column(0).as_any().downcast_ref::<Int32Array>(),
        Some(&Int32Array::from(vec![None, Some(11)]))
    );
    assert_eq!(stream.size_hint(), (1, Some(1)));
    let Some(Err(error)) = stream.next().await else {
        return internal_err!("missing deferred error");
    };
    assert_original(&error, &source);
    assert_eq!(stream.size_hint(), (0, Some(0)));
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_calls(&expr, &[4])?;
    let metrics = plan
        .metrics()
        .ok_or_else(|| DataFusionError::Internal("missing metrics".into()))?;
    assert_eq!(metrics.output_rows(), Some(2));
    assert!(
        metrics
            .iter()
            .any(|metric| metric.value().name() == "expr_0_eval_time")
    );
    assert_eq!(
        metrics
            .iter()
            .filter(|metric| metric.value().name() == "output_batches")
            .map(|metric| metric.value().as_usize())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        metrics
            .iter()
            .filter(|metric| metric.value().name() == "output_bytes")
            .map(|metric| metric.value().as_usize())
            .sum::<usize>(),
        datafusion::common::utils::memory::get_record_batch_memory_size(&prefix)
    );
    Ok(())
}

#[tokio::test]
async fn row_zero_opaque_and_upstream_failures_terminate_without_empty_output_or_retry()
-> Result<()> {
    let batch = batch()?;
    for (row, opaque, upstream) in [
        (Some(0), false, false),
        (None, true, false),
        (None, false, true),
    ] {
        let polls = Arc::new(AtomicUsize::new(0));
        let (first, source) = probe(4, row, opaque);
        let (later, _) = probe(5, None, false);
        let plan = ProjectionExec::try_new(
            [
                ProjectionExpr::new(first.clone(), "first"),
                ProjectionExpr::new(later.clone(), "later"),
            ],
            input(&batch, &polls, upstream.then(|| Arc::clone(&source)))?,
        )?
        .with_row_error_prefix(true);
        let mut stream = plan.execute(0, SessionContext::new().task_ctx())?;
        let Some(Err(error)) = stream.next().await else {
            return internal_err!("error must be emitted directly");
        };
        assert_original(&error, &source);
        assert_eq!(stream.size_hint(), (0, Some(0)));
        assert!(stream.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_calls(&first, if upstream { &[] } else { &[4] })?;
        assert_calls(&later, &[])?;
        assert_eq!(
            plan.metrics().and_then(|metrics| metrics.output_rows()),
            Some(0)
        );
    }
    Ok(())
}

#[tokio::test]
async fn ordinary_projection_keeps_its_all_or_error_batch_path() -> Result<()> {
    let batch = batch()?;
    let polls = Arc::new(AtomicUsize::new(0));
    let (expr, source) = probe(7, Some(2), false);
    let plan = ProjectionExec::try_new(
        [ProjectionExpr::new(expr.clone(), "r")],
        input(&batch, &polls, None)?,
    )?;
    let mut stream = plan.execute(0, SessionContext::new().task_ctx())?;
    for _ in 0..2 {
        let Some(Err(error)) = stream.next().await else {
            return internal_err!("ordinary batch must fail without a prefix");
        };
        assert_original(&error, &source);
    }
    assert_calls(&expr, &[4, 4])?;
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    Ok(())
}

#[test]
fn projector_rejects_malformed_prefixes_and_row_zero_output_types() -> Result<()> {
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct Malformed(usize);
    impl Display for Malformed {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("malformed")
        }
    }
    impl PhysicalExpr for Malformed {
        fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            Display::fmt(self, f)
        }
        fn data_type(&self, _: &Schema) -> Result<DataType> {
            Ok(DataType::Int32)
        }
        fn nullable(&self, _: &Schema) -> Result<bool> {
            Ok(true)
        }
        fn evaluate(&self, _: &RecordBatch) -> Result<ColumnarValue> {
            internal_err!("prefix path required")
        }
        fn evaluate_rows_until_error(&self, batch: &RecordBatch) -> Result<EvaluatedRowPrefix> {
            let (values, row_index) = match self.0 {
                0 => (
                    ColumnarValue::Array(Arc::new(Int32Array::from(vec![1]))),
                    batch.num_rows(),
                ),
                1 => (ColumnarValue::Array(Arc::new(Int32Array::from(vec![1]))), 2),
                2 => (ColumnarValue::Scalar(ScalarValue::Int32(Some(1))), 0),
                _ => (
                    ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new()))),
                    0,
                ),
            };
            Ok(EvaluatedRowPrefix {
                values,
                failure: Some(RowEvaluationFailure {
                    row_index,
                    error: DataFusionError::Execution("original failure".into()),
                }),
            })
        }
        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }
    }
    let batch = batch()?;
    for malformed in 0..4 {
        let projector =
            ProjectionExprs::new([ProjectionExpr::new(Arc::new(Malformed(malformed)), "r")])
                .make_projector(batch.schema_ref())?;
        assert!(
            projector.project_batch_until_error(&batch).is_err(),
            "malformed response {malformed}"
        );
    }
    Ok(())
}

#[test]
fn opt_in_survives_rebuilds_and_cannot_be_inlined_into_its_consumer() -> Result<()> {
    let batch = batch()?;
    let polls = Arc::new(AtomicUsize::new(0));
    let child = input(&batch, &polls, None)?;
    let (expr, _) = probe(6, Some(2), false);
    let producer = Arc::new(
        ProjectionExec::try_new([ProjectionExpr::new(expr, "value")], child.clone())?
            .with_row_error_prefix(true),
    );
    let rebuilt = producer.clone().with_new_children(vec![child.clone()])?;
    assert!(
        rebuilt
            .downcast_ref::<ProjectionExec>()
            .is_some_and(ProjectionExec::preserves_row_error_prefix)
    );
    let made = make_with_child(&producer, &child)?;
    assert!(
        made.downcast_ref::<ProjectionExec>()
            .is_some_and(ProjectionExec::preserves_row_error_prefix)
    );
    let consumer = Arc::new(ProjectionExec::try_new(
        [ProjectionExpr::new(
            Arc::new(Column::new("value", 0)),
            "renamed",
        )],
        producer.clone(),
    )?);
    let optimized = remove_unnecessary_projections(consumer)?.data;
    assert!(
        optimized
            .downcast_ref::<ProjectionExec>()
            .is_some_and(|projection| projection
                .input()
                .downcast_ref::<ProjectionExec>()
                .is_some_and(ProjectionExec::preserves_row_error_prefix))
    );
    let ordinary = ProjectionExec::try_new(
        [ProjectionExpr::new(
            Arc::new(Column::new("value", 0)),
            "value",
        )],
        child,
    )?;
    assert!(!ordinary.preserves_row_error_prefix());
    let projector = ProjectionExprs::from_expressions(Vec::<ProjectionExpr>::new())
        .make_projector(batch.schema_ref())?;
    let output = projector.project_batch_until_error(&batch)?;
    assert_eq!(output.batch.num_columns(), 0);
    assert_eq!(output.batch.num_rows(), 4);
    assert!(output.failure.is_none());
    let empty = projector.project_batch_until_error(&batch.slice(0, 0))?;
    assert_eq!(empty.batch.num_rows(), 0);
    assert!(empty.failure.is_none());
    Ok(())
}
