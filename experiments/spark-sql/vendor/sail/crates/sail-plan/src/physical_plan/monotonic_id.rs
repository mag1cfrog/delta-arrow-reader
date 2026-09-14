// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    RecordBatchStream,
};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, Result, Statistics, exec_err, internal_err};
use tokio_stream::Stream;

#[derive(Debug, Clone)]
pub struct MonotonicIdExec {
    input: Arc<dyn ExecutionPlan>,
    column_name: String,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl MonotonicIdExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column_name: impl Into<String>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let column_name = column_name.into();
        // Basic sanity: ensure the column exists in schema and is Int64
        let idx = schema.index_of(&column_name)?;
        let field = schema.field(idx);
        if field.data_type() != &DataType::Int64 {
            return exec_err!(
                "MonotonicIdExec expects Int64 field for {}, got {:?}",
                column_name,
                field.data_type()
            );
        }

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone())
                .extend(input.equivalence_properties().clone())?,
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Ok(Self {
            input,
            column_name,
            schema,
            properties,
        })
    }
}

impl DisplayAs for MonotonicIdExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "MonotonicIdExec: col={}", self.column_name)
    }
}

impl ExecutionPlan for MonotonicIdExec {
    fn name(&self) -> &'static str {
        "MonotonicIdExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.as_slice() else {
            return internal_err!("MonotonicIdExec requires exactly one child");
        };
        Ok(Arc::new(Self::try_new(
            input.clone(),
            self.column_name.clone(),
            self.schema.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(MonotonicIdStream::new(
            input,
            self.schema.clone(),
            self.column_name.clone(),
            partition,
        )?))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let mut stats = Arc::unwrap_or_clone(self.input.partition_statistics(partition)?);
        let col_idx = self.schema.index_of(&self.column_name)?;
        let unknown_col_stats = ColumnStatistics::new_unknown();
        if col_idx <= stats.column_statistics.len() {
            stats.column_statistics.insert(col_idx, unknown_col_stats);
        } else {
            while stats.column_statistics.len() < col_idx {
                stats
                    .column_statistics
                    .push(ColumnStatistics::new_unknown());
            }
            stats.column_statistics.push(unknown_col_stats);
        }

        // One additional Int64 output column contributes 8 bytes per row when row counts are known.
        let added_bytes = stats
            .num_rows
            .multiply(&Precision::Exact(std::mem::size_of::<i64>()));
        stats.total_byte_size = stats.total_byte_size.add(&added_bytes);

        Ok(Arc::new(stats))
    }
}

struct MonotonicIdStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    col_idx: usize,
    partition: usize,
    offset: u64,
}

impl MonotonicIdStream {
    fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        column_name: String,
        partition: usize,
    ) -> Result<Self> {
        let col_idx = schema.index_of(&column_name)?;
        if partition > i32::MAX as usize {
            return exec_err!(
                "monotonically_increasing_id: partition index {partition} does not fit in i32"
            );
        }
        Ok(Self {
            input,
            schema,
            col_idx,
            partition,
            offset: 0,
        })
    }

    fn make_ids(&mut self, len: usize) -> Result<ArrayRef> {
        // Spark: (partitionId << 33) + recordNumberWithinPartition
        let base = (self.partition as u64) << 33;
        if len as u64 > (1u64 << 33) - self.offset {
            return exec_err!(
                "monotonically_increasing_id overflow: exceeded 2^33 rows in one partition"
            );
        }
        let start = self.offset;
        self.offset += len as u64;
        let values = (0..len)
            .map(|i| (base + start + (i as u64)) as i64)
            .collect::<Vec<_>>();
        Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
    }
}

impl RecordBatchStream for MonotonicIdStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for MonotonicIdStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(batch))) => {
                let mut cols = batch.columns().to_vec();
                let id_col = match self.make_ids(batch.num_rows()) {
                    Ok(c) => c,
                    Err(e) => return Poll::Ready(Some(Err(e))),
                };
                if self.col_idx > cols.len() {
                    return Poll::Ready(Some(internal_err!(
                        "MonotonicIdExec output column index {0} exceeds input column count {1}",
                        self.col_idx,
                        cols.len()
                    )));
                }
                cols.insert(self.col_idx, id_col);
                Poll::Ready(Some(
                    RecordBatch::try_new(self.schema.clone(), cols).map_err(Into::into),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    #[test]
    fn monotonic_ids_check_counter_and_partition_boundaries() -> Result<()> {
        let input = Arc::new(
            EmptyExec::new(Arc::new(Schema::empty())).with_partitions(i32::MAX as usize + 2),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("mid", DataType::Int64, false)]));
        let context = Arc::new(TaskContext::default());
        let stream = |partition| {
            MonotonicIdStream::new(
                input.execute(partition, context.clone())?,
                schema.clone(),
                "mid".into(),
                partition,
            )
        };
        let mut first = stream(0)?;
        let mut second = stream(1)?;
        let values = |ids: ArrayRef| {
            ids.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        };
        assert_eq!(values(first.make_ids(2)?), [0, 1]);
        assert_eq!(values(second.make_ids(2)?), [1i64 << 33, (1i64 << 33) + 1]);
        assert!(first.make_ids(0)?.is_empty());
        assert_eq!(values(first.make_ids(1)?), [2]);
        first.offset = (1u64 << 33) - 1;
        assert_eq!(values(first.make_ids(1)?), [(1i64 << 33) - 1]);
        assert!(first.make_ids(0)?.is_empty());
        assert!(
            first
                .make_ids(1)
                .unwrap_err()
                .to_string()
                .contains("exceeded 2^33")
        );
        assert_eq!(first.offset, 1u64 << 33);
        // Reject an oversized batch before allocating, without overflowing the check itself.
        assert!(second.make_ids(usize::MAX).is_err());
        assert_eq!(values(second.make_ids(1)?), [(1i64 << 33) + 2]);
        assert!(stream(i32::MAX as usize).is_ok());
        let Err(error) = stream(i32::MAX as usize + 1) else {
            return internal_err!("out-of-range partition ID unexpectedly succeeded");
        };
        assert!(error.to_string().contains("does not fit in i32"));
        let plan = Arc::new(MonotonicIdExec::try_new(input, "mid", schema)?);
        assert!(plan.with_new_children(vec![]).is_err());
        Ok(())
    }
}
