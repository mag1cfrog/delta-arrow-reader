//! Keep rows accepted by predicates on NULL-padded join inputs.

use std::error::Error;
use std::sync::Arc;

use arrow::array::{Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

type TestResult = Result<(), Box<dyn Error + Send + Sync>>;

#[tokio::test]
async fn native_outer_join_predicates_keep_their_null_context() -> TestResult {
    let session = SessionContext::new();
    let left_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    session.register_batch(
        "left_input",
        RecordBatch::try_new(left_schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?,
    )?;
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]));
    session.register_batch(
        "right_input",
        RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![None])),
                Arc::new(Int32Array::from(vec![Some(5)])),
            ],
        )?,
    )?;
    for (predicate, expected) in [
        ("r.a IS NOT NULL OR r.b IS NOT NULL", vec![(1, Some(1))]),
        ("r.a > 0 OR r.b > 0", vec![(1, Some(1))]),
        ("(r.a > 0 AND l.id > 0) OR r.b > 0", vec![(1, Some(1))]),
        (
            "r.a IS NULL OR r.b IS NOT NULL",
            vec![(1, Some(1)), (2, None)],
        ),
        (
            "(r.a IS NOT NULL) IS NOT NULL",
            vec![(1, Some(1)), (2, None)],
        ),
        ("(r.a IS NOT NULL) = false", vec![(1, Some(1)), (2, None)]),
        ("COALESCE(r.b, 0) > 0", vec![(1, Some(1))]),
    ] {
        let sql = format!(
            "SELECT l.id AS left_id,r.id AS right_id FROM left_input l \
             LEFT JOIN right_input r ON l.id=r.id WHERE {predicate} ORDER BY l.id"
        );
        let mut actual = Vec::new();
        for batch in session.sql(&sql).await?.collect().await? {
            let left = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("left ID type changed")?;
            let right = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("right ID type changed")?;
            for row in 0..batch.num_rows() {
                actual.push((
                    left.value(row),
                    (!right.is_null(row)).then(|| right.value(row)),
                ));
            }
        }
        assert_eq!(actual, expected, "{predicate}");
    }
    Ok(())
}
