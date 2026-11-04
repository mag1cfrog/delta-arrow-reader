//! Prefix ownership and selection connect strict casts to the actual Spark SLICE.

use std::error::Error;
use std::fmt::{self, Display};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Int32Array, Int64Array, LargeStringArray, ListArray,
    StringArray, StringViewArray,
};
use arrow::datatypes::{DataType, Field, FieldRef, Int32Type, Schema};
use arrow::error::ArrowError;
use datafusion::common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{
    ColumnarValue, EvaluatedRowPrefix, LogicalPlan, ReturnFieldArgs, RowEvaluationFailure,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::prelude::SessionContext;
use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
use sail_function::scalar::spark_cast_string_to_integer::{
    SparkCastStringToInteger, StringIntegerCastMode,
};

async fn spark_expression(
    session: &SessionContext,
    expression: &str,
) -> Result<Arc<dyn PhysicalExpr>> {
    let planned = plan_query(
        session,
        &format!("SELECT {expression} AS r FROM prefix_input"),
        &SparkSqlOptions::default(),
    )
    .await?;
    let LogicalPlan::Projection(projection) = planned.logical_plan else {
        return internal_err!("expected a resolved projection");
    };
    session.create_physical_expr(projection.expr[0].clone(), projection.input.schema())
}

fn rebuild_children(expression: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    let children = expression
        .children()
        .into_iter()
        .map(|child| rebuild_children(Arc::clone(child)))
        .collect::<Result<Vec<_>>>()?;
    expression.with_new_children(children)
}

fn spark_session() -> SessionContext {
    SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    )
}

fn assert_cast_cause(error: &DataFusionError, input: &str) {
    let expected = format!("Cannot cast string '{input}' to value of Int32 type");
    assert!(
        error.to_string().starts_with("[CAST_INVALID_INPUT]"),
        "{error}"
    );
    assert!(
        matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
        if matches!(cause.as_ref(), ArrowError::CastError(message) if message == &expected))
    );
}

#[tokio::test]
async fn eager_array_children_choose_the_earlier_row_and_same_row_operand() -> Result<()> {
    for (left, right, expected_row, expected_input) in [
        (
            vec!["10", "11", "array_left_bad", "13"],
            vec!["20", "array_right_bad", "22", "23"],
            1,
            "array_right_bad",
        ),
        (
            vec!["array_left_bad", "11"],
            vec!["array_right_bad", "21"],
            0,
            "array_left_bad",
        ),
    ] {
        let session = spark_session();
        let batch = arrow::record_batch::RecordBatch::try_from_iter([
            ("x", Arc::new(StringArray::from(left)) as ArrayRef),
            ("y", Arc::new(StringArray::from(right)) as ArrayRef),
        ])?;
        session.register_batch("prefix_input", batch.clone())?;
        let expression = spark_expression(
            &session,
            "SLICE(ARRAY(CAST(x AS INT), CAST(y AS INT)), 1, 1)",
        )
        .await?;
        let rebuilt = rebuild_children(Arc::clone(&expression))?;
        assert_cast_cause(
            &rebuilt.evaluate(&batch).expect_err("rebuilt SLICE fails"),
            expected_input,
        );
        let prefix = expression.evaluate_rows_until_error(&batch)?;
        prefix.validate(batch.num_rows())?;
        assert_eq!(
            prefix.values.data_type(),
            expression.data_type(batch.schema_ref())?
        );
        let failure = prefix.failure.expect("localized child failure");
        assert_eq!(failure.row_index, expected_row);
        assert_cast_cause(&failure.error, expected_input);
        let ColumnarValue::Array(values) = prefix.values else {
            return internal_err!("a failed expression must retain its array prefix");
        };
        values.to_data().validate_full()?;
        assert_eq!(values.len(), expected_row);
        if expected_row == 1 {
            assert_eq!(
                values
                    .as_list::<i32>()
                    .value(0)
                    .as_primitive::<Int32Type>()
                    .values(),
                &[10],
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn slice_null_selection_and_nested_parameter_failures_keep_real_prefix_rows() -> Result<()> {
    let session = spark_session();
    let arrays = ListArray::from_iter_primitive::<Int32Type, _, _>([
        None,
        Some(vec![Some(10), Some(11)]),
        Some(vec![Some(20), Some(21)]),
        Some(vec![Some(30), Some(31)]),
        Some(vec![Some(40), Some(41)]),
    ]);
    let batch = arrow::record_batch::RecordBatch::try_from_iter([
        ("a", Arc::new(arrays) as ArrayRef),
        (
            "s",
            Arc::new(StringArray::from(vec![
                Some("skipped_start_bad"),
                Some("1"),
                None,
                Some("1"),
                Some("later_start_bad"),
            ])) as ArrayRef,
        ),
        (
            "l",
            Arc::new(StringArray::from(vec![
                "skipped_length_bad",
                "1",
                "skipped_length_bad",
                "-1",
                "1",
            ])) as ArrayRef,
        ),
        (
            "inner_start",
            Arc::new(Int64Array::from(vec![1, 1, 1, 0, 1])) as ArrayRef,
        ),
        (
            "outer_length",
            Arc::new(Int64Array::from(vec![1, 1, -1, 1, 1])) as ArrayRef,
        ),
    ])?;
    session.register_batch("prefix_input", batch.clone())?;
    for (sql, expected_row, expected_nulls) in [
        (
            "SLICE(a, CAST(s AS INT), CAST(l AS INT))",
            3,
            vec![true, false, true],
        ),
        (
            "SLICE(SLICE(a, inner_start, 1), 1, outer_length)",
            2,
            vec![true, false],
        ),
    ] {
        let expression = spark_expression(&session, sql).await?;
        let prefix = expression.evaluate_rows_until_error(&batch)?;
        prefix.validate(batch.num_rows())?;
        let failure = prefix.failure.expect("localized parameter failure");
        assert_eq!(failure.row_index, expected_row);
        assert!(
            matches!(failure.error.find_root(), DataFusionError::Execution(message)
            if message == "[INVALID_PARAMETER_VALUE.LENGTH] SLICE length must be nonnegative")
        );
        let values = prefix.values.into_array(expected_row)?;
        values.to_data().validate_full()?;
        assert_eq!(
            values.data_type(),
            &expression.data_type(batch.schema_ref())?
        );
        assert_eq!(
            (0..values.len())
                .map(|row| values.is_null(row))
                .collect::<Vec<_>>(),
            expected_nulls,
        );
        assert_eq!(
            values
                .as_list::<i32>()
                .value(1)
                .as_primitive::<Int32Type>()
                .values(),
            &[10],
        );
    }
    // A reached same-row length cast must finish before START validation.
    let expression = spark_expression(&session, "SLICE(a, 0, CAST(l AS INT))").await?;
    let error = expression
        .evaluate(&batch.slice(2, 1))
        .expect_err("length cast fails");
    assert_cast_cause(&error, "skipped_length_bad");
    Ok(())
}

#[tokio::test]
async fn selected_cast_failure_maps_to_presented_rows_with_nulls_and_sliced_storage() -> Result<()>
{
    let session = spark_session();
    let input: ArrayRef = Arc::new(StringArray::from(vec![
        Some("outside"),
        Some("skip_bad"),
        Some("7"),
        Some("skip_bad"),
        None,
        Some("skip_bad"),
        Some("selected_bad"),
        Some("outside"),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_from_iter([("s", input)])?.slice(1, 6);
    session.register_batch("prefix_input", batch.clone())?;
    let expression = spark_expression(&session, "CAST(s AS INT)").await?;
    let selected = BooleanArray::from(vec![false, true, false, true, false, true]);
    let prefix = expression.evaluate_selected_rows_until_error(&batch, &selected)?;
    prefix.validate(6)?;
    let failure = prefix.failure.expect("selected row failure");
    assert_eq!(failure.row_index, 5);
    assert_cast_cause(&failure.error, "selected_bad");
    let array = prefix.values.into_array(5)?;
    array.to_data().validate_full()?;
    assert_eq!(
        array.as_primitive::<Int32Type>(),
        &Int32Array::from(vec![None, Some(7), None, None, None]),
    );
    Ok(())
}

fn integer_args(value: ColumnarValue, data_type: &DataType, rows: usize) -> ScalarFunctionArgs {
    ScalarFunctionArgs {
        arg_fields: vec![Arc::new(Field::new("input", value.data_type(), true))],
        args: vec![value],
        number_rows: rows,
        return_field: Arc::new(Field::new("result", data_type.clone(), true)),
        config_options: Default::default(),
    }
}

#[test]
fn ansi_cast_prefixes_preserve_trim_width_layout_scalar_and_original_error_context() -> Result<()> {
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
    ] {
        let cast = ScalarUDF::from(SparkCastStringToInteger::try_new(
            data_type.clone(),
            StringIntegerCastMode::Ansi,
        )?)
        .with_aliases(["ansi_integer_prefix_alias"]);
        let values = vec![
            Some("outside"),
            Some("\u{7f} +7\0"),
            None,
            Some("bad"),
            Some("bad"),
        ];
        let layouts: [ArrayRef; 3] = [
            Arc::new(StringArray::from(values.clone())),
            Arc::new(LargeStringArray::from(values.clone())),
            Arc::new(StringViewArray::from(values)),
        ];
        for array in layouts {
            let value = ColumnarValue::Array(array.slice(1, 4));
            let ordinary = cast
                .invoke_with_args(integer_args(value.clone(), &data_type, 4))
                .expect_err("strict ordinary CAST fails");
            let prefix = cast.invoke_rows_until_error(integer_args(value, &data_type, 4))?;
            prefix.validate(4)?;
            let failure = prefix.failure.expect("strict prefix CAST fails");
            assert_eq!(failure.row_index, 2);
            assert_eq!(failure.error.to_string(), ordinary.to_string());
            let values = prefix.values.into_array(2)?;
            values.to_data().validate_full()?;
            assert_eq!(values.data_type(), &data_type);
            assert_eq!(
                ScalarValue::try_from_array(&values, 0)?.cast_to(&DataType::Int64)?,
                ScalarValue::Int64(Some(7))
            );
            assert!(values.is_null(1));
        }
        let overflow = match data_type {
            DataType::Int8 => "128",
            DataType::Int16 => "32768",
            DataType::Int32 => "2147483648",
            _ => "9223372036854775808",
        };
        let value = ColumnarValue::Array(Arc::new(StringArray::from(vec!["7", overflow])));
        let ordinary = cast
            .invoke_with_args(integer_args(value.clone(), &data_type, 2))
            .expect_err("ordinary integer overflow");
        let prefix = cast.invoke_rows_until_error(integer_args(value, &data_type, 2))?;
        prefix.validate(2)?;
        let failure = prefix.failure.expect("localized overflow");
        assert_eq!(failure.row_index, 1);
        assert_eq!(failure.error.to_string(), ordinary.to_string());
        for (input, failure) in [("7", false), ("bad", true)] {
            let prefix = cast.invoke_rows_until_error(integer_args(
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(input.into()))),
                &data_type,
                4,
            ))?;
            prefix.validate(4)?;
            if failure {
                assert_eq!(
                    prefix.failure.as_ref().map(|error| error.row_index),
                    Some(0)
                );
                assert!(matches!(prefix.values, ColumnarValue::Array(array) if array.is_empty()));
            } else {
                assert!(prefix.failure.is_none());
                assert!(matches!(prefix.values, ColumnarValue::Scalar(_)));
            }
        }
        let empty = cast.invoke_rows_until_error(integer_args(
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("bad".into()))),
            &data_type,
            0,
        ))?;
        empty.validate(0)?;
        assert!(empty.failure.is_none());
    }
    for mode in [StringIntegerCastMode::Legacy, StringIntegerCastMode::Try] {
        let cast = ScalarUDF::from(SparkCastStringToInteger::try_new(DataType::Int32, mode)?);
        let value = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("1.9"),
            Some("bad"),
            None,
        ])));
        let expected = cast.invoke_with_args(integer_args(value.clone(), &DataType::Int32, 3))?;
        let actual = cast.invoke_rows_until_error(integer_args(value, &DataType::Int32, 3))?;
        assert!(actual.failure.is_none());
        assert_eq!(
            actual.values.into_array(3)?.to_data(),
            expected.into_array(3)?.to_data()
        );
    }
    Ok(())
}

#[derive(Debug)]
struct CallbackFailure(Arc<()>);

impl Display for CallbackFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original callback failure")
    }
}

impl Error for CallbackFailure {}

fn has_original_source(error: &DataFusionError, source: &Arc<()>) -> bool {
    matches!(error.find_root(), DataFusionError::External(error)
        if error.downcast_ref::<CallbackFailure>()
            .is_some_and(|error| Arc::ptr_eq(&error.0, source)))
}

#[test]
fn opaque_callbacks_keep_their_batches_and_owned_errors_without_replay() -> Result<()> {
    static CALLS: Mutex<Vec<Vec<i32>>> = Mutex::new(Vec::new());

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct OpaqueCallback {
        source: Arc<()>,
        fail: bool,
        volatile: bool,
    }

    impl ScalarUDFImpl for OpaqueCallback {
        fn name(&self) -> &str {
            "opaque_prefix_callback"
        }
        fn signature(&self) -> &Signature {
            static IMMUTABLE: std::sync::LazyLock<Signature> =
                std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
            static VOLATILE: std::sync::LazyLock<Signature> =
                std::sync::LazyLock::new(|| Signature::any(1, Volatility::Volatile));
            if self.volatile { &VOLATILE } else { &IMMUTABLE }
        }
        fn return_type(&self, types: &[DataType]) -> Result<DataType> {
            Ok(types[0].clone())
        }
        fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
            Ok(Arc::clone(&args.arg_fields[0]))
        }
        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let keys = args.args[0].to_array(args.number_rows)?;
            CALLS
                .lock()
                .expect("callback trace")
                .push(keys.as_primitive::<Int32Type>().values().to_vec());
            if self.fail {
                return Err(DataFusionError::External(Box::new(CallbackFailure(
                    Arc::clone(&self.source),
                ))));
            }
            Ok(args.args[0].clone())
        }
    }

    let batch = arrow::record_batch::RecordBatch::try_from_iter([(
        "k",
        Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
    )])?;
    for volatile in [false, true] {
        for fail in [false, true] {
            let source = Arc::new(());
            let udf = Arc::new(
                ScalarUDF::from(OpaqueCallback {
                    source: Arc::clone(&source),
                    fail,
                    volatile,
                })
                .with_aliases(["opaque_prefix_alias"]),
            );
            let physical = ScalarFunctionExpr::try_new(
                udf,
                vec![Arc::new(Column::new("k", 0))],
                batch.schema_ref(),
                Default::default(),
            )?;
            for selected in [false, true] {
                let selection = BooleanArray::from(vec![true, false, true, false]);
                let output = if selected {
                    physical.evaluate_selected_rows_until_error(&batch, &selection)
                } else {
                    physical.evaluate_rows_until_error(&batch)
                };
                assert_eq!(
                    std::mem::take(&mut *CALLS.lock().expect("callback trace")),
                    vec![if selected {
                        vec![0, 2]
                    } else {
                        vec![0, 1, 2, 3]
                    }],
                );
                if fail {
                    assert!(has_original_source(
                        &output.expect_err("opaque failure"),
                        &source
                    ));
                } else {
                    let output = output?;
                    assert!(output.failure.is_none());
                    let values = output.values.into_array(4)?;
                    let expected = if selected {
                        vec![Some(0), None, Some(2), None]
                    } else {
                        vec![Some(0), Some(1), Some(2), Some(3)]
                    };
                    assert_eq!(
                        values.as_primitive::<Int32Type>(),
                        &Int32Array::from(expected)
                    );
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct OpaquePhysicalExpression(Arc<()>);

static PHYSICAL_CALLS: AtomicUsize = AtomicUsize::new(0);
static PHYSICAL_SELECTED_CALLS: AtomicUsize = AtomicUsize::new(0);

impl Display for OpaquePhysicalExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("opaque_expression")
    }
}

impl PhysicalExpr for OpaquePhysicalExpression {
    fn data_type(&self, _: &Schema) -> Result<DataType> {
        Ok(DataType::Int32)
    }
    fn nullable(&self, _: &Schema) -> Result<bool> {
        Ok(true)
    }
    fn evaluate(&self, _: &arrow::record_batch::RecordBatch) -> Result<ColumnarValue> {
        PHYSICAL_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(DataFusionError::External(Box::new(CallbackFailure(
            Arc::clone(&self.0),
        ))))
    }
    fn evaluate_selection(
        &self,
        _: &arrow::record_batch::RecordBatch,
        _: &BooleanArray,
    ) -> Result<ColumnarValue> {
        PHYSICAL_SELECTED_CALLS.fetch_add(1, Ordering::SeqCst);
        Err(DataFusionError::External(Box::new(CallbackFailure(
            Arc::clone(&self.0),
        ))))
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

#[test]
fn default_physical_prefix_method_moves_its_opaque_error_once() -> Result<()> {
    let source = Arc::new(());
    let expression = OpaquePhysicalExpression(Arc::clone(&source));
    let batch = arrow::record_batch::RecordBatch::try_from_iter([(
        "k",
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
    )])?;
    let output = expression.evaluate_rows_until_error(&batch);
    assert!(has_original_source(
        &output.expect_err("opaque error"),
        &source
    ));
    assert_eq!(PHYSICAL_CALLS.load(Ordering::SeqCst), 1);
    let selected = expression
        .evaluate_selected_rows_until_error(&batch, &BooleanArray::from(vec![false, true]));
    assert!(has_original_source(
        &selected.expect_err("custom selected error"),
        &source
    ));
    assert_eq!(PHYSICAL_SELECTED_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(PHYSICAL_CALLS.load(Ordering::SeqCst), 1);
    Ok(())
}

#[test]
fn malformed_udf_prefixes_are_rejected_at_the_invocation_boundary() -> Result<()> {
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MalformedPrefix(usize);

    impl ScalarUDFImpl for MalformedPrefix {
        fn name(&self) -> &str {
            "malformed_prefix"
        }
        fn signature(&self) -> &Signature {
            static SIGNATURE: std::sync::LazyLock<Signature> =
                std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
            &SIGNATURE
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(args.args[0].clone())
        }
        fn invoke_rows_until_error(&self, args: ScalarFunctionArgs) -> Result<EvaluatedRowPrefix> {
            let values = match self.0 {
                0 | 1 | 4 => ColumnarValue::Array(Arc::new(Int32Array::from(vec![7]))),
                2 => ColumnarValue::Scalar(ScalarValue::Int32(Some(7))),
                _ => ColumnarValue::Array(Arc::new(Int64Array::from(vec![7]))),
            };
            Ok(EvaluatedRowPrefix {
                values,
                failure: (self.0 != 4).then_some(RowEvaluationFailure {
                    row_index: match self.0 {
                        0 => args.number_rows,
                        2 => 0,
                        _ => 2,
                    },
                    error: DataFusionError::Execution("malformed source".into()),
                }),
            })
        }
    }

    let batch = arrow::record_batch::RecordBatch::try_from_iter([(
        "k",
        Arc::new(Int32Array::from(vec![0, 1, 2])) as ArrayRef,
    )])?;
    for malformed in [0, 1, 2, 4] {
        let physical = ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(MalformedPrefix(malformed))),
            vec![Arc::new(Column::new("k", 0))],
            batch.schema_ref(),
            Default::default(),
        )?;
        assert!(
            physical.evaluate_rows_until_error(&batch).is_err(),
            "{malformed}"
        );
    }
    #[cfg(debug_assertions)]
    {
        let udf = ScalarUDF::from(MalformedPrefix(3));
        let error = udf
            .invoke_rows_until_error(integer_args(
                ColumnarValue::Scalar(ScalarValue::Int32(Some(7))),
                &DataType::Int32,
                3,
            ))
            .expect_err("promised UDF return type must still be checked");
        assert!(error.to_string().contains("returned value of type"));
    }
    Ok(())
}
