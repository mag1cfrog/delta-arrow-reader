use std::sync::Arc;

use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::Result;
use datafusion_expr::function::{
    ExpressionArgs, PartitionEvaluatorArgs, WindowFunctionSimplification, WindowUDFFieldArgs,
};
use datafusion_expr::{
    Documentation, LimitEffect, PartitionEvaluator, ReversedUDWF, Signature, WindowUDF,
    WindowUDFImpl,
};

pub(super) fn spark_lag_udwf() -> Arc<WindowUDF> {
    with_spark_offset_fields(lag_udwf())
}

pub(super) fn spark_lead_udwf() -> Arc<WindowUDF> {
    with_spark_offset_fields(lead_udwf())
}

fn with_spark_offset_fields(native: Arc<WindowUDF>) -> Arc<WindowUDF> {
    Arc::new(WindowUDF::from(SparkOffsetWindowFieldAdapter { native }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkOffsetWindowFieldAdapter {
    native: Arc<WindowUDF>,
}

#[warn(clippy::missing_trait_methods)]
impl WindowUDFImpl for SparkOffsetWindowFieldAdapter {
    fn name(&self) -> &str {
        self.native.name()
    }

    fn aliases(&self) -> &[String] {
        self.native.aliases()
    }

    fn signature(&self) -> &Signature {
        self.native.signature()
    }

    fn expressions(&self, args: ExpressionArgs) -> Vec<Arc<dyn PhysicalExpr>> {
        self.native.expressions(args)
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        self.native.partition_evaluator_factory(args)
    }

    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        self.native.simplify()
    }

    fn field(&self, args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let uncoerced_scalar_default = args
            .input_fields()
            .first()
            .zip(args.input_fields().get(2))
            .is_some_and(|(payload, default)| {
                let payload = payload.data_type();
                let default = default.data_type();
                !payload.is_null()
                    && !default.is_null()
                    && !payload.is_nested()
                    && !default.is_nested()
                    && !payload.equals_datatype(default)
            });
        // Spark requires both the payload and an explicit default to be required.
        let nullable = args
            .input_fields()
            .first()
            .is_none_or(|field| field.is_nullable())
            || args
                .input_fields()
                .get(2)
                .is_none_or(|field| field.is_nullable());
        let field = self.native.field(args)?;
        if uncoerced_scalar_default {
            return Ok(field);
        }
        Ok(Arc::new(field.as_ref().clone().with_nullable(nullable)))
    }

    fn sort_options(&self) -> Option<SortOptions> {
        self.native.sort_options()
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.native.coerce_types(arg_types)
    }

    fn reverse_expr(&self) -> ReversedUDWF {
        match self.native.reverse_expr() {
            ReversedUDWF::Reversed(native) => {
                ReversedUDWF::Reversed(with_spark_offset_fields(native))
            }
            other => other,
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.native.documentation()
    }

    fn limit_effect(&self, args: &[Arc<dyn PhysicalExpr>]) -> LimitEffect {
        self.native.inner().limit_effect(args)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{Field, Schema};
    use datafusion::physical_expr::expressions::{Column, Literal};
    use datafusion_common::ScalarValue;

    use super::*;

    #[test]
    fn offset_window_fields_keep_nested_declarations_and_native_contracts() -> Result<()> {
        let child = Field::new("child", DataType::Int32, true)
            .with_metadata(HashMap::from([("child_source".into(), "retained".into())]));
        let data_type = DataType::List(Arc::new(Field::new(
            "element",
            DataType::Struct(vec![child].into()),
            false,
        )));
        for (spark, native) in [
            (spark_lag_udwf(), lag_udwf()),
            (spark_lead_udwf(), lead_udwf()),
        ] {
            for (payload_nullable, default_nullable, result_nullable) in [
                (false, false, false),
                (false, true, true),
                (true, false, true),
                (true, true, true),
            ] {
                let fields = [
                    Arc::new(
                        Field::new("payload", data_type.clone(), payload_nullable).with_metadata(
                            HashMap::from([("payload_source".into(), "kept".into())]),
                        ),
                    ),
                    Arc::new(Field::new("offset", DataType::Int64, false)),
                    Arc::new(
                        Field::new("default", data_type.clone(), default_nullable).with_metadata(
                            HashMap::from([("default_source".into(), "kept".into())]),
                        ),
                    ),
                ];
                let native_field = native.field(WindowUDFFieldArgs::new(&fields, "result"))?;
                assert!(native_field.is_nullable());
                let expected = native_field.as_ref().clone().with_nullable(result_nullable);
                assert_eq!(
                    spark
                        .field(WindowUDFFieldArgs::new(&fields, "result"))?
                        .as_ref(),
                    &expected
                );
                for count in [1, 2] {
                    assert_eq!(
                        spark.field(WindowUDFFieldArgs::new(&fields[..count], "result"))?,
                        native.field(WindowUDFFieldArgs::new(&fields[..count], "result"))?
                    );
                }
                let mut null_payload = fields.clone();
                null_payload[0] = Arc::new(Field::new("payload", DataType::Null, true));
                assert_eq!(
                    spark.field(WindowUDFFieldArgs::new(&null_payload, "result"))?,
                    native.field(WindowUDFFieldArgs::new(&null_payload, "result"))?
                );
                for function in [&spark, &native] {
                    let ReversedUDWF::Reversed(reversed) = function.reverse_expr() else {
                        return datafusion_common::internal_err!(
                            "offset window must retain native reversal"
                        );
                    };
                    assert_eq!(
                        reversed.field(WindowUDFFieldArgs::new(&fields, "result"))?,
                        function.field(WindowUDFFieldArgs::new(&fields, "result"))?
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn uncoerced_scalar_defaults_keep_native_fields() -> Result<()> {
        let fields = [
            Arc::new(
                Field::new("payload", DataType::Int32, false)
                    .with_metadata(HashMap::from([("source".into(), "payload".into())])),
            ),
            Arc::new(Field::new("offset", DataType::Int64, false)),
            Arc::new(
                Field::new("default", DataType::Utf8, false)
                    .with_metadata(HashMap::from([("source".into(), "default".into())])),
            ),
        ];
        for (spark, native) in [
            (spark_lag_udwf(), lag_udwf()),
            (spark_lead_udwf(), lead_udwf()),
        ] {
            let expected = native.field(WindowUDFFieldArgs::new(&fields, "shifted"))?;
            assert!(expected.is_nullable());
            assert_eq!(
                spark
                    .field(WindowUDFFieldArgs::new(&fields, "shifted"))?
                    .as_ref(),
                expected.as_ref()
            );
        }
        Ok(())
    }

    #[test]
    fn offset_window_keeps_native_refinement_evaluation_and_limits() -> Result<()> {
        let schema = Schema::new(vec![Field::new("payload", DataType::Int32, true)]);
        let values: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![
            Some(10),
            None,
            Some(30),
            Some(40),
        ]))];
        for (spark, native) in [
            (spark_lag_udwf(), lag_udwf()),
            (spark_lead_udwf(), lead_udwf()),
        ] {
            for offset in [-1, 0, 1] {
                let args: Vec<Arc<dyn PhysicalExpr>> = vec![
                    Arc::new(Column::new("payload", 0)),
                    Arc::new(Literal::new(ScalarValue::Int64(Some(offset)))),
                    Arc::new(Literal::new(ScalarValue::Int32(Some(-99)))),
                ];
                let fields = args
                    .iter()
                    .map(|argument| argument.return_field(&schema))
                    .collect::<Result<Vec<_>>>()?;
                match (
                    spark.inner().limit_effect(&args),
                    native.inner().limit_effect(&args),
                ) {
                    (LimitEffect::None, LimitEffect::None)
                    | (LimitEffect::Unknown, LimitEffect::Unknown) => {}
                    (LimitEffect::Relative(left), LimitEffect::Relative(right))
                    | (LimitEffect::Absolute(left), LimitEffect::Absolute(right)) => {
                        assert_eq!(left, right);
                    }
                    _ => {
                        return datafusion_common::internal_err!(
                            "offset field adaptation changed native limit requirements"
                        );
                    }
                }
                for reversed in [false, true] {
                    for ignore_nulls in [false, true] {
                        let mut actual = spark.partition_evaluator_factory(
                            PartitionEvaluatorArgs::new(&args, &fields, reversed, ignore_nulls),
                        )?;
                        let mut expected = native.partition_evaluator_factory(
                            PartitionEvaluatorArgs::new(&args, &fields, reversed, ignore_nulls),
                        )?;
                        assert!(actual.supports_bounded_execution());
                        assert_eq!(actual.is_causal(), expected.is_causal());
                        assert_eq!(actual.uses_window_frame(), expected.uses_window_frame());
                        assert_eq!(actual.include_rank(), expected.include_rank());
                        for row in 0..4 {
                            let range = actual.get_range(row, 4)?;
                            assert_eq!(range, expected.get_range(row, 4)?);
                            assert_eq!(
                                actual.evaluate(&values, &range)?,
                                expected.evaluate(&values, &range)?
                            );
                        }
                        let actual_values = actual.evaluate_all(&values, 4)?;
                        let expected_values = expected.evaluate_all(&values, 4)?;
                        assert_eq!(actual_values.as_ref(), expected_values.as_ref());
                    }
                }
            }
            let args: Vec<Arc<dyn PhysicalExpr>> = vec![
                Arc::new(Literal::new(ScalarValue::Null)),
                Arc::new(Literal::new(ScalarValue::Int64(Some(1)))),
                Arc::new(Literal::new(ScalarValue::Int32(Some(-99)))),
            ];
            let fields = args
                .iter()
                .map(|argument| argument.return_field(&schema))
                .collect::<Result<Vec<_>>>()?;
            let actual = spark.expressions(ExpressionArgs::new(&args, &fields));
            let expected = native.expressions(ExpressionArgs::new(&args, &fields));
            assert_eq!(actual.len(), 1);
            assert_eq!(actual.len(), expected.len());
            assert_eq!(actual[0].data_type(&schema)?, DataType::Int32);
            assert_eq!(
                actual[0].return_field(&schema)?,
                expected[0].return_field(&schema)?
            );
            let mut invalid_default = args.clone();
            invalid_default[2] = Arc::new(Column::new("payload", 0));
            let fields = invalid_default
                .iter()
                .map(|argument| argument.return_field(&schema))
                .collect::<Result<Vec<_>>>()?;
            let actual = spark.partition_evaluator_factory(PartitionEvaluatorArgs::new(
                &invalid_default,
                &fields,
                false,
                false,
            ));
            let expected = native.partition_evaluator_factory(PartitionEvaluatorArgs::new(
                &invalid_default,
                &fields,
                false,
                false,
            ));
            assert!(actual.is_err());
            assert!(expected.is_err());
            assert_eq!(
                actual.err().map(|error| error.to_string()),
                expected.err().map(|error| error.to_string())
            );
        }
        Ok(())
    }
}
