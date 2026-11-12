use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::compute::{SortOptions, cast, interleave};
use arrow::datatypes::{DataType, FieldRef};
use datafusion::functions_window::lead_lag::{lag_udwf, lead_udwf};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Literal;
use datafusion_common::Result;
use datafusion_expr::function::{
    ExpressionArgs, PartitionEvaluatorArgs, WindowFunctionSimplification, WindowUDFFieldArgs,
};
use datafusion_expr::{
    Documentation, LimitEffect, PartitionEvaluator, ReversedUDWF, Signature, WindowUDF,
    WindowUDFImpl,
};

pub(super) fn spark_lag_udwf() -> Arc<WindowUDF> {
    spark_offset_window(lag_udwf())
}

pub(super) fn spark_lead_udwf() -> Arc<WindowUDF> {
    spark_offset_window(lead_udwf())
}

fn spark_offset_window(native: Arc<WindowUDF>) -> Arc<WindowUDF> {
    Arc::new(WindowUDF::from(SparkOffsetWindow { native }))
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkOffsetWindow {
    native: Arc<WindowUDF>,
}

#[warn(clippy::missing_trait_methods)]
impl WindowUDFImpl for SparkOffsetWindow {
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
        if let Some([payload, _, default]) = row_default_args(args.input_exprs()) {
            return vec![Arc::clone(payload), Arc::clone(default)];
        }
        self.native.expressions(args)
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let Some([_, offset, _]) = row_default_args(args.input_exprs()) else {
            return self.native.partition_evaluator_factory(args);
        };
        let Some(offset) = offset.downcast_ref::<Literal>().map(Literal::value) else {
            return datafusion_common::not_impl_err!(
                "There is only support Literal types for field at idx: 1 in Window Function"
            );
        };
        let offset: i64 = if offset.is_null() {
            0
        } else if offset.data_type().is_integer() {
            offset.cast_to(&DataType::Int64)?.try_into()?
        } else {
            return datafusion_common::exec_err!("Expected an integer value");
        };
        // The native evaluator uses positive shifts for earlier rows. Reversal flips that direction.
        let shift = if (self.native.name() == "lag") != args.is_reversed() {
            offset
        } else {
            offset.wrapping_neg()
        };
        let result = self
            .native
            .field(WindowUDFFieldArgs::new(args.input_fields(), self.name()))?;
        Ok(Box::new(RowDefaultEvaluator {
            shift,
            ignore_nulls: args.ignore_nulls(),
            data_type: result.data_type().clone(),
        }))
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
            ReversedUDWF::Reversed(native) => ReversedUDWF::Reversed(spark_offset_window(native)),
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

fn row_default_args(args: &[Arc<dyn PhysicalExpr>]) -> Option<[&Arc<dyn PhysicalExpr>; 3]> {
    let [payload, offset, default] = args else {
        return None;
    };
    default
        .downcast_ref::<Literal>()
        .is_none()
        .then_some([payload, offset, default])
}

#[derive(Debug)]
struct RowDefaultEvaluator {
    shift: i64,
    ignore_nulls: bool,
    data_type: DataType,
}

impl PartitionEvaluator for RowDefaultEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let [payload, default] = values else {
            return datafusion_common::internal_err!("Expected payload and default arrays");
        };
        // Interleave requires matching Arrow declarations, including nested child fields.
        let align_type = |value: &ArrayRef| -> Result<ArrayRef> {
            if value.data_type() == &self.data_type {
                Ok(Arc::clone(value))
            } else {
                Ok(cast(value.as_ref(), &self.data_type)?)
            }
        };
        let payload = align_type(payload)?;
        if self.shift == 0 {
            return Ok(payload);
        }
        let default = align_type(default)?;
        let distance = usize::try_from(self.shift.unsigned_abs()).unwrap_or(usize::MAX);
        if distance >= num_rows {
            return Ok(default);
        }

        // ponytail: DataFusion buffers a physical partition, then we index each window group.
        // Use bounded lookahead if dynamic defaults need streaming execution.
        let eligible = if self.ignore_nulls {
            payload
                .logical_nulls()
                .filter(|nulls| nulls.null_count() > 0)
                .map(|nulls| {
                    let rows = nulls.valid_indices().collect::<Vec<_>>();
                    (nulls, rows)
                })
        } else {
            None
        };
        let looking_back = self.shift > 0;
        let mut valid_before = 0_usize;
        let indices = (0..num_rows)
            .map(|row| {
                let target = if let Some((nulls, rows)) = &eligible {
                    let valid_through = valid_before + usize::from(nulls.is_valid(row));
                    let position = if looking_back {
                        valid_before.checked_sub(distance)
                    } else {
                        valid_through.checked_add(distance - 1)
                    };
                    valid_before = valid_through;
                    position.and_then(|index| rows.get(index).copied())
                } else if looking_back {
                    row.checked_sub(distance)
                } else {
                    row.checked_add(distance).filter(|&index| index < num_rows)
                };
                target.map_or((1, row), |index| (0, index))
            })
            .collect::<Vec<_>>();
        Ok(interleave(&[payload.as_ref(), default.as_ref()], &indices)?)
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
        let schema = Schema::new(vec![
            Field::new("payload", DataType::Int32, true),
            Field::new("default", DataType::Int32, false),
        ]);
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
            let row_default = [
                Arc::new(Column::new("payload", 0)) as Arc<dyn PhysicalExpr>,
                Arc::clone(&args[1]),
                Arc::new(Column::new("default", 1)),
            ];
            let fields = row_default
                .iter()
                .map(|argument| argument.return_field(&schema))
                .collect::<Result<Vec<_>>>()?;
            let native_result = native.partition_evaluator_factory(PartitionEvaluatorArgs::new(
                &row_default,
                &fields,
                false,
                false,
            ));
            assert!(native_result.is_err());
            let row_values = [
                Arc::clone(&values[0]),
                Arc::new(Int32Array::from(vec![-1, -2, -3, -4])) as ArrayRef,
            ];
            assert_eq!(
                spark
                    .expressions(ExpressionArgs::new(&row_default, &fields))
                    .len(),
                2
            );
            for reversed in [false, true] {
                let mut evaluator = spark.partition_evaluator_factory(
                    PartitionEvaluatorArgs::new(&row_default, &fields, reversed, false),
                )?;
                let expected: ArrayRef =
                    Arc::new(Int32Array::from(if (spark.name() == "lag") != reversed {
                        vec![Some(-1), Some(10), None, Some(30)]
                    } else {
                        vec![None, Some(30), Some(40), Some(-4)]
                    }));
                let actual = evaluator.evaluate_all(&row_values, 4)?;
                assert_eq!(actual.as_ref(), expected.as_ref());
            }
        }
        Ok(())
    }
}
