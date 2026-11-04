//! Typed list scalars retain their declared fields at conversion and cast boundaries.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, LargeListArray, ListArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, ExprSchemable, cast, lit};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use sail_common_datafusion::literal::LiteralEvaluator;

fn list_rows(
    large: bool,
    element: FieldRef,
    values: ArrayRef,
    lengths: &[usize],
    nulls: Option<NullBuffer>,
) -> Result<ArrayRef> {
    Ok(if large {
        Arc::new(LargeListArray::try_new(
            element,
            OffsetBuffer::from_lengths(lengths.iter().copied()),
            values,
            nulls,
        )?)
    } else {
        Arc::new(ListArray::try_new(
            element,
            OffsetBuffer::from_lengths(lengths.iter().copied()),
            values,
            nulls,
        )?)
    })
}

fn list_value(array: &dyn Array, row: usize) -> ArrayRef {
    match array.data_type() {
        DataType::List(_) => array.as_list::<i32>().value(row),
        DataType::LargeList(_) => array.as_list::<i64>().value(row),
        other => unreachable!("test fixture must be a list, got {other}"),
    }
}

#[test]
fn typed_list_scalar_conversion_and_casts_preserve_complete_fields() -> Result<()> {
    for large in [false, true] {
        for nullable in [false, true] {
            let primitive: ArrayRef = Arc::new(Int32Array::from(if nullable {
                vec![Some(1), None, Some(3)]
            } else {
                vec![Some(1), Some(2), Some(3)]
            }));
            let inner_element = Arc::new(
                Field::new("nested_value", DataType::Int32, nullable)
                    .with_metadata([("source".into(), "nested-literal-test".into())].into()),
            );
            let nested = list_rows(
                !large,
                inner_element,
                primitive.clone(),
                &[2, 0, 1],
                nullable.then(|| NullBuffer::from(vec![true, false, true])),
            )?;
            let mut children = vec![primitive.clone(), nested];
            if nullable {
                children.push(Arc::new(StructArray::try_new(
                    vec![
                        Field::new("required_value", DataType::Int32, false)
                            .with_metadata([("source".into(), "struct-child-test".into())].into()),
                    ]
                    .into(),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                    None,
                )?));
            }
            for values in children {
                let element = Arc::new(
                    Field::new("kept_element", values.data_type().clone(), nullable).with_metadata(
                        [("source".into(), "constant-coordinate-test".into())].into(),
                    ),
                );
                let declared = if large {
                    DataType::LargeList(element.clone())
                } else {
                    DataType::List(element.clone())
                };
                let input = list_rows(
                    large,
                    element.clone(),
                    values,
                    &[2, 0, 1],
                    Some(NullBuffer::from(vec![true, true, false])),
                )?;
                input.to_data().validate_full()?;
                assert_eq!(input.data_type(), &declared);
                for row in 0..3 {
                    let single = input.slice(row, 1);
                    let direct = if large {
                        ScalarValue::LargeList(Arc::new(single.as_list::<i64>().clone()))
                    } else {
                        ScalarValue::List(Arc::new(single.as_list::<i32>().clone()))
                    };
                    assert_eq!(direct.data_type(), declared);
                    let converted = ScalarValue::try_from_array(input.as_ref(), row)?;
                    let casted = direct.cast_to(&declared)?;
                    let target_element = Arc::new(
                        element
                            .as_ref()
                            .clone()
                            .with_name("cast_element")
                            .with_metadata(
                                [("source".into(), "literal-cast-target-test".into())].into(),
                            ),
                    );
                    let target = if large {
                        DataType::List(target_element)
                    } else {
                        DataType::LargeList(target_element)
                    };
                    let changed = direct.cast_to(&target)?;
                    for (label, scalar, expected_type) in [
                        ("direct", direct, &declared),
                        ("converted", converted, &declared),
                        ("same_type_cast", casted, &declared),
                        ("target_field_cast", changed, &target),
                    ] {
                        assert_eq!(scalar.data_type(), *expected_type, "{label} row={row}");
                        assert_eq!(scalar.is_null(), row == 2, "{label} row={row}");
                        let result = scalar.to_array()?;
                        result.to_data().validate_full()?;
                        assert_eq!(result.len(), 1);
                        assert_eq!(result.is_null(0), row == 2);
                        if row != 2 {
                            assert_eq!(
                                list_value(result.as_ref(), 0).as_ref(),
                                list_value(input.as_ref(), row).as_ref(),
                                "{label} row={row}"
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn typed_list_literal_cast_folding_keeps_fields_and_native_rejections() -> Result<()> {
    let schema = Arc::new(DFSchema::empty());
    let info = SimplifyContext::builder()
        .with_schema(schema.clone())
        .build();
    let simplifier = ExprSimplifier::new(info);
    for large in [false, true] {
        let element = Arc::new(
            Field::new("kept_element", DataType::Int32, true)
                .with_metadata([("source".into(), "literal-cast-test".into())].into()),
        );
        let input = list_rows(
            large,
            element.clone(),
            Arc::new(Int32Array::from(vec![Some(1), None])),
            &[2],
            None,
        )?;
        let literal = if large {
            ScalarValue::LargeList(Arc::new(input.as_list::<i64>().clone()))
        } else {
            ScalarValue::List(Arc::new(input.as_list::<i32>().clone()))
        };
        let target = if large {
            DataType::List(element.clone())
        } else {
            DataType::LargeList(element.clone())
        };
        let expression = cast(lit(literal.clone()), target.clone());
        let coerced = simplifier.coerce(expression, schema.as_ref())?;
        assert_eq!(coerced.get_type(schema.as_ref())?, target);
        let evaluated = LiteralEvaluator::new().evaluate(&coerced)?;
        let prepared = simplifier.simplify(coerced)?;
        let Expr::Literal(folded, _) = prepared else {
            return datafusion::common::internal_err!("typed list cast did not fold");
        };
        for value in [evaluated, folded] {
            assert_eq!(value.data_type(), target);
            assert!(!value.is_null());
            let result = value.to_array()?;
            result.to_data().validate_full()?;
            assert_eq!(
                list_value(result.as_ref(), 0).as_ref(),
                &Int32Array::from(vec![Some(1), None])
            );
        }
        let required_element = Arc::new(element.as_ref().clone().with_nullable(false));
        let required_type = if large {
            DataType::LargeList(required_element)
        } else {
            DataType::List(required_element)
        };
        assert!(literal.cast_to(&required_type).is_err());
        let incompatible_element = Arc::new(Field::new(
            "incompatible",
            DataType::Struct(vec![Field::new("value", DataType::Int32, true)].into()),
            true,
        ));
        let incompatible_type = if large {
            DataType::LargeList(incompatible_element)
        } else {
            DataType::List(incompatible_element)
        };
        assert!(literal.cast_to(&incompatible_type).is_err());
        assert_eq!(literal.data_type(), *input.data_type());
        input.to_data().validate_full()?;
    }
    Ok(())
}
