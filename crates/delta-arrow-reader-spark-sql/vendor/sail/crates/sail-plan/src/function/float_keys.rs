//! Grouping and DISTINCT use the same zero and NaN equality inside complex keys.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, FixedSizeListArray, LargeListArray, ListArray, StructArray,
};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::config::ConfigOptions;
use datafusion_common::utils::{adjust_offsets_for_slice, list_values};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

use crate::function::common::{
    SparkComparisonFloat, literal_value_without_evaluation, without_metadata_annotations,
};
use crate::function::result_coercion::is_constructor_declaration_cast;

// Spark NormalizeFloatingNumbers distributes through constructors and conditionals.
// Constructor casts that only align declarations keep that behavior. Other ARRAY
// values require a nonfoldable ArrayTransform before comparison.
pub(crate) fn normalize_spark_float_array_comparison(
    expression: Expr,
    schema: &DFSchema,
) -> Result<Expr> {
    let expression = without_metadata_annotations(&expression).clone();
    let data_type = expression.get_type(schema)?;
    if !contains_float(&data_type) {
        return Ok(expression);
    }
    // Spark's NULL STRUCT branch discards its field transforms; NULL ARRAY keeps ArrayTransform.
    if matches!(&data_type, DataType::Struct(_))
        && let Some(value) = literal_value_without_evaluation(&expression)
        && value.is_null()
    {
        return Ok(datafusion_expr::lit(value));
    }
    Ok(match expression {
        Expr::Alias(mut alias) => {
            alias.expr = Box::new(normalize_spark_float_array_comparison(*alias.expr, schema)?);
            Expr::Alias(alias)
        }
        Expr::ScalarFunction(mut function)
            if function
                .func
                .inner()
                .is::<sail_function::scalar::array::spark_array::SparkArray>()
                || function
                    .func
                    .inner()
                    .is::<sail_function::scalar::struct_function::StructFunction>()
                || function
                    .func
                    .inner()
                    .is::<datafusion::functions::core::coalesce::CoalesceFunc>()
                || is_constructor_declaration_cast(&function) =>
        {
            function.args = function
                .args
                .into_iter()
                .map(|arg| normalize_spark_float_array_comparison(arg, schema))
                .collect::<Result<_>>()?;
            Expr::ScalarFunction(function)
        }
        Expr::Case(mut case) => {
            for (_, value) in &mut case.when_then_expr {
                **value = normalize_spark_float_array_comparison(*value.clone(), schema)?;
            }
            if let Some(value) = &mut case.else_expr {
                **value = normalize_spark_float_array_comparison(*value.clone(), schema)?;
            }
            Expr::Case(case)
        }
        expression if float_normalization_uses_array_transform(&data_type) => {
            ScalarUDF::from(NormalizeNestedFloatKeys { foldable: false }).call(vec![expression])
        }
        expression => normalize_spark_float_key(expression, schema)?,
    })
}

fn float_normalization_uses_array_transform(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => contains_float(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| float_normalization_uses_array_transform(field.data_type())),
        _ => false,
    }
}

pub(crate) fn normalize_spark_float_key(expression: Expr, schema: &DFSchema) -> Result<Expr> {
    let data_type = expression.get_type(schema)?;
    if matches!(data_type, DataType::Float32 | DataType::Float64) {
        Ok(ScalarUDF::from(SparkComparisonFloat { widen: false }).call(vec![expression]))
    } else if contains_float(&data_type) {
        Ok(ScalarUDF::from(NormalizeNestedFloatKeys::default()).call(vec![expression]))
    } else {
        Ok(expression)
    }
}

fn contains_float(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float32 | DataType::Float64 => true,
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => contains_float(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_float(field.data_type())),
        _ => false,
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct NormalizeNestedFloatKeys {
    foldable: bool,
}

impl Default for NormalizeNestedFloatKeys {
    fn default() -> Self {
        Self { foldable: true }
    }
}

impl ScalarUDFImpl for NormalizeNestedFloatKeys {
    fn name(&self) -> &str {
        "spark_normalize_nested_float_keys"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn supports_constant_folding(&self) -> bool {
        self.foldable
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [data_type] = args else {
            return datafusion_common::plan_err!("expected one grouping key");
        };
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(
            args.arg_fields[0].as_ref().clone().with_name(self.name()),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = datafusion_common::utils::take_function_args(self.name(), args.args)?;
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let normalized = normalize_nested_float_keys(value.to_array(1)?, &args.config_options)?;
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&normalized, 0)?)
        } else {
            ColumnarValue::Array(normalized)
        })
    }
}

pub(crate) fn normalize_nested_float_keys(
    array: ArrayRef,
    config_options: &Arc<ConfigOptions>,
) -> Result<ArrayRef> {
    if !contains_float(array.data_type()) {
        return Ok(array);
    }
    Ok(match array.data_type() {
        DataType::Float32 | DataType::Float64 => {
            let rows = array.len();
            let field = Arc::new(Field::new("value", array.data_type().clone(), true));
            SparkComparisonFloat { widen: false }
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(array)],
                    arg_fields: vec![field.clone()],
                    number_rows: rows,
                    return_field: field,
                    config_options: config_options.clone(),
                })?
                .into_array(rows)?
        }
        DataType::Struct(fields) => Arc::new(StructArray::try_new(
            fields.clone(),
            array
                .as_struct()
                .columns()
                .iter()
                .map(|column| normalize_nested_float_keys(column.clone(), config_options))
                .collect::<Result<_>>()?,
            array.nulls().cloned(),
        )?),
        DataType::List(field) => Arc::new(ListArray::try_new(
            field.clone(),
            adjust_offsets_for_slice(array.as_list::<i32>()),
            normalize_nested_float_keys(list_values(array.as_ref())?, config_options)?,
            array.nulls().cloned(),
        )?),
        DataType::LargeList(field) => Arc::new(LargeListArray::try_new(
            field.clone(),
            adjust_offsets_for_slice(array.as_list::<i64>()),
            normalize_nested_float_keys(list_values(array.as_ref())?, config_options)?,
            array.nulls().cloned(),
        )?),
        DataType::FixedSizeList(field, size) => Arc::new(FixedSizeListArray::try_new_with_length(
            field.clone(),
            *size,
            normalize_nested_float_keys(list_values(array.as_ref())?, config_options)?,
            array.nulls().cloned(),
            array.len(),
        )?),
        DataType::ListView(field) | DataType::LargeListView(field) => {
            let packed_type = if matches!(array.data_type(), DataType::ListView(_)) {
                DataType::List(field.clone())
            } else {
                DataType::LargeList(field.clone())
            };
            // Pack only the visible view rows before normalizing their children.
            let packed = arrow::compute::cast(array.as_ref(), &packed_type)?;
            let normalized = normalize_nested_float_keys(packed, config_options)?;
            arrow::compute::cast(normalized.as_ref(), array.data_type())?
        }
        _ => array,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, LargeListViewArray, ListViewArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};

    #[test]
    fn nested_keys_preserve_fields_nulls_and_visible_slices() -> Result<()> {
        let input_values = Float64Array::from(vec![
            Some(-0.0),
            Some(f64::from_bits(0xfff8000000004321)),
            None,
            Some(1.5),
        ]);
        let expected_values = Float64Array::from(vec![Some(0.0), Some(f64::NAN), None, Some(1.5)]);
        for value_type in [DataType::Float32, DataType::Float64] {
            let make_values = |floats: &Float64Array| -> Result<ArrayRef> {
                let floats = arrow::compute::cast(floats, &value_type)?;
                let repeated = arrow::compute::concat(&vec![floats.as_ref(); 1024])?;
                Ok(Arc::new(StructArray::try_new(
                    vec![
                        Field::new("value", value_type.clone(), true)
                            .with_metadata([("origin".into(), "fixture".into())].into()),
                    ]
                    .into(),
                    vec![repeated],
                    Some(NullBuffer::from(
                        (0..4096).map(|i| i % 4 != 3).collect::<Vec<_>>(),
                    )),
                )?))
            };
            let source = make_values(&input_values)?;
            let expected = make_values(&expected_values)?;
            let field = Arc::new(Field::new("item", source.data_type().clone(), true));
            let mut cases = Vec::new();
            for data_type in [
                DataType::List(field.clone()),
                DataType::LargeList(field.clone()),
                DataType::FixedSizeList(field.clone(), 2),
            ] {
                let make_list = |values: ArrayRef| -> Result<ArrayRef> {
                    let list = ListArray::try_new(
                        field.clone(),
                        OffsetBuffer::from_repeated_length(2, 2048),
                        values,
                        Some(NullBuffer::from(
                            (0..2048).map(|i| i % 3 != 0).collect::<Vec<_>>(),
                        )),
                    )?;
                    Ok(arrow::compute::cast(&list, &data_type)?)
                };
                cases.push((make_list(source.clone())?, make_list(expected.clone())?));
            }
            for nulls in [None, Some(NullBuffer::from(vec![true, false, true]))] {
                cases.push((
                    Arc::new(ListViewArray::try_new(
                        field.clone(),
                        vec![4000, 2, 4000].into(),
                        vec![2, 1, 2].into(),
                        source.clone(),
                        nulls.clone(),
                    )?) as ArrayRef,
                    Arc::new(ListViewArray::try_new(
                        field.clone(),
                        vec![4000, 2, 4000].into(),
                        vec![2, 1, 2].into(),
                        expected.clone(),
                        nulls.clone(),
                    )?) as ArrayRef,
                ));
                cases.push((
                    Arc::new(LargeListViewArray::try_new(
                        field.clone(),
                        vec![4000, 2, 4000].into(),
                        vec![2, 1, 2].into(),
                        source.clone(),
                        nulls.clone(),
                    )?) as ArrayRef,
                    Arc::new(LargeListViewArray::try_new(
                        field.clone(),
                        vec![4000, 2, 4000].into(),
                        vec![2, 1, 2].into(),
                        expected.clone(),
                        nulls.clone(),
                    )?) as ArrayRef,
                ));
            }
            for (input, expected) in cases {
                let start = if input.len() > 1000 { 1001 } else { 1 };
                for len in [0, 1, 2] {
                    let input = input.slice(start, len);
                    let expected = expected.slice(start, len);
                    let labels: ArrayRef = Arc::new(Int32Array::from_iter_values(0..len as i32));
                    let fields = vec![
                        Field::new("key", input.data_type().clone(), true),
                        Field::new("label", DataType::Int32, false),
                    ]
                    .into();
                    let row = Arc::new(StructArray::try_new(
                        fields,
                        vec![input.clone(), labels.clone()],
                        None,
                    )?);
                    let field = Arc::new(
                        Field::new("row", row.data_type().clone(), false)
                            .with_metadata([("origin".into(), "provider".into())].into()),
                    );
                    let returned = NormalizeNestedFloatKeys::default().return_field_from_args(
                        ReturnFieldArgs {
                            arg_fields: std::slice::from_ref(&field),
                            scalar_arguments: &[None],
                        },
                    )?;
                    assert_eq!(
                        returned.as_ref(),
                        &field
                            .as_ref()
                            .clone()
                            .with_name("spark_normalize_nested_float_keys")
                    );
                    let args = ScalarFunctionArgs {
                        args: vec![ColumnarValue::Array(row)],
                        arg_fields: vec![field.clone()],
                        number_rows: len,
                        return_field: field,
                        config_options: Arc::default(),
                    };
                    let result = NormalizeNestedFloatKeys::default()
                        .invoke_with_args(args)?
                        .into_array(len)?;
                    assert_eq!(result.as_struct().column(0).to_data(), expected.to_data());
                    assert!(Arc::ptr_eq(result.as_struct().column(1), &labels));
                    let key = result.as_struct().column(0);
                    let packed = arrow::compute::cast(
                        key.as_ref(),
                        &DataType::List(list_element_field(key.data_type())?),
                    )?;
                    assert!(packed.as_list::<i32>().values().len() <= len * 2);
                    if len > 0 {
                        let value = ScalarValue::try_from_array(&input, 0)?;
                        let field = Arc::new(Field::new("key", input.data_type().clone(), true));
                        let scalar = NormalizeNestedFloatKeys::default().invoke_with_args(
                            ScalarFunctionArgs {
                                args: vec![ColumnarValue::Scalar(value)],
                                arg_fields: vec![field.clone()],
                                number_rows: 1,
                                return_field: field,
                                config_options: Arc::default(),
                            },
                        )?;
                        assert_eq!(
                            scalar.into_array(1)?.to_data(),
                            expected.slice(0, 1).to_data()
                        );
                    }
                }
            }
        }
        let empty = Arc::new(FixedSizeListArray::try_new_with_length(
            Arc::new(Field::new("item", DataType::Float64, true)),
            0,
            Arc::new(Float64Array::from(Vec::<f64>::new())),
            None,
            3,
        )?);
        assert_eq!(
            normalize_nested_float_keys(empty.clone(), &Arc::default())?.to_data(),
            empty.to_data()
        );
        Ok(())
    }

    fn list_element_field(data_type: &DataType) -> Result<FieldRef> {
        match data_type {
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::ListView(field)
            | DataType::LargeListView(field) => Ok(field.clone()),
            _ => datafusion_common::internal_err!("expected list"),
        }
    }
}
