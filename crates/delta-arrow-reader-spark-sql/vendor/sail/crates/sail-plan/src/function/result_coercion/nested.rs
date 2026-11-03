// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Lift the existing scalar result policy through Spark ARRAY and STRUCT values.

use std::sync::{Arc, LazyLock};

use arrow::array::{Array, ArrayRef, AsArray, LargeListArray, ListArray, StructArray, make_array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::utils::{
    adjust_offsets_for_slice, list_values, remove_list_null_values, take_function_args,
};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::type_coercion::binary::type_union_resolution;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility, col, expr,
};
use sail_common::spec::SetOpType;
use sail_function::scalar::spark_cast_numeric_to_decimal::decimal_cast_can_overflow;

use super::{
    cast_result_value, common_numeric_type, common_result_type_with_strings,
    common_temporal_type, is_string_type, is_temporal_type,
};
use crate::error::{PlanError, PlanResult};
use crate::function::common::{FunctionContextInput, drop_inherited_column_metadata};

fn list_field(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => Some(field),
        _ => None,
    }
}

pub(crate) fn is_nested_result_type(data_type: &DataType) -> bool {
    list_field(data_type).is_some() || matches!(data_type, DataType::Struct(_))
}

#[derive(Clone, Copy, PartialEq, Eq)]
// Positional rename is reserved for UNION inputs whose leaf types already match.
enum StructFieldNames {
    RequireMatching,
    AllowPositionalRename,
}

fn has_string_type(data_type: &DataType) -> bool {
    is_string_type(data_type)
        || list_field(data_type).is_some_and(|field| has_string_type(field.data_type()))
}

fn cast_adds_nulls(source: &DataType, target: &DataType) -> bool {
    if source == &DataType::Null || source == target || is_string_type(target) {
        false
    } else if is_string_type(source) {
        target != &DataType::Binary
    } else if let DataType::Decimal128(precision, scale) = target {
        decimal_cast_can_overflow(source, *precision, *scale)
    } else if matches!((source, target),
        (DataType::Date32 | DataType::Date64, DataType::Timestamp(_, None)))
    {
        true
    } else {
        matches!(source, DataType::Float32 | DataType::Float64) && target.is_signed_integer()
    }
}

// Adapted structural policy: Spark TypeCoercionHelper.findTypeForComplex and
// Cast.forceNullable at revision 32f7299601108917fb01920a54e084595b7b3bf8.
fn common_type(
    left: &DataType,
    right: &DataType,
    context: &FunctionContextInput<'_>,
    struct_field_names: StructFieldNames,
) -> PlanResult<Option<DataType>> {
    if left == right || right == &DataType::Null {
        return Ok(Some(left.clone()));
    }
    if left == &DataType::Null {
        return Ok(Some(right.clone()));
    }
    if let (Some(left_field), Some(right_field)) = (list_field(left), list_field(right)) {
        let Some(target) = common_type(left_field.data_type(), right_field.data_type(), context, struct_field_names)?
        else {
            return Ok(None);
        };
        let nullable = left_field.is_nullable()
            || right_field.is_nullable()
            || cast_adds_nulls(left_field.data_type(), &target)
            || cast_adds_nulls(right_field.data_type(), &target);
        let field = Arc::new(Field::new_list_field(target, nullable));
        return Ok(Some(if matches!(left, DataType::LargeList(_) | DataType::LargeListView(_))
            || matches!(right, DataType::LargeList(_) | DataType::LargeListView(_))
        {
            DataType::LargeList(field)
        } else {
            DataType::List(field)
        }));
    }
    if let (DataType::Struct(left), DataType::Struct(right)) = (left, right) {
        if left.len() != right.len() {
            return Ok(None);
        }
        let mut fields = Vec::with_capacity(left.len());
        for (left, right) in left.iter().zip(right) {
            if struct_field_names == StructFieldNames::RequireMatching
                && !left.name().eq_ignore_ascii_case(right.name()) {
                return Ok(None);
            }
            let Some(target) = common_type(left.data_type(), right.data_type(), context, struct_field_names)? else {
                return Ok(None);
            };
            let nullable = left.is_nullable()
                || right.is_nullable()
                || cast_adds_nulls(left.data_type(), &target)
                || cast_adds_nulls(right.data_type(), &target);
            fields.push(Field::new(left.name(), target, nullable));
        }
        return Ok(Some(DataType::Struct(fields.into())));
    }
    if is_nested_result_type(left) || is_nested_result_type(right) {
        return Ok(None);
    }
    let types = [left.clone(), right.clone()];
    if let Some(target) = common_numeric_type(&types, context.plan_config.ansi_mode) {
        return Ok(Some(target));
    }
    if let Some(target) =
        common_result_type_with_strings(&types, context.plan_config.ansi_mode)?
    {
        return Ok(Some(target));
    }
    if types.iter().all(|data_type| is_temporal_type(data_type) || is_string_type(data_type)) {
        return Ok(common_temporal_type(&types, &context.plan_config.session_timezone));
    }
    Ok(type_union_resolution(&types))
}

// Declaration-only differences must remain available to CASE's physical refinement.
fn same_value_type(source: &DataType, target: &DataType) -> bool {
    if let (Some(source), Some(target)) = (list_field(source), list_field(target)) {
        return same_value_type(source.data_type(), target.data_type());
    }
    if let (DataType::Struct(source), DataType::Struct(target)) = (source, target) {
        return source.len() == target.len()
            && source.iter().zip(target).all(|(source, target)| {
                source.name() == target.name()
                    && same_value_type(source.data_type(), target.data_type())
            });
    }
    source == target
}

fn structurally_equal(left: &DataType, right: &DataType) -> bool {
    if let (Some(left), Some(right)) = (list_field(left), list_field(right)) {
        return structurally_equal(left.data_type(), right.data_type());
    }
    if let (DataType::Struct(left), DataType::Struct(right)) = (left, right) {
        return left.len() == right.len()
            && left.iter().zip(right).all(|(left, right)| {
                structurally_equal(left.data_type(), right.data_type())
            });
    }
    left == right || (is_string_type(left) && is_string_type(right))
}

pub(super) fn coerce_nested_result_values(
    arguments: Vec<expr::Expr>,
    data_types: &[DataType],
    context: &FunctionContextInput<'_>,
    set_op_type: Option<&SetOpType>,
) -> PlanResult<Vec<expr::Expr>> {
    // Spark UNION validates current structural equality; widening still requires matching names.
    let struct_field_names = if matches!(set_op_type, Some(SetOpType::Union))
        && data_types.windows(2).all(|pair| structurally_equal(&pair[0], &pair[1]))
    {
        StructFieldNames::AllowPositionalRename
    } else {
        StructFieldNames::RequireMatching
    };
    let error_condition = if set_op_type.is_some() {
        "INCOMPATIBLE_COLUMN_TYPE"
    } else {
        "DATATYPE_MISMATCH.DATA_DIFF_TYPES"
    };
    let mut types = data_types.iter().collect::<Vec<_>>();
    if !context.plan_config.ansi_mode {
        // Spark's legacy common-type fold puts STRING and ARRAY<STRING> first.
        types.sort_by_key(|data_type| !has_string_type(data_type));
    }
    let mut target = DataType::Null;
    for source in types {
        target = common_type(&target, source, context, struct_field_names).map_err(|cause| {
            PlanError::invalid(format!(
                "[{error_condition}] Incompatible nested result types: {data_types:?}: {cause}"
            ))
        })?.ok_or_else(|| {
            PlanError::invalid(format!(
                "[{error_condition}] Incompatible nested result types: {data_types:?}"
            ))
        })?;
    }
    arguments
        .into_iter()
        .zip(data_types)
        .map(|(argument, source)| {
            if same_value_type(source, &target) {
                return Ok(argument);
            }
            let conversion = NestedResultCast {
                target: target.clone(),
                conversion: ResultCast::new(source, &target, context)?,
            };
            Ok(drop_inherited_column_metadata(
                ScalarUDF::from(conversion).call(vec![argument]),
            ))
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq, Hash)]
enum ResultCast {
    Identity,
    List { target: DataType, element: Box<Self> },
    Struct { fields: Fields, children: Vec<Self> },
    Leaf(Arc<dyn PhysicalExpr>),
}

impl ResultCast {
    fn new(
        source: &DataType,
        target: &DataType,
        context: &FunctionContextInput<'_>,
    ) -> PlanResult<Self> {
        if source == target {
            return Ok(Self::Identity);
        }
        if let (Some(source), Some(target_field)) = (list_field(source), list_field(target)) {
            return Ok(Self::List {
                target: target.clone(),
                element: Box::new(Self::new(source.data_type(), target_field.data_type(), context)?),
            });
        }
        if let (DataType::Struct(source), DataType::Struct(target)) = (source, target) {
            return Ok(Self::Struct {
                fields: target.clone(),
                children: source.iter().zip(target).map(|(source, target)| {
                    Self::new(source.data_type(), target.data_type(), context)
                }).collect::<PlanResult<_>>()?,
            });
        }
        let schema = DFSchema::from_unqualified_fields(
            vec![Field::new("value", source.clone(), true)].into(),
            Default::default(),
        )?;
        let expression = cast_result_value(col("value"), source, target)?;
        Ok(Self::Leaf(context.session_context.create_physical_expr(expression, &schema)?))
    }

    fn cast_array(&self, array: &ArrayRef) -> Result<ArrayRef> {
        match self {
            Self::Identity => Ok(array.clone()),
            Self::Leaf(expression) => {
                let batch = RecordBatch::try_from_iter([("value", array.clone())])?;
                expression.evaluate(&batch)?.into_array(array.len())
            }
            Self::Struct { fields, children } => {
                let source = array.as_struct();
                let values = source.columns().iter().zip(children).map(|(value, conversion)| {
                    // Spark never converts the stored children of a NULL struct.
                    let value = make_array(value.to_data().into_builder().nulls(
                        NullBuffer::union(value.nulls(), source.nulls()),
                    ).build()?);
                    conversion.cast_array(&value)
                }).collect::<Result<Vec<_>>>()?;
                Ok(Arc::new(StructArray::try_new(fields.clone(), values, source.nulls().cloned())?))
            }
            Self::List { target, element } => {
                let source_field = list_field(array.data_type()).ok_or_else(|| {
                    datafusion_common::DataFusionError::Internal("nested cast expected an ARRAY".into())
                })?;
                let storage_type = match target {
                    DataType::LargeList(_) => DataType::LargeList(source_field.clone()),
                    _ => DataType::List(source_field.clone()),
                };
                let source = ColumnarValue::Array(array.clone()).cast_to(&storage_type, None)?
                    .into_array(array.len())?;
                let source = remove_list_null_values(&source)?;
                let values = element.cast_array(&list_values(source.as_ref())?)?;
                Ok(match target {
                    DataType::List(field) => {
                        let list = source.as_list::<i32>();
                        Arc::new(ListArray::try_new(field.clone(), adjust_offsets_for_slice(list),
                            values, list.nulls().cloned())?) as ArrayRef
                    }
                    DataType::LargeList(field) => {
                        let list = source.as_list::<i64>();
                        Arc::new(LargeListArray::try_new(field.clone(), adjust_offsets_for_slice(list),
                            values, list.nulls().cloned())?)
                    }
                    _ => unreachable!("common ARRAY types use List or LargeList storage"),
                })
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NestedResultCast {
    target: DataType,
    conversion: ResultCast,
}

impl ScalarUDFImpl for NestedResultCast {
    fn name(&self) -> &str {
        "spark_nested_result_cast"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _arguments: &[DataType]) -> Result<DataType> {
        Ok(self.target.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = take_function_args(self.name(), args.arg_fields)?;
        Ok(Arc::new(field.as_ref().clone().with_name(self.name()).with_data_type(self.target.clone())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = take_function_args(self.name(), args.args)?;
        let scalar = matches!(value, ColumnarValue::Scalar(_));
        let result = self.conversion.cast_array(&value.to_array(1)?)?;
        Ok(if scalar {
            ColumnarValue::Scalar(ScalarValue::try_from_array(&result, 0)?)
        } else {
            ColumnarValue::Array(result)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{StringArray, StringViewArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::Int64Type;
    use datafusion::prelude::SessionContext;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::ExprSchemable;

    #[test]
    fn names_only_union_keeps_logical_string_and_list_storage_compatibility() -> PlanResult<()> {
        let session = SessionContext::new();
        let config = Arc::new(crate::config::PlanConfig::default());
        let view: ArrayRef = Arc::new(StringViewArray::from(vec!["1"]));
        let text: ArrayRef = Arc::new(StringArray::from(vec!["2"]));
        let string_element = Arc::new(Field::new_list_field(DataType::Utf8, false));
        let pairs = [
            (DataType::Utf8, view.clone(), text.clone()),
            (
                DataType::LargeList(string_element.clone()),
                Arc::new(ListArray::try_new(
                    Arc::new(Field::new_list_field(DataType::Utf8View, false)),
                    OffsetBuffer::from_lengths([1]), view, None,
                )?) as ArrayRef,
                Arc::new(LargeListArray::try_new(
                    string_element, OffsetBuffer::from_lengths([1]), text, None,
                )?) as ArrayRef,
            ),
        ];
        for (target_child, left, right) in pairs {
            let left: ArrayRef = Arc::new(StructArray::try_new(
                vec![Field::new("x", left.data_type().clone(), false)].into(), vec![left], None,
            )?);
            let right: ArrayRef = Arc::new(StructArray::try_new(
                vec![Field::new("y", right.data_type().clone(), false)].into(), vec![right], None,
            )?);
            let schema = Arc::new(DFSchema::from_unqualified_fields(vec![
                Field::new("left", left.data_type().clone(), false),
                Field::new("right", right.data_type().clone(), false),
            ].into(), Default::default())?);
            let context = FunctionContextInput {
                argument_display_names: &[], plan_config: &config,
                session_context: &session, schema: &schema,
            };
            let arguments = vec![col("left"), col("right")];
            assert!(super::super::coerce_result_values(arguments.clone(), &context)
                .err().is_some_and(|error| error.to_string().contains("[DATATYPE_MISMATCH.DATA_DIFF_TYPES]")));
            let converted = super::super::coerce_set_operation_result_values(
                arguments, &context, &SetOpType::Union,
            )?;
            let target = DataType::Struct(vec![Field::new("x", target_child, false)].into());
            let batch = RecordBatch::try_new(Arc::clone(schema.inner()), vec![left, right])?;
            for (expression, expected) in converted.into_iter().zip(["1", "2"]) {
                assert_eq!(expression.get_type(&schema)?, target);
                let result = session.create_physical_expr(expression, &schema)?
                    .evaluate(&batch)?.into_array(1)?;
                result.to_data().validate_full()?;
                let child = result.as_struct().column(0);
                let values = if matches!(child.data_type(), DataType::LargeList(_)) {
                    child.as_list::<i64>().value(0)
                } else {
                    child.clone()
                };
                assert_eq!(ScalarValue::try_from_array(&values, 0)?, ScalarValue::Utf8(Some(expected.into())));
            }
        }
        Ok(())
    }

    #[test]
    fn nested_casts_skip_hidden_values_and_keep_slices_scalars_and_empty_arrays() -> PlanResult<()> {
        let session = SessionContext::new();
        let config = Arc::new(crate::config::PlanConfig::default());
        let schema = Arc::new(DFSchema::empty());
        let context = FunctionContextInput {
            argument_display_names: &[],
            plan_config: &config,
            session_context: &session,
            schema: &schema,
        };
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("outside"), Some(" 2 "), Some("hidden"), None, Some("3"), Some("outside"),
        ]));
        let field = Arc::new(Field::new("x", DataType::Utf8, true)
            .with_metadata([("source".into(), "child".into())].into()));
        let nulls = Some(NullBuffer::from(vec![true, true, false, true, true, true]));
        let mut inputs: Vec<ArrayRef> = vec![
            Arc::new(StructArray::try_new(vec![field.clone()].into(), vec![strings.clone()], nulls.clone())?),
            Arc::new(ListArray::try_new(field.clone(), OffsetBuffer::from_lengths([1; 6]), strings, nulls)?),
        ];
        for storage in [
            DataType::LargeList(field.clone()),
            DataType::FixedSizeList(field.clone(), 1),
            DataType::ListView(field.clone()),
            DataType::LargeListView(field),
        ] {
            inputs.push(arrow::compute::cast(inputs[1].as_ref(), &storage)?);
        }
        for input in inputs {
            let target_field = Arc::new(Field::new("x", DataType::Int64, true));
            let target = match input.data_type() {
                DataType::Struct(_) => DataType::Struct(vec![target_field].into()),
                DataType::LargeList(_) | DataType::LargeListView(_) => DataType::LargeList(target_field),
                _ => DataType::List(target_field),
            };
            let conversion = NestedResultCast {
                target: target.clone(),
                conversion: ResultCast::new(input.data_type(), &target, &context)?,
            };
            for len in [0, 4] {
                let input = input.slice(1, len);
                let field = Arc::new(Field::new("input", input.data_type().clone(), true));
                let result = conversion.invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(input.clone())],
                    arg_fields: vec![field],
                    number_rows: len,
                    return_field: Arc::new(Field::new("result", target.clone(), true)),
                    config_options: Arc::new(ConfigOptions::default()),
                })?.into_array(len)?;
                assert_eq!(result.data_type(), &target);
                assert_eq!(result.len(), len);
                assert_eq!(result.nulls(), input.nulls());
                result.to_data().validate_full()?;
                let values = match result.data_type() {
                    DataType::Struct(_) => result.as_struct().column(0).clone(),
                    DataType::LargeList(_) => result.as_list::<i64>().values().clone(),
                    _ => result.as_list::<i32>().values().clone(),
                };
                let expected = if len == 0 {
                    vec![]
                } else if matches!(result.data_type(), DataType::Struct(_)) {
                    vec![Some(2), None, None, Some(3)]
                } else {
                    vec![Some(2), None, Some(3)]
                };
                assert_eq!(values.as_primitive::<Int64Type>().iter().collect::<Vec<_>>(), expected);
            }
            for row in [1, 2] {
                let scalar = ScalarValue::try_from_array(&input, row)?;
                let field = Arc::new(Field::new("input", input.data_type().clone(), true));
                let result = conversion.invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Scalar(scalar)],
                    arg_fields: vec![field],
                    number_rows: 1,
                    return_field: Arc::new(Field::new("result", target.clone(), true)),
                    config_options: Arc::new(ConfigOptions::default()),
                })?;
                let ColumnarValue::Scalar(result) = result else {
                    return Err(PlanError::internal("nested scalar became an array"));
                };
                assert_eq!(result.data_type(), target);
                assert_eq!(result.is_null(), row == 2);
                let result = result.to_array()?;
                result.to_data().validate_full()?;
                if row == 1 {
                    let values = match result.data_type() {
                        DataType::Struct(_) => result.as_struct().column(0).clone(),
                        DataType::LargeList(_) => result.as_list::<i64>().value(0),
                        _ => result.as_list::<i32>().value(0),
                    };
                    assert_eq!(ScalarValue::try_from_array(&values, 0)?, ScalarValue::Int64(Some(2)));
                }
            }
        }
        Ok(())
    }
}
