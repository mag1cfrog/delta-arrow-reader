// Frozen NULL-first selection/invocation from scalar_function.rs:233-360 at
// d69d6bc3c9b50c76bd645287d28803447c864390; the NULLIF kernel is unchanged.
fn original_invoke(
    function: &ScalarFunctionExpr,
    batch: &RecordBatch,
    args: Vec<ColumnarValue>,
) -> Result<ColumnarValue> {
    let all_scalar = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let input_empty = args.is_empty();
    let result = function.fun().invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields: function
            .args()
            .iter()
            .map(|expr| expr.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?,
        number_rows: batch.num_rows(),
        return_field: function.return_field(batch.schema_ref())?,
        config_options: Arc::new(function.config_options().clone()),
    })?;
    if let ColumnarValue::Array(array) = &result
        && array.len() != batch.num_rows()
    {
        if array.len() == 1 && !input_empty && all_scalar {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                array, 0,
            )?));
        }
        return datafusion_common::internal_err!(
            "UDF {} returned a different number of rows than expected. Expected: {}, Got: {}",
            function.name(),
            batch.num_rows(),
            array.len()
        );
    }
    Ok(result)
}

fn original_selection(function: &ScalarFunctionExpr, batch: &RecordBatch) -> Result<ColumnarValue> {
    if batch.num_rows() == 0 {
        return Ok(ColumnarValue::Array(arrow::array::new_empty_array(
            function.return_type(),
        )));
    }
    let first = function.args()[0].evaluate(batch)?;
    match &first {
        ColumnarValue::Scalar(value) if value.is_null() => {
            return Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                function.return_type(),
            )?));
        }
        ColumnarValue::Array(array) => {
            if let Some(nulls) = array.logical_nulls()
                && nulls.null_count() > 0
            {
                if nulls.null_count() == array.len() {
                    return Ok(ColumnarValue::Array(arrow::array::new_null_array(
                        function.return_type(),
                        array.len(),
                    )));
                }
                let mask = arrow::array::BooleanArray::new(nulls.inner().clone(), None);
                let selected = arrow::compute::filter_record_batch(batch, &mask)?;
                let selected_first =
                    if let Some(column) = function.args()[0].downcast_ref::<Column>() {
                        selected.column(column.index()).clone()
                    } else {
                        arrow::compute::filter(array.as_ref(), &mask)?
                    };
                let mut args = vec![ColumnarValue::Array(selected_first)];
                for expr in &function.args()[1..] {
                    args.push(expr.evaluate(&selected)?);
                }
                let result =
                    original_invoke(function, &selected, args)?.into_array(selected.num_rows())?;
                return Ok(ColumnarValue::Array(
                    datafusion::physical_expr_common::utils::scatter(&mask, result.as_ref())?,
                ));
            }
        }
        _ => {}
    }
    let mut args = vec![first];
    for expr in &function.args()[1..] {
        args.push(expr.evaluate(batch)?);
    }
    original_invoke(function, batch, args)
}
