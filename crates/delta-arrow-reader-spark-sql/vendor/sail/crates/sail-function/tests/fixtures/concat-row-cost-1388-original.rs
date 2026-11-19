// Frozen invocation from staging 05cde76fe6d46d505916e349ef23436a94416c7d.
// The retained conversion, binary and list helpers are unchanged by this adaptation.
fn original_concat(args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    if args.args.is_empty() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
            Some(String::new()),
        )));
    }
    // `return_type` is cloned because `args.return_field` is moved into the
    // delegate call below, but the null-typed early-return still needs it.
    let return_type = args.return_field.data_type().clone();
    let mut null_mask = None;
    for arg in &args.args {
        match arg {
            ColumnarValue::Scalar(s) if s.is_null() => {
                return Ok(ColumnarValue::Scalar(ScalarValue::try_from(&return_type)?));
            }
            ColumnarValue::Array(a) => {
                let mask = is_null(a)?;
                null_mask = match null_mask {
                    Some(existing) => Some(or(&existing, &mask)?),
                    None => Some(mask),
                };
            }
            _ => (),
        }
    }
    let null_mask = null_mask.unwrap_or_else(|| BooleanArray::from(vec![false; 1]));

    let return_field = args.return_field.clone();
    let return_type = return_field.data_type();

    let concatenated = if args
        .args
        .iter()
        .any(|arg| matches!(arg.data_type(), DataType::List(_)))
    {
        // Cast arrays with Null element type to the return type for proper
        // concatenation. This handles cases like `concat(array(), array(1, 2, 3))`
        // where the first array has type `List(Null)` and needs to be cast to
        // `List(Int32)` so `ArrayConcat` can merge them.
        let casted_args = cast_list_columnar_values(args.args, return_type)?;
        let casted_scalar_args = ScalarFunctionArgs {
            args: casted_args,
            arg_fields: args.arg_fields,
            number_rows: args.number_rows,
            return_field: args.return_field,
            config_options: args.config_options,
        };
        ArrayConcat::new().invoke_with_args(casted_scalar_args)
    } else if matches!(return_type, DataType::Binary | DataType::LargeBinary) {
        concat_binary_values(&args.args, return_type)
    } else {
        let casted_columns = if args
            .args
            .iter()
            .any(|arg| matches!(arg.data_type(), DataType::LargeUtf8 | DataType::LargeBinary))
        {
            cast_columnar_values(args.args, &DataType::LargeUtf8)?
        } else {
            cast_columnar_values(args.args, &DataType::Utf8)?
        };

        let casted_args = ScalarFunctionArgs {
            args: casted_columns,
            arg_fields: args.arg_fields,
            number_rows: args.number_rows,
            return_field: args.return_field,
            config_options: args.config_options,
        };

        ConcatFunc::new().invoke_with_args(casted_args)
    }?;

    let concatenated_array = match concatenated {
        ColumnarValue::Array(arr) => arr,
        ColumnarValue::Scalar(s) => s.to_array()?,
    };

    Ok(ColumnarValue::Array(nullif(
        cast(&concatenated_array, return_type)?.as_ref(),
        &null_mask,
    )?))
}
