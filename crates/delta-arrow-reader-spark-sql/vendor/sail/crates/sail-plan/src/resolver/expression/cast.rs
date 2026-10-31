// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::ops::{Div, Mul};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, IntervalUnit, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::{ExprSchemable, ScalarUDF, cast, expr, lit, try_cast};
use sail_common::spec;
use sail_common::utils::datetime::time_unit_to_multiplier;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_common_datafusion::variant::is_variant_storage_field;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_interval::{
    SparkCalendarInterval, SparkDayTimeInterval, SparkYearMonthInterval,
};
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_cast_float_to_decimal::SparkCastFloatToDecimal;
use sail_function::scalar::spark_cast_string_to_decimal::SparkCastStringToDecimal;
use sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat;
use sail_function::scalar::spark_cast_string_to_int32::SparkCastStringToInteger;
use sail_function::scalar::spark_struct_rename::SparkStructRename;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::scalar::variant::spark_variant_get::SparkVariantGet;
use sail_function::scalar::variant::spark_variant_to_json::SparkVariantToJsonUdf;

use crate::error::{PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;
use crate::function::common::cast_local_timestamp_to_ltz;
use crate::function::is_spark_compatible_arrow_fixed_offset;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_cast(
        &self,
        expr: spec::Expr,
        cast_to_type: spec::DataType,
        is_try: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        // CAST(expr AS VARIANT) → rewrite to SparkCastToVariant UDF
        // Must intercept before resolve_data_type converts Variant to Struct.
        if matches!(cast_to_type, spec::DataType::Variant) {
            let NamedExpr { expr, name, .. } =
                self.resolve_named_expression(expr, schema, state).await?;
            let name = if need_rename_cast(&expr) {
                let prefix = if is_try { "TRY_" } else { "" };
                vec![format!("{}CAST({} AS VARIANT)", prefix, name.one()?)]
            } else {
                name
            };
            let expr = ScalarUDF::new_from_impl(SparkCastToVariant::new()).call(vec![expr]);
            return Ok(NamedExpr::new(name, expr));
        }

        // Extract the DayTimeInterval field unit before resolving to Arrow type,
        // since it determines the multiplier for numeric-to-interval casts.
        // Spark uses the end field (or start field for single-field intervals)
        // to interpret the numeric value: e.g. DayTimeIntervalType(DAY, DAY) treats
        // the value as days, while DayTimeIntervalType(DAY, SECOND) treats it as seconds.
        let day_time_interval_field = match &cast_to_type {
            spec::DataType::Interval {
                interval_unit: spec::IntervalUnit::DayTime,
                start_field,
                end_field,
            } => end_field.or(*start_field),
            _ => None,
        };
        let cast_to_type = self.resolve_data_type(&cast_to_type, state)?;
        let NamedExpr { expr, name, .. } =
            self.resolve_named_expression(expr, schema, state).await?;
        let expr_field = expr.to_field(schema)?.1;
        let expr_type = expr_field.data_type().clone();
        let expr_is_variant = is_variant_storage_field(expr_field.as_ref());
        let name = if need_rename_cast(&expr) {
            let data_type_string = SparkPlanFormatter.data_type_to_simple_string(&cast_to_type)?;
            vec![format!(
                "{}CAST({} AS {})",
                if is_try { "TRY_" } else { "" },
                name.one()?,
                data_type_string.to_ascii_uppercase()
            )]
        } else {
            name
        };
        let override_string_cast = matches!(
            expr_type,
            DataType::Date32
                | DataType::Date64
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Timestamp(_, _)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::Struct(_)
                | DataType::Map(_, _)
        );
        let expr = match (expr_type, cast_to_type.clone(), is_try) {
            (
                DataType::Timestamp(_, None),
                DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)),
                is_try,
            ) => {
                if is_spark_compatible_arrow_fixed_offset(timezone.as_ref()) {
                    if is_try {
                        try_cast(expr, cast_to_type)
                    } else {
                        cast(expr, cast_to_type)
                    }
                } else {
                    cast_local_timestamp_to_ltz(expr, &timezone)
                }
            }
            (_, DataType::Utf8, _) if expr_is_variant => cast(
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr]),
                DataType::Utf8,
            ),
            (_, DataType::LargeUtf8, _) if expr_is_variant => cast(
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr]),
                DataType::LargeUtf8,
            ),
            (_, DataType::Utf8View, _) if expr_is_variant => {
                ScalarUDF::new_from_impl(SparkVariantToJsonUdf::new()).call(vec![expr])
            }
            (_, to, is_try) if expr_is_variant => {
                let data_type_string = SparkPlanFormatter.data_type_to_simple_string(&to)?;
                ScalarUDF::new_from_impl(SparkVariantGet::new(is_try)).call(vec![
                    expr,
                    lit("$"),
                    lit(data_type_string),
                ])
            }
            (
                DataType::Float32 | DataType::Float64,
                DataType::Decimal128(precision, scale),
                false,
            ) if self.config.ansi_mode => {
                ScalarUDF::new_from_impl(SparkCastFloatToDecimal::new(precision, scale))
                    .call(vec![expr])
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Decimal128(precision, scale),
                _,
            ) => ScalarUDF::new_from_impl(SparkCastStringToDecimal::new(
                precision,
                scale,
                is_try || !self.config.ansi_mode,
            ))
            .call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                to @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64),
                false,
            ) if !self.config.ansi_mode => {
                ScalarUDF::new_from_impl(SparkCastStringToInteger::try_new(to)?).call(vec![expr])
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                to @ (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64),
                is_try,
            ) => {
                // UTF8String's exact integer parser trims ASCII controls, including DEL.
                let trim: String = (0_u8..=32).chain([127]).map(char::from).collect();
                let value = expr_fn::btrim(vec![expr, lit(trim)]);
                if is_try {
                    try_cast(value, to)
                } else {
                    cast(value, to)
                }
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                to @ (DataType::Float32 | DataType::Float64),
                is_try,
            ) => ScalarUDF::new_from_impl(SparkCastStringToFloat::try_new(
                to,
                is_try || !self.config.ansi_mode,
            )?)
            .call(vec![expr]),
            (from, DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), _)
                if from.is_numeric() =>
            {
                let multiplier = match (day_time_interval_field, &cast_to_type) {
                    (Some(field), DataType::Duration(_)) => day_time_field_to_microseconds(field),
                    _ => time_unit_to_multiplier(&time_unit),
                };
                cast(expr.mul(lit(multiplier)), cast_to_type)
            }
            (DataType::Timestamp(time_unit, _) | DataType::Duration(time_unit), to, _)
                if to.is_numeric() =>
            {
                cast(
                    lit(1.0)
                        .div(lit(time_unit_to_multiplier(&time_unit)))
                        .mul(cast(expr, DataType::Int64)),
                    to,
                )
            }
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::YearMonth),
                _,
            ) => ScalarUDF::new_from_impl(SparkYearMonthInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Duration(TimeUnit::Microsecond),
                _,
            ) => ScalarUDF::new_from_impl(SparkDayTimeInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Interval(IntervalUnit::MonthDayNano),
                _,
            ) => ScalarUDF::new_from_impl(SparkCalendarInterval::new()).call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Date32,
                is_try,
            ) => ScalarUDF::new_from_impl(SparkDate::new(is_try || !self.config.ansi_mode))
                .call(vec![expr]),
            (
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
                DataType::Timestamp(TimeUnit::Microsecond, tz),
                is_try,
            ) => Arc::new(ScalarUDF::new_from_impl(SparkTimestamp::try_new(
                tz,
                self.config.ansi_mode,
                is_try,
            )?))
            .call(vec![expr]),
            (_, DataType::Utf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8::new()).call(vec![expr])
            }
            (_, DataType::LargeUtf8, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToLargeUtf8::new()).call(vec![expr])
            }
            (_, DataType::Utf8View, _) if override_string_cast => {
                ScalarUDF::new_from_impl(SparkToUtf8View::new()).call(vec![expr])
            }
            (DataType::Date32 | DataType::Date64, to, _)
                if to.is_numeric() || matches!(to, DataType::Boolean) =>
            {
                if !is_try && self.config.ansi_mode {
                    return Err(PlanError::invalid(format!("cannot cast date to {to}")));
                }
                lit(ScalarValue::try_from(&to)?)
            }
            (from, to, _) if needs_struct_field_rename(&from, &to) => {
                // Pre-rename the source struct fields positionally so the cast
                // becomes a no-op or a valid name-matched one (see
                // `needs_struct_field_rename`).
                let renamed_target = build_rename_target_type(&from, &to);
                let renamed =
                    ScalarUDF::new_from_impl(SparkStructRename::new(renamed_target.clone()))
                        .call(vec![expr]);
                if renamed_target == to {
                    renamed
                } else if is_try {
                    try_cast(renamed, to)
                } else {
                    cast(renamed, to)
                }
            }
            (
                from,
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale),
                _,
            ) if from.is_numeric()
                && !self.config.ansi_mode
                && (is_try || decimal_cast_can_overflow(&from, precision, scale)) =>
            {
                try_cast(expr, cast_to_type)
            }
            (_, to, true) => try_cast(expr, to),
            (_, to, _) => cast(expr, to),
        };
        Ok(NamedExpr::new(name, expr))
    }
}

fn decimal_cast_can_overflow(from: &DataType, precision: u8, scale: i8) -> bool {
    let integer_digits = i16::from(precision) - i16::from(scale);
    match from {
        DataType::Decimal128(from_precision, from_scale)
        | DataType::Decimal256(from_precision, from_scale) => {
            let source_integer_digits = i16::from(*from_precision) - i16::from(*from_scale);
            integer_digits < source_integer_digits
                || (integer_digits == source_integer_digits && scale < *from_scale)
        }
        DataType::Int8 | DataType::UInt8 => scale < 0 || integer_digits < 3,
        DataType::Int16 | DataType::UInt16 => scale < 0 || integer_digits < 5,
        DataType::Int32 | DataType::UInt32 => scale < 0 || integer_digits < 10,
        DataType::Int64 | DataType::UInt64 => scale < 0 || integer_digits < 20,
        _ => true,
    }
}

/// Returns true if the cast from `from` to `to` involves a Struct
/// (possibly nested in a List/LargeList/FixedSizeList/Map) whose field names
/// don't share enough overlap for DataFusion's struct cast validator.
fn needs_struct_field_rename(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .any(|(fa, fb)| fa.name() != fb.name())
        }
        (DataType::List(a), DataType::List(b))
        | (DataType::LargeList(a), DataType::LargeList(b)) => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        (DataType::FixedSizeList(a, sa), DataType::FixedSizeList(b, sb)) if sa == sb => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        (DataType::Map(a, _), DataType::Map(b, _)) => {
            needs_struct_field_rename(a.data_type(), b.data_type())
        }
        _ => false,
    }
}

/// Build a target type that has the names from `to` but the data types from
/// `from`. The result is what `SparkStructRename` produces; the subsequent
/// regular CAST then handles any leaf-type conversion.
fn build_rename_target_type(from: &DataType, to: &DataType) -> DataType {
    match (from, to) {
        (DataType::Struct(src_fields), DataType::Struct(tgt_fields))
            if src_fields.len() == tgt_fields.len() =>
        {
            let fields: Fields = src_fields
                .iter()
                .zip(tgt_fields.iter())
                .map(|(src, tgt)| {
                    Arc::new(
                        Field::new(
                            tgt.name(),
                            build_rename_target_type(src.data_type(), tgt.data_type()),
                            src.is_nullable(),
                        )
                        .with_metadata(src.metadata().clone()),
                    )
                })
                .collect();
            DataType::Struct(fields)
        }
        (DataType::List(src), DataType::List(tgt)) => DataType::List(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::LargeList(src), DataType::LargeList(tgt)) => DataType::LargeList(Arc::new(
            Field::new(
                tgt.name(),
                build_rename_target_type(src.data_type(), tgt.data_type()),
                src.is_nullable(),
            )
            .with_metadata(src.metadata().clone()),
        )),
        (DataType::FixedSizeList(src, sa), DataType::FixedSizeList(tgt, _)) => {
            DataType::FixedSizeList(
                Arc::new(
                    Field::new(
                        tgt.name(),
                        build_rename_target_type(src.data_type(), tgt.data_type()),
                        src.is_nullable(),
                    )
                    .with_metadata(src.metadata().clone()),
                ),
                *sa,
            )
        }
        (DataType::Map(src, sorted), DataType::Map(tgt, _)) => DataType::Map(
            Arc::new(
                Field::new(
                    tgt.name(),
                    build_rename_target_type(src.data_type(), tgt.data_type()),
                    src.is_nullable(),
                )
                .with_metadata(src.metadata().clone()),
            ),
            *sorted,
        ),
        // Leaves: keep the source data type unchanged.
        _ => from.clone(),
    }
}

fn day_time_field_to_microseconds(field: spec::IntervalFieldType) -> i64 {
    match field {
        spec::IntervalFieldType::Day => 86_400_000_000,
        spec::IntervalFieldType::Hour => 3_600_000_000,
        spec::IntervalFieldType::Minute => 60_000_000,
        // Second, or Year/Month (shouldn't appear for DayTime intervals)
        _ => 1_000_000,
    }
}

fn need_rename_cast(expr: &expr::Expr) -> bool {
    match expr {
        expr::Expr::Alias(_) | expr::Expr::Column(_) | expr::Expr::OuterReferenceColumn(..) => {
            false
        }
        expr::Expr::Cast(cast) => need_rename_cast(cast.expr.as_ref()),
        expr::Expr::TryCast(try_cast) => need_rename_cast(try_cast.expr.as_ref()),
        _ => true,
    }
}

#[cfg(test)]
mod decimal_cast_tests {
    use super::*;
    use crate::config::PlanConfig;
    use datafusion::arrow::array::Decimal128Array;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn numeric_decimal_cast_preserves_values_errors_and_nullability()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for ansi in [true, false] {
            // Safe widening keeps Spark's non-null field even outside ANSI mode.
            for (source, target) in [
                ("TINYINT", "DECIMAL(3,0)"),
                ("SMALLINT", "DECIMAL(5,0)"),
                ("INT", "DECIMAL(10,0)"),
                ("BIGINT", "DECIMAL(20,0)"),
                ("DECIMAL(20,0)", "DECIMAL(21,0)"),
            ] {
                let ctx = SessionContext::new();
                let sql =
                    format!("SELECT CAST(CAST(id AS {source}) AS {target}) AS r FROM range(1)");
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                assert!(
                    !named.plan.schema().field(0).is_nullable(),
                    "{sql}, ANSI {ansi}"
                );
                let batches = ctx
                    .execute_logical_plan(named.plan)
                    .await?
                    .collect()
                    .await?;
                assert!(
                    !batches[0].schema().field(0).is_nullable(),
                    "{sql}, ANSI {ansi}"
                );
                let values = batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(0)]);
            }
            for is_try in [true, false] {
                let ctx = SessionContext::new();
                let keyword = if is_try { "TRY_CAST" } else { "CAST" };
                let sql = format!(
                    "SELECT {keyword}(id AS DECIMAL(2,0)) AS r FROM range(98,102) ORDER BY id"
                );
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                if is_try || !ansi {
                    assert!(named.plan.schema().field(0).is_nullable());
                }
                let batches = ctx.execute_logical_plan(named.plan).await?.collect().await;
                if ansi && !is_try {
                    assert!(batches.is_err(), "ANSI CAST must raise on 100");
                } else {
                    let mut values = Vec::new();
                    for batch in batches? {
                        assert!(batch.schema().field(0).is_nullable());
                        values.extend(
                            batch
                                .column(0)
                                .as_any()
                                .downcast_ref::<Decimal128Array>()
                                .unwrap()
                                .iter(),
                        );
                    }
                    assert_eq!(values, vec![Some(98), Some(99), None, None]);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod strict_integer_cast_tests {
    use super::*;
    use crate::config::PlanConfig;
    use datafusion::arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    #[tokio::test]
    async fn integer_string_trim_keeps_cast_modes_and_widths()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for (target, data_type, min, max) in [
            ("TINYINT", DataType::Int8, i8::MIN as i64, i8::MAX as i64),
            (
                "SMALLINT",
                DataType::Int16,
                i16::MIN as i64,
                i16::MAX as i64,
            ),
            ("INT", DataType::Int32, i32::MIN as i64, i32::MAX as i64),
            ("BIGINT", DataType::Int64, i64::MIN, i64::MAX),
        ] {
            let mut valid: Vec<Option<String>> = (0_u8..=32)
                .chain([127])
                .map(|b| Some(format!("{}7{}", char::from(b), char::from(b))))
                .collect();
            valid.extend([
                None,
                Some(format!("\x00{min}\x7f")),
                Some(format!("\t+{max}\n")),
            ]);
            let invalid = vec![
                Some(" 1.25 ".to_owned()),
                Some("bad".to_owned()),
                Some(format!(" {} ", i128::from(max) + 1)),
                Some("\u{a0}7\u{a0}".to_owned()),
            ];
            for (is_invalid, strings) in [(false, valid), (true, invalid)] {
                let values: Vec<_> = strings.iter().map(|s| s.as_deref()).collect();
                let arrays: [ArrayRef; 3] = [
                    Arc::new(StringArray::from(values.clone())),
                    Arc::new(LargeStringArray::from(values.clone())),
                    Arc::new(StringViewArray::from(values)),
                ];
                for input in arrays {
                    let ctx = SessionContext::new_with_config(
                        SessionConfig::new()
                            .with_target_partitions(1)
                            .with_batch_size(2),
                    );
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "v",
                        input.data_type().clone(),
                        true,
                    )]));
                    let batch = RecordBatch::try_new(schema.clone(), vec![input])?;
                    ctx.register_table(
                        "t",
                        Arc::new(MemTable::try_new(schema, vec![vec![batch]])?),
                    )?;
                    for ansi in [true, false, true] {
                        for is_try in [false, true] {
                            let keyword = if is_try { "TRY_CAST" } else { "CAST" };
                            let sql = format!("SELECT {keyword}(v AS {target}) AS r FROM t");
                            let result = async {
                                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                                let config = PlanConfig {
                                    ansi_mode: ansi,
                                    ..Default::default()
                                };
                                let named = PlanResolver::new(&ctx, Arc::new(config))
                                    .resolve_named_plan(spec)
                                    .await?;
                                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                                    ctx.execute_logical_plan(named.plan)
                                        .await?
                                        .collect()
                                        .await?,
                                )
                            }
                            .await;
                            if is_invalid && ansi && !is_try {
                                assert!(
                                    result.is_err(),
                                    "{sql} must reject fractional text in ANSI mode"
                                );
                                continue;
                            }
                            let mut actual = Vec::new();
                            for batch in result? {
                                let values = batch.column(0);
                                assert_eq!(values.data_type(), &data_type);
                                for row in 0..values.len() {
                                    actual.push(
                                        ScalarValue::try_from_array(values, row)?
                                            .cast_to(&DataType::Int64)?,
                                    );
                                }
                            }
                            let expected: Vec<_> = if is_invalid {
                                vec![if is_try { None } else { Some(1) }, None, None, None]
                            } else {
                                let mut result = vec![Some(7); 34];
                                result.extend([None, Some(min), Some(max)]);
                                result
                            };
                            assert_eq!(
                                actual,
                                expected
                                    .into_iter()
                                    .map(ScalarValue::Int64)
                                    .collect::<Vec<_>>(),
                                "{sql}, ANSI {ansi}"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod floating_string_cast_tests {
    use super::*;
    use crate::config::PlanConfig;
    use datafusion::arrow::array::{ArrayRef, LargeStringArray, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    #[tokio::test]
    async fn floating_cast_query_modes_do_not_leak()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for values in [
            vec![Some("1.25f"), Some("0x1.8p2"), None],
            vec![Some("bad"), None],
        ] {
            let invalid = values[0] == Some("bad");
            let arrays: [ArrayRef; 3] = [
                Arc::new(StringArray::from(values.clone())),
                Arc::new(LargeStringArray::from(values.clone())),
                Arc::new(StringViewArray::from(values)),
            ];
            for input in arrays {
                let ctx = SessionContext::new_with_config(
                    SessionConfig::new()
                        .with_target_partitions(1)
                        .with_batch_size(2),
                );
                let schema = Arc::new(Schema::new(vec![Field::new(
                    "v",
                    input.data_type().clone(),
                    true,
                )]));
                let batch = RecordBatch::try_new(schema.clone(), vec![input])?;
                ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))?;
                for ansi in [true, false, true] {
                    for (target, dtype) in
                        [("FLOAT", DataType::Float32), ("DOUBLE", DataType::Float64)]
                    {
                        for is_try in [false, true] {
                            let keyword = if is_try { "TRY_CAST" } else { "CAST" };
                            let sql = format!("SELECT {keyword}(v AS {target}) AS r FROM t");
                            let result = async {
                                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                                let config = PlanConfig {
                                    ansi_mode: ansi,
                                    ..Default::default()
                                };
                                let named = PlanResolver::new(&ctx, Arc::new(config))
                                    .resolve_named_plan(spec)
                                    .await?;
                                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                                    ctx.execute_logical_plan(named.plan)
                                        .await?
                                        .collect()
                                        .await?,
                                )
                            }
                            .await;
                            if invalid && ansi && !is_try {
                                assert!(result.is_err(), "{sql}");
                                continue;
                            }
                            let mut actual = Vec::new();
                            for batch in result? {
                                let array = batch.column(0);
                                assert_eq!(array.data_type(), &dtype);
                                for row in 0..array.len() {
                                    actual.push(
                                        ScalarValue::try_from_array(array, row)?
                                            .cast_to(&DataType::Float64)?,
                                    );
                                }
                            }
                            let expected = if invalid {
                                vec![None, None]
                            } else {
                                vec![Some(1.25), Some(6.0), None]
                            };
                            assert_eq!(
                                actual,
                                expected
                                    .into_iter()
                                    .map(ScalarValue::Float64)
                                    .collect::<Vec<_>>(),
                                "{sql}, ANSI {ansi}"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
