// Local Spark interval qualifiers. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
//! The qualifier Spark gives a year-month or day-time interval value, such as `DAY` or `HOUR TO
//! SECOND` (#1152). DAR types every year-month interval as Arrow's month interval and every
//! day-time interval as a microsecond Duration, so the resolver keeps the qualifier beside each
//! resolved expression and each field: from the SQL of a literal or a CAST, from the function
//! that computed the value, and from the columns of subqueries, VALUES and set operations.
//! Spark casts an interval to a number as a count of the qualifier's end field, and adds a DAY
//! interval to a DATE as days (#952). An unknown qualifier is read as the type Spark gives an
//! Arrow column: `DAY TO SECOND` or `YEAR TO MONTH`.

use datafusion::arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::DFSchemaRef;
use datafusion_expr::{BinaryExpr, ExprSchemable, Operator, expr};
use datafusion_spark::function::datetime::make_dt_interval::SparkMakeDtInterval;
use sail_common::spec::IntervalFieldType;
use sail_function::scalar::datetime::spark_subtract_dates::SparkSubtractDates;
use sail_function::scalar::datetime::spark_subtract_timestamps::SparkSubtractTimestamps;

use crate::function::common::without_inherited_column_metadata;

/// A year-month or day-time interval qualifier, from its start field to its end field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IntervalQualifier {
    start: IntervalFieldType,
    end: IntervalFieldType,
}

/// What a value contributes to the qualifier of an expression over it: nothing when it is not
/// a year-month or day-time interval, as a condition or an untyped NULL, and its family and
/// qualifier, if known, when it is one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IntervalOperand {
    NotInterval,
    Interval {
        year_month: bool,
        qualifier: Option<IntervalQualifier>,
    },
}

impl IntervalQualifier {
    pub(crate) const DAY: Self = Self {
        start: IntervalFieldType::Day,
        end: IntervalFieldType::Day,
    };
    pub(crate) const DAY_TO_SECOND: Self = Self {
        start: IntervalFieldType::Day,
        end: IntervalFieldType::Second,
    };
    pub(crate) const YEAR_TO_MONTH: Self = Self {
        start: IntervalFieldType::Year,
        end: IntervalFieldType::Month,
    };

    /// The qualifier from `start` to `end`, or of the one field given, as the SQL writes it.
    pub(crate) fn new(
        start: Option<IntervalFieldType>,
        end: Option<IntervalFieldType>,
    ) -> Option<Self> {
        let start = start.or(end)?;
        let end = end.unwrap_or(start);
        (start <= end && is_year_month_field(start) == is_year_month_field(end))
            .then_some(Self { start, end })
    }

    pub(crate) fn start(self) -> IntervalFieldType {
        self.start
    }

    pub(crate) fn end(self) -> IntervalFieldType {
        self.end
    }

    pub(crate) fn is_year_month(self) -> bool {
        is_year_month_field(self.start)
    }

    /// The qualifier Spark gives an interval of `data_type` it cannot say more about, as an
    /// Arrow column, a scaled interval or an average.
    pub(crate) fn of_type(data_type: &DataType) -> Option<Self> {
        match data_type {
            DataType::Interval(IntervalUnit::YearMonth) => Some(Self::YEAR_TO_MONTH),
            DataType::Duration(TimeUnit::Microsecond) => Some(Self::DAY_TO_SECOND),
            _ => None,
        }
    }

    fn family_default(year_month: bool) -> Self {
        if year_month {
            Self::YEAR_TO_MONTH
        } else {
            Self::DAY_TO_SECOND
        }
    }

    /// The qualifier of intervals combined into one, as a sum, a difference, the branches of a
    /// conditional or the rows of a set operation: Spark widens it from the earliest start to
    /// the latest end. Values that are not intervals do not take part; an interval whose
    /// qualifier is unknown makes the result unknown.
    pub(crate) fn widen(operands: impl IntoIterator<Item = IntervalOperand>) -> Option<Self> {
        let mut widened: Option<Self> = None;
        for operand in operands {
            let IntervalOperand::Interval { qualifier, .. } = operand else {
                continue;
            };
            let qualifier = qualifier?;
            widened = Some(match widened {
                None => qualifier,
                Some(current) if current.is_year_month() == qualifier.is_year_month() => Self {
                    start: current.start.min(qualifier.start),
                    end: current.end.max(qualifier.end),
                },
                Some(_) => return None,
            });
        }
        widened
    }
}

fn is_year_month_field(field: IntervalFieldType) -> bool {
    matches!(field, IntervalFieldType::Year | IntervalFieldType::Month)
}

impl IntervalOperand {
    /// The contribution of a resolved `expr` of `qualifier`, by its type.
    pub(crate) fn of(
        expr: &expr::Expr,
        qualifier: Option<IntervalQualifier>,
        schema: &DFSchemaRef,
    ) -> Self {
        match expr
            .get_type(schema)
            .ok()
            .as_ref()
            .and_then(IntervalQualifier::of_type)
        {
            Some(default) => Self::Interval {
                year_month: default.is_year_month(),
                qualifier,
            },
            None => Self::NotInterval,
        }
    }

    fn qualifier(self) -> Option<IntervalQualifier> {
        match self {
            Self::Interval { qualifier, .. } => qualifier,
            Self::NotInterval => None,
        }
    }

    fn family_default(self) -> Option<IntervalQualifier> {
        match self {
            Self::Interval { year_month, .. } => {
                Some(IntervalQualifier::family_default(year_month))
            }
            Self::NotInterval => None,
        }
    }
}

/// The qualifier of the interval that `function` computes from `operands`, where Spark fixes it
/// by the operands' qualifiers: a negation, ABS, NULLIF, an aggregate or window function that
/// returns its argument's values keeps it, and a sum, a difference and a conditional widen
/// them. Spark types an interval scaled by a number and an average as `DAY TO SECOND` or `YEAR
/// TO MONTH`, as MAKE_DT_INTERVAL and MAKE_YM_INTERVAL. `None` for any other function, and when
/// the result is not an interval.
pub(crate) fn function_qualifier(
    function: &str,
    operands: &[IntervalOperand],
) -> Option<IntervalQualifier> {
    // Spark scales an interval or averages intervals into the family's widest qualifier.
    let family_default = || operands.iter().find_map(|operand| operand.family_default());
    match (function, operands) {
        ("-" | "+" | "negative" | "positive" | "abs", [operand]) => operand.qualifier(),
        ("-" | "+" | "try_add" | "try_subtract", [_, _])
        | ("coalesce" | "nvl" | "ifnull" | "greatest" | "least", _) => {
            IntervalQualifier::widen(operands.iter().copied())
        }
        ("if" | "nvl2", [_, then, otherwise]) => IntervalQualifier::widen([*then, *otherwise]),
        // CASE: each condition precedes its result, and an ELSE result comes last.
        ("when", _) => IntervalQualifier::widen(
            operands
                .iter()
                .enumerate()
                .filter(|(index, _)| index % 2 == 1 || *index + 1 == operands.len())
                .map(|(_, result)| *result),
        ),
        (
            "nullif" | "sum" | "try_sum" | "min" | "max" | "first" | "first_value" | "last"
            | "last_value" | "any_value" | "lag" | "lead" | "nth_value",
            [value, ..],
        ) => value.qualifier(),
        ("*" | "/" | "try_multiply" | "try_divide", [_, _]) | ("avg" | "mean" | "try_avg", _) => {
            family_default()
        }
        ("make_dt_interval", _) => Some(IntervalQualifier::DAY_TO_SECOND),
        ("make_ym_interval", _) => Some(IntervalQualifier::YEAR_TO_MONTH),
        _ => None,
    }
}

/// The qualifier of a resolved day-time interval `expr` where the function that computed it
/// fixes it whatever its SQL: Spark subtracts two DATE values as a DAY interval, and two
/// TIMESTAMP or TIMESTAMP_NTZ values and MAKE_DT_INTERVAL as `DAY TO SECOND`.
pub(crate) fn resolved_qualifier(
    expr: &expr::Expr,
    schema: &DFSchemaRef,
) -> Option<IntervalQualifier> {
    let expr = match without_inherited_column_metadata(expr) {
        expr::Expr::Alias(alias) => alias.expr.as_ref(),
        expr => expr,
    };
    let expr = without_inherited_column_metadata(expr);
    // DATE, TIMESTAMP and interval subtractions are Spark functions here, so a native
    // subtraction with a TIMESTAMP_NTZ operand, beside another or an untyped NULL, gives a
    // `DAY TO SECOND` interval.
    if let expr::Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Minus,
        right,
    }) = expr
    {
        let timestamp = |operand: &expr::Expr| {
            operand
                .get_type(schema)
                .is_ok_and(|data_type| matches!(data_type, DataType::Timestamp(_, _)))
        };
        return (timestamp(left) || timestamp(right)).then_some(IntervalQualifier::DAY_TO_SECOND);
    }
    let expr::Expr::ScalarFunction(function) = expr else {
        return None;
    };
    let inner = function.func.inner();
    if inner.is::<SparkSubtractDates>() {
        Some(IntervalQualifier::DAY)
    } else if inner.is::<SparkSubtractTimestamps>() || inner.is::<SparkMakeDtInterval>() {
        Some(IntervalQualifier::DAY_TO_SECOND)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use sail_common::spec::IntervalFieldType::{Day, Hour, Minute, Month, Second, Year};

    use super::{IntervalOperand, IntervalQualifier, function_qualifier};

    fn qualifier(
        start: sail_common::spec::IntervalFieldType,
        end: sail_common::spec::IntervalFieldType,
    ) -> IntervalQualifier {
        IntervalQualifier::new(Some(start), Some(end)).unwrap_or(IntervalQualifier::DAY)
    }

    #[test]
    fn qualifiers_widen_from_the_earliest_start_to_the_latest_end() {
        let interval = |start, end| {
            let qualifier = qualifier(start, end);
            IntervalOperand::Interval {
                year_month: qualifier.is_year_month(),
                qualifier: Some(qualifier),
            }
        };
        assert_eq!(
            IntervalQualifier::new(Some(Hour), None),
            Some(qualifier(Hour, Hour))
        );
        assert_eq!(IntervalQualifier::new(Some(Second), Some(Day)), None);
        assert_eq!(IntervalQualifier::new(Some(Year), Some(Day)), None);
        assert_eq!(
            IntervalQualifier::widen([
                interval(Hour, Minute),
                IntervalOperand::NotInterval,
                interval(Day, Hour)
            ]),
            Some(qualifier(Day, Minute))
        );
        assert_eq!(
            IntervalQualifier::widen([
                interval(Day, Day),
                IntervalOperand::Interval {
                    year_month: false,
                    qualifier: None
                }
            ]),
            None
        );
        assert_eq!(
            IntervalQualifier::widen([interval(Year, Year), interval(Day, Day)]),
            None
        );
        assert_eq!(
            IntervalQualifier::widen([IntervalOperand::NotInterval]),
            None
        );
        assert_eq!(
            function_qualifier("*", &[interval(Month, Month), IntervalOperand::NotInterval]),
            Some(IntervalQualifier::YEAR_TO_MONTH)
        );
        assert_eq!(
            function_qualifier(
                "when",
                &[
                    IntervalOperand::NotInterval,
                    interval(Day, Day),
                    IntervalOperand::NotInterval,
                    interval(Hour, Hour),
                    interval(Minute, Minute),
                ]
            ),
            Some(qualifier(Day, Minute))
        );
    }
}
