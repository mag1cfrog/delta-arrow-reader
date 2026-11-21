// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, FieldRef, Int64Type};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::math::negative::SparkNegative as DataFusionNegative;

/// `ConfigOptions` snapshots with `execution.enable_ansi_mode` pinned. Negation
/// only reads the ANSI flag, so the two possible snapshots are built once and
/// shared process-wide; each batch clones an `Arc` instead of deep-cloning the
/// session config.
static ANSI_CONFIG: LazyLock<Arc<ConfigOptions>> = LazyLock::new(|| make_config(true));
static NON_ANSI_CONFIG: LazyLock<Arc<ConfigOptions>> = LazyLock::new(|| make_config(false));

fn make_config(ansi_mode: bool) -> Arc<ConfigOptions> {
    let mut config = ConfigOptions::default();
    config.execution.enable_ansi_mode = ansi_mode;
    Arc::new(config)
}

/// Spark unary minus / `negative(x)` that honors `spark.sql.ansi.enabled`.
///
/// The ANSI flag is captured at planning time (via the constructor) and
/// serialized through the physical codec, so the value the client requested
/// reaches every worker — unlike reading DataFusion's session-level
/// `execution.enable_ansi_mode`, which only reflects the driver's context.
///
/// Name, signature and return type are delegated to upstream
/// `datafusion_spark::SparkNegative`. Its kernel is invoked with the
/// pinned-ANSI [`ConfigOptions`] unless a BIGINT batch can be negated
/// without overflow.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkNegative {
    inner: DataFusionNegative,
    ansi_mode: bool,
}

impl Default for SparkNegative {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkNegative {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            inner: DataFusionNegative::new(),
            ansi_mode,
        }
    }
}

impl ScalarUDFImpl for SparkNegative {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [input] = datafusion_common::utils::take_function_args(self.name(), args.arg_fields)?;
        let nullable = input.is_nullable();
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.ansi_mode
            && let [ColumnarValue::Array(array)] = args.args.as_slice()
            && let Some(values) = array.as_primitive_opt::<Int64Type>()
            && !values.values().contains(&i64::MIN)
        {
            return Ok(ColumnarValue::Array(Arc::new(
                values.unary::<_, Int64Type>(i64::wrapping_neg),
            )));
        }
        let config_options = if self.ansi_mode {
            Arc::clone(&ANSI_CONFIG)
        } else {
            Arc::clone(&NON_ANSI_CONFIG)
        };
        let args = ScalarFunctionArgs {
            config_options,
            ..args
        };
        self.inner.invoke_with_args(args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, IntervalUnit};

    #[test]
    fn unary_negation_refines_only_the_delegated_parent_nullability() -> Result<()> {
        for data_type in [
            DataType::Int32,
            DataType::Int64,
            DataType::Decimal128(12, 3),
            DataType::Interval(IntervalUnit::YearMonth),
        ] {
            for ansi in [true, false] {
                for nullable in [true, false] {
                    let input = Arc::new(
                        Field::new("source_name", data_type.clone(), nullable)
                            .with_metadata([("source_tag".into(), "kept_on_input".into())].into()),
                    );
                    let args = || ReturnFieldArgs {
                        arg_fields: std::slice::from_ref(&input),
                        scalar_arguments: &[None],
                    };
                    let delegated = DataFusionNegative::new().return_field_from_args(args())?;
                    let actual = SparkNegative::new(ansi).return_field_from_args(args())?;
                    assert_eq!(
                        actual.as_ref(),
                        &delegated.as_ref().clone().with_nullable(nullable)
                    );
                    assert_eq!(actual.data_type(), &data_type);
                    assert!(actual.metadata().is_empty());
                }
            }
        }
        Ok(())
    }
}
