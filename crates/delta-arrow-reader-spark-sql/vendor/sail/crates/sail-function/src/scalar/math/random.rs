// Modified from Sail v0.7.1 for the DAR Spark SQL runtime. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue, exec_err, internal_datafusion_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use rand::{RngExt, rng};

use super::xorshift::SparkXorShiftRandom;
use crate::error::{invalid_arg_count_exec_err, unsupported_data_types_exec_err};

#[derive(Debug)]
pub struct Random {
    signature: Signature,
    stream: Option<Arc<RandomStream>>,
}

#[derive(Debug)]
struct RandomStream {
    partition: usize,
    rng: Mutex<Option<SparkXorShiftRandom>>,
}

impl PartialEq for Random {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
            && match (&self.stream, &other.stream) {
                (None, None) => true,
                (Some(left), Some(right)) => Arc::ptr_eq(left, right),
                _ => false,
            }
    }
}

impl Eq for Random {}

impl Hash for Random {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        self.stream.as_ref().map(Arc::as_ptr).hash(state);
    }
}

impl Default for Random {
    fn default() -> Self {
        Self::new()
    }
}

impl Random {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Volatile),
            stream: None,
        }
    }

    /// Bind one expression occurrence to a fresh execution partition stream.
    pub fn for_partition(partition: usize) -> Self {
        Self {
            stream: Some(Arc::new(RandomStream {
                partition,
                rng: Mutex::new(None),
            })),
            ..Self::new()
        }
    }
}

impl ScalarUDFImpl for Random {
    fn name(&self) -> &str {
        "random"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(self.name(), DataType::Float64, false)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args, number_rows, ..
        } = args;
        if args.is_empty() {
            return invoke_no_seed(number_rows);
        }

        let [seed] = args.as_slice() else {
            return exec_err!(
                "random should be called with at most 1 argument, got {}",
                args.len()
            );
        };

        match seed {
            ColumnarValue::Scalar(scalar) => {
                let seed = match scalar {
                    ScalarValue::Int64(Some(value)) => *value,
                    ScalarValue::UInt64(Some(value)) => *value as i64,
                    ScalarValue::Int64(None) | ScalarValue::UInt64(None) | ScalarValue::Null => {
                        return invoke_no_seed(number_rows);
                    }
                    _ => return exec_err!("`random` expects an integer seed, got {scalar}"),
                };
                match &self.stream {
                    Some(stream) => {
                        let mut state = stream
                            .rng
                            .lock()
                            .map_err(|_| internal_datafusion_err!("RAND stream lock poisoned"))?;
                        let rng = state.get_or_insert_with(|| {
                            SparkXorShiftRandom::new(seed.wrapping_add(stream.partition as i64))
                        });
                        Ok(invoke_seeded(rng, number_rows))
                    }
                    None => Ok(invoke_seeded(
                        &mut SparkXorShiftRandom::new(seed),
                        number_rows,
                    )),
                }
            }
            _ => exec_err!(
                "`random` expects a scalar seed argument, got {}",
                seed.data_type()
            ),
        }
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            Ok(vec![])
        } else if arg_types.len() == 1 {
            if arg_types[0].is_signed_integer() {
                Ok(vec![DataType::Int64])
            } else if arg_types[0].is_unsigned_integer() {
                Ok(vec![DataType::UInt64])
            } else if arg_types[0].is_null() {
                Ok(vec![DataType::Null])
            } else {
                Err(unsupported_data_types_exec_err(
                    "random",
                    "Integer Type for seed",
                    arg_types,
                ))
            }
        } else {
            Err(invalid_arg_count_exec_err(
                "random",
                (0, 1),
                arg_types.len(),
            ))
        }
    }
}

fn invoke_seeded(rng: &mut SparkXorShiftRandom, number_rows: usize) -> ColumnarValue {
    let values = std::iter::repeat_with(|| rng.next_double()).take(number_rows);
    ColumnarValue::Array(Arc::new(Float64Array::from_iter_values(values)))
}

fn invoke_no_seed(number_rows: usize) -> Result<ColumnarValue> {
    let mut rng = rng();
    let values = std::iter::repeat_with(|| rng.random_range(0.0..1.0)).take(number_rows);
    let array = Float64Array::from_iter_values(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}
