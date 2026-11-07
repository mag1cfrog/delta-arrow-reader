//! Shared native-only producer instrumentation for literal regexp preparation tests.
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Result, exec_err};
use datafusion::logical_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use serde_json::{Value, json};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[expect(
    clippy::enum_variant_names,
    reason = "Variant Debug names are frozen observation labels."
)]
pub(crate) enum Kind {
    ImmutableNoFold,
    VolatileFold,
    AsyncFold,
    ImmutableFold,
}

#[derive(Debug, Default)]
pub(crate) struct Counts {
    pub(crate) calls: AtomicUsize,
    pub(crate) phase: AtomicUsize,
    events: Mutex<Vec<Value>>,
}
impl Counts {
    #[expect(
        clippy::expect_used,
        reason = "A poisoned instrumentation mutex must fail the test."
    )]
    pub(crate) fn snapshot(&self) -> Value {
        json!({"calls":self.calls.load(Ordering::SeqCst),
            "events":self.events.lock().expect("producer event mutex").clone()})
    }
}

#[derive(Debug)]
struct Producer {
    kind: Kind,
    signature: Signature,
    counts: Arc<Counts>,
}
// Counters observe calls; they do not change the identity producer's value semantics.
impl PartialEq for Producer {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind && self.signature == other.signature
    }
}
impl Eq for Producer {}
impl Hash for Producer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.signature.hash(state);
    }
}
impl Producer {
    fn capture(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.counts.calls.fetch_add(1, Ordering::SeqCst);
        self.counts
            .events
            .lock()
            .map_err(|_| {
                datafusion::common::DataFusionError::Internal(
                    "producer event mutex poisoned".into(),
                )
            })?
            .push(json!({
                "phase":self.counts.phase.load(Ordering::SeqCst),"number_rows":args.number_rows,
                "arguments":format!("{:?}",args.args),"arg_fields":format!("{:?}",args.arg_fields),
                "return_field":format!("{:?}",args.return_field)}));
        Ok(args.args[0].clone())
    }
}
impl ScalarUDFImpl for Producer {
    fn name(&self) -> &str {
        match self.kind {
            Kind::ImmutableNoFold => "literal_immutable_no_fold",
            Kind::VolatileFold => "literal_volatile_fold",
            Kind::AsyncFold => "literal_async_fold",
            Kind::ImmutableFold => "literal_counted_producer",
        }
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn supports_constant_folding(&self) -> bool {
        self.kind != Kind::ImmutableNoFold
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        Ok(types[0].clone())
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::clone(&args.arg_fields[0]))
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.kind == Kind::AsyncFold {
            return exec_err!("literal producer requires async invocation");
        }
        self.capture(args)
    }
}
#[async_trait::async_trait]
impl AsyncScalarUDFImpl for Producer {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.capture(args)
    }
}

pub(crate) fn producer(kind: Kind) -> (ScalarUDF, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    let value = Producer {
        kind,
        signature: Signature::any(
            1,
            if kind == Kind::VolatileFold {
                Volatility::Volatile
            } else {
                Volatility::Immutable
            },
        ),
        counts: Arc::clone(&counts),
    };
    let udf = if kind == Kind::AsyncFold {
        AsyncScalarUDF::new(Arc::new(value)).into_scalar_udf()
    } else {
        ScalarUDF::from(value)
    };
    (udf, counts)
}
