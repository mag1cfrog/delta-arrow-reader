use std::{
    error::Error,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use arrow::{
    datatypes::{DataType, Field, Fields, Schema, SchemaRef},
    error::ArrowError,
    ffi::FFI_ArrowSchema,
    record_batch::{RecordBatch, RecordBatchReader},
};
use datafusion::{common::DataFusionError, dataframe::DataFrame};
use delta_arrow_reader::{DeltaBatchStream, DeltaReaderError, error_source_kind};
use futures_util::{StreamExt, TryStreamExt, stream::BoxStream};
use pyo3::{
    exceptions::{
        PyKeyboardInterrupt, PyNotImplementedError, PyRuntimeError, PyTypeError, PyValueError,
    },
    prelude::*,
    types::PyCapsule,
};
use tracing::{Dispatch, instrument::WithSubscriber};

use crate::runtime::Runtime;
use crate::session::SessionState;

/// Hand a scan to a consumer that supports the Arrow stream protocol.
///
/// Created by `DeltaTable.scan()`; it has no public constructor. For normal
/// PyArrow usage, `DeltaTable.to_reader()` does this conversion for you.
///
/// Examples:
///     ```python
///     import pyarrow as pa
///
///     stream = table.scan(limit=10)
///     with pa.RecordBatchReader.from_stream(stream) as reader:
///         print(reader.read_all())
///     ```
///
/// Each stream can be exported once. After export, close the consuming reader
/// to release resources; closing this object does not close the consumer.
/// If you decide not to export it, call `close()` to release the scan.
#[pyclass(module = "delta_arrow_reader", frozen)]
pub(crate) struct RecordBatchStream {
    reader: Mutex<Option<BlockingBatchReader>>,
}

impl RecordBatchStream {
    pub(crate) fn new(stream: DeltaBatchStream, runtime: Arc<Runtime>) -> Self {
        Self::from_source(
            stream.schema(),
            BatchStream::Delta(Box::new(stream)),
            runtime,
        )
    }

    pub(crate) fn from_sql(frame: DataFrame, session: Arc<SessionState>) -> Self {
        let schema = Arc::new(frame.schema().as_arrow().clone());
        // Starting the physical plan can spawn work, so wait for the first pull.
        let stream = futures_util::stream::once(async move {
            let task = frame.task_ctx();
            let config = task
                .session_config()
                .clone()
                .with_extension(Arc::new(tracing::dispatcher::get_default(Clone::clone)));
            let plan = frame.create_physical_plan().await?;
            datafusion::physical_plan::execute_stream(
                plan,
                Arc::new(task.with_session_config(config)),
            )
        })
        .try_flatten()
        .boxed();
        let runtime = Arc::clone(&session.runtime);
        Self::from_source(
            schema,
            BatchStream::Sql {
                stream,
                _session: session,
            },
            runtime,
        )
    }

    fn from_source(schema: SchemaRef, stream: BatchStream, runtime: Arc<Runtime>) -> Self {
        Self {
            reader: Mutex::new(Some(BlockingBatchReader {
                schema,
                stream: Some(stream),
                runtime,
                terminal_error: None,
                dispatch: None,
                interrupted: Arc::new(AtomicBool::new(false)),
            })),
        }
    }

    pub(crate) fn into_reader(self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        py.import("pyarrow")?
            .getattr("RecordBatchReader")?
            .call_method1("from_stream", (Py::new(py, self)?,))
    }
}

#[pymethods]
impl RecordBatchStream {
    /// Transfer the stream to an Arrow consumer. Usually called by the consumer.
    ///
    /// Args:
    ///     requested_schema: `None` or an Arrow schema capsule identical to the
    ///         scan schema. This method does not cast or convert the data.
    ///
    /// Returns:
    ///     An Arrow stream capsule that owns the scan resources.
    ///
    /// Raises:
    ///     TypeError: The requested schema is not a capsule.
    ///     ValueError: The schema capsule is invalid or incompatible.
    ///     NotImplementedError: The consumer requests an alternate representation.
    ///     RuntimeError: The stream has already been exported or closed.
    ///
    /// A rejected schema request leaves the stream available for another attempt.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        crate::logging::reject_logging_reentry()?;
        let mut slot = self
            .reader
            .lock()
            .map_err(|_| PyRuntimeError::new_err("stream is unavailable"))?;
        let reader = slot
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("stream has already been exported or closed"))?;
        if let Some(requested) = requested_schema {
            validate_requested_schema(requested, &reader.schema)?;
        }
        let reader = slot
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("stream has already been exported or closed"))?;
        drop(slot);
        PyCapsule::new_with_value(
            py,
            crate::arrow_stream::export_reader(Box::new(reader)),
            c"arrow_array_stream",
        )
    }

    /// Release an unexported stream. Repeated calls have no effect.
    ///
    /// After export, call `close()` on the consuming reader instead.
    fn close(&self) -> PyResult<()> {
        crate::logging::reject_logging_reentry()?;
        let reader = self
            .reader
            .lock()
            .map_err(|_| PyRuntimeError::new_err("stream is unavailable"))?
            .take();
        // Drop scan resources after releasing the mutex.
        drop(reader);
        Ok(())
    }

    /// Return this exporter for use in a `with` block.
    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Close any unexported stream on leaving the block. Exceptions are not suppressed.
    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_value: &Bound<'_, PyAny>,
        _traceback: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        self.close()
    }
}

fn validate_requested_schema(requested: &Bound<'_, PyAny>, actual: &Schema) -> PyResult<()> {
    let capsule = requested.cast::<PyCapsule>().map_err(|_| {
        PyTypeError::new_err("requested_schema must be an arrow_schema capsule or None")
    })?;
    let pointer = capsule
        .pointer_checked(Some(c"arrow_schema"))
        .map_err(|_| PyValueError::new_err("invalid requested schema capsule"))?;
    // SAFETY: The named capsule promises an ArrowSchema and remains alive here.
    // Borrow without consuming it; no Python code runs while reading its fields.
    let ffi_schema = unsafe { pointer.cast::<FFI_ArrowSchema>().as_ref() };
    if ffi_schema.release.is_none() || ffi_schema.format.is_null() {
        return Err(PyValueError::new_err(
            "requested schema is released or invalid",
        ));
    }
    let requested = Schema::try_from(ffi_schema)
        .map_err(|_| PyValueError::new_err("invalid requested schema"))?;
    let same_metadata = actual.metadata == requested.metadata;
    if same_metadata && matches_requested_fields(&actual.fields, &requested.fields, false) {
        return Ok(());
    }
    if same_metadata && matches_requested_fields(&actual.fields, &requested.fields, true) {
        return Err(PyNotImplementedError::new_err(
            "alternate Arrow representations are not supported",
        ));
    }
    Err(PyValueError::new_err(
        "requested schema is incompatible with the scan schema",
    ))
}

// Alternate representations only select an error category; the exporter never casts.
fn matches_requested_fields(actual: &Fields, requested: &Fields, allow_alternates: bool) -> bool {
    actual.len() == requested.len()
        && actual
            .iter()
            .zip(requested)
            .all(|(a, b)| matches_requested_field(a, b, allow_alternates))
}

fn matches_requested_field(actual: &Field, requested: &Field, allow_alternates: bool) -> bool {
    actual.name() == requested.name()
        && actual.is_nullable() == requested.is_nullable()
        && actual.metadata() == requested.metadata()
        && matches_requested_type(actual.data_type(), requested.data_type(), allow_alternates)
}

fn matches_requested_type(actual: &DataType, requested: &DataType, allow_alternates: bool) -> bool {
    use DataType::*;
    if actual == requested {
        return true;
    }
    match (actual, requested) {
        (Struct(a), Struct(b)) => matches_requested_fields(a, b, allow_alternates),
        (List(a), List(b)) => matches_requested_field(a, b, allow_alternates),
        (Map(a, a_sorted), Map(b, b_sorted)) => {
            // PyArrow normalizes the map wrapper name from "key_value" to "entries".
            // Compare the key/value fields, wrapper metadata, and nullability.
            a_sorted == b_sorted
                && a.is_nullable() == b.is_nullable()
                && a.metadata() == b.metadata()
                && matches_requested_type(a.data_type(), b.data_type(), allow_alternates)
        }
        _ if !allow_alternates => false,
        (_, Dictionary(_, value)) => matches_requested_type(actual, value, true),
        (_, RunEndEncoded(_, values)) => matches_requested_type(actual, values.data_type(), true),
        (List(a), LargeList(b) | ListView(b) | LargeListView(b) | FixedSizeList(b, _)) => {
            matches_requested_field(a, b, true)
        }
        (Timestamp(_, a_zone), Timestamp(_, b_zone)) => a_zone == b_zone,
        (Date32, Date64) => true,
        (
            Decimal128(a_precision, a_scale),
            Decimal32(b_precision, b_scale)
            | Decimal64(b_precision, b_scale)
            | Decimal128(b_precision, b_scale)
            | Decimal256(b_precision, b_scale),
        ) => a_precision == b_precision && a_scale == b_scale,
        _ => {
            (actual.is_integer() && requested.is_integer())
                || (actual.is_floating() && requested.is_floating())
                || (actual.is_string() && requested.is_string())
                || (actual.is_binary() && requested.is_binary())
        }
    }
}

enum BatchStream {
    Delta(Box<DeltaBatchStream>),
    Sql {
        stream: BoxStream<'static, Result<RecordBatch, DataFusionError>>,
        _session: Arc<SessionState>,
    },
}

impl BatchStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, BatchReadError> {
        match self {
            Self::Delta(stream) => stream.next_batch().await.map_err(BatchReadError::Delta),
            Self::Sql { stream, .. } => stream
                .try_next()
                .await
                .inspect_err(|error| {
                    if !sql_error_has_core_diagnostic(error) {
                        tracing::error!(target: "delta_arrow_reader_python",
                            event = "sql_execution.failed", operation = "sql_execution", outcome = "failed",
                            error_phase = "datafusion", error_code = "sql_execution",
                            error_reason = "sql_execution_failed", error_source_kind = error_source_kind(error));
                    }
                })
                .map_err(BatchReadError::Sql),
        }
    }
}

// SQL plans in this binding only obtain reader errors from DAR-owned provider boundaries.
fn sql_error_has_core_diagnostic(error: &DataFusionError) -> bool {
    let mut error: &(dyn Error + 'static) = error;
    for _ in 0..16 {
        let datafusion = error
            .downcast_ref::<DataFusionError>()
            .or_else(|| {
                error
                    .downcast_ref::<Box<DataFusionError>>()
                    .map(AsRef::as_ref)
            })
            .or_else(|| {
                error
                    .downcast_ref::<Arc<DataFusionError>>()
                    .map(AsRef::as_ref)
            });
        error = if let Some(datafusion) = datafusion {
            match datafusion {
                DataFusionError::External(source) => source.as_ref(),
                DataFusionError::Context(_, source) | DataFusionError::Diagnostic(_, source) => {
                    source.as_ref()
                }
                DataFusionError::Shared(source) => source.as_ref(),
                DataFusionError::ArrowError(source, _) => source.as_ref(),
                // Collections may also contain failures from unrelated SQL operators.
                _ => return false,
            }
        } else if let Some(ArrowError::ExternalError(source)) = error.downcast_ref::<ArrowError>() {
            source.as_ref()
        } else {
            return error.is::<DeltaReaderError>()
                || error.is::<Box<DeltaReaderError>>()
                || error.is::<Arc<DeltaReaderError>>();
        };
    }
    false
}

// Retain typed causes for diagnostics while keeping Arrow's messages redacted.
enum BatchReadError {
    Delta(DeltaReaderError),
    Sql(DataFusionError),
    Interrupted,
}

impl fmt::Display for BatchReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delta(error) => fmt::Display::fmt(error, formatter),
            Self::Sql(_) => {
                formatter.write_str("delta reader error: phase=datafusion code=sql_execution")
            }
            Self::Interrupted => formatter.write_str(
                "delta reader error: phase=execution code=python_signal reason=reader_wait_interrupted",
            ),
        }
    }
}

impl fmt::Debug for BatchReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error for BatchReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Delta(error) => Some(error),
            Self::Sql(error) => Some(error),
            Self::Interrupted => None,
        }
    }
}

/// Adapts an async batch stream to Arrow's synchronous reader callbacks.
struct BlockingBatchReader {
    schema: SchemaRef,
    stream: Option<BatchStream>,
    runtime: Arc<Runtime>,
    // Repeat failures so a failed scan never appears successfully exhausted.
    terminal_error: Option<Arc<BatchReadError>>,
    // Reuse the first pull's subscriber, including for later worker events.
    dispatch: Option<Dispatch>,
    interrupted: Arc<AtomicBool>,
}

impl Iterator for BlockingBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = &self.terminal_error {
            return Some(Err(ArrowError::ExternalError(Box::new(Arc::clone(error)))));
        }
        let stream = self.stream.as_mut()?;
        let result = Python::attach(|py| {
            let mut read = || {
                let dispatch = match &mut self.dispatch {
                    Some(dispatch) => dispatch,
                    slot => slot.insert(crate::logging::new_stream_dispatch(Arc::clone(
                        &self.interrupted,
                    ))?),
                };
                self.runtime.wait_with_interrupts(
                    py,
                    stream.next_batch().with_subscriber(dispatch.clone()),
                    || {
                        self.interrupted
                            .load(Ordering::Relaxed)
                            .then(|| PyKeyboardInterrupt::new_err(()))
                    },
                )
            };
            // Arrow reports control flow as a safe error, without retaining a traceback.
            read().map_err(|_| BatchReadError::Interrupted)
        })
        .flatten();
        match result {
            Ok(Some(batch)) => Some(Ok(batch)),
            Ok(None) => {
                self.stream = None;
                self.dispatch = None;
                None
            }
            Err(error) => {
                self.stream = None;
                self.dispatch = None;
                let error = Arc::new(error);
                self.terminal_error = Some(Arc::clone(&error));
                Some(Err(ArrowError::ExternalError(Box::new(error))))
            }
        }
    }
}

impl RecordBatchReader for BlockingBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use delta_arrow_reader::{DeltaScanExecutionOptions, error_source_kind};

    #[test]
    fn rejected_schema_requests_leave_the_stream_available() -> PyResult<()> {
        use pyo3::types::PyString;
        use std::collections::HashMap;

        Python::initialize();
        Python::attach(|py| {
            let table = crate::binding_tests::table(py)?;
            let actual = table
                .extract::<PyRef<'_, crate::DeltaTable>>()?
                .table
                .schema();
            let metadata = HashMap::from([("secret".into(), "value".into())]);
            let with_first_field = |field| {
                let mut fields = actual.fields().to_vec();
                fields[0] = Arc::new(field);
                Schema::new_with_metadata(fields, actual.metadata().clone())
            };
            let first = actual.field(0);
            let mut cases = vec![(
                PyString::new(py, "secret-object").into_any(),
                py.get_type::<PyTypeError>(),
            )];
            for schema in [
                Schema::empty(),
                with_first_field(first.clone().with_name("secret-field")),
                with_first_field(first.clone().with_nullable(!first.is_nullable())),
                actual.as_ref().clone().with_metadata(metadata.clone()),
                with_first_field(first.clone().with_metadata(metadata)),
            ] {
                cases.push((
                    PyCapsule::new_with_value(
                        py,
                        FFI_ArrowSchema::try_from(&schema).unwrap(),
                        c"arrow_schema",
                    )?
                    .into_any(),
                    py.get_type::<PyValueError>(),
                ));
            }
            for (requested, expected_error) in cases {
                let stream = table.call_method0("scan")?;
                let error = stream
                    .call_method1("__arrow_c_stream__", (requested,))
                    .unwrap_err();
                assert!(error.get_type(py).is(&expected_error), "{error}");
                assert!(!error.to_string().contains("secret"), "{error}");
                let capsule = stream.call_method1(
                    "__arrow_c_stream__",
                    (table.call_method0("__arrow_c_schema__")?,),
                )?;
                capsule
                    .cast::<PyCapsule>()?
                    .pointer_checked(Some(c"arrow_array_stream"))?;
            }
            Ok(())
        })
    }

    #[test]
    fn requested_schema_types_distinguish_alternates_from_incompatible_changes() {
        use DataType::*;
        use arrow::datatypes::TimeUnit;

        let field = |name, data_type| Arc::new(Field::new(name, data_type, true));
        let list = List(field("element", Utf8));
        let nested = Struct(vec![field("child", Int64)].into());
        let map = |entries, key, key_type, sorted| {
            Field::new_map(
                "id",
                entries,
                Field::new(key, key_type, false),
                Field::new("value", Int64, true),
                sorted,
                true,
            )
            .data_type()
            .clone()
        };
        let mapping = map("key_value", "key", Utf8, false);
        for (actual, requested, alternate) in [
            (Int64, Int32, true),
            (Int64, UInt64, true),
            (Int64, Dictionary(Box::new(Int8), Box::new(Int64)), true),
            (
                Int64,
                RunEndEncoded(
                    Arc::new(Field::new("run_ends", Int16, false)),
                    field("values", Int64),
                ),
                true,
            ),
            (Int64, Utf8, false),
            (Utf8, LargeUtf8, true),
            (Utf8, Utf8View, true),
            (Binary, LargeBinary, true),
            (Float32, Float64, true),
            (Decimal128(10, 2), Decimal256(10, 2), true),
            (Decimal128(10, 2), Decimal128(11, 2), false),
            (
                Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                true,
            ),
            (
                Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            (Date32, Date64, true),
            (list.clone(), LargeList(field("element", LargeUtf8)), true),
            (list, List(field("secret-child", Utf8)), false),
            (
                nested.clone(),
                Struct(vec![field("child", Int32)].into()),
                true,
            ),
            (
                nested,
                Struct(vec![field("secret-child", Int64)].into()),
                false,
            ),
            (
                mapping.clone(),
                map("entries", "key", LargeUtf8, false),
                true,
            ),
            (
                mapping.clone(),
                map("entries", "secret-key", Utf8, false),
                false,
            ),
            (mapping.clone(), map("entries", "key", Utf8, true), false),
        ] {
            assert!(matches_requested_type(&actual, &actual, false));
            assert!(
                !matches_requested_type(&actual, &requested, false),
                "{actual:?} -> {requested:?}"
            );
            assert_eq!(
                matches_requested_type(&actual, &requested, true),
                alternate,
                "{actual:?} -> {requested:?}"
            );
        }
        assert!(matches_requested_type(
            &mapping,
            &map("entries", "key", Utf8, false),
            false
        ));
    }

    fn failed_reader(error: BatchReadError, runtime: &Arc<Runtime>) -> BlockingBatchReader {
        BlockingBatchReader {
            schema: Arc::new(Schema::empty()),
            stream: None,
            runtime: Arc::clone(runtime),
            terminal_error: Some(Arc::new(error)),
            dispatch: None,
            interrupted: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn sql_error_replays_keep_distinct_typed_causes_and_safe_messages() {
        let runtime = Arc::new(Runtime::new().unwrap());
        for (source, kind) in [
            (
                DataFusionError::IoError(io::Error::new(io::ErrorKind::NotFound, "secret path")),
                "not_found",
            ),
            (
                DataFusionError::IoError(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "secret credentials",
                )),
                "permission_denied",
            ),
            (
                DataFusionError::Shared(Arc::new(DataFusionError::Context(
                    "secret SQL".into(),
                    Box::new(DataFusionError::ArrowError(
                        Box::new(ArrowError::DivideByZero),
                        Some("secret backtrace".into()),
                    )),
                ))),
                "divide_by_zero",
            ),
            (
                DataFusionError::External(Box::new(Unformattable)),
                "unknown",
            ),
        ] {
            let mut reader = failed_reader(BatchReadError::Sql(source), &runtime);
            for _ in 0..3 {
                let error = reader.next().unwrap().unwrap_err();
                assert_eq!(
                    error.to_string(),
                    "External error: delta reader error: phase=datafusion code=sql_execution"
                );
                assert!(!format!("{error:?}").contains("secret"));
                assert_eq!(error_source_kind(&error), kind);
                assert!(
                    error
                        .source()
                        .unwrap()
                        .source()
                        .unwrap()
                        .is::<DataFusionError>()
                );
            }
        }
    }

    #[test]
    fn direct_and_sql_errors_retain_the_original_reader_error() {
        let runtime = Arc::new(Runtime::new().unwrap());
        for sql in [false, true] {
            let source = DeltaScanExecutionOptions::new()
                .with_output_buffer_batches_per_partition(0)
                .unwrap_err();
            let failure = if sql {
                BatchReadError::Sql(DataFusionError::Context(
                    "secret operator".into(),
                    Box::new(DataFusionError::External(Box::new(source))),
                ))
            } else {
                BatchReadError::Delta(source)
            };
            let mut reader = failed_reader(failure, &runtime);
            for _ in 0..3 {
                let error = reader.next().unwrap().unwrap_err();
                let reader_error =
                    std::iter::successors(Some(&error as &(dyn Error + 'static)), |&error| {
                        error.source()
                    })
                    .find_map(|error| error.downcast_ref::<DeltaReaderError>())
                    .unwrap();
                assert_eq!(reader_error.code(), "invalid_configuration");
                assert_eq!(
                    reader_error.reason(),
                    "output_buffer_batches_per_partition_must_be_positive"
                );
                assert!(!format!("{error} {error:?}").contains("secret"));
            }
        }
    }

    #[test]
    fn interruption_replays_preserve_the_arrow_message() {
        let runtime = Arc::new(Runtime::new().unwrap());
        let mut reader = failed_reader(BatchReadError::Interrupted, &runtime);
        for _ in 0..3 {
            let error = reader.next().unwrap().unwrap_err();
            assert_eq!(
                error.to_string(),
                "External error: delta reader error: phase=execution code=python_signal reason=reader_wait_interrupted"
            );
            assert!(!format!("{error:?}").contains("secret"));
            assert!(error.source().unwrap().source().is_none());
        }
    }

    #[test]
    fn sql_ownership_recognizes_typed_wrappers_without_hiding_other_failures() {
        let reader_error = || {
            DeltaScanExecutionOptions::new()
                .with_output_buffer_batches_per_partition(0)
                .unwrap_err()
        };
        let wrapped = DataFusionError::Shared(Arc::new(DataFusionError::Context(
            "secret context".into(),
            Box::new(DataFusionError::ArrowError(
                Box::new(ArrowError::ExternalError(Box::new(
                    DataFusionError::External(Box::new(reader_error())),
                ))),
                Some("secret backtrace".into()),
            )),
        )));
        assert!(sql_error_has_core_diagnostic(&wrapped));
        assert!(sql_error_has_core_diagnostic(&DataFusionError::External(
            Box::new(Box::new(reader_error()))
        )));
        assert!(sql_error_has_core_diagnostic(&DataFusionError::External(
            Box::new(Arc::new(reader_error()))
        )));
        let collection =
            DataFusionError::External(Box::new(Arc::new(DataFusionError::Collection(vec![
                wrapped,
                DataFusionError::ArrowError(Box::new(ArrowError::DivideByZero), None),
            ]))));
        assert!(!sql_error_has_core_diagnostic(&collection));
        assert_eq!(error_source_kind(&collection), "multiple_errors");
        for error in [
            DataFusionError::Execution("delta reader error: secret".into()),
            DataFusionError::External(Box::new(Unformattable)),
        ] {
            assert!(!sql_error_has_core_diagnostic(&error));
        }
        let mut deep = DataFusionError::External(Box::new(reader_error()));
        for _ in 0..32 {
            deep = DataFusionError::Context("secret context".into(), Box::new(deep));
        }
        assert!(!sql_error_has_core_diagnostic(&deep));
        assert_eq!(error_source_kind(&deep), "source_chain_limit");
    }

    #[derive(Debug)]
    struct Unformattable;

    impl fmt::Display for Unformattable {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("raw dependency errors must not be formatted")
        }
    }

    impl Error for Unformattable {}
}
