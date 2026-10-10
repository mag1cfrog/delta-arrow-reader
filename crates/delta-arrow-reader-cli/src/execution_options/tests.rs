use std::process::ExitCode;

use serde_json::json;
use snafu::ResultExt;

use super::*;
use crate::{ConfigurationSnafu, Error, InputJsonSnafu, input::JsonObject};

fn parse(text: &str) -> Result<DeltaScanExecutionOptions, Error> {
    serde_json::from_str::<JsonObject<ExecutionOptionsInput>>(text)
        .map_err(|_| InputJsonSnafu.build())?
        .0
        .into_execution_options()
        .context(ConfigurationSnafu)
}

#[test]
fn omitted_fields_keep_native_defaults() {
    let defaults = DeltaScanExecutionOptions::new();
    assert_eq!(parse("{}").unwrap(), defaults);
    assert_eq!(parse(r#"{"parquet_backend":"direct"}"#).unwrap(), defaults);
    assert_eq!(
        parse(r#"{"prefetch_files_per_partition":0}"#).unwrap(),
        defaults.with_prefetch_files_per_partition(0)
    );
}

#[test]
fn maps_every_field_to_its_native_option() {
    let options = parse(
        r#"{
            "parquet_backend":"delta_kernel",
            "max_concurrent_file_reads_per_scan":7,
            "max_concurrent_file_reads_per_partition":11,
            "output_buffer_batches_per_partition":13,
            "prefetch_files_per_partition":0,
            "parquet_metadata_size_hint_bytes":8192,
            "parquet_full_file_read_threshold_bytes":32768
        }"#,
    )
    .unwrap();
    assert_eq!(options.parquet_backend(), ParquetReaderBackend::DeltaKernel);
    assert_eq!(options.max_concurrent_file_reads_per_scan(), Some(7));
    assert_eq!(options.max_concurrent_file_reads_per_partition(), 11);
    assert_eq!(options.output_buffer_batches_per_partition(), 13);
    assert_eq!(options.prefetch_files_per_partition(), 0);
    assert_eq!(options.parquet_metadata_size_hint_bytes(), Some(8192));
    assert_eq!(
        options.parquet_full_file_read_threshold_bytes(),
        Some(32768)
    );
}

#[test]
fn explicit_null_clears_each_optional_value() {
    let options = parse(
        r#"{
            "max_concurrent_file_reads_per_scan":null,
            "parquet_metadata_size_hint_bytes":null,
            "parquet_full_file_read_threshold_bytes":null
        }"#,
    )
    .unwrap();
    assert_eq!(options.max_concurrent_file_reads_per_scan(), None);
    assert_eq!(options.parquet_metadata_size_hint_bytes(), None);
    assert_eq!(options.parquet_full_file_read_threshold_bytes(), None);
    assert_ne!(options, parse("{}").unwrap());
}

#[test]
fn rejects_wrong_shapes_types_and_duplicate_fields() {
    for text in [
        "",
        "null",
        "true",
        "1",
        r#""direct""#,
        "[]",
        "{}{}",
        "[null,null,null,null,null,null,null]",
        r#"{"unknown":1}"#,
        r#"{"experimental_intra_page_reads":false}"#,
        r#"{"parquet_backend":"DIRECT"}"#,
        r#"{"parquet_backend":"unknown"}"#,
        r#"{"parquet_backend":" direct "}"#,
        r#"{"parquet_backend":null}"#,
        r#"{"parquet_backend":1}"#,
        r#"{"parquet_backend":true}"#,
        r#"{"parquet_backend":["direct"]}"#,
        r#"{"parquet_backend":{"direct":null}}"#,
        r#"{"parquet_backend":"direct","parquet_backend":"delta_kernel"}"#,
    ] {
        assert!(matches!(parse(text), Err(Error::InputJson)), "{text}");
    }
    for (field, nullable) in [
        ("max_concurrent_file_reads_per_scan", true),
        ("max_concurrent_file_reads_per_partition", false),
        ("output_buffer_batches_per_partition", false),
        ("prefetch_files_per_partition", false),
        ("parquet_metadata_size_hint_bytes", true),
        ("parquet_full_file_read_threshold_bytes", true),
    ] {
        for value in [
            "-1", "-0", "1.0", "1e0", "true", "false", r#""1""#, "[]", "{}",
        ] {
            let text = format!(r#"{{"{field}":{value}}}"#);
            assert!(matches!(parse(&text), Err(Error::InputJson)), "{text}");
        }
        let overflow = usize::MAX as u128 + 1;
        let text = format!(r#"{{"{field}":{overflow}}}"#);
        assert!(matches!(parse(&text), Err(Error::InputJson)), "{text}");
        if !nullable {
            let text = format!(r#"{{"{field}":null}}"#);
            assert!(matches!(parse(&text), Err(Error::InputJson)), "{text}");
        }
        for value in if nullable {
            &["1", "null"][..]
        } else {
            &["1"][..]
        } {
            let escaped_key = format!(r#"\u{:04x}{}"#, field.as_bytes()[0], &field[1..]);
            for duplicate in [field, &escaped_key] {
                let text = format!(r#"{{"{field}":{value},"{duplicate}":{value}}}"#);
                assert!(matches!(parse(&text), Err(Error::InputJson)), "{text}");
            }
        }
    }
}

#[test]
fn native_setters_enforce_zero_and_capacity_limits() {
    for field in [
        "max_concurrent_file_reads_per_scan",
        "max_concurrent_file_reads_per_partition",
        "output_buffer_batches_per_partition",
        "parquet_metadata_size_hint_bytes",
        "parquet_full_file_read_threshold_bytes",
    ] {
        let text = format!(r#"{{"{field}":0}}"#);
        let error = parse(&text).unwrap_err();
        assert!(matches!(error, Error::Configuration { .. }), "{text}");
        assert_eq!(error.exit_code(), ExitCode::from(2));
    }
    let maximum = tokio::sync::Semaphore::MAX_PERMITS;
    let options = parse(
        &json!({
            "max_concurrent_file_reads_per_scan":maximum,
            "max_concurrent_file_reads_per_partition":maximum,
            "output_buffer_batches_per_partition":maximum,
        })
        .to_string(),
    )
    .unwrap();
    assert_eq!(options.max_concurrent_file_reads_per_scan(), Some(maximum));
    assert_eq!(options.max_concurrent_file_reads_per_partition(), maximum);
    assert_eq!(options.output_buffer_batches_per_partition(), maximum);
    for field in [
        "max_concurrent_file_reads_per_scan",
        "max_concurrent_file_reads_per_partition",
        "output_buffer_batches_per_partition",
    ] {
        let text = format!(r#"{{"{field}":{}}}"#, maximum + 1);
        let error = parse(&text).unwrap_err();
        assert!(matches!(error, Error::Configuration { .. }), "{text}");
        assert_eq!(error.exit_code(), ExitCode::from(2));
    }
}

#[test]
fn size_and_prefetch_values_accept_the_full_usize_range() {
    let options = parse(
        &json!({
            "prefetch_files_per_partition":usize::MAX,
            "parquet_metadata_size_hint_bytes":usize::MAX,
            "parquet_full_file_read_threshold_bytes":usize::MAX,
        })
        .to_string(),
    )
    .unwrap();
    assert_eq!(options.prefetch_files_per_partition(), usize::MAX);
    assert_eq!(options.parquet_metadata_size_hint_bytes(), Some(usize::MAX));
    assert_eq!(
        options.parquet_full_file_read_threshold_bytes(),
        Some(usize::MAX)
    );
}

#[test]
fn native_configuration_errors_keep_their_diagnostics_and_source() {
    use std::error::Error as _;

    let native = DeltaScanExecutionOptions::new()
        .with_output_buffer_batches_per_partition(0)
        .unwrap_err();
    let error = parse(r#"{"output_buffer_batches_per_partition":0}"#).unwrap_err();
    assert_eq!(error.exit_code(), ExitCode::from(2));
    assert_eq!(
        error.diagnostic(),
        json!({"phase":native.phase().as_str(), "code":native.code(), "message":native.to_string()})
    );
    assert!(error.source().unwrap().is::<DeltaReaderError>());
}
