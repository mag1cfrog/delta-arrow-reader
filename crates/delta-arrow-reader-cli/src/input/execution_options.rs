//! CLI overrides for the reader's scan execution settings.
//!
//! Execution settings control how a scan reads and buffers data, independently
//! of which rows and columns it selects. A file can specify a subset of settings:
//! omitted fields retain the reader's defaults, and supported null values clear
//! optional settings. Validation limits come from the reader's configuration API.
//!
//! For example, a file passed to `--execution-options-file` can contain:
//!
//! ```json
//! {
//!   "prefetch_files_per_partition": 0,
//!   "parquet_metadata_size_hint_bytes": null
//! }
//! ```
//!
//! This sets prefetch to zero and clears the metadata size hint. Omitting
//! `parquet_metadata_size_hint_bytes` instead keeps the reader's default hint.

use delta_arrow_reader::{DeltaReaderError, DeltaScanExecutionOptions, ParquetReaderBackend};
use serde::{Deserialize, Deserializer, de::value::StringDeserializer};

use crate::{
    diagnostics::InputCorrection,
    input::{JsonInputKind, deserialize_present_field},
};

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ExecutionOptionsInput {
    #[serde(deserialize_with = "deserialize_parquet_backend")]
    parquet_backend: Option<ParquetReaderBackend>,
    #[serde(deserialize_with = "deserialize_present_field")]
    max_concurrent_file_reads_per_scan: Option<Option<usize>>,
    #[serde(deserialize_with = "deserialize_present_field")]
    max_concurrent_file_reads_per_partition: Option<usize>,
    #[serde(deserialize_with = "deserialize_present_field")]
    output_buffer_batches_per_partition: Option<usize>,
    #[serde(deserialize_with = "deserialize_present_field")]
    prefetch_files_per_partition: Option<usize>,
    #[serde(deserialize_with = "deserialize_present_field")]
    parquet_metadata_size_hint_bytes: Option<Option<usize>>,
    #[serde(deserialize_with = "deserialize_present_field")]
    parquet_full_file_read_threshold_bytes: Option<Option<usize>>,
}

impl ExecutionOptionsInput {
    pub(crate) fn into_execution_options(
        self,
    ) -> Result<DeltaScanExecutionOptions, DeltaReaderError> {
        let mut options = DeltaScanExecutionOptions::new();
        if let Some(backend) = self.parquet_backend {
            options = options.with_parquet_backend(backend);
        }
        if let Some(limit) = self.max_concurrent_file_reads_per_scan {
            options = options.with_max_concurrent_file_reads_per_scan(limit)?;
        }
        if let Some(limit) = self.max_concurrent_file_reads_per_partition {
            options = options.with_max_concurrent_file_reads_per_partition(limit)?;
        }
        if let Some(capacity) = self.output_buffer_batches_per_partition {
            options = options.with_output_buffer_batches_per_partition(capacity)?;
        }
        if let Some(count) = self.prefetch_files_per_partition {
            options = options.with_prefetch_files_per_partition(count);
        }
        if let Some(bytes) = self.parquet_metadata_size_hint_bytes {
            options = options.with_parquet_metadata_size_hint_bytes(bytes)?;
        }
        if let Some(bytes) = self.parquet_full_file_read_threshold_bytes {
            options = options.with_parquet_full_file_read_threshold_bytes(bytes)?;
        }
        Ok(options)
    }
}

pub(crate) fn execution_options_correction(reason: &str) -> Option<InputCorrection> {
    let field = match reason {
        "max_concurrent_file_reads_per_scan_must_be_positive"
        | "max_concurrent_file_reads_per_scan_exceeds_tokio_max_permits" => {
            "max_concurrent_file_reads_per_scan"
        }
        "max_concurrent_file_reads_per_partition_must_be_positive"
        | "max_concurrent_file_reads_per_partition_exceeds_tokio_max_permits" => {
            "max_concurrent_file_reads_per_partition"
        }
        "output_buffer_batches_per_partition_must_be_positive"
        | "output_buffer_batches_per_partition_exceeds_tokio_max_permits" => {
            "output_buffer_batches_per_partition"
        }
        "parquet_metadata_size_hint_bytes_must_be_positive" => "parquet_metadata_size_hint_bytes",
        "parquet_full_file_read_threshold_bytes_must_be_positive" => {
            "parquet_full_file_read_threshold_bytes"
        }
        _ => return None,
    };
    Some(InputCorrection {
        argument: JsonInputKind::ExecutionOptions.argument().to_owned(),
        field_path: Some(format!("/{field}")),
        expected: describe_integer_requirement(field)?,
        supplied: None,
        hint: "Set this field within the accepted range, or omit it to use the default.",
    })
}

pub(super) fn describe_integer_requirement(field: &str) -> Option<String> {
    let (minimum, maximum, nullable) = match field {
        "max_concurrent_file_reads_per_scan" => (1, tokio::sync::Semaphore::MAX_PERMITS, true),
        "max_concurrent_file_reads_per_partition" | "output_buffer_batches_per_partition" => {
            (1, tokio::sync::Semaphore::MAX_PERMITS, false)
        }
        "parquet_metadata_size_hint_bytes" | "parquet_full_file_read_threshold_bytes" => {
            (1, usize::MAX, true)
        }
        "prefetch_files_per_partition" => (0, usize::MAX, false),
        _ => return None,
    };
    Some(format!(
        "JSON integer from {minimum} to {maximum}{}",
        if nullable { ", or null" } else { "" },
    ))
}

fn deserialize_parquet_backend<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ParquetReaderBackend>, D::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum ParquetBackendName {
        Direct,
        DeltaKernel,
    }

    let name = String::deserialize(deserializer)?;
    let backend = match ParquetBackendName::deserialize(StringDeserializer::<D::Error>::new(name))?
    {
        ParquetBackendName::Direct => ParquetReaderBackend::Direct,
        ParquetBackendName::DeltaKernel => ParquetReaderBackend::DeltaKernel,
    };
    Ok(Some(backend))
}

#[cfg(test)]
mod tests {
    use std::process::ExitCode;

    use serde_json::json;
    use snafu::ResultExt;

    use super::*;
    use crate::{ConfigurationSnafu, Error, input::JsonObject};

    fn parse_with_schema_assertions(text: &str) -> Result<DeltaScanExecutionOptions, Error> {
        let result = crate::input::parse_json::<JsonObject<ExecutionOptionsInput>>(
            text.as_bytes(),
            JsonInputKind::ExecutionOptions,
        )?
        .0
        .into_execution_options()
        .context(ConfigurationSnafu);
        if result.is_ok() {
            static SCHEMA_VALIDATOR: std::sync::LazyLock<jsonschema::Validator> =
                std::sync::LazyLock::new(|| {
                    jsonschema::validator_for(
                        &JsonInputKind::ExecutionOptions.schema_document()["schema"],
                    )
                    .unwrap()
                });
            SCHEMA_VALIDATOR
                .validate(&serde_json::from_str(text).unwrap())
                .unwrap();
            assert!(
                crate::input::schema_errors::diagnose_schema_error(
                    text.as_bytes(),
                    JsonInputKind::ExecutionOptions
                )
                .is_none(),
                "{text}"
            );
        }
        result
    }

    #[test]
    fn omitted_fields_keep_native_defaults() {
        let defaults = DeltaScanExecutionOptions::new();
        assert_eq!(parse_with_schema_assertions("{}").unwrap(), defaults);
        assert_eq!(
            parse_with_schema_assertions(r#"{"parquet_backend":"direct"}"#).unwrap(),
            defaults
        );
        assert_eq!(
            parse_with_schema_assertions(r#"{"prefetch_files_per_partition":0}"#).unwrap(),
            defaults.with_prefetch_files_per_partition(0)
        );
    }

    #[test]
    fn maps_every_field_to_its_native_option() {
        let options = parse_with_schema_assertions(
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
        let options = parse_with_schema_assertions(
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
        assert_ne!(options, parse_with_schema_assertions("{}").unwrap());
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
            assert!(
                matches!(
                    parse_with_schema_assertions(text),
                    Err(Error::InputJson { .. })
                ),
                "{text}"
            );
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
                assert!(
                    matches!(
                        parse_with_schema_assertions(&text),
                        Err(Error::InputJson { .. })
                    ),
                    "{text}"
                );
            }
            let overflow = usize::MAX as u128 + 1;
            let text = format!(r#"{{"{field}":{overflow}}}"#);
            assert!(
                matches!(
                    parse_with_schema_assertions(&text),
                    Err(Error::InputJson { .. })
                ),
                "{text}"
            );
            if !nullable {
                let text = format!(r#"{{"{field}":null}}"#);
                assert!(
                    matches!(
                        parse_with_schema_assertions(&text),
                        Err(Error::InputJson { .. })
                    ),
                    "{text}"
                );
            }
            for value in if nullable {
                &["1", "null"][..]
            } else {
                &["1"][..]
            } {
                let escaped_key = format!(r#"\u{:04x}{}"#, field.as_bytes()[0], &field[1..]);
                for duplicate in [field, &escaped_key] {
                    let text = format!(r#"{{"{field}":{value},"{duplicate}":{value}}}"#);
                    assert!(
                        matches!(
                            parse_with_schema_assertions(&text),
                            Err(Error::InputJson { .. })
                        ),
                        "{text}"
                    );
                }
            }
        }
    }

    #[test]
    fn native_setters_enforce_zero_and_capacity_limits() {
        for (field, nullable, maximum) in [
            (
                "max_concurrent_file_reads_per_scan",
                true,
                tokio::sync::Semaphore::MAX_PERMITS,
            ),
            (
                "max_concurrent_file_reads_per_partition",
                false,
                tokio::sync::Semaphore::MAX_PERMITS,
            ),
            (
                "output_buffer_batches_per_partition",
                false,
                tokio::sync::Semaphore::MAX_PERMITS,
            ),
            ("parquet_metadata_size_hint_bytes", true, usize::MAX),
            ("parquet_full_file_read_threshold_bytes", true, usize::MAX),
        ] {
            let text = format!(r#"{{"{field}":0}}"#);
            let error = parse_with_schema_assertions(&text).unwrap_err();
            assert!(matches!(error, Error::Configuration { .. }), "{text}");
            assert_eq!(error.exit_code(), ExitCode::from(2));
            assert_configuration_correction(&error, field, nullable, maximum);
        }
        let maximum = tokio::sync::Semaphore::MAX_PERMITS;
        let options = parse_with_schema_assertions(
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
            let error = parse_with_schema_assertions(&text).unwrap_err();
            assert!(matches!(error, Error::Configuration { .. }), "{text}");
            assert_eq!(error.exit_code(), ExitCode::from(2));
            assert_configuration_correction(
                &error,
                field,
                field == "max_concurrent_file_reads_per_scan",
                maximum,
            );
        }
        assert!(execution_options_correction("secret-unknown-reason").is_none());
    }

    fn assert_configuration_correction(error: &Error, field: &str, nullable: bool, maximum: usize) {
        let record = error.to_json_diagnostic();
        assert_eq!(record["argument"], "--execution-options-file");
        assert_eq!(record["field_path"], format!("/{field}"));
        assert_eq!(
            record["expected"],
            format!(
                "JSON integer from 1 to {maximum}{}",
                if nullable { ", or null" } else { "" },
            )
        );
        assert_eq!(record["recovery"], "fix_input");
        assert_eq!(record["retryable"], false);
        assert!(record["hint"].as_str().unwrap().contains("omit"));
        let text =
            crate::diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
        assert!(text.contains(&format!("field_path=\"/{field}\"")), "{text}");
        assert!(
            text.contains(record["expected"].as_str().unwrap()),
            "{text}"
        );
    }

    #[test]
    fn size_and_prefetch_values_accept_the_full_usize_range() {
        let options = parse_with_schema_assertions(
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
        let error = parse_with_schema_assertions(r#"{"output_buffer_batches_per_partition":0}"#)
            .unwrap_err();
        assert_eq!(error.exit_code(), ExitCode::from(2));
        let text =
            crate::diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
        for (field, value) in [
            ("error_phase", native.phase().as_str()),
            ("error_code", native.code()),
            ("error_reason", native.reason()),
        ] {
            assert!(text.contains(&format!("{field}=\"{value}\"")), "{text}");
        }
        assert!(error.source().unwrap().is::<DeltaReaderError>());
    }
}
