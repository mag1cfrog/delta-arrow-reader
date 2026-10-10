mod input;

use std::{
    fmt,
    io::{self, Write},
    process::ExitCode,
};

use arrow_schema::Schema;
use clap::{Parser, Subcommand, error::ErrorKind};
use delta_arrow_reader::{DeltaReaderError, DeltaSnapshotSelection, DeltaTableBuilder, WarmupMode};
use serde_json::{Value, json};
use snafu::{ResultExt, Snafu, ensure};

/// Read Delta Lake snapshot metadata.
#[derive(Parser)]
#[command(name = "dar", bin_name = "dar", version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Print one Delta snapshot's version and full Arrow schema as JSON.
    ///
    /// Only snapshot metadata is loaded; data files are not read.
    Inspect {
        /// UTF-8 local path or URL; relative paths use the working directory.
        /// Use '--' before a table path beginning with '-'.
        table: String,
        /// Unsigned decimal snapshot version; default: latest.
        #[arg(long, value_name = "N", value_parser = parse_table_version)]
        table_version: Option<u64>,
        /// Local JSON object of string keys and string values.
        /// At most 1 MiB; '-' is a filename, not stdin.
        #[arg(long, value_name = "PATH")]
        storage_options_file: Option<String>,
    },
}

#[derive(Snafu)]
enum Error {
    #[snafu(display("Invalid command-line arguments."))]
    Argument,
    #[snafu(display(
        "Invalid JSON input; expected a string map of at most 1 MiB with unique keys."
    ))]
    InputJson,
    #[snafu(display("Could not read the input file."))]
    InputIo { source: io::Error },
    #[snafu(display("The reader runtime failed."))]
    Runtime { source: io::Error },
    #[snafu(display("Could not write or flush stdout."))]
    Output { source: io::Error },
    #[snafu(display("{source}"))]
    Reader { source: DeltaReaderError },
}

impl fmt::Debug for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl Error {
    fn diagnostic(&self) -> Value {
        let (phase, code) = match self {
            Self::Argument => ("configuration", "invalid_cli_argument"),
            Self::InputJson => ("configuration", "invalid_input_json"),
            Self::InputIo { .. } => ("configuration", "input_file_io"),
            Self::Runtime { .. } => ("execution", "runtime_initialization"),
            Self::Output { .. } => ("execution", "output_write"),
            Self::Reader { source } => (source.phase().as_str(), source.code()),
        };
        json!({"phase": phase, "code": code, "message": self.to_string()})
    }

    fn exit_code(&self) -> ExitCode {
        ExitCode::from(match self {
            Self::Argument | Self::InputJson | Self::InputIo { .. } => 2,
            Self::Runtime { .. } | Self::Reader { .. } => 1,
            Self::Output { .. } => 3,
        })
    }

    fn write_diagnostic(&self) {
        let mut stderr = io::stderr().lock();
        let _ = writeln!(stderr, "{}", self.diagnostic()).and_then(|()| stderr.flush());
    }
}

fn parse_table_version(value: &str) -> Result<u64, Error> {
    ensure!(
        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()),
        ArgumentSnafu
    );
    value.parse().map_err(|_| ArgumentSnafu.build())
}

fn encode_inspection_json(version: u64, schema: &Schema) -> Vec<u8> {
    // Arrow Schema's Serde representation contains only JSON-compatible types.
    // Keep upstream serialization, including metadata and nested field order.
    let mut output = json!({
        "format_version": 1,
        "table_version": version.to_string(),
        "schema": schema,
    })
    .to_string()
    .into_bytes();
    output.push(b'\n');
    output
}

fn run() -> Result<(), Error> {
    let output = match Cli::try_parse() {
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            error.to_string().into_bytes()
        }
        // Clap's other diagnostics may contain argument values. Emit our redacted error.
        Err(_) => return ArgumentSnafu.fail(),
        Ok(Cli {
            command:
                Command::Inspect {
                    table,
                    table_version,
                    storage_options_file,
                },
        }) => {
            let storage_options = match storage_options_file {
                Some(path) => input::read_json_file::<input::StorageOptionsInput>(&path)?.0,
                None => Default::default(),
            };
            // Inspection needs only version and schema, even for unscannable tables.
            let builder = DeltaTableBuilder::new(table)
                .with_warmup(WarmupMode::None)
                .with_snapshot_selection(table_version.map_or(
                    DeltaSnapshotSelection::Latest,
                    DeltaSnapshotSelection::Version,
                ))
                .with_storage_options(storage_options);
            // Kernel starts threads lazily and can panic without waking its caller.
            // The CLI owns the process: fail immediately with a redacted diagnostic,
            // including for worker panics, instead of unwinding into a blocking join.
            std::panic::set_hook(Box::new(|_| {
                Error::Runtime {
                    source: io::Error::other("reader runtime panicked"),
                }
                .write_diagnostic();
                std::process::exit(1);
            }));
            // Drive async work on the calling thread. A dedicated Tokio worker can
            // occupy the last OS thread and leave blocking loads queued forever.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context(RuntimeSnafu)?;
            let result = runtime.block_on(async {
                let table = builder.load_table().await.context(ReaderSnafu)?;
                Ok(encode_inspection_json(
                    table.version(),
                    table.schema().as_ref(),
                ))
            });
            // Blocking core work must not delay process shutdown after this command finishes.
            runtime.shutdown_background();
            result?
        }
    };
    let mut stdout = io::stdout().lock();
    stdout
        .write_all(&output)
        .and_then(|()| stdout.flush())
        .context(OutputSnafu)
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            error.write_diagnostic();
            error.exit_code()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_schema::{DataType, Field, TimeUnit};

    use super::*;

    #[test]
    fn inspection_format_preserves_nested_schema_and_all_metadata() {
        let schema = Schema::new_with_metadata(
            vec![
                Field::new(
                    "profile",
                    DataType::Struct(
                        vec![
                            Field::new("amount", DataType::Decimal128(20, 4), false).with_metadata(
                                HashMap::from([("comment".into(), "secret user metadata".into())]),
                            ),
                        ]
                        .into(),
                    ),
                    true,
                ),
                Field::new(
                    "times",
                    DataType::List(Arc::new(Field::new(
                        "element",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                        false,
                    ))),
                    false,
                ),
            ],
            HashMap::from([("owner".into(), "table author".into())]),
        );
        let encoded = encode_inspection_json(u64::MAX, &schema);
        assert_eq!(encoded.last(), Some(&b'\n'));
        assert_eq!(encoded.iter().filter(|&&byte| byte == b'\n').count(), 1);
        let actual: Value = serde_json::from_slice(&encoded).unwrap();
        // This literal freezes the upstream Arrow encoding within format version 1.
        assert_eq!(
            actual,
            json!({
                "format_version": 1, "table_version": "18446744073709551615",
                "schema": {"fields": [
                    {"name": "profile", "nullable": true, "dict_id": 0, "dict_is_ordered": false, "metadata": {},
                     "data_type": {"Struct": [
                         {"name": "amount", "data_type": {"Decimal128": [20, 4]}, "nullable": false,
                          "dict_id": 0, "dict_is_ordered": false, "metadata": {"comment": "secret user metadata"}}
                     ]}},
                    {"name": "times", "nullable": false, "dict_id": 0, "dict_is_ordered": false, "metadata": {},
                     "data_type": {"List": {
                         "name": "element", "data_type": {"Timestamp": ["Microsecond", "UTC"]}, "nullable": false,
                         "dict_id": 0, "dict_is_ordered": false, "metadata": {}
                     }}}
                ], "metadata": {"owner": "table author"}}
            })
        );
        let decoded: Schema = serde_json::from_value(actual["schema"].clone()).unwrap();
        assert_eq!(decoded, schema);
    }

    #[test]
    fn table_version_accepts_full_unsigned_range() {
        for text in ["0", "000", "18446744073709551615"] {
            let cli =
                Cli::try_parse_from(["dar", "inspect", "--table-version", text, "table"]).unwrap();
            assert!(
                matches!(cli.command, Command::Inspect {table_version: Some(version), ..} if version == text.parse::<u64>().unwrap())
            );
        }
    }

    #[test]
    fn storage_options_preserve_strings() {
        let options: input::StorageOptionsInput = serde_json::from_str(
            r#"{"AWS_ACCESS_KEY_ID":" secret, value ","MixedCase":"unchanged"}"#,
        )
        .unwrap();
        assert_eq!(options.0["AWS_ACCESS_KEY_ID"], " secret, value ");
        assert_eq!(options.0["MixedCase"], "unchanged");
        assert_eq!(options.0.len(), 2);
    }

    #[test]
    fn snafu_retains_io_sources_without_exposing_them_in_diagnostics() {
        use std::error::Error as _;

        let error = Err::<(), _>(io::Error::other("secret input path"))
            .context(InputIoSnafu)
            .unwrap_err();
        assert!(error.source().unwrap().is::<io::Error>());
        assert_eq!(
            error.diagnostic(),
            json!({
                "phase": "configuration", "code": "input_file_io",
                "message": "Could not read the input file.",
            })
        );
        assert_eq!(format!("{error:?}"), error.to_string());
    }
}
