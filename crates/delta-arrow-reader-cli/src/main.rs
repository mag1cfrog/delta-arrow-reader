mod input;

use std::{
    env,
    ffi::OsString,
    io::{self, Write},
    process::ExitCode,
};

use arrow_schema::Schema;
use delta_arrow_reader::{DeltaReaderError, DeltaSnapshotSelection, DeltaTableBuilder};
use serde_json::{Value, json};

const HELP: &str = "Read Delta Lake snapshot metadata.

Usage: dar <COMMAND>

Commands:
  inspect    Print the snapshot version and Arrow schema as JSON

Options:
  --help     Print help
  --version  Print the version

Run 'dar inspect --help' for inspection options.
";

const INSPECT_HELP: &str = "Print one Delta snapshot's version and full Arrow schema as JSON.

Usage: dar inspect [--table-version N] [--storage-options-file PATH] [--] TABLE

Arguments:
  TABLE    UTF-8 local path or URL; relative paths use the working directory

Options:
  --table-version N            Unsigned decimal snapshot version; default: latest
  --storage-options-file PATH   Local JSON object of string keys and string values
                               At most 1 MiB; '-' is a filename, not stdin
  --help                       Print help

Use '--' before a table path beginning with '-'. Options are single-use.
Only snapshot metadata is loaded; data files are not read.
";

enum Command {
    Text(&'static str),
    Inspect {
        table: String,
        version: Option<u64>,
        storage_file: Option<String>,
    },
}

enum Error {
    Argument,
    InputJson,
    InputIo,
    Runtime,
    Output,
    Reader(DeltaReaderError),
}

impl Error {
    fn diagnostic(&self) -> Value {
        let (phase, code, message) = match self {
            Self::Argument => (
                "configuration",
                "invalid_cli_argument",
                "Invalid command-line arguments.",
            ),
            Self::InputJson => (
                "configuration",
                "invalid_input_json",
                "Invalid JSON input; expected a string map of at most 1 MiB with unique keys.",
            ),
            Self::InputIo => (
                "configuration",
                "input_file_io",
                "Could not read the input file.",
            ),
            Self::Runtime => (
                "execution",
                "runtime_initialization",
                "Could not initialize the runtime.",
            ),
            Self::Output => (
                "execution",
                "output_write",
                "Could not write or flush stdout.",
            ),
            Self::Reader(error) => {
                return json!({
                    "phase": error.phase().as_str(),
                    "code": error.code(),
                    "message": error.to_string(),
                });
            }
        };
        json!({"phase": phase, "code": code, "message": message})
    }

    fn status(&self) -> ExitCode {
        ExitCode::from(match self {
            Self::Argument | Self::InputJson | Self::InputIo => 2,
            Self::Runtime | Self::Reader(_) => 1,
            Self::Output => 3,
        })
    }
}

fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Command, Error> {
    let args = args
        .into_iter()
        .map(|arg| arg.into_string().map_err(|_| Error::Argument))
        .collect::<Result<Vec<_>, _>>()?;
    match args.as_slice() {
        [flag] if flag == "--help" => return Ok(Command::Text(HELP)),
        [flag] if flag == "--version" => {
            return Ok(Command::Text(concat!(
                "dar ",
                env!("CARGO_PKG_VERSION"),
                "\n"
            )));
        }
        [command, ..] if command == "inspect" => {}
        _ => return Err(Error::Argument),
    }

    let mut args = args.into_iter().skip(1);
    let mut table = None;
    let mut version = None;
    let mut storage_file = None;
    let mut positional = false;
    let mut help = false;
    while let Some(arg) = args.next() {
        if !positional {
            match arg.as_str() {
                "--" => {
                    positional = true;
                    continue;
                }
                "--help" => {
                    help = true;
                    continue;
                }
                "--table-version" if version.is_none() => {
                    let value = args.next().ok_or(Error::Argument)?;
                    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                        return Err(Error::Argument);
                    }
                    version = Some(value.parse().map_err(|_| Error::Argument)?);
                    continue;
                }
                "--storage-options-file" if storage_file.is_none() => {
                    storage_file = Some(args.next().ok_or(Error::Argument)?);
                    continue;
                }
                value if value.starts_with('-') => return Err(Error::Argument),
                _ => {}
            }
        }
        if table.replace(arg).is_some() {
            return Err(Error::Argument);
        }
    }
    if help {
        return Ok(Command::Text(INSPECT_HELP));
    }
    Ok(Command::Inspect {
        table: table.ok_or(Error::Argument)?,
        version,
        storage_file,
    })
}

fn inspection_json(version: u64, schema: &Schema) -> Vec<u8> {
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
    let output = match parse(env::args_os().skip(1))? {
        Command::Text(text) => text.as_bytes().to_vec(),
        Command::Inspect {
            table,
            version,
            storage_file,
        } => {
            let storage_options = match storage_file {
                Some(path) => input::read_json::<input::StorageOptions>(&path)?.0,
                None => Default::default(),
            };
            let builder = DeltaTableBuilder::new(table)
                .with_snapshot_selection(version.map_or(
                    DeltaSnapshotSelection::Latest,
                    DeltaSnapshotSelection::Version,
                ))
                .with_storage_options(storage_options);
            let runtime = tokio::runtime::Runtime::new().map_err(|_| Error::Runtime)?;
            let result = runtime.block_on(async {
                let table = builder.load_table().await.map_err(Error::Reader)?;
                Ok(inspection_json(table.version(), table.schema().as_ref()))
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
        .map_err(|_| Error::Output)
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let mut stderr = io::stderr().lock();
            let _ = writeln!(stderr, "{}", error.diagnostic()).and_then(|()| stderr.flush());
            error.status()
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
        let encoded = inspection_json(u64::MAX, &schema);
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
    fn version_accepts_full_unsigned_range_and_storage_preserves_strings() {
        for text in ["0", "000", "18446744073709551615"] {
            let command = parse(["inspect", "--table-version", text, "table"].map(OsString::from));
            assert!(
                matches!(command, Ok(Command::Inspect {version: Some(version), ..}) if version == text.parse::<u64>().unwrap())
            );
        }
        let options: input::StorageOptions = serde_json::from_str(
            r#"{"AWS_ACCESS_KEY_ID":" secret, value ","MixedCase":"unchanged"}"#,
        )
        .unwrap();
        assert_eq!(options.0["AWS_ACCESS_KEY_ID"], " secret, value ");
        assert_eq!(options.0["MixedCase"], "unchanged");
        assert_eq!(options.0.len(), 2);
    }
}
