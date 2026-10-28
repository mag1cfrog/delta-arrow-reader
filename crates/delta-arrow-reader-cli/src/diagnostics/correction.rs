//! Safe input corrections derived from declared arguments and typed reader errors.

use clap::{
    CommandFactory,
    error::{ContextKind, ContextValue, ErrorKind},
};

#[derive(Clone)]
pub(crate) struct InputCorrection {
    pub(crate) argument: String,
    pub(crate) field_path: Option<String>,
    pub(crate) expected: String,
    pub(crate) supplied: Option<Box<str>>,
    pub(crate) hint: &'static str,
}

impl InputCorrection {
    pub(crate) fn from_clap(error: &clap::Error) -> Option<Self> {
        if matches!(
            error.kind(),
            ErrorKind::UnknownArgument | ErrorKind::InvalidSubcommand | ErrorKind::InvalidUtf8
        ) {
            return None;
        }
        let mut command = crate::cli::Cli::command();
        command.build();
        match error.get(ContextKind::InvalidArg)? {
            ContextValue::String(argument) => {
                Self::from_declaration(&command, argument, error.kind())
            }
            ContextValue::Strings(arguments) => arguments
                .iter()
                .find_map(|argument| Self::from_declaration(&command, argument, error.kind())),
            _ => None,
        }
    }

    fn from_declaration(command: &clap::Command, reported: &str, kind: ErrorKind) -> Option<Self> {
        // Only return declaration text, never values copied from the failing invocation.
        for argument in command.get_arguments() {
            if argument.to_string() == reported {
                let expected = match argument.get_id().as_str() {
                    "limit" => format!("unsigned decimal integer from 0 to {}", usize::MAX),
                    "target_partitions" | "max_output_bytes" | "deadline_ms" => {
                        format!("unsigned decimal integer from 1 to {}", usize::MAX)
                    }
                    "table_version" => format!("unsigned decimal integer from 0 to {}", u64::MAX),
                    "diagnostic_format" => "json (use --diagnostics=json)".to_owned(),
                    // Inspect and row-reading commands have different help for this shared value.
                    "columns" => "a logical column name in the selected snapshot".to_owned(),
                    _ => argument.get_help()?.to_string(),
                };
                return Some(Self {
                    argument: argument
                        .get_long()
                        .map_or_else(|| argument.get_id().to_string(), |name| format!("--{name}")),
                    field_path: None,
                    expected,
                    supplied: None,
                    hint: argument_error_hint(kind),
                });
            }
        }
        command
            .get_subcommands()
            .find_map(|command| Self::from_declaration(command, reported, kind))
    }

    pub(crate) fn from_reader(error: &delta_arrow_reader::DeltaReaderError) -> Option<Self> {
        if let delta_arrow_reader::DeltaReaderError::PredicateTypeMismatch {
            expected,
            supplied,
            path,
            ..
        } = error
        {
            return Some(crate::input::predicate_type_mismatch_correction(
                expected, supplied, path,
            ));
        }
        let (argument, expected, hint) = match (error.code(), error.reason()) {
            ("invalid_projection", "column_not_found") => (
                "--column",
                "a logical column name in the selected snapshot",
                "Use inspect TABLE --view summary --column-prefix PREFIX with the same authorization and --table-version N to find column names.",
            ),
            ("invalid_projection", "duplicate_column") => (
                "--column",
                "each logical column selected at most once",
                "Remove repeated --column values.",
            ),
            ("unsupported_predicate", "column_not_found" | "ambiguous_column") => (
                "--predicate-file",
                "an unambiguous top-level logical column in the selected snapshot",
                "Use inspect TABLE --view summary --column-prefix PREFIX with the same authorization and --table-version N; choose an unambiguous name for the predicate's column field.",
            ),
            ("unsupported_predicate", "invalid_column_reference") => (
                "--predicate-file",
                "a nonempty top-level logical column name without dots",
                "Use inspect TABLE --view summary --column-prefix PREFIX with the same authorization and --table-version N. Predicates accept top-level names without dots.",
            ),
            ("unsupported_predicate", "invalid_decimal") => (
                "--predicate-file",
                "a decimal128 value within its declared precision and valid scale",
                "Use inspect TABLE --view summary --column NAME with the same authorization and --table-version N for precision/scale; ensure the unscaled value fits.",
            ),
            _ => return None,
        };
        Some(Self {
            argument: argument.to_owned(),
            field_path: None,
            expected: expected.to_owned(),
            supplied: None,
            hint,
        })
    }
}

pub(crate) fn argument_error_hint(kind: ErrorKind) -> &'static str {
    match kind {
        ErrorKind::MissingRequiredArgument => "Supply the required argument.",
        ErrorKind::ArgumentConflict => "Remove the conflicting or repeated argument.",
        ErrorKind::NoEquals => "Attach the value to the option with '='.",
        ErrorKind::InvalidUtf8 => "Use UTF-8 text for command-line arguments.",
        ErrorKind::UnknownArgument | ErrorKind::InvalidSubcommand => {
            "Use --help to choose a supported command or option."
        }
        ErrorKind::ValueValidation
        | ErrorKind::InvalidValue
        | ErrorKind::TooFewValues
        | ErrorKind::TooManyValues
        | ErrorKind::WrongNumberOfValues => {
            "Supply a value matching the argument's expected format."
        }
        _ => "Use --help to check the command's arguments.",
    }
}
