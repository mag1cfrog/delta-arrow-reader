//! Variables supplied by an explicitly selected credential file.
//!
//! For example, `AWS_SECRET_ACCESS_KEY='$VALUE'` supplies the literal `$VALUE`.
//! Quotes delimit a value; backslashes, dollar signs and backticks are data.
//! Only whole lines beginning with `#` are comments.

use std::collections::HashMap;

use crate::diagnostics::Error;

use super::{MAX_INPUT_BYTES, read_bounded_input_bytes};

pub(crate) const MAX_ENV_FILE_RECORDS: usize = 1024;

pub(crate) fn read_env_file(path: &str) -> Result<HashMap<String, String>, Error> {
    parse_env_file(&read_bounded_input_bytes(path, "--env-file")?)
}

fn parse_env_file(bytes: &[u8]) -> Result<HashMap<String, String>, Error> {
    if bytes.len() as u64 > MAX_INPUT_BYTES {
        return Err(env_file_input_error(
            "input_file_too_large",
            "at most 1 MiB",
            None,
        ));
    }
    let text = std::str::from_utf8(bytes)
        .map_err(|_| env_file_input_error("env_file_not_utf8", "UTF-8 text", None))?;
    let mut variables = HashMap::new();
    for (index, line) in text.lines().enumerate() {
        let line_number = Some(index + 1);
        let invalid_record = || {
            env_file_input_error(
                "env_file_invalid_record",
                "NAME=VALUE with optional enclosing quotes and no control characters",
                line_number,
            )
        };
        if line.chars().any(|c| c.is_control() && c != '\t') {
            return Err(invalid_record());
        }
        let line = line.trim_matches([' ', '\t']);
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if variables.len() == MAX_ENV_FILE_RECORDS {
            return Err(env_file_input_error(
                "env_file_too_many_records",
                "at most 1024 variable records",
                line_number,
            ));
        }
        let (name, value) = line.split_once('=').ok_or_else(invalid_record)?;
        let name = name.trim_matches([' ', '\t']);
        if !name
            .as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
            || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(invalid_record());
        }
        let value = value.trim_matches([' ', '\t']);
        let value = if value.starts_with(['\'', '"']) {
            let quote = value.as_bytes()[0] as char;
            let enclosed = value[1..].strip_suffix(quote).ok_or_else(invalid_record)?;
            if enclosed.contains(quote) {
                return Err(invalid_record());
            }
            enclosed
        } else {
            if value.contains(['\'', '"']) {
                return Err(invalid_record());
            }
            value
        };
        if value.trim().is_empty() || value.chars().any(char::is_control) {
            return Err(invalid_record());
        }
        if variables
            .insert(name.to_owned(), value.to_owned())
            .is_some()
        {
            return Err(env_file_input_error(
                "env_file_duplicate_name",
                "each variable name exactly once",
                line_number,
            ));
        }
    }
    if variables.is_empty() {
        return Err(env_file_input_error(
            "env_file_empty",
            "at least one NAME=VALUE record",
            None,
        ));
    }
    Ok(variables)
}

fn env_file_input_error(
    reason: &'static str,
    expected: &'static str,
    line: Option<usize>,
) -> Error {
    Error::EnvFileInput {
        reason,
        line,
        expected,
    }
}

#[cfg(test)]
mod tests;
