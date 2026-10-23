//! Variables supplied by an explicitly selected credential file.
//!
//! For example, `AWS_SECRET_ACCESS_KEY='$VALUE'` supplies the literal `$VALUE`.
//! Quotes delimit a value; backslashes, dollar signs and backticks are data.
//! Only whole lines beginning with `#` are comments.

use std::collections::HashMap;

use crate::Error;

use super::{MAX_INPUT_BYTES, read_input_file_bytes};

pub(crate) const MAX_ENV_FILE_RECORDS: usize = 1024;

pub(crate) fn read_env_file(path: &str) -> Result<HashMap<String, String>, Error> {
    parse_env_file(&read_input_file_bytes(path, "--env-file")?)
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
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn whitespace_quotes_and_shell_metacharacters_preserve_literal_values() {
        let variables = parse_env_file(
            b" # comment\r\n\r\n A = one=two \r\nB=' $VALUE `command` #literal '\nC=\"C:\\path\\file\"\nD=raw#literal",
        )
        .unwrap();
        assert_eq!(variables["A"], "one=two");
        assert_eq!(variables["B"], " $VALUE `command` #literal ");
        assert_eq!(variables["C"], "C:\\path\\file");
        assert_eq!(variables["D"], "raw#literal");
    }

    #[test]
    fn invalid_records_report_the_line_without_echoing_names_or_values() {
        for record in [
            "export SECRET=value",
            "SECRET",
            "1SECRET=value",
            "SECRET-NAME=value",
            "SECRET=",
            "SECRET=''",
            "SECRET='  '",
            "SECRET='value",
            "SECRET=\"value\" trailing",
            "SECRET='a'b'",
            "SECRET=a\"b",
            "SECRET='first\nsecond'",
            "SECRET=bad\0value",
            "SECRET=bad\rvalue",
        ] {
            let contents = format!("# comment\n{record}");
            let result = parse_env_file(contents.as_bytes());
            assert!(
                matches!(result, Err(Error::EnvFileInput { line: Some(2), .. })),
                "{record}"
            );
            let displayed = result.unwrap_err().to_string();
            assert!(!displayed.contains("SECRET") && !displayed.contains("value"));
        }
        assert!(matches!(
            parse_env_file(b"SECRET=one\nSECRET=two"),
            Err(Error::EnvFileInput {
                reason: "env_file_duplicate_name",
                line: Some(2),
                ..
            })
        ));
    }

    #[test]
    fn files_have_exact_byte_and_record_bounds() {
        for bytes in [b"".as_slice(), b"  \n# comment"] {
            assert!(matches!(
                parse_env_file(bytes),
                Err(Error::EnvFileInput {
                    reason: "env_file_empty",
                    ..
                })
            ));
        }
        assert!(matches!(
            parse_env_file(b"KEY=\xff"),
            Err(Error::EnvFileInput {
                reason: "env_file_not_utf8",
                ..
            })
        ));
        let mut bytes = b"KEY=".to_vec();
        bytes.resize(MAX_INPUT_BYTES as usize, b'x');
        assert!(parse_env_file(&bytes).is_ok());
        bytes.push(b'x');
        assert!(matches!(
            parse_env_file(&bytes),
            Err(Error::EnvFileInput {
                reason: "input_file_too_large",
                ..
            })
        ));
        let records = (0..MAX_ENV_FILE_RECORDS)
            .map(|n| format!("KEY_{n}=value\n"))
            .collect::<String>();
        assert_eq!(
            parse_env_file(records.as_bytes()).unwrap().len(),
            MAX_ENV_FILE_RECORDS
        );
        assert!(matches!(
            parse_env_file(format!("{records}EXTRA=value").as_bytes()),
            Err(Error::EnvFileInput {
                reason: "env_file_too_many_records",
                ..
            })
        ));
    }
}
