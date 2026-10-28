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
