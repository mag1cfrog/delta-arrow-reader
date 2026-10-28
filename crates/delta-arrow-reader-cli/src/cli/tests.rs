use super::*;
use crate::table_access;

#[test]
fn deadline_defaults_and_explicit_values_preserve_legacy_commands() {
    for (args, expected) in [
        (vec!["inspect", "table"], None),
        (vec!["inspect", "table", "--view", "full"], None),
        (vec!["inspect", "table", "--view", "summary"], Some(30000)),
        (vec!["scan", "table"], None),
        (vec!["scan", "table", "--output", "rows"], Some(300000)),
        (vec!["preview", "table"], Some(30000)),
        (
            vec!["explain", "table", "--sql-file", "query.sql"],
            Some(30000),
        ),
        (
            vec!["query", "table", "--sql-file", "query.sql"],
            Some(300000),
        ),
    ] {
        for explicit in [None, Some("1234"), Some("0"), Some("-1")] {
            let mut argv = vec!["dar"];
            argv.extend(args.iter().copied());
            if let Some(value) = explicit {
                argv.extend(["--deadline-ms", value]);
            }
            let parsed = Cli::try_parse_from(argv);
            if matches!(explicit, Some("0" | "-1")) {
                assert!(parsed.is_err());
                continue;
            }
            let deadline = match parsed.unwrap().command {
                Command::Inspect { deadline_ms, .. } | Command::Scan { deadline_ms, .. } => {
                    deadline_ms
                }
                Command::Preview(args) => Some(args.deadline_ms),
                Command::Query(args) => Some(args.deadline_ms),
                Command::Explain(args) => Some(args.deadline_ms),
                Command::Capabilities(_) => unreachable!(),
            };
            assert_eq!(
                deadline.map(NonZeroUsize::get),
                if explicit.is_some() {
                    Some(1234)
                } else {
                    expected
                }
            );
        }
    }
}

#[test]
fn file_and_stdout_defaults_match_their_distinct_output_budgets() {
    for format in ["arrow", "json"] {
        for output in [false, true] {
            let mut argv = vec![
                "dar",
                "query",
                "table",
                "--sql-file",
                "query.sql",
                "--format",
                format,
            ];
            if output {
                argv.extend(["--output", "rows"]);
            }
            let Command::Query(args) = Cli::try_parse_from(&argv).unwrap().command else {
                unreachable!()
            };
            let expected = match (format, output) {
                ("json", false) => 8192,
                ("json", true) => 16777216,
                _ => 1073741824,
            };
            assert_eq!(args.output_byte_limit(), expected);
            argv.extend(["--max-output-bytes", "123"]);
            let Command::Query(args) = Cli::try_parse_from(&argv).unwrap().command else {
                unreachable!()
            };
            assert_eq!(args.output_byte_limit(), 123);
        }
    }
    let Command::Scan {
        max_rows,
        max_output_bytes,
        ..
    } = Cli::try_parse_from(["dar", "scan", "table", "--output", "rows"])
        .unwrap()
        .command
    else {
        unreachable!()
    };
    assert_eq!(max_rows, Some(1000000));
    assert_eq!(max_output_bytes.map(NonZeroUsize::get), Some(1073741824));
    for flag in ["--max-rows", "--max-output-bytes"] {
        assert!(Cli::try_parse_from(["dar", "scan", "table", flag, "1"]).is_err());
    }
}

#[test]
fn table_version_accepts_full_unsigned_range() {
    for text in ["0", "000", "18446744073709551615"] {
        let cli =
            Cli::try_parse_from(["dar", "inspect", "--table-version", text, "table"]).unwrap();
        assert!(
            matches!(cli.command, Command::Inspect { table: TableArgs {options: table_access::TableOptions { table_version: Some(version), .. }, ..}, .. } if version == text.parse::<u64>().unwrap())
        );
    }
}

#[test]
fn scan_arguments_preserve_column_names_and_full_limit_range() {
    for limit in ["0".to_owned(), "0001".to_owned(), usize::MAX.to_string()] {
        let cli = Cli::try_parse_from([
            "dar", "scan", "--column", "a,b", "--column", "a.b", "--column", "a,b", "--column", "",
            "--limit", &limit, "table",
        ])
        .unwrap();
        let Command::Scan {
            read:
                RowReadArgs {
                    columns,
                    no_columns,
                    ..
                },
            limit: parsed_limit,
            ..
        } = cli.command
        else {
            panic!("expected scan command");
        };
        assert_eq!(columns, ["a,b", "a.b", "a,b", ""]);
        assert!(!no_columns);
        assert_eq!(parsed_limit, Some(limit.parse::<usize>().unwrap()));
    }
}

#[test]
fn partition_targets_accept_positive_usize_values_and_preserve_omission() {
    for text in [
        None,
        Some("1".into()),
        Some("0002".into()),
        Some(usize::MAX.to_string()),
    ] {
        let mut args = vec!["dar", "scan", "table"];
        if let Some(text) = &text {
            args.extend(["--target-partitions", text]);
        }
        let Command::Scan {
            read: RowReadArgs {
                target_partitions, ..
            },
            ..
        } = Cli::try_parse_from(args).unwrap().command
        else {
            panic!("expected scan command");
        };
        assert_eq!(
            target_partitions.map(NonZeroUsize::get),
            text.map(|text| text.parse::<usize>().unwrap())
        );
    }
    let overflow = (usize::MAX as u128 + 1).to_string();
    for text in ["0", "000", &overflow] {
        assert!(
            Cli::try_parse_from(["dar", "scan", "--target-partitions", text, "table"]).is_err()
        );
    }
}
