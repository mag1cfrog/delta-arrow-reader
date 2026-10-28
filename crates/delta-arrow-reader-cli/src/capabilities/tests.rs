#![allow(clippy::unwrap_used)]

use clap::Parser;

use super::*;
use crate::cli::Cli;

#[test]
fn discovery_selectors_are_mutually_exclusive_and_describe_their_conflicts() {
    let details = catalog::build_capability_details(Some("capabilities")).unwrap();
    let arguments = details["command"]["arguments"].as_array().unwrap();
    let selectors = [
        vec!["--command", "query"],
        vec!["--schema", "predicate"],
        vec!["--authorization", "s3"],
        vec!["--all"],
    ];
    for (index, selector) in selectors.iter().enumerate() {
        let argument = arguments
            .iter()
            .find(|argument| argument["name"] == selector[0])
            .unwrap();
        for (other_index, other) in selectors.iter().enumerate() {
            if index == other_index {
                continue;
            }
            assert!(
                argument["conflicts_with"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(other[0]))
            );
            let args: Vec<_> = ["dar", "capabilities"]
                .iter()
                .chain(selector)
                .chain(other)
                .copied()
                .collect();
            let error = Cli::try_parse_from(args).err().unwrap();
            assert_eq!(error.kind(), ErrorKind::ArgumentConflict);
        }
    }
    for name in ["unknown", "analyze", "query --help", "", "QUERY"] {
        assert!(Cli::try_parse_from(["dar", "capabilities", "--command", name]).is_err());
        assert!(catalog::build_capability_details(Some(name)).is_err());
    }
    for invalid in [
        vec!["--full"],
        vec!["--all", "--full"],
        vec!["--detail", "rows"],
        vec!["--authorization", "s3", "--detail", "env-file", "--full"],
        vec!["--all", "--detail", "rows"],
        vec!["--schema", "predicate", "--detail", "int64", "--full"],
    ] {
        assert!(
            Cli::try_parse_from(
                ["dar", "capabilities"]
                    .into_iter()
                    .chain(invalid.iter().copied())
            )
            .is_err(),
            "{invalid:?}"
        );
    }
    for valid in [
        vec!["--authorization", "s3", "--full"],
        vec!["--command", "query", "--full"],
        vec!["--command", "query", "--detail", "runtime"],
        vec!["--full", "--schema", "predicate"],
        vec!["--detail", "rows", "--schema", "completion"],
        vec!["--authorization", "s3", "--detail", "env-file"],
        vec!["--detail", "env-file-grammar", "--authorization", "gcs"],
    ] {
        assert!(Cli::try_parse_from(["dar", "capabilities"].into_iter().chain(valid)).is_ok());
    }
    assert!(command_guides::build_command_detail("scan", "rows").is_err());
    assert!(command_guides::build_command_detail("scan", "resources").is_err());
    let resources = command_guides::build_command_detail("query", "resources").unwrap();
    assert!(resources.to_string().len() < MAX_COMMAND_DETAIL_BYTES);
}

#[test]
fn response_limits_include_json_framing_and_the_final_newline() {
    for limit in [
        MAX_INDEX_BYTES,
        MAX_COMMAND_GUIDE_BYTES,
        MAX_FULL_RESPONSE_BYTES,
    ] {
        let exact = json!("a".repeat(limit - 3));
        let encoded = serialize_discovery_response(&exact, limit).unwrap();
        assert_eq!(encoded.len(), limit);
        assert_eq!(encoded.last(), Some(&b'\n'));
        assert_eq!(serde_json::from_slice::<Value>(&encoded).unwrap(), exact);
        let oversized = json!("a".repeat(limit - 2));
        assert!(matches!(
            serialize_discovery_response(&oversized, limit),
            Err(Error::OutputLimit {
                reason: "discovery_output_too_large"
            })
        ));
    }
    assert!(serialize_discovery_response(&json!("\u{e9}"), 4).is_err());
    assert_eq!(
        serialize_discovery_response(&json!("\u{e9}"), 5)
            .unwrap()
            .len(),
        5
    );
}

#[test]
fn completion_fits_the_minimum_atomic_pipe_write_size() {
    let record = build_completion_record(
        "capabilities",
        None,
        OutputStats {
            bytes_written: u64::MAX,
            ..Default::default()
        },
        None,
    );
    assert!(format!("{record}\n").len() <= 512);
}
