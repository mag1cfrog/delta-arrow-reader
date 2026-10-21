use clap::Parser;
use object_store::{aws::AmazonS3ConfigKey, azure::AzureConfigKey, gcp::GoogleConfigKey};

use super::*;
use crate::{Cli, unity_catalog::UnityCatalogTableTarget};

#[test]
fn advertised_credential_aliases_resolve_to_the_same_backend_option() {
    let authorization = authorization_sources();
    let backends = authorization["direct_storage"]["credential_option_aliases"]
        .as_object()
        .unwrap();
    for (backend, options) in backends {
        for (canonical, aliases) in options.as_object().unwrap() {
            for alias in aliases.as_array().unwrap() {
                let alias = alias.as_str().unwrap();
                match backend.as_str() {
                    "s3" => assert_eq!(
                        alias.parse::<AmazonS3ConfigKey>().unwrap(),
                        canonical.parse().unwrap()
                    ),
                    "azure" => assert_eq!(
                        alias.parse::<AzureConfigKey>().unwrap(),
                        canonical.parse().unwrap()
                    ),
                    "gcs" => assert_eq!(
                        alias.parse::<GoogleConfigKey>().unwrap(),
                        canonical.parse().unwrap()
                    ),
                    other => panic!("unverified backend: {other}"),
                }
            }
        }
    }
}

#[test]
fn authorization_command_lists_match_clap_and_table_target_validation() {
    let authorization = authorization_sources();
    let support = command_read_support();
    assert_eq!(
        support
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["inspect", "query", "scan"]
    );
    for (name, command) in support.as_object().unwrap() {
        let mut args = vec!["dar", name, "local-table"];
        if name == "query" {
            args.extend(["--sql-file", "query.sql"]);
        }
        assert!(Cli::try_parse_from(&args).is_ok());
        args.extend([
            authorization["direct_storage"]["storage_options_argument"]
                .as_str()
                .unwrap(),
            "storage.json",
        ]);
        assert!(Cli::try_parse_from(&args).is_ok());
        args[2] = "uc://main.sales.orders";
        args.extend([
            authorization["unity_catalog"]["profile_argument"]
                .as_str()
                .unwrap(),
            "work",
        ]);
        let supports_uc = command["authorization"]
            .as_array()
            .unwrap()
            .contains(&json!("unity_catalog"));
        assert_eq!(Cli::try_parse_from(&args).is_ok(), supports_uc, "{name}");
        for (source, details) in authorization.as_object().unwrap() {
            assert_eq!(
                details["commands"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(name)),
                command["authorization"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(source)),
                "{name}: {source}",
            );
        }
    }
    let target = authorization["unity_catalog"]["target"].as_str().unwrap();
    assert!(
        UnityCatalogTableTarget::parse(target, Some("work"))
            .unwrap()
            .is_some()
    );
    assert!(UnityCatalogTableTarget::parse(target, None).is_err());
    assert!(UnityCatalogTableTarget::parse(target, Some(" ")).is_err());
    assert!(UnityCatalogTableTarget::parse("local-table", Some("work")).is_err());
    assert!(UnityCatalogTableTarget::parse("uc://only.two", Some("work")).is_err());
    assert!(
        UnityCatalogTableTarget::parse("local-table", None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn unsupported_source_output_and_deadline_options_are_not_accepted() {
    let authorization = authorization_sources();
    assert_eq!(
        authorization["direct_storage"]["explicit_source_selection"],
        false
    );
    assert_eq!(authorization["direct_storage"]["env_file_input"], false);
    let support = command_read_support();
    for name in ["inspect", "scan", "query"] {
        assert!(support[name]["deadline_ms"].is_null());
        for (option, value) in [
            ("--auth-source", "env"),
            ("--env-file", "credentials.env"),
            ("--output", "result.arrow"),
            ("--format", "json"),
            ("--deadline-ms", "1000"),
            ("--tables-file", "tables.json"),
        ] {
            let mut args = vec!["dar", name, "table"];
            if name == "query" {
                args.extend(["--sql-file", "query.sql"]);
            }
            args.extend([option, value]);
            let error = Cli::try_parse_from(args).err().unwrap();
            assert_eq!(
                error.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "{name} {option}"
            );
        }
    }
    let cli = Cli::try_parse_from(["dar", "scan", "table"]).unwrap();
    let crate::Command::Scan { limit, .. } = cli.command else {
        panic!("expected scan")
    };
    assert_eq!(support["scan"]["default_row_limit"], json!(limit));
}
