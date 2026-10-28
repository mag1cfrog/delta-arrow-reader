use clap::Parser;
use object_store::{aws::AmazonS3ConfigKey, azure::AzureConfigKey, gcp::GoogleConfigKey};

use super::*;
use crate::capabilities::read_support::command_read_support;
use crate::{cli::Cli, table_access::unity_catalog::UnityCatalogTableTarget};

#[test]
fn full_authorization_examples_configure_each_advertised_identity_provider() {
    use delta_arrow_reader::{DeltaStorageOptions, DeltaTableBuilder};

    use crate::table_access::direct_credentials::CredentialSource;

    for (service, location, expected_sources) in [
        (
            AuthorizationService::S3,
            "s3://bucket/table",
            ["aws-container", "aws-web-identity"],
        ),
        (
            AuthorizationService::AzureBlob,
            "az://container@account.blob.core.windows.net/table",
            ["azure-managed-identity", "azure-workload-identity"],
        ),
    ] {
        let full = service.build_full_authorization_guide();
        let providers = full["authorization"]["provider_configuration"]
            .as_object()
            .expect("full discovery must explain the advertised identity providers");
        assert_eq!(
            providers.keys().map(String::as_str).collect::<Vec<_>>(),
            expected_sources,
        );
        for (source, configuration) in providers {
            let options: DeltaStorageOptions =
                serde_json::from_value(configuration["json_example"].clone()).unwrap();
            assert!(
                CredentialSource::from_str(source, false)
                    .unwrap()
                    .configure_table_credentials(
                        DeltaTableBuilder::new(location),
                        location,
                        options,
                        None,
                    )
                    .is_ok(),
                "invalid advertised {source} configuration",
            );
        }
        assert!(
            service.build_authorization_guide()["authorization"]
                .get("provider_configuration")
                .is_none()
        );
    }
}

#[test]
fn authorization_guides_defer_credential_configuration_and_keep_selection_rules() {
    let credential_keys = [
        "aws_access_key_id",
        "azure_storage_account_key",
        "google_application_credentials",
    ];
    for (service, expected_key) in [
        (AuthorizationService::S3, credential_keys[0]),
        (AuthorizationService::AzureBlob, credential_keys[1]),
        (AuthorizationService::Gcs, credential_keys[2]),
    ] {
        let document = service.build_full_authorization_guide();
        let authorization = &document["authorization"];
        assert_eq!(document["format_version"], 1);
        for (selector, policy) in [
            ("with_auth_source", "rejected"),
            ("without_auth_source", "ignored_by_backend"),
            ("with_env_file", "rejected"),
        ] {
            assert_eq!(authorization["unknown_options"][selector], policy);
        }
        assert_eq!(
            authorization["unknown_options"]["without_auth_source_scope"],
            "stdout_only"
        );
        let aliases = authorization["credential_option_aliases"]
            .as_object()
            .unwrap();
        assert!(aliases.contains_key(expected_key));
        let concise = service.build_authorization_guide();
        let selected = &concise["authorization"];
        assert_eq!(
            selected["credential_handling"],
            authorization["credential_handling"]
        );
        let handling = selected["credential_handling"].as_str().unwrap();
        assert!(handling.contains("storage-options file path"));
        assert!(!handling.contains("profile"));
        assert!(handling.contains("never put credential values in arguments"));
        assert!(handling.contains("env and --env-file reject credentials in storage-options"));
        assert!(handling.contains("never fall back to another identity"));
        assert!(handling.contains("--output requires an explicit source"));
        assert!(handling.contains("a storage-options file alone selects json"));
        assert!(selected.get("credential_keys").is_none());
        assert!(selected.get("credential_option_aliases").is_none());
        if matches!(service, AuthorizationService::S3) {
            let selected_text = selected.to_string();
            assert!(selected_text.contains("AWS profiles/SSO are unsupported"));
            assert!(
                selected_text.contains("Stdout without source selectors uses backend defaults")
            );
            assert!(!selected_text.contains("AWS_ACCESS_KEY_ID"));
            assert!(!selected_text.contains("Workload sources use provisioned"));
            assert!(authorization.to_string().contains("AWS_ACCESS_KEY_ID"));
            assert!(
                authorization
                    .to_string()
                    .contains("Workload sources use provisioned")
            );
        } else {
            assert!(!selected["notes"].to_string().contains("CLIENT_SECRET"));
            assert!(
                !selected["notes"]
                    .to_string()
                    .contains("APPLICATION_CREDENTIALS")
            );
        }
        assert_eq!(
            concise["full_command"],
            json!([
                crate::cli::command_name(),
                "capabilities",
                "--authorization",
                service,
                "--full"
            ])
        );
        for other in credential_keys
            .into_iter()
            .filter(|key| *key != expected_key)
        {
            assert!(!aliases.contains_key(other));
        }
        assert!(authorization.get("backend_notes").is_none());
        assert!(authorization.get("profile_argument").is_none());
        assert_eq!(authorization["storage_options_schema"], "storage-options");
        assert_eq!(selected["source_argument"], "--auth-source");
        assert_eq!(
            selected["env_file_details_command"],
            json!([
                crate::cli::command_name(),
                "capabilities",
                "--authorization",
                service,
                "--detail",
                "env-file"
            ])
        );
        assert_eq!(selected["sources"], authorization["sources"]);
    }
    let document = AuthorizationService::UnityCatalog.build_full_authorization_guide();
    let concise = AuthorizationService::UnityCatalog.build_authorization_guide();
    assert_eq!(
        concise["authorization"]["credential_handling"],
        document["authorization"]["credential_handling"]
    );
    let handling = concise["authorization"]["credential_handling"]
        .as_str()
        .unwrap();
    assert!(handling.contains("Databricks profile name"));
    assert!(!handling.contains("file path"));
    assert!(handling.contains("never put credential values in arguments"));
    assert_eq!(
        document["authorization"]["profile_argument"],
        "--databricks-profile"
    );
    assert!(
        document["authorization"]
            .get("credential_option_aliases")
            .is_none()
    );
}

#[test]
fn advertised_credential_aliases_resolve_to_the_same_backend_option() {
    let authorization = build_authorization_capabilities();
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
                    "azure-blob" => assert_eq!(
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
    let authorization = build_authorization_capabilities();
    let support = command_read_support();
    assert_eq!(
        support
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["explain", "inspect", "preview", "query", "scan"]
    );
    for (name, command) in support.as_object().unwrap() {
        let mut args = vec!["dar", name, "local-table"];
        if name == "query" || name == "explain" {
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
fn advertised_sources_match_clap_and_unsupported_options_stay_rejected() {
    let authorization = build_authorization_capabilities();
    assert_eq!(
        authorization["direct_storage"]["explicit_source_selection"],
        true
    );
    assert_eq!(authorization["direct_storage"]["env_file_input"], true);
    let advertised_sources: std::collections::BTreeSet<_> =
        authorization["direct_storage"]["sources"]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|sources| sources.as_array().unwrap())
            .map(|source| source.as_str().unwrap())
            .collect();
    let installed_sources: std::collections::BTreeSet<_> =
        crate::table_access::direct_credentials::CredentialSource::value_variants()
            .iter()
            .map(|source| source.to_possible_value().unwrap().get_name().to_owned())
            .collect();
    assert_eq!(
        advertised_sources,
        installed_sources.iter().map(String::as_str).collect()
    );
    let support = command_read_support();
    for name in ["inspect", "scan", "preview", "query", "explain"] {
        match name {
            "preview" | "explain" => assert_eq!(support[name]["deadline_ms"], 30000),
            "query" => assert_eq!(support[name]["deadline_ms"], 300000),
            _ => assert!(support[name]["deadline_ms"].is_null()),
        }
        for source in &advertised_sources {
            let mut args = vec!["dar", name, "s3://bucket/table", "--auth-source", source];
            if name == "query" || name == "explain" {
                args.extend(["--sql-file", "query.sql"]);
            }
            assert!(Cli::try_parse_from(args).is_ok());
        }
        let mut args = vec![
            "dar",
            name,
            "s3://bucket/table",
            authorization["direct_storage"]["env_file_argument"]
                .as_str()
                .unwrap(),
            "credentials.env",
        ];
        if name == "query" || name == "explain" {
            args.extend(["--sql-file", "query.sql"]);
        }
        assert!(Cli::try_parse_from(args).is_ok());
        for (option, value) in [("--output", "result.arrow"), ("--format", "json")] {
            if (name == "query" && option == "--format")
                || (matches!(name, "scan" | "query") && option == "--output")
            {
                continue;
            }
            let mut args = vec!["dar", name, "table"];
            if name == "query" || name == "explain" {
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
    let crate::cli::Command::Scan { limit, .. } = cli.command else {
        panic!("expected scan")
    };
    assert_eq!(support["scan"]["default_row_limit"], json!(limit));
}

#[test]
fn selected_authorization_details_keep_configuration_and_exclude_other_providers() {
    for service in [
        AuthorizationService::S3,
        AuthorizationService::AzureBlob,
        AuthorizationService::Gcs,
    ] {
        let index = service.build_authorization_guide();
        for source in index["authorization"]["sources"].as_array().unwrap() {
            let source = source.as_str().unwrap();
            let detail = service.build_authorization_detail(source).unwrap();
            assert_eq!(
                &detail["arguments"].as_array().unwrap()[..2],
                &[json!("--auth-source"), json!(source)]
            );
            assert!(detail.to_string().len() < super::super::MAX_AUTHORIZATION_GUIDE_BYTES);
            if source != "json" && source != "env" {
                let full = service.build_full_authorization_guide();
                let expected = &full["authorization"]["provider_configuration"][source];
                for field in ["json_example", "environment", "rules"] {
                    assert_eq!(detail[field], expected[field]);
                    assert!(!detail[field].is_null());
                }
                let providers = full["authorization"]["provider_configuration"]
                    .as_object()
                    .unwrap();
                for other in providers.keys().filter(|name| name.as_str() != source) {
                    assert!(!detail.to_string().contains(other));
                }
            }
        }
        let connection = service.build_authorization_detail("connection").unwrap();
        assert!(
            connection["usage"]
                .as_str()
                .unwrap()
                .contains("JSON strings")
        );
        for key in connection["settings"]
            .as_object()
            .unwrap()
            .keys()
            .chain(connection["http"].as_object().unwrap().keys())
        {
            match service {
                AuthorizationService::S3 => {
                    key.parse::<AmazonS3ConfigKey>().unwrap();
                }
                AuthorizationService::AzureBlob => {
                    key.parse::<AzureConfigKey>().unwrap();
                }
                AuthorizationService::Gcs => {
                    key.parse::<GoogleConfigKey>().unwrap();
                }
                AuthorizationService::UnityCatalog => unreachable!(),
            }
        }
    }
    assert!(
        AuthorizationService::S3
            .build_authorization_detail("azure-managed-identity")
            .is_err()
    );
    assert!(
        AuthorizationService::UnityCatalog
            .build_authorization_detail("env-file")
            .is_err()
    );
    let file = AuthorizationService::Gcs
        .build_authorization_detail("env-file")
        .unwrap();
    let env = AuthorizationService::Gcs
        .build_authorization_detail("env")
        .unwrap();
    assert!(
        file["variables"]["notes"]
            .as_str()
            .unwrap()
            .contains("env file's directory")
    );
    assert!(
        env["variables"]["notes"]
            .as_str()
            .unwrap()
            .contains("working directory")
    );
}
