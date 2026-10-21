//! Authorization choices and read limits for the installed commands.
//!
//! These constraints apply across arguments and are not expressed by Clap's
//! argument rules. For example, `--databricks-profile` requires a UC table name;
//! a SQL query can join aliases of `t`, but cannot open a second Delta table.
//! Discovery describes these rules without checking the caller's credentials,
//! installed provider tools, or access to any particular table.

use serde_json::{Value, json};

use crate::{query, unity_catalog};

pub(super) fn authorization_sources() -> Value {
    json!({
        "local": {
            "commands": ["inspect", "scan", "query"],
            "credentials_required": false,
        },
        "direct_storage": {
            "commands": ["inspect", "scan", "query"],
            "storage_options_argument": "--storage-options-file",
            "storage_options_schema": "storage-options",
            "credential_selection": "storage_backend",
            "explicit_source_selection": false,
            "env_file_input": false,
            "imports_storage_options_from_environment": false,
            "option_names_case_sensitive": false,
            "unknown_options": "ignored_by_backend",
            "credential_option_aliases": {
                "s3": {
                    "aws_access_key_id": ["access_key_id"],
                    "aws_secret_access_key": ["secret_access_key"],
                    "aws_session_token": ["aws_token", "session_token", "token"],
                },
                "azure": {
                    "azure_storage_account_key": ["azure_storage_access_key", "azure_storage_master_key", "master_key", "account_key", "access_key"],
                    "azure_storage_sas_key": ["azure_storage_sas_token", "sas_key", "sas_token"],
                    "azure_storage_token": ["bearer_token", "token"],
                    "azure_storage_client_id": ["azure_client_id", "client_id"],
                    "azure_storage_client_secret": ["azure_client_secret", "client_secret"],
                    "azure_storage_tenant_id": ["azure_storage_authority_id", "azure_tenant_id", "azure_authority_id", "tenant_id", "authority_id"],
                },
                "gcs": {
                    "google_service_account": ["service_account", "google_service_account_path", "service_account_path"],
                    "google_service_account_key": ["service_account_key"],
                    "google_application_credentials": ["application_credentials"],
                },
            },
            "notes": [
                "Local paths need no cloud credentials. Cloud reads use the selected backend's existing credential resolution.",
                "The listed credential keys cover static credentials and GCS credential files. Backend provider and transport settings are also accepted through the JSON file.",
                "S3 static credentials require an access key and secret key; a session token is optional. AWS environment variables and profile/SSO files are not imported as storage options.",
                "Azure accepts an account key, SAS token, bearer token, or client ID/client secret/tenant ID settings. Backend precedence applies if several credential families are supplied.",
                "GCS accepts a service-account file path, service-account JSON string, or application-credentials file path. The two service-account forms cannot be combined.",
                "Without explicit credential settings, S3 uses instance metadata, Azure uses managed identity, and GCS checks the well-known ADC file before using metadata credentials. These lookups happen during table access, not discovery.",
            ],
        },
        "unity_catalog": {
            "commands": ["inspect", "scan"],
            "target": "uc://catalog.schema.table",
            "profile_argument": "--databricks-profile",
            "profile_required": true,
            "executable": "databricks",
            "executable_bundled": false,
            "interactive_login": false,
            "credential_operation": "READ",
            "credential_backends": ["s3", "r2", "azure", "gcs"],
            "credential_refresh": true,
            "fallback_to_other_identity": false,
            "storage_options": "transport_and_region_only",
            "s3_region_required": true,
            "subprocess_timeout_ms": unity_catalog::DATABRICKS_COMMAND_TIMEOUT.as_millis(),
            "max_subprocess_stream_bytes": unity_catalog::MAX_COMMAND_OUTPUT_BYTES,
            "notes": [
                "Use three unquoted name components and an explicit, nonempty profile. A profile is invalid for direct table locations.",
                "Requires an existing Databricks login and external read permissions. Discovery does not test the executable, profile, or grants.",
                "Only ordinary managed/external Delta tables with a supported read protocol are accepted. Views, pipeline tables, shallow clones, row filters, column masks, catalog-managed commits, and coordinated commits are rejected.",
                "Credentials are renewed for the same table ID and profile without changing the selected snapshot. Renewal failure stops the command.",
            ],
        },
    })
}

pub(super) fn command_read_support() -> Value {
    json!({
        "inspect": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "scope": "snapshot_metadata",
            "reads_data_files": false,
            "requires_supported_read_protocol": {"direct_storage": false, "unity_catalog": true},
            "max_output_bytes": null,
            "deadline_ms": null,
        },
        "scan": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "scope": "selected_rows_and_columns",
            "row_limit_argument": "--limit",
            "default_row_limit": null,
            "row_limit_behavior": "stop_after_selected_rows",
            "requires_redirected_stdout": true,
            "max_output_bytes": null,
            "deadline_ms": null,
        },
        "query": {
            "authorization": ["local", "direct_storage"],
            "scope": "complete_sql_result",
            "registered_tables": ["t"],
            "max_table_bindings": 1,
            "self_joins": true,
            "external_table_discovery": false,
            "sql_input": {
                "argument": "--sql-file",
                "encoding": "utf-8",
                "stdin": false,
                "max_file_bytes": query::MAX_SQL_BYTES,
                "max_statements": 1,
                "max_parser_and_expression_depth": query::SQL_RECURSION_LIMIT,
                "statement_kind": "read_only_query",
                "recursive_ctes": false,
                "table_functions": false,
                "examples": [
                    "SELECT count(*) AS row_count FROM t",
                    "SELECT id, count(*) AS n FROM t GROUP BY id ORDER BY id",
                    "WITH selected AS (SELECT id FROM t) SELECT id FROM selected ORDER BY id",
                    "SELECT a.id FROM t AS a JOIN t AS b ON a.id = b.id ORDER BY a.id",
                ],
            },
            "result_limit_arguments": ["--max-rows", "--max-output-bytes"],
            "result_limit_behavior": "fail_if_exceeded",
            "memory_limit_argument": "--memory-limit-bytes",
            "memory_limit_scope": "datafusion_execution_pool",
            "disk_spill": false,
            "requires_redirected_stdout": true,
            "deadline_ms": null,
        },
    })
}

#[cfg(test)]
mod tests {
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
}
