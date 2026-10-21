//! Authorization choices and read limits for the installed commands.
//!
//! These constraints apply across arguments and are not expressed by Clap's
//! argument rules. For example, `--databricks-profile` requires a UC table name;
//! a SQL query can join aliases of `t`, but cannot open a second Delta table.
//! Discovery describes these rules without checking the caller's credentials,
//! installed provider tools, or access to any particular table.

use clap::ValueEnum;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{query, unity_catalog};

const UNITY_CATALOG_PROFILE_USAGE: &str = "Use three unquoted name components and an explicit, nonempty profile. A profile is invalid for direct table locations.";
const UNITY_CATALOG_ACCESS_REQUIREMENTS: &str = "Requires an existing Databricks login and external read permissions. Discovery does not test the executable, profile, or grants.";

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum AuthorizationGuide {
    S3,
    AzureBlob,
    Gcs,
    UnityCatalog,
}

impl AuthorizationGuide {
    pub(super) fn guide_document(self) -> Value {
        let sources = authorization_sources();
        let mut authorization = match self {
            Self::UnityCatalog => {
                let catalog = &sources["unity_catalog"];
                json!({
                    "commands": catalog["commands"],
                    "target": catalog["target"],
                    "profile_argument": catalog["profile_argument"],
                    "executable": catalog["executable"],
                    "storage_options": catalog["storage_options"],
                    "s3_region_required": catalog["s3_region_required"],
                    "fallback_to_other_identity": catalog["fallback_to_other_identity"],
                    "notes": [UNITY_CATALOG_PROFILE_USAGE, UNITY_CATALOG_ACCESS_REQUIREMENTS],
                })
            }
            _ => {
                let storage = &sources["direct_storage"];
                let keys: Vec<_> = storage["credential_option_aliases"][self.name()]
                    .as_object()
                    .into_iter()
                    .flat_map(|keys| keys.keys())
                    .collect();
                json!({
                    "commands": storage["commands"],
                    "storage_options_argument": storage["storage_options_argument"],
                    "credential_keys": keys,
                    "notes": storage["backend_notes"][self.name()],
                    "schema_command": [crate::command_name(), "capabilities", "--schema", "storage-options"],
                })
            }
        };
        authorization["credential_handling"] = json!(self.credential_handling());
        json!({
            "format_version": 1,
            "kind": "authorization_guide",
            "name": self,
            "authorization": authorization,
            "full_command": [crate::command_name(), "capabilities", "--authorization", self, "--full"],
        })
    }

    fn name(self) -> &'static str {
        match self {
            Self::S3 => "s3",
            Self::AzureBlob => "azure-blob",
            Self::Gcs => "gcs",
            Self::UnityCatalog => "unity-catalog",
        }
    }

    fn credential_handling(self) -> &'static str {
        match self {
            Self::UnityCatalog => {
                "Pass an existing Databricks profile name; never put credential values in arguments."
            }
            _ => {
                "For explicit credentials, pass an existing storage-options file path; never put credential values in arguments."
            }
        }
    }

    pub(super) fn full_document(self) -> Value {
        let sources = authorization_sources();
        let mut authorization = if matches!(self, Self::UnityCatalog) {
            sources["unity_catalog"].clone()
        } else {
            let name = self.name();
            let mut storage = sources["direct_storage"].clone();
            storage["schema_command"] = json!([
                crate::command_name(),
                "capabilities",
                "--schema",
                "storage-options"
            ]);
            storage["credential_option_aliases"] =
                storage["credential_option_aliases"][name].clone();
            storage["notes"] = storage["notes"]
                .as_array()
                .into_iter()
                .flatten()
                .chain(
                    storage["backend_notes"][name]
                        .as_array()
                        .into_iter()
                        .flatten(),
                )
                .cloned()
                .collect();
            if let Some(object) = storage.as_object_mut() {
                object.remove("backend_notes");
            }
            storage
        };
        authorization["credential_handling"] = json!(self.credential_handling());
        json!({
            "format_version": 1,
            "kind": "authorization_capabilities",
            "cli_version": env!("CARGO_PKG_VERSION"),
            "name": self,
            "authorization": authorization,
            "limits": super::discovery_limits(super::MAX_FULL_RESPONSE_BYTES),
        })
    }
}

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
                "azure-blob": {
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
                "Cloud reads use the selected backend's existing credential resolution.",
                "The listed keys cover authentication inputs. Backend provider and transport settings are also accepted through the JSON file.",
            ],
            "backend_notes": {
                "s3": [
                    "Static credentials require an access key and secret key; a session token is optional. AWS environment variables and profile/SSO files are not imported as storage options.",
                    "Without explicit credential settings, the backend uses instance metadata during table access.",
                ],
                "azure-blob": [
                    "Accepts an account key, SAS token, bearer token, or client ID/client secret/tenant ID settings. Backend precedence applies if several credential families are supplied.",
                    "Without explicit credential settings, the backend uses managed identity during table access.",
                ],
                "gcs": [
                    "Accepts a service-account file path, service-account JSON string, or application-credentials file path. The two service-account forms cannot be combined.",
                    "Without explicit credential settings, the backend checks the well-known ADC file before using metadata credentials during table access.",
                ],
            },
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
            "credential_backends": ["s3", "r2", "azure-blob", "gcs"],
            "credential_refresh": true,
            "fallback_to_other_identity": false,
            "storage_options": "transport_and_region_only",
            "s3_region_required": true,
            "subprocess_timeout_ms": unity_catalog::DATABRICKS_COMMAND_TIMEOUT.as_millis(),
            "max_subprocess_stream_bytes": unity_catalog::MAX_COMMAND_OUTPUT_BYTES,
            "notes": [
                UNITY_CATALOG_PROFILE_USAGE,
                UNITY_CATALOG_ACCESS_REQUIREMENTS,
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
    fn authorization_guides_defer_aliases_and_keep_selected_credential_rules() {
        let credential_keys = [
            "aws_access_key_id",
            "azure_storage_account_key",
            "google_service_account",
        ];
        for (guide, expected_key) in [
            (AuthorizationGuide::S3, credential_keys[0]),
            (AuthorizationGuide::AzureBlob, credential_keys[1]),
            (AuthorizationGuide::Gcs, credential_keys[2]),
        ] {
            let document = guide.full_document();
            let authorization = &document["authorization"];
            let aliases = authorization["credential_option_aliases"]
                .as_object()
                .unwrap();
            assert!(aliases.contains_key(expected_key));
            let concise = guide.guide_document();
            let selected = &concise["authorization"];
            assert_eq!(
                selected["credential_handling"],
                authorization["credential_handling"]
            );
            let handling = selected["credential_handling"].as_str().unwrap();
            assert!(handling.contains("storage-options file path"));
            assert!(!handling.contains("profile"));
            assert!(handling.contains("never put credential values in arguments"));
            assert!(
                selected["credential_keys"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(expected_key))
            );
            assert!(selected.get("credential_option_aliases").is_none());
            assert_eq!(
                selected["notes"],
                authorization_sources()["direct_storage"]["backend_notes"][guide.name()]
            );
            assert_eq!(
                concise["full_command"],
                json!([
                    crate::command_name(),
                    "capabilities",
                    "--authorization",
                    guide,
                    "--full"
                ])
            );
            for other in credential_keys
                .into_iter()
                .filter(|key| *key != expected_key)
            {
                assert!(!aliases.contains_key(other));
                assert!(
                    !selected["credential_keys"]
                        .as_array()
                        .unwrap()
                        .contains(&json!(other))
                );
            }
            assert!(authorization.get("backend_notes").is_none());
            assert!(authorization.get("profile_argument").is_none());
            assert_eq!(authorization["storage_options_schema"], "storage-options");
        }
        let document = AuthorizationGuide::UnityCatalog.full_document();
        let concise = AuthorizationGuide::UnityCatalog.guide_document();
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
