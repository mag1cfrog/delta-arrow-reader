//! Authorization choices and read limits for the installed commands.
//!
//! These constraints apply across arguments and are not expressed by Clap's
//! argument rules. For example, `--databricks-profile` requires a UC table name;
//! a SQL query can join declared snapshots but cannot discover tables from SQL text.
//! Discovery describes these rules without checking the caller's credentials,
//! installed provider tools, or access to any particular table.

use clap::ValueEnum;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{query, unity_catalog};

const UNITY_CATALOG_PROFILE_USAGE: &str = "Use three unquoted name components and an explicit, nonempty profile. A profile is invalid for direct table locations.";
const UNITY_CATALOG_ACCESS_REQUIREMENTS: &str = "Requires an existing Databricks login and external read permissions. Discovery does not test the executable, profile, or grants.";
pub(super) const QUERY_SNAPSHOT_SELECTION: &str = "Each alias loads once. Actual versions are decimal strings in declaration order: stderr bindings for stdout results or SQL/result failures with --diagnostics=json; manifest bindings for saved results. Pin every alias on retry. Snapshots are not a cross-table transaction.";
pub(super) const QUERY_ROW_ORDERING: &str =
    "Use ORDER BY with tie-breaking columns for repeatable row order.";

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum AuthorizationService {
    S3,
    AzureBlob,
    Gcs,
    UnityCatalog,
}

impl AuthorizationService {
    pub(super) fn build_authorization_guide(self) -> Value {
        let capabilities = build_authorization_capabilities();
        let mut authorization = match self {
            Self::UnityCatalog => {
                let catalog = &capabilities["unity_catalog"];
                json!({
                    "commands": catalog["commands"],
                    "target": catalog["target"],
                    "profile_argument": catalog["profile_argument"],
                    "manifest_authorization": catalog["manifest_authorization"],
                    "executable": catalog["executable"],
                    "storage_options": catalog["storage_options"],
                    "s3_region_required": catalog["s3_region_required"],
                    "fallback_to_other_identity": catalog["fallback_to_other_identity"],
                    "notes": [UNITY_CATALOG_PROFILE_USAGE, UNITY_CATALOG_ACCESS_REQUIREMENTS],
                })
            }
            _ => {
                let storage = &capabilities["direct_storage"];
                let keys: Vec<_> = storage["credential_option_aliases"][self.name()]
                    .as_object()
                    .into_iter()
                    .flat_map(|keys| keys.keys())
                    .filter(|key| {
                        !matches!(self, Self::Gcs) || *key == "google_application_credentials"
                    })
                    .collect();
                json!({
                    "commands": storage["commands"],
                    "source_argument": storage["source_argument"],
                    "sources": storage["sources"][self.name()],
                    "env_file_details_command": [crate::command_name(), "capabilities", "--authorization", self, "--detail", "env-file"],
                    "storage_options_argument": storage["storage_options_argument"],
                    "credential_keys": keys,
                    "notes": storage["backend_notes"][self.name()],
                    "schema_command": [crate::command_name(), "capabilities", "--schema", "storage-options"],
                })
            }
        };
        authorization["credential_handling"] = json!(self.credential_handling_guidance());
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

    fn credential_handling_guidance(self) -> &'static str {
        match self {
            Self::UnityCatalog => {
                "Pass an existing Databricks profile name; never put credential values in arguments."
            }
            _ => {
                "Use json with a storage-options file path, env with provisioned variables, or --env-file with a credential file path. env and --env-file reject credentials in storage-options. Explicit sources never fall back to another identity; never put credential values in arguments."
            }
        }
    }

    pub(super) fn build_full_authorization_guide(self) -> Value {
        let capabilities = build_authorization_capabilities();
        let mut authorization = if matches!(self, Self::UnityCatalog) {
            capabilities["unity_catalog"].clone()
        } else {
            let name = self.name();
            let mut storage = capabilities["direct_storage"].clone();
            storage["env_file_details_command"] = json!([
                crate::command_name(),
                "capabilities",
                "--authorization",
                self,
                "--detail",
                "env-file"
            ]);
            storage["sources"] = storage["sources"][name].clone();
            storage["schema_command"] = json!([
                crate::command_name(),
                "capabilities",
                "--schema",
                "storage-options"
            ]);
            storage["credential_option_aliases"] =
                storage["credential_option_aliases"][name].clone();
            if let Some(configuration) = storage["provider_configuration"].get(name).cloned() {
                storage["provider_configuration"] = configuration;
            } else if let Some(storage) = storage.as_object_mut() {
                storage.remove("provider_configuration");
            }
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
        authorization["credential_handling"] = json!(self.credential_handling_guidance());
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

pub(super) fn build_authorization_capabilities() -> Value {
    json!({
        "local": {
            "commands": ["inspect", "scan", "preview", "query"],
            "credentials_required": false,
        },
        "direct_storage": {
            "commands": ["inspect", "scan", "preview", "query"],
            "storage_options_argument": "--storage-options-file",
            "storage_options_schema": "storage-options",
            "credential_selection": "explicit_source_or_storage_backend_default",
            "file_output_credential_selection": "explicit_source_required; a storage-options file alone selects json",
            "explicit_source_selection": true,
            "source_argument": "--auth-source",
            "manifest_source_field": "tables[].authorization.source",
            "sources": {
                "s3": ["json", "env", "aws-web-identity", "aws-container"],
                "azure-blob": ["json", "env", "azure-workload-identity", "azure-managed-identity"],
                "gcs": ["json", "env"],
            },
            "provider_configuration": {
                "s3": {
                    "aws-web-identity": {
                        "json_example": {"aws_role_arn": "arn:aws:iam::123456789012:role/reader", "aws_web_identity_token_file": "/run/secrets/identity.jwt"},
                        "environment": {
                            "aws_role_arn": "AWS_ROLE_ARN", "aws_web_identity_token_file": "AWS_WEB_IDENTITY_TOKEN_FILE",
                            "aws_role_session_name": "AWS_ROLE_SESSION_NAME", "aws_endpoint_url_sts": "AWS_ENDPOINT_URL_STS",
                        },
                        "rules": [
                            "Role ARN and token file are required. Each JSON field overrides its environment variable; omitted fields use the environment.",
                            "Session defaults to WebIdentitySession; STS defaults to https://sts.<region>.amazonaws.com and requires HTTPS even when storage allows HTTP.",
                            "The role, session, endpoint and file path stay fixed; refresh rereads the token file. Other credential families in JSON are rejected; ambient credentials are ignored.",
                        ],
                    },
                    "aws-container": {
                        "json_example": {"aws_container_credentials_relative_uri": "/v2/credentials/task-id"},
                        "environment": {
                            "aws_container_credentials_relative_uri": "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
                            "aws_container_credentials_full_uri": "AWS_CONTAINER_CREDENTIALS_FULL_URI",
                            "aws_container_authorization_token_file": "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
                        },
                        "rules": [
                            "Select either an ECS relative URI, or an EKS full URI plus token file. Any JSON reference selects the entire family and disables all environment references.",
                            "ECS appends the URI to http://169.254.170.2; it must start with one slash and contain no backslashes. Full URIs require HTTP or HTTPS. Userinfo and fragments are rejected; queries are allowed.",
                            "Refresh uses the fixed endpoint and rereads the EKS token file. AWS_CONTAINER_AUTHORIZATION_TOKEN is unsupported. Other credential families in JSON are rejected; ambient credentials are ignored.",
                        ],
                    },
                },
                "azure-blob": {
                    "azure-workload-identity": {
                        "json_example": {"azure_storage_client_id": "reader-client", "azure_storage_tenant_id": "reader-tenant", "azure_federated_token_file": "/run/secrets/identity.jwt"},
                        "environment": {
                            "azure_storage_client_id": "AZURE_CLIENT_ID", "azure_storage_tenant_id": "AZURE_TENANT_ID",
                            "azure_federated_token_file": "AZURE_FEDERATED_TOKEN_FILE", "azure_storage_authority_host": "AZURE_AUTHORITY_HOST",
                        },
                        "rules": [
                            "Client ID, tenant ID and token file are required. Each JSON field overrides its environment variable; omitted fields use the environment. Authority defaults to the public Azure authority.",
                            "The principal, authority and file path stay fixed; refresh rereads the token file. Other credential families in JSON are rejected; ambient credentials are ignored.",
                        ],
                    },
                    "azure-managed-identity": {
                        "json_example": {"azure_storage_client_id": "reader-client"},
                        "environment": {
                            "azure_storage_client_id": "AZURE_CLIENT_ID", "azure_object_id": "AZURE_OBJECT_ID",
                            "azure_msi_resource_id": "AZURE_MSI_RESOURCE_ID", "azure_msi_endpoint": "IDENTITY_ENDPOINT",
                        },
                        "rules": [
                            "Select exactly one client ID, object ID or resource ID. One JSON selector overrides all environment selectors; otherwise exactly one environment selector is required.",
                            "Endpoint precedence: JSON, IDENTITY_ENDPOINT, then Azure IMDS. IDENTITY_HEADER is read only from the environment and sent only to the token endpoint; AZURE_MSI_ENDPOINT is ignored.",
                            "Refresh keeps the selected identity and endpoint. No default identity is selected. Other credential families in JSON are rejected; ambient credentials are ignored.",
                        ],
                    },
                },
            },
            "env_file_input": true,
            "env_file_argument": "--env-file",
            "env_file_details_command": [crate::command_name(), "capabilities", "--authorization", "NAME", "--detail", "env-file"],
            "imports_storage_options_from_environment": true,
            "option_names_case_sensitive": false,
            "unknown_options": {"with_auth_source": "rejected", "without_auth_source": "ignored_by_backend", "with_env_file": "rejected"},
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
                "Explicit sources reject duplicate aliases and conflicting credential families. Local and Unity Catalog targets reject --auth-source and --env-file. These selectors cannot be combined.",
                "Without --auth-source or --env-file, native backend precedence and credential defaults apply; environment options are not imported.",
            ],
            "backend_notes": {
                "s3": [
                    "json/env require an access key and secret key; a session token is optional. env uses AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN, AWS_REGION and AWS_DEFAULT_REGION.",
                    "Workload sources use provisioned role/token-file or container endpoint references. AWS profiles/SSO are unsupported. Omitting both source selectors uses backend defaults.",
                ],
                "azure-blob": [
                    "json/env require exactly one complete credential family. env accepts AZURE_STORAGE_ACCOUNT_KEY, AZURE_STORAGE_SAS_KEY or AZURE_STORAGE_TOKEN, or AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID. If the URL omits the account, supply azure_storage_account_name in JSON or AZURE_STORAGE_ACCOUNT_NAME with env.",
                    "Workload identity requires a client ID, tenant ID and token file; managed identity requires one client, object or resource ID. Omitting both source selectors uses backend defaults.",
                ],
                "gcs": [
                    "env reads GOOGLE_APPLICATION_CREDENTIALS. json/env select an explicit service-account or authorized-user ADC file without home-directory or metadata fallback.",
                    "Service-account path/key aliases apply only without an explicit source selector, which uses backend defaults.",
                ],
            },
        },
        "unity_catalog": {
            "commands": ["inspect", "scan", "preview", "query"],
            "target": "uc://catalog.schema.table",
            "profile_argument": "--databricks-profile",
            "profile_required": true,
            "manifest_authorization": {"source": "databricks", "databricks_profile": "PROFILE"},
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
            "summary_max_output_bytes": crate::inspection::DEFAULT_SUMMARY_BYTES,
            "summary_budget_argument": "--max-output-bytes",
            "summary_only_options": ["--column", "--column-prefix", "--include-metadata", "--max-output-bytes"],
            "deadline_ms": null,
            "summary_deadline_ms": 30000,
            "deadline_argument": "--deadline-ms",
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
            "deadline_argument": "--deadline-ms",
            "file_output": {"argument": "--output", "requires_redirected_stdout": false,
                "max_rows": 1000000, "max_output_bytes": 1073741824, "deadline_ms": 300000},
        },
        "preview": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "scope": "sample",
            "row_limit_argument": "--limit",
            "default_row_limit": 20,
            "max_output_bytes": crate::output::json::DEFAULT_MAX_BYTES,
            "byte_limit_argument": "--max-output-bytes",
            "deadline_ms": 30000,
            "deadline_argument": "--deadline-ms",
            "cleanup_grace_ms": 1000,
            "failure_reporting_grace_ms": crate::diagnostics::FAILURE_DIAGNOSTIC_TIMEOUT.as_millis(),
            "requires_redirected_stdout": false,
        },
        "query": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "unity_catalog_input": "positional_table_or_tables_file",
            "scope": "complete_sql_result",
            "positional_alias": "t",
            "required_table_input": {"one_of": ["TABLE", "--tables-file"]},
            "tables_file_argument": "--tables-file",
            "max_table_bindings": query::tables_manifest::MAX_TABLES,
            "snapshot_selection": QUERY_SNAPSHOT_SELECTION,
            "ordering": QUERY_ROW_ORDERING,
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
            "disk_spill": true,
            "spill": {
                "default": "disabled",
                "budget_argument": "--spill-limit-bytes",
                "directory_argument": "--spill-dir",
                "quota_check": "after_writes",
                "details_command": ["dar", "capabilities", "--command", "query", "--detail", "resources"],
            },
            "requires_redirected_stdout": true,
            "json_requires_redirected_stdout": false,
            "file_output": {"argument": "--output", "requires_redirected_stdout": false,
                "default_max_bytes": {"arrow": 1073741824, "json": 16777216}},
            "deadline_ms": 300000,
            "deadline_argument": "--deadline-ms",
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
    fn full_authorization_examples_configure_each_advertised_identity_provider() {
        use delta_arrow_reader::{DeltaStorageOptions, DeltaTableBuilder};

        use crate::direct_credentials::CredentialSource;

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
    fn authorization_guides_defer_aliases_and_keep_selected_credential_rules() {
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
            assert!(
                selected["credential_keys"]
                    .as_array()
                    .unwrap()
                    .contains(&json!(expected_key))
            );
            assert!(selected.get("credential_option_aliases").is_none());
            assert_eq!(
                selected["notes"],
                build_authorization_capabilities()["direct_storage"]["backend_notes"]
                    [service.name()]
            );
            assert_eq!(
                concise["full_command"],
                json!([
                    crate::command_name(),
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
            assert_eq!(selected["source_argument"], "--auth-source");
            assert_eq!(
                selected["env_file_details_command"],
                json!([
                    crate::command_name(),
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
            ["inspect", "preview", "query", "scan"]
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
            crate::direct_credentials::CredentialSource::value_variants()
                .iter()
                .map(|source| source.to_possible_value().unwrap().get_name().to_owned())
                .collect();
        assert_eq!(
            advertised_sources,
            installed_sources.iter().map(String::as_str).collect()
        );
        let support = command_read_support();
        for name in ["inspect", "scan", "preview", "query"] {
            match name {
                "preview" => assert_eq!(support[name]["deadline_ms"], 30000),
                "query" => assert_eq!(support[name]["deadline_ms"], 300000),
                _ => assert!(support[name]["deadline_ms"].is_null()),
            }
            for source in &advertised_sources {
                let mut args = vec!["dar", name, "s3://bucket/table", "--auth-source", source];
                if name == "query" {
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
            if name == "query" {
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
