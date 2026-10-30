//! Authorization choices and credential guidance for the installed commands.
//!
//! Discovery describes these rules without checking credentials, installed
//! provider tools, or access to any particular table.

mod env_file;

use env_file::build_env_file_guide;

use clap::ValueEnum;
use serde::Serialize;
use serde_json::{Value, json};

use crate::{
    diagnostics::{Error, InputCorrection},
    table_access::unity_catalog,
};

const UNITY_CATALOG_PROFILE_USAGE: &str = "Use three unquoted name components and an explicit, nonempty profile. A profile is invalid for direct table locations.";
const UNITY_CATALOG_ACCESS_REQUIREMENTS: &str = "Requires an existing Databricks login and external read permissions. Discovery does not test the executable, profile, or grants.";
const S3_AUTHORIZATION_LIMITS: &str =
    "AWS profiles/SSO are unsupported. Stdout without source selectors uses backend defaults.";

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
                json!({
                    "commands": storage["commands"],
                    "source_argument": storage["source_argument"],
                    "sources": storage["sources"][self.name()],
                    "source_details_command": [crate::cli::command_name(), "capabilities", "--authorization", self, "--detail", "SOURCE"],
                    "env_file_details_command": [crate::cli::command_name(), "capabilities", "--authorization", self, "--detail", "env-file"],
                    "storage_connection_details_command": [crate::cli::command_name(), "capabilities", "--authorization", self, "--detail", "connection"],
                    "storage_options_argument": storage["storage_options_argument"],
                    "notes": if matches!(self, Self::S3) {
                        json!([S3_AUTHORIZATION_LIMITS])
                    } else {
                        json!(["Stdout without source selectors uses backend defaults."])
                    },
                    "schema_command": [crate::cli::command_name(), "capabilities", "--schema", "storage-options"],
                })
            }
        };
        authorization["credential_handling"] = json!(self.credential_handling_guidance());
        json!({
            "format_version": 1,
            "kind": "authorization_guide",
            "name": self,
            "authorization": authorization,
            "full_command": [crate::cli::command_name(), "capabilities", "--authorization", self, "--full"],
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
                "Use json with a storage-options file path, env with provisioned variables, or --env-file with a credential file path. --output requires an explicit source; a storage-options file alone selects json. env and --env-file reject credentials in storage-options. Explicit sources never fall back to another identity; never put credential values in arguments."
            }
        }
    }

    pub(super) fn build_authorization_detail(self, detail: &str) -> Result<Value, Error> {
        if !matches!(self, Self::UnityCatalog) && matches!(detail, "env-file" | "env-file-grammar")
        {
            return build_env_file_guide(self, detail);
        }
        let capabilities = build_authorization_capabilities();
        let storage = &capabilities["direct_storage"];
        let sources = &storage["sources"][self.name()];
        let is_source = sources
            .as_array()
            .is_some_and(|sources| sources.iter().any(|source| source == detail));
        if matches!(self, Self::UnityCatalog) || (!is_source && detail != "connection") {
            return Err(Error::Argument {
                kind: clap::error::ErrorKind::InvalidValue,
                correction: Some(InputCorrection {
                    argument: "--detail".into(),
                    field_path: None,
                    supplied: None,
                    expected: if matches!(self, Self::UnityCatalog) {
                        "no detail selector; omit --detail for the profile guide".into()
                    } else {
                        format!(
                            "a source from {sources}, env-file, env-file-grammar, or connection"
                        )
                    },
                    hint: "Keep the selected service and use its listed detail or omit --detail.",
                }),
            });
        }
        let mut guide = match detail {
            "connection" => json!({
                "argument": "--storage-options-file",
                "settings": match self {
                    Self::S3 => json!({
                        "aws_region": "Bucket region; default us-east-1. JSON overrides environment region settings.",
                        "aws_endpoint_url": "Optional S3-compatible endpoint URL.",
                        "aws_virtual_hosted_style_request": "true for virtual-hosted requests; false for path-style requests.",
                    }),
                    Self::AzureBlob => json!({
                        "azure_storage_account_name": "Required if the URL omits the account name.",
                        "azure_storage_endpoint": "Optional storage endpoint URL.",
                    }),
                    Self::Gcs => json!({"google_base_url": "Optional storage endpoint URL."}),
                    Self::UnityCatalog => unreachable!("handled above"),
                },
                "http": {"allow_http": "Allow HTTP requests to table storage; false by default."},
                "usage": "Common connection settings; all values are JSON strings. Correct or remove unsupported settings, keeping the selected authorization. With --env-file or --auth-source env, keep credentials out of this JSON file.",
            }),
            "json" => json!({
                "arguments": ["--auth-source", "json", "--storage-options-file", "PATH"],
                "credential_fields": match self {
                    Self::S3 => json!({"required": ["aws_access_key_id", "aws_secret_access_key"], "optional": ["aws_session_token"]}),
                    Self::AzureBlob => json!({"one_complete_family": [["azure_storage_account_key"], ["azure_storage_sas_key"], ["azure_storage_token"], ["azure_storage_client_id", "azure_storage_client_secret", "azure_storage_tenant_id"]]}),
                    Self::Gcs => json!({"required": ["google_application_credentials"], "usage": "Path to a service-account or authorized-user ADC file; relative paths use the options file's directory."}),
                    Self::UnityCatalog => unreachable!("handled above"),
                },
                "usage": "Pass a provisioned file path without reading or expanding credentials into arguments. JSON values are strings. Environment credentials are ignored; failures do not select another identity.",
            }),
            "env" => {
                let mut variables = build_env_file_guide(self, "env-file")?["variables"].clone();
                if matches!(self, Self::Gcs) {
                    variables["notes"] = json!(
                        "Reference a service-account or authorized-user ADC file. Relative paths use the working directory. No default ADC lookup; tokens renew for the loaded identity."
                    );
                }
                variables["names"] = json!("Only these case-sensitive environment names are read.");
                json!({
                    "arguments": ["--auth-source", "env"], "variables": variables,
                    "usage": "Use provisioned environment credentials without expanding values into arguments. An optional --storage-options-file may contain connection settings only. Failures never fall back to another identity.",
                })
            }
            _ => storage["provider_configuration"][self.name()][detail].clone(),
        };
        guide["format_version"] = json!(1);
        guide["kind"] = json!("authorization_detail");
        guide["name"] = json!(self);
        guide["detail"] = json!(detail);
        if is_source && guide.get("arguments").is_none() {
            guide["arguments"] = json!(["--auth-source", detail]);
        }
        Ok(guide)
    }

    pub(super) fn build_full_authorization_guide(self) -> Value {
        let capabilities = build_authorization_capabilities();
        let mut authorization = if matches!(self, Self::UnityCatalog) {
            capabilities["unity_catalog"].clone()
        } else {
            let name = self.name();
            let mut storage = capabilities["direct_storage"].clone();
            storage["env_file_details_command"] = json!([
                crate::cli::command_name(),
                "capabilities",
                "--authorization",
                self,
                "--detail",
                "env-file"
            ]);
            storage["sources"] = storage["sources"][name].clone();
            storage["schema_command"] = json!([
                crate::cli::command_name(),
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
            "commands": ["inspect", "scan", "preview", "query", "explain"],
            "credentials_required": false,
        },
        "direct_storage": {
            "commands": ["inspect", "scan", "preview", "query", "explain"],
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
            "env_file_details_command": [crate::cli::command_name(), "capabilities", "--authorization", "NAME", "--detail", "env-file"],
            "imports_storage_options_from_environment": true,
            "option_names_case_sensitive": false,
            "unknown_options": {"with_auth_source": "rejected", "without_auth_source": "ignored_by_backend", "without_auth_source_scope": "stdout_only", "with_env_file": "rejected"},
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
                "For stdout without --auth-source or --env-file, native backend precedence and credential defaults apply; environment options are not imported.",
            ],
            "backend_notes": {
                "s3": [
                    "json/env require an access key and secret key; a session token is optional. env uses AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, optional AWS_SESSION_TOKEN, AWS_REGION and AWS_DEFAULT_REGION.",
                    format!("Workload sources use provisioned role/token-file or container endpoint references. {S3_AUTHORIZATION_LIMITS}"),
                ],
                "azure-blob": [
                    "json/env require exactly one complete credential family. env accepts AZURE_STORAGE_ACCOUNT_KEY, AZURE_STORAGE_SAS_KEY or AZURE_STORAGE_TOKEN, or AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID. If the URL omits the account, supply azure_storage_account_name in JSON or AZURE_STORAGE_ACCOUNT_NAME with env.",
                    "Workload identity requires a client ID, tenant ID and token file; managed identity requires one client, object or resource ID. Stdout without source selectors uses backend defaults.",
                ],
                "gcs": [
                    "env reads GOOGLE_APPLICATION_CREDENTIALS. json/env select an explicit service-account or authorized-user ADC file without home-directory or metadata fallback.",
                    "Service-account path/key aliases apply only to stdout without source selectors, which uses backend defaults.",
                ],
            },
        },
        "unity_catalog": {
            "commands": ["inspect", "scan", "preview", "query", "explain"],
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
                "Managed/external Delta tables with a supported read protocol are accepted. Catalog-managed commits require workspace Delta API 1.0 reads. Views, pipeline tables, shallow clones, row filters, column masks, and coordinated commits are rejected.",
                "Credentials are renewed for the same table ID and profile without changing the selected snapshot. Renewal failure stops the command.",
            ],
        },
    })
}

#[cfg(test)]
mod tests;
