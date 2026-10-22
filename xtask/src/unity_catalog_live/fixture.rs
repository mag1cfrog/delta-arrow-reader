//! Private fixture inputs and the public labels allowed in a validation report.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, ensure};

use crate::error::{FileSnafu, InvalidInputSnafu, JsonSnafu, Result};

pub(super) fn check_input_size(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path).context(FileSnafu { path })?;
    ensure!(
        metadata.is_file() && metadata.len() <= 16 * 1024 * 1024,
        InvalidInputSnafu {
            message: "Fixture input must be a regular file at most 16 MiB"
        }
    );
    Ok(())
}

/// One prepared table and independent reference outputs. Never serialize profile or table names.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LiveFixtureConfig {
    pub databricks_profile: String,
    pub table: String,
    pub table_version: u64,
    pub expected_table: PathBuf,
    pub scan: ScanExpectation,
    pub storage_options_file: Option<PathBuf>,
    pub context: FixtureContext,
}

/// A filtered projection and its complete expected Arrow file, including schema and duplicate rows.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ScanExpectation {
    pub columns: Vec<String>,
    pub predicate_file: PathBuf,
    pub expected_result: PathBuf,
}

/// Operator-declared fixture properties; they are not inferred from a successful read.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FixtureContext {
    pub credential_form: CredentialForm,
    pub auth_mode: AuthenticationMode,
    pub table_kind: TableKind,
    pub protocol: FixtureProtocol,
}

/// Public credential labels, without any credential values.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CredentialForm {
    S3Session,
    AzureBlobSas,
    AzureBlobBearer,
    GcsOauth,
    R2Session,
}

impl CredentialForm {
    pub(super) fn cloud(&self) -> &'static str {
        match self {
            Self::S3Session => "aws",
            Self::AzureBlobSas | Self::AzureBlobBearer => "azure",
            Self::GcsOauth => "gcp",
            Self::R2Session => "cloudflare",
        }
    }
}

/// Authentication already configured in the selected Databricks CLI profile.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum AuthenticationMode {
    UserOauth,
    ServicePrincipalOauth,
    Pat,
}

/// Table kinds eligible for the existing direct-read path.
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum TableKind {
    Managed,
    External,
}

/// Reader requirements recorded by the fixture administrator at the selected snapshot.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FixtureProtocol {
    min_reader_version: u32,
    min_writer_version: u32,
    reader_features: Vec<String>,
}

impl LiveFixtureConfig {
    pub(super) fn load(path: &Path) -> Result<Self> {
        let path = fs::canonicalize(path).context(FileSnafu { path })?;
        check_input_size(&path)?;
        let mut fixture: Self = serde_json::from_slice(
            &fs::read(&path).context(FileSnafu { path: &path })?,
        )
        .context(JsonSnafu {
            subject: "Unity Catalog fixture",
        })?;
        ensure!(
            !fixture.databricks_profile.trim().is_empty()
                && fixture.table.starts_with("uc://")
                && !fixture.scan.columns.is_empty()
                && fixture
                    .scan
                    .columns
                    .iter()
                    .enumerate()
                    .all(|(index, name)| {
                        !name.is_empty() && !fixture.scan.columns[..index].contains(name)
                    }),
            InvalidInputSnafu {
                message: "Fixture requires an explicit profile, UC table, and distinct projected columns"
            }
        );
        let protocol = &fixture.context.protocol;
        // Only public protocol names may reach the report; never echo arbitrary fixture strings.
        ensure!(
            matches!(protocol.min_reader_version, 1..=3)
                && protocol.min_writer_version > 0
                && protocol.reader_features.iter().all(|feature| matches!(
                    feature.as_str(),
                    "timestampNtz"
                        | "deletionVectors"
                        | "columnMapping"
                        | "v2Checkpoint"
                        | "vacuumProtocolCheck"
                        | "typeWidening"
                        | "typeWidening-preview"
                )),
            InvalidInputSnafu {
                message: "Fixture protocol is outside the report's supported labels"
            }
        );
        let directory = path.parent().ok_or_else(|| {
            InvalidInputSnafu {
                message: "Fixture configuration has no parent directory",
            }
            .build()
        })?;
        for input in [
            &mut fixture.expected_table,
            &mut fixture.scan.expected_result,
            &mut fixture.scan.predicate_file,
        ]
        .into_iter()
        .chain(fixture.storage_options_file.iter_mut())
        {
            *input = directory.join(&*input);
        }
        Ok(fixture)
    }
}
