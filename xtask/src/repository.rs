use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use snafu::ResultExt;

use crate::error::{FileSnafu, Result, TomlSnafu};

pub(crate) fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

pub(crate) fn read_toml<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let text = fs::read_to_string(path).context(FileSnafu { path })?;
    toml::from_str(&text).context(TomlSnafu { path })
}

#[derive(Deserialize)]
pub(crate) struct WorkspaceManifest {
    pub(crate) workspace: Workspace,
}

#[derive(Deserialize)]
pub(crate) struct Workspace {
    pub(crate) package: WorkspacePackage,
    pub(crate) metadata: WorkspaceMetadata,
}

#[derive(Deserialize)]
pub(crate) struct WorkspacePackage {
    pub(crate) version: String,
    #[serde(rename = "rust-version")]
    pub(crate) rust_version: String,
}

#[derive(Deserialize)]
pub(crate) struct WorkspaceMetadata {
    #[serde(rename = "cli-release")]
    pub(crate) cli_release: ReleaseTools,
}

#[derive(Deserialize)]
pub(crate) struct ReleaseTools {
    #[serde(rename = "cargo-about-version")]
    pub(crate) cargo_about_version: String,
}
