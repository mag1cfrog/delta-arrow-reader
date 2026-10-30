//! Catalog-confirmed commits used to load a catalog-managed Delta snapshot.

use delta_kernel::{
    LogPath,
    path::{LogPathFileType, ParsedLogPath},
};
use url::Url;

/// One staged commit confirmed by the table's catalog.
#[derive(Clone, Debug)]
pub struct DeltaCatalogCommit {
    /// Delta version assigned to this commit.
    pub version: u64,
    /// Basename supplied by the catalog, without directories or URL components.
    pub file_name: String,
    /// Size of the staged commit file, in bytes.
    pub file_size: u64,
    /// Modification time of the staged commit file, in Unix milliseconds.
    pub last_modified_ms: i64,
}

/// Catalog confirmation for one immutable table snapshot.
///
/// The caller must obtain this information from the table's authorized catalog and
/// verify its table identity and storage root. Filesystem discovery alone cannot
/// establish which staged commits the catalog accepted.
#[derive(Clone, Debug)]
pub struct DeltaCatalogSnapshot {
    /// Latest version confirmed by the catalog, including already published commits.
    pub max_version: u64,
    /// Contiguous, ascending tail of confirmed staged commits. May be empty when published.
    pub commits: Vec<DeltaCatalogCommit>,
}

impl DeltaCatalogSnapshot {
    pub(crate) fn kernel_log_tail(
        &self,
        table_root: &Url,
    ) -> delta_kernel::DeltaResult<Vec<LogPath>> {
        if self.max_version == u64::MAX {
            return Err(delta_kernel::Error::unsupported(
                "catalog version exceeds the supported Kernel range",
            ));
        }
        self.commits
            .iter()
            .map(|commit| {
                // URL joining accepts absolute paths and escapes; catalog filenames must not.
                if commit.file_name.is_empty()
                    || !commit
                        .file_name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
                    || commit.version > self.max_version
                    || commit.file_size == 0
                    || commit.last_modified_ms < 0
                {
                    return Err(delta_kernel::Error::generic(
                        "invalid catalog commit metadata",
                    ));
                }
                let path = LogPath::staged_commit(
                    table_root.clone(),
                    &commit.file_name,
                    commit.last_modified_ms,
                    commit.file_size,
                )
                .map_err(|_| delta_kernel::Error::generic("invalid catalog commit filename"))?;
                let parsed = ParsedLogPath::from(path.clone());
                if parsed.version != commit.version
                    || parsed.file_type != LogPathFileType::StagedCommit
                {
                    return Err(delta_kernel::Error::generic(
                        "catalog commit version mismatch",
                    ));
                }
                Ok(path)
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn catalog_filenames_are_rejected_before_they_reach_storage() {
        let root = Url::parse("s3://bucket/table/").unwrap();
        let mut snapshot = DeltaCatalogSnapshot {
            max_version: 1,
            commits: vec![DeltaCatalogCommit {
                version: 1,
                file_name: "00000000000000000001.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json".into(),
                file_size: 100,
                last_modified_ms: 1,
            }],
        };
        assert_eq!(snapshot.kernel_log_tail(&root).unwrap().len(), 1);
        for file_name in [
            "../00000000000000000001.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json",
            "https://secret.invalid/00000000000000000001.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json",
            "/00000000000000000001.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json",
            "00000000000000000001.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json?secret=token",
            "%2e%2e%2fsecret",
            "00000000000000000001.json",
            "00000000000000000002.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa.json",
        ] {
            snapshot.commits[0].file_name = file_name.into();
            let error = snapshot.kernel_log_tail(&root).unwrap_err();
            assert!(!error.to_string().contains("secret"));
        }
    }
}
