use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io,
    path::{Component, Path, PathBuf},
};

use flate2::{Compression, GzBuilder, read::GzDecoder};
use snafu::{ResultExt, ensure};
use tar::{Archive, Builder, HeaderMode};

use crate::{
    error::{FileSnafu, InvalidInputSnafu, IoSnafu, Result},
    repository::{WorkspaceManifest, read_toml, repository_root},
};

pub(crate) fn complete(source: &Path, destination: &Path) -> Result<()> {
    complete_workspace_sdist(&repository_root(), source, destination)
}

fn complete_workspace_sdist(repository: &Path, source: &Path, destination: &Path) -> Result<()> {
    let manifest: WorkspaceManifest = read_toml(&repository.join("Cargo.toml"))?;
    let root = PathBuf::from(format!(
        "delta_arrow_reader-{}",
        manifest.workspace.package.version
    ));
    let mut sources = BTreeMap::new();
    for relative in [
        "Cargo.toml",
        "Cargo.lock",
        "LICENSE",
        "NOTICE",
        ".cargo/config.toml",
        "crates/delta-arrow-reader-cli/Cargo.toml",
        "crates/delta-arrow-reader-cli/Dockerfile",
        "crates/delta-arrow-reader-cli/src",
        "crates/delta-arrow-reader-cli/release",
        "xtask/Cargo.toml",
        "xtask/src",
    ] {
        collect_sources(repository, Path::new(relative), &mut sources)?;
    }
    let parent = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    fs::create_dir_all(parent).context(FileSnafu { path: parent })?;
    let mut temporary =
        tempfile::NamedTempFile::new_in(parent).context(FileSnafu { path: parent })?;
    let compressed = GzBuilder::new().write(temporary.as_file_mut(), Compression::default());
    let mut output = Builder::new(compressed);
    output.mode(HeaderMode::Deterministic);
    let mut input = Archive::new(GzDecoder::new(
        File::open(source).context(FileSnafu { path: source })?,
    ));
    let mut seen = BTreeSet::new();
    for entry in input.entries().context(FileSnafu { path: source })? {
        let mut entry = entry.context(FileSnafu { path: source })?;
        let path = entry
            .path()
            .context(FileSnafu { path: source })?
            .into_owned();
        ensure!(
            path.components()
                .all(|part| matches!(part, Component::Normal(_)))
                && path.starts_with(&root)
                && entry.header().entry_type().is_file()
                && seen.insert(path.clone()),
            InvalidInputSnafu {
                message: format!("invalid or duplicate sdist entry: {}", path.display())
            }
        );
        let relative = path.strip_prefix(&root).map_err(|_| {
            InvalidInputSnafu {
                message: "sdist root differs from workspace version",
            }
            .build()
        })?;
        // Maturin omits non-dependency workspace members; retain the original manifest and lockfile.
        if let Some(source) = sources.remove(relative) {
            output
                .append_path_with_name(&source, &path)
                .context(FileSnafu { path: source })?;
        } else {
            output
                .append_data(&mut entry.header().clone(), &path, &mut entry)
                .context(FileSnafu { path: &path })?;
        }
    }
    // Tar stops at its end marker before gzip verifies the checksum and trailer.
    io::copy(&mut input.into_inner(), &mut io::sink()).context(FileSnafu { path: source })?;
    for required in [
        "Cargo.toml",
        "Cargo.lock",
        "pyproject.toml",
        "PKG-INFO",
        "build_backend.py",
    ] {
        ensure!(
            seen.contains(&root.join(required)),
            InvalidInputSnafu {
                message: format!("sdist is missing {required}")
            }
        );
    }
    for (relative, source) in sources {
        output
            .append_path_with_name(&source, root.join(relative))
            .context(FileSnafu { path: source })?;
    }
    output
        .into_inner()
        .and_then(|compressed| compressed.finish())
        .context(IoSnafu {
            operation: "finish the workspace source archive",
        })?;
    temporary
        .persist(destination)
        .map_err(|error| error.error)
        .context(FileSnafu { path: destination })?;
    Ok(())
}

fn collect_sources(
    repository: &Path,
    relative: &Path,
    sources: &mut BTreeMap<PathBuf, PathBuf>,
) -> Result<()> {
    let path = repository.join(relative);
    let metadata = fs::symlink_metadata(&path).context(FileSnafu { path: &path })?;
    if metadata.is_dir() {
        for entry in fs::read_dir(&path).context(FileSnafu { path: &path })? {
            let entry = entry.context(FileSnafu { path: &path })?;
            collect_sources(repository, &relative.join(entry.file_name()), sources)?;
        }
    } else {
        ensure!(
            metadata.is_file(),
            InvalidInputSnafu {
                message: format!("sdist source must be a regular file: {}", path.display())
            }
        );
        sources.insert(relative.to_owned(), path);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
