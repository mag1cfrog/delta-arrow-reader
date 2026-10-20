use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

use flate2::read::DeflateDecoder;
use rawzip::{CompressionMethod, ZipArchive};
use snafu::{OptionExt, ResultExt, ensure};

use crate::{
    artifacts,
    error::{ArtifactZipSnafu, FileSnafu, InvalidInputSnafu, Result},
    release_selection::github::{Artifact, download_artifact},
};

pub(super) fn extract_artifact(
    artifact: &Artifact,
    destination: &Path,
    names: &[String],
) -> Result<()> {
    let download = download_artifact(artifact)?;
    let file = download.reopen().context(FileSnafu {
        path: download.path(),
    })?;
    let mut directory_buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    let archive = ZipArchive::from_file(file, &mut directory_buffer).context(ArtifactZipSnafu)?;
    ensure!(
        archive.entries_hint() == names.len() as u64,
        InvalidInputSnafu {
            message: format!(
                "artifact {} ZIP has an incomplete or extra file inventory",
                artifact.id
            ),
        }
    );
    artifacts::create_directory(destination)?;
    let mut remaining: Vec<_> = names.iter().collect();
    let mut entries = archive.entries(&mut directory_buffer);
    let mut header_buffer = vec![0; rawzip::RECOMMENDED_BUFFER_SIZE];
    while let Some(entry) = entries.next_entry().context(ArtifactZipSnafu)? {
        let position = remaining
            .iter()
            .position(|name| name.as_bytes() == entry.file_path().as_bytes())
            .filter(|_| {
                matches!(entry.mode().value() & 0o170000, 0 | 0o100000)
                    && !entry.flags().is_encrypted()
            })
            .context(InvalidInputSnafu {
                message: format!(
                    "artifact {} ZIP has an unexpected, duplicate, encrypted, or non-regular entry",
                    artifact.id
                ),
            })?;
        let name = remaining.remove(position);
        let local = archive
            .get_entry(entry.wayfinder())
            .context(ArtifactZipSnafu)?;
        let header = local
            .local_header(&mut header_buffer)
            .context(ArtifactZipSnafu)?;
        ensure!(
            header.file_path().as_bytes() == entry.file_path().as_bytes()
                && header.compression_method() == entry.compression_method()
                && !header.flags().is_encrypted(),
            InvalidInputSnafu {
                message: "artifact ZIP local header differs from its directory entry"
            }
        );
        let reader: Box<dyn Read + '_> = match entry.compression_method() {
            CompressionMethod::STORE => Box::new(local.reader()),
            CompressionMethod::DEFLATE => Box::new(DeflateDecoder::new(local.reader())),
            _ => {
                return InvalidInputSnafu {
                    message: "unsupported artifact ZIP compression",
                }
                .fail();
            }
        };
        let limit = entry
            .uncompressed_size_hint()
            .checked_add(1)
            .ok_or_else(|| {
                InvalidInputSnafu {
                    message: "artifact ZIP entry size overflows",
                }
                .build()
            })?;
        let path = destination.join(name);
        let mut output = File::create_new(&path).context(FileSnafu { path: &path })?;
        let copied = io::copy(&mut local.verifying_reader(reader).take(limit), &mut output)
            .context(FileSnafu { path: &path })?;
        ensure!(
            copied == entry.uncompressed_size_hint(),
            InvalidInputSnafu {
                message: "artifact ZIP entry size differs"
            }
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
                .context(FileSnafu { path: &path })?;
        }
    }
    ensure!(
        remaining.is_empty(),
        InvalidInputSnafu {
            message: "artifact ZIP is incomplete"
        }
    );
    Ok(())
}
