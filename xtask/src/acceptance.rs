use std::{
    fs::{self, File},
    io::Cursor,
    path::{Path, PathBuf},
};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::reader::{FileReader, StreamReader};
use clap::Args;
use serde_json::Value;
use snafu::{ResultExt, ensure};

use crate::{
    artifacts::{MEMBERS, check_files, file_sha256, verify_platform},
    binary::check_binary,
    build::BuildIdentity,
    error::{ArrowSnafu, FileSnafu, InvalidInputSnafu, JsonSnafu, Result},
    platform::Architecture,
    process::run_process_tests,
    scan::run_scan_tests,
};

#[derive(Args)]
pub(crate) struct TestArchiveArgs {
    /// Directory containing one platform archive and its manifest.
    directory: PathBuf,
    #[command(flatten)]
    identity: BuildIdentity,
    #[arg(long)]
    architecture: Architecture,
    /// Extracted dar, LICENSE, and NOTICE, outside the source checkout.
    #[arg(long)]
    extracted: PathBuf,
    /// Directory containing inspect.json and scan.arrow from the minimal runtime.
    #[arg(long)]
    smoke_output: PathBuf,
    /// Source checkout containing the acceptance tests and external-writer fixtures.
    #[arg(long)]
    repository: PathBuf,
}

impl TestArchiveArgs {
    pub(crate) fn run(self) -> Result<()> {
        self.identity.validate()?;
        let repository = fs::canonicalize(&self.repository).context(FileSnafu {
            path: &self.repository,
        })?;
        let manifest = verify_platform(&self.directory, &self.identity, self.architecture)?;
        let target = &manifest.targets[0];
        let binary = check_extracted_files(&self.extracted, &repository)?;
        let metadata = check_binary(
            &binary,
            self.architecture,
            &self.identity.version,
            &target.dar_sha256,
        )?;
        ensure!(
            metadata.elf.runtime_dependencies == target.runtime_dependencies,
            InvalidInputSnafu {
                message: "manifest runtime dependencies differ from the executed binary"
            }
        );
        let expected = repository
            .join("crates/delta-arrow-reader/tests/reader/fixtures/external_writer/corpus/partitioned/expected.arrow");
        check_smoke_output(&self.smoke_output, &expected)?;

        run_process_tests(&binary, &self.identity.version, &repository)?;
        run_scan_tests(&binary, &repository)?;
        verify_platform(&self.directory, &self.identity, self.architecture)?;
        ensure!(
            file_sha256(&binary)? == target.dar_sha256,
            InvalidInputSnafu {
                message: "binary changed during acceptance tests"
            }
        );
        println!("Archive acceptance passed: {}", self.architecture.name());
        Ok(())
    }
}

fn check_extracted_files(extracted: &Path, repository: &Path) -> Result<PathBuf> {
    ensure!(
        cfg!(unix),
        InvalidInputSnafu {
            message: "archive acceptance requires Unix file permissions"
        }
    );
    let names: Vec<_> = MEMBERS.iter().map(|(name, _)| (*name).into()).collect();
    check_files(extracted, &names)?;
    let directory = fs::canonicalize(extracted).context(FileSnafu { path: extracted })?;
    ensure!(
        !directory.starts_with(repository),
        InvalidInputSnafu {
            message: "the archive must be extracted outside the source checkout"
        }
    );
    #[cfg(unix)]
    for (name, mode) in MEMBERS {
        use std::os::unix::fs::PermissionsExt;
        let path = directory.join(name);
        ensure!(
            fs::metadata(&path)
                .context(FileSnafu { path: &path })?
                .permissions()
                .mode()
                & 0o7777
                == mode,
            InvalidInputSnafu {
                message: format!("unexpected extracted permissions: {name}")
            }
        );
    }
    Ok(directory.join("dar"))
}

fn check_smoke_output(directory: &Path, expected_path: &Path) -> Result<()> {
    // These files were produced before the container installed test instrumentation.
    let path = directory.join("inspect.json");
    let inspection: Value = serde_json::from_slice(
        &fs::read(&path).context(FileSnafu { path: &path })?,
    )
    .context(JsonSnafu {
        subject: "minimal-runtime inspection",
    })?;
    ensure!(
        inspection["format_version"] == 1 && inspection["table_version"] == "0",
        InvalidInputSnafu {
            message: "minimal-runtime inspection returned an unexpected snapshot"
        }
    );
    let expected = FileReader::try_new(
        File::open(expected_path).context(FileSnafu {
            path: expected_path,
        })?,
        None,
    )
    .context(ArrowSnafu {
        path: expected_path,
    })?;
    let expected_schema = expected.schema();
    let id_index = expected_schema.index_of("id").context(ArrowSnafu {
        path: expected_path,
    })?;
    let expected = expected
        .collect::<std::result::Result<Vec<RecordBatch>, _>>()
        .context(ArrowSnafu {
            path: expected_path,
        })?;

    let path = directory.join("scan.arrow");
    let payload = fs::read(&path).context(FileSnafu { path: &path })?;
    ensure!(
        payload.ends_with(&[255, 255, 255, 255, 0, 0, 0, 0]),
        InvalidInputSnafu {
            message: "minimal-runtime scan is missing its IPC end marker"
        }
    );
    let mut cursor = Cursor::new(&payload);
    let actual = StreamReader::try_new(&mut cursor, None).context(ArrowSnafu { path: &path })?;
    let schema = actual.schema();
    let field = expected_schema.field(id_index);
    ensure!(
        schema.fields().len() == 1
            && schema.field(0).name() == field.name()
            && schema.field(0).data_type() == field.data_type()
            && schema.field(0).is_nullable() == field.is_nullable(),
        InvalidInputSnafu {
            message: "minimal-runtime scan schema differs from the external-writer fixture"
        }
    );
    let mut rows: Vec<ArrayRef> = Vec::new();
    for batch in actual {
        let batch = batch.context(ArrowSnafu { path: &path })?;
        ensure!(
            batch.num_rows() <= 2 - rows.len(),
            InvalidInputSnafu {
                message: "minimal-runtime scan returned more than two rows"
            }
        );
        for index in 0..batch.num_rows() {
            let row = batch.column(0).slice(index, 1);
            ensure!(
                expected.iter().any(|expected_batch| {
                    (0..expected_batch.num_rows()).any(|expected_index| {
                        expected_batch
                            .column(id_index)
                            .slice(expected_index, 1)
                            .as_ref()
                            == row.as_ref()
                    })
                }),
                InvalidInputSnafu {
                    message: "minimal-runtime scan returned a row absent from the fixture"
                }
            );
            rows.push(row);
        }
    }
    ensure!(
        rows.len() == 2 && rows[0].as_ref() != rows[1].as_ref(),
        InvalidInputSnafu {
            message: "minimal-runtime scan must return two distinct fixture rows"
        }
    );
    ensure!(
        cursor.position() == payload.len() as u64,
        InvalidInputSnafu {
            message: "unexpected data after the minimal-runtime scan IPC end marker"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests;
