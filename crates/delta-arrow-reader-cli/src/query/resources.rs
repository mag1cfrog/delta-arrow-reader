//! Query memory and temporary storage belong to one invocation.
//!
//! `--spill-limit-bytes 67108864 --spill-dir ./scratch` permits intermediate
//! results on disk. The supplied directory survives; its query-specific child
//! is removed after the command runtime stops, including cancellation.

use std::{
    io,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use datafusion::execution::{
    disk_manager::{DiskManagerBuilder, DiskManagerMode},
    memory_pool::FairSpillPool,
    runtime_env::{RuntimeEnv, RuntimeEnvBuilder},
};
use snafu::ResultExt;

use super::{InitializationSnafu, QueryArgs, SpillCleanupSnafu, SpillDirectorySnafu};
use crate::{Error, InputCorrection, QuerySnafu};

/// The caller must keep the returned directory outside the cancellable query future.
pub(super) fn build_query_runtime(
    args: &QueryArgs,
) -> Result<(Arc<RuntimeEnv>, Option<tempfile::TempDir>), Error> {
    if (args.spill_limit_bytes > 0) != args.spill_dir.is_some() {
        return Err(Error::Argument {
            kind: clap::error::ErrorKind::MissingRequiredArgument,
            correction: Some(InputCorrection {
                argument: if args.spill_dir.is_some() {
                    "--spill-limit-bytes"
                } else {
                    "--spill-dir"
                }
                .into(),
                expected: "a positive byte budget together with --spill-dir PATH".into(),
                field_path: None,
                supplied: None,
                hint: "Set both options to enable spill; omit --spill-dir and use zero to disable it.",
            }),
        });
    }
    let spill_directory = args
        .spill_dir
        .as_ref()
        .map(|parent| {
            let mut builder = tempfile::Builder::new();
            builder.prefix("dar-query-");
            // DataFusion's child directory uses the ambient umask; protect the enclosing directory.
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                builder.permissions(std::fs::Permissions::from_mode(0o700));
            }
            builder.tempdir_in(parent)
        })
        .transpose()
        .context(SpillDirectorySnafu)
        .context(QuerySnafu)?;
    let mode = match &spill_directory {
        Some(directory) => DiskManagerMode::Directories(vec![directory.path().to_owned()]),
        None => DiskManagerMode::Disabled,
    };
    let datafusion = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(args.memory_limit_bytes.get())))
        .with_disk_manager_builder(
            DiskManagerBuilder::default()
                .with_mode(mode)
                .with_max_temp_directory_size(args.spill_limit_bytes),
        )
        .build_arc()
        .context(InitializationSnafu)
        .context(QuerySnafu)?;
    Ok((datafusion, spill_directory))
}

pub(crate) fn remove_spill_files(directory: tempfile::TempDir) -> Result<(), Error> {
    let started = Instant::now();
    loop {
        match std::fs::remove_dir_all(directory.path()) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            // A stdout worker already polling a batch can finish a spill after cancellation.
            Err(error)
                if error.kind() == io::ErrorKind::DirectoryNotEmpty
                    && started.elapsed() < crate::command_runtime::CLEANUP_GRACE =>
            {
                thread::sleep(Duration::from_millis(1));
            }
            Err(source) => return Err(source).context(SpillCleanupSnafu).context(QuerySnafu),
        }
    }
}
