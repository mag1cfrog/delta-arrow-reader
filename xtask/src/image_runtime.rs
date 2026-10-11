use std::path::PathBuf;

use clap::Args;

use crate::{error::Result, platform::Architecture};

#[cfg(target_os = "linux")]
mod linux;

#[derive(Args)]
pub(crate) struct TestImageArgs {
    /// Local CLI image to resolve to an immutable image ID before testing.
    image: String,
    #[arg(long)]
    architecture: Architecture,
    /// Exact version expected from dar --version.
    #[arg(long)]
    expected_version: String,
    /// Source checkout containing the external-writer fixtures.
    #[arg(long)]
    repository: PathBuf,
}

impl TestImageArgs {
    pub(crate) fn run(self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            linux::run(self)
        }
        #[cfg(not(target_os = "linux"))]
        crate::error::InvalidInputSnafu {
            message: "CLI image runtime checks require a Linux Docker host",
        }
        .fail()
    }
}
