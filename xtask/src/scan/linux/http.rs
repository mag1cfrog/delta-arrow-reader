use std::{collections::HashSet, fs, process::Command, sync::Mutex};

use snafu::ResultExt;

use super::ScanFixture;
use crate::error::{FileSnafu, InvalidInputSnafu, Result};

impl ScanFixture<'_> {
    pub(super) fn http_command(&self, origin: &str, storage_options: &str) -> Result<Command> {
        let options = self.directory.path().join("storage.json");
        fs::write(&options, storage_options).context(FileSnafu { path: &options })?;
        let mut command = self.command();
        command
            .arg("--storage-options-file")
            .arg(options)
            .arg(format!("{origin}/"));
        Ok(command)
    }
}

#[derive(Default)]
pub(super) struct DataRequests(Mutex<HashSet<String>>);

impl DataRequests {
    pub fn record(&self, path: &str) -> Result<()> {
        if path.ends_with(".parquet") {
            self.0
                .lock()
                .map_err(|_| {
                    InvalidInputSnafu {
                        message: "data request tracking lock was poisoned",
                    }
                    .build()
                })?
                .insert(path.to_owned());
        }
        Ok(())
    }

    pub fn count(&self) -> Result<usize> {
        Ok(self
            .0
            .lock()
            .map_err(|_| {
                InvalidInputSnafu {
                    message: "data request tracking lock was poisoned",
                }
                .build()
            })?
            .len())
    }
}
