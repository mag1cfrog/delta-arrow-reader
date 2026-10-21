use std::{
    path::Path,
    process::{Child, ChildStdout, Command, Stdio},
};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;

use super::{Result, require};

pub(super) struct Parquet {
    child: Child,
    stream: StreamReader<ChildStdout>,
}

impl Parquet {
    pub(super) fn open(
        python: &Path,
        harness: &Path,
        path: &Path,
        columns: &[String],
    ) -> Result<Self> {
        let mut child = Command::new(python)
            .args(["-I", "-B", "-u"])
            .arg(harness.join("oracle_io.py"))
            .arg(path)
            .args(columns)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stream = StreamReader::try_new(
            child.stdout.take().ok_or("missing oracle Arrow stream")?,
            None,
        );
        match stream {
            Ok(stream) => Ok(Self { child, stream }),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                Err(error.into())
            }
        }
    }

    pub(super) fn finish(&mut self) -> Result<()> {
        // Drain even when LIMIT matched early, so a later decode error cannot pass.
        for batch in &mut self.stream {
            batch?;
        }
        require(
            self.child.wait()?.success(),
            "independent Parquet read failed",
        )
    }
}

impl Iterator for Parquet {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.stream.next().map(|batch| Ok(batch?))
    }
}

impl Drop for Parquet {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
