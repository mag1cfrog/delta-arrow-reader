use super::{Result, require};
use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use std::os::unix::process::CommandExt;
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command, Stdio},
};

pub(super) struct Decoder {
    python: PathBuf,
    harness: PathBuf,
    process: Option<(Child, ChildStdout)>,
}
impl Decoder {
    pub(super) fn new(python: &Path, harness: &Path) -> Self {
        Self {
            python: python.into(),
            harness: harness.into(),
            process: None,
        }
    }
    pub(super) fn open(&mut self, path: &Path, columns: &[String]) -> Result<Parquet<'_>> {
        self.open_paths(&[path.to_owned()], columns)
    }
    pub(super) fn open_paths(
        &mut self,
        paths: &[PathBuf],
        columns: &[String],
    ) -> Result<Parquet<'_>> {
        require(!paths.is_empty(), "missing Parquet input")?;
        if let Some((child, _)) = &mut self.process
            && child.try_wait()?.is_some()
        {
            self.process = None;
        }
        if self.process.is_none() {
            let mut command = Command::new(&self.python);
            command
                .args(["-I", "-B", "-u"])
                .arg(self.harness.join("oracle_io.py"))
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            let parent = std::process::id();
            // SAFETY: the child only installs a parent-death signal before exec.
            unsafe {
                command.pre_exec(move || {
                    if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::getppid() as u32 != parent {
                        return Err(std::io::Error::other("oracle parent exited"));
                    }
                    Ok(())
                });
            }
            let mut child = command.spawn()?;
            let output = child.stdout.take().ok_or("missing oracle Arrow stream")?;
            self.process = Some((child, output));
        }
        let (child, output) = self
            .process
            .as_mut()
            .ok_or("missing oracle decoder process")?;
        let input = child.stdin.as_mut().ok_or("missing decoder input")?;
        serde_json::to_writer(
            &mut *input,
            &serde_json::json!({"paths":paths,"columns":columns}),
        )?;
        input.write_all(b"\n")?;
        input.flush()?;
        match StreamReader::try_new(output, None) {
            Ok(stream) => Ok(Parquet {
                child,
                stream,
                finished: false,
            }),
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                Err(error.into())
            }
        }
    }
}
impl Drop for Decoder {
    fn drop(&mut self) {
        if let Some((child, _)) = &mut self.process {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
pub(super) struct Parquet<'a> {
    child: &'a mut Child,
    stream: StreamReader<&'a mut ChildStdout>,
    finished: bool,
}
impl Parquet<'_> {
    pub(super) fn schema(&self) -> arrow_schema::SchemaRef {
        self.stream.schema()
    }
    pub(super) fn finish(&mut self) -> Result<()> {
        for batch in &mut self.stream {
            batch?;
        }
        self.finished = true;
        require(
            self.child.try_wait()?.is_none(),
            "independent Parquet read failed",
        )
    }
}
impl Iterator for Parquet<'_> {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.stream.next().map(|b| Ok(b?))
    }
}
impl Drop for Parquet<'_> {
    fn drop(&mut self) {
        if !self.finished && self.finish().is_err() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

struct Limited {
    file: std::fs::File,
    written: u64,
    max: Option<u64>,
}
impl std::io::Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .max
            .is_some_and(|max| self.written.saturating_add(bytes.len() as u64) > max)
        {
            return Err(std::io::Error::other(
                "reference Parquet exceeds its disk allowance",
            ));
        }
        let n = std::io::Write::write(&mut self.file, bytes)?;
        self.written += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::Write::flush(&mut self.file)
    }
}
pub(super) struct Reference<'a> {
    writer: parquet::arrow::ArrowWriter<Limited>,
    layout: &'a super::rows::Layout,
    rows: Vec<super::rows::Row>,
    previous: Option<[i64; 2]>,
}
impl<'a> Reference<'a> {
    pub(super) fn new(
        path: &Path,
        layout: &'a super::rows::Layout,
        max: Option<u64>,
    ) -> Result<Self> {
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
            .set_max_row_group_row_count(Some(131072))
            .build();
        let sink = Limited {
            file: std::fs::File::create_new(path)?,
            written: 0,
            max,
        };
        Ok(Self {
            writer: parquet::arrow::ArrowWriter::try_new(
                sink,
                layout.schema.clone(),
                Some(properties),
            )?,
            layout,
            rows: Vec::with_capacity(super::rows::BATCH_ROWS),
            previous: None,
        })
    }
    pub(super) fn push(&mut self, row: super::rows::Row) -> Result<()> {
        require(
            self.previous.is_none_or(|key| key < row.key),
            "duplicated key or unordered reference",
        )?;
        self.previous = Some(row.key);
        self.rows.push(row);
        if self.rows.len() == super::rows::BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<()> {
        if !self.rows.is_empty() {
            self.writer.write(&self.layout.batch(&self.rows)?)?;
            self.rows.clear();
        }
        Ok(())
    }
    pub(super) fn finish(mut self) -> Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }
}
