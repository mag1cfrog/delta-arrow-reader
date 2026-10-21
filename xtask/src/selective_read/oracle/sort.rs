use std::{
    cell::Cell,
    cmp::Reverse,
    collections::{BinaryHeap, VecDeque},
    fs::{self, File},
    io::{self, Write},
    path::{Path, PathBuf},
    rc::Rc,
};

use arrow_array::RecordBatch;
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use tempfile::TempDir;

use super::{
    Result, require,
    rows::{BATCH_ROWS, Layout, Row},
};

struct Limited {
    file: File,
    used: Rc<Cell<u64>>,
    max: Option<u64>,
}
impl Write for Limited {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .max
            .is_some_and(|max| self.used.get().saturating_add(bytes.len() as u64) > max)
        {
            return Err(io::Error::other(
                "oracle sort exceeds its disk allowance (max_temp_directory_size)",
            ));
        }
        let count = self.file.write(bytes)?;
        self.used.set(self.used.get() + count as u64);
        Ok(count)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

struct Run<'a> {
    stream: StreamReader<File>,
    pending: VecDeque<Row>,
    layout: &'a Layout,
}
impl Run<'_> {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            if let Some(row) = self.pending.pop_front() {
                return Ok(Some(row));
            }
            let Some(batch) = self.stream.next() else {
                return Ok(None);
            };
            self.pending = self.layout.rows(batch?)?.into();
        }
    }
}

struct Merge<'a> {
    runs: Vec<Run<'a>>,
    heap: BinaryHeap<Reverse<(Row, usize)>>,
}
impl<'a> Merge<'a> {
    fn open(paths: &[PathBuf], layout: &'a Layout) -> Result<Self> {
        let mut result = Self {
            runs: Vec::new(),
            heap: BinaryHeap::new(),
        };
        for path in paths {
            let mut run = Run {
                stream: StreamReader::try_new(File::open(path)?, None)?,
                pending: VecDeque::new(),
                layout,
            };
            if let Some(row) = run.next()? {
                result.heap.push(Reverse((row, result.runs.len())));
            }
            result.runs.push(run);
        }
        Ok(result)
    }
}
impl Iterator for Merge<'_> {
    type Item = Result<Row>;
    fn next(&mut self) -> Option<Self::Item> {
        let Reverse((row, run)) = self.heap.pop()?;
        match self.runs[run].next() {
            Ok(Some(next)) => self.heap.push(Reverse((next, run))),
            Ok(None) => {}
            Err(error) => return Some(Err(error)),
        }
        Some(Ok(row))
    }
}

fn write_run(
    layout: &Layout,
    path: &Path,
    rows: impl Iterator<Item = Result<Row>>,
    used: &Rc<Cell<u64>>,
    max: Option<u64>,
) -> Result<()> {
    let sink = Limited {
        file: File::create_new(path)?,
        used: used.clone(),
        max,
    };
    let mut writer = StreamWriter::try_new(sink, &layout.schema)?;
    let mut batch = Vec::with_capacity(BATCH_ROWS);
    for row in rows {
        batch.push(row?);
        if batch.len() == BATCH_ROWS {
            writer.write(&layout.batch(&batch)?)?;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        writer.write(&layout.batch(&batch)?)?;
    }
    writer.finish()?;
    Ok(())
}

pub(super) struct Sorted<'a> {
    merge: Merge<'a>,
    _scratch: TempDir,
}
impl<'a> Sorted<'a> {
    pub(super) fn new(
        layout: &'a Layout,
        batches: impl Iterator<Item = Result<RecordBatch>>,
        parent: &Path,
        max: Option<u64>,
        memory: usize,
    ) -> Result<Self> {
        let mut spool = Spool::new(layout, parent, max, memory)?;
        for batch in batches {
            for row in layout.rows(batch?)? {
                spool.push(row)?;
            }
        }
        spool.finish()
    }
}

pub(super) struct Spool<'a> {
    layout: &'a Layout,
    scratch: TempDir,
    used: Rc<Cell<u64>>,
    max: Option<u64>,
    memory: usize,
    paths: Vec<PathBuf>,
    rows: Vec<Row>,
    bytes: usize,
    serial: usize,
}
impl<'a> Spool<'a> {
    pub(super) fn new(
        layout: &'a Layout,
        parent: &Path,
        max: Option<u64>,
        memory: usize,
    ) -> Result<Self> {
        require(memory >= 1024, "oracle sort memory allowance is too small")?;
        Ok(Self {
            layout,
            scratch: tempfile::Builder::new()
                .prefix("oracle-sort-")
                .tempdir_in(parent)?,
            used: Rc::new(Cell::new(0)),
            max,
            memory,
            paths: Vec::new(),
            rows: Vec::new(),
            bytes: 0,
            serial: 0,
        })
    }
    fn next_path(&mut self) -> PathBuf {
        let path = self
            .scratch
            .path()
            .join(format!("run-{}.arrow", self.serial));
        self.serial += 1;
        path
    }
    fn flush(&mut self) -> Result<()> {
        self.rows.sort_unstable();
        let path = self.next_path();
        write_run(
            self.layout,
            &path,
            self.rows.drain(..).map(Ok),
            &self.used,
            self.max,
        )?;
        self.paths.push(path);
        self.bytes = 0;
        Ok(())
    }
    pub(super) fn push(&mut self, row: Row) -> Result<()> {
        self.bytes += row.bytes();
        self.rows.push(row);
        if self.bytes >= self.memory / 4 {
            self.flush()?;
        }
        Ok(())
    }
    pub(super) fn finish(mut self) -> Result<Sorted<'a>> {
        if !self.rows.is_empty() {
            self.flush()?;
        }
        // Bound both open files and decoded batches during every merge pass.
        while self.paths.len() > 8 {
            let paths = std::mem::take(&mut self.paths);
            for chunk in paths.chunks(8) {
                let path = self.next_path();
                write_run(
                    self.layout,
                    &path,
                    Merge::open(chunk, self.layout)?,
                    &self.used,
                    self.max,
                )?;
                for old in chunk {
                    self.used.set(self.used.get() - fs::metadata(old)?.len());
                    fs::remove_file(old)?;
                }
                self.paths.push(path);
            }
        }
        Ok(Sorted {
            merge: Merge::open(&self.paths, self.layout)?,
            _scratch: self.scratch,
        })
    }
}

impl Iterator for Sorted<'_> {
    type Item = Result<Row>;
    fn next(&mut self) -> Option<Self::Item> {
        self.merge.next()
    }
}

pub(super) fn compare(
    actual: impl Iterator<Item = Result<Row>>,
    wanted: impl Iterator<Item = Result<Row>>,
    expected: u64,
    subset: bool,
) -> Result<u64> {
    let mut wanted = wanted.peekable();
    let mut previous_actual = None;
    let mut previous_wanted = None;
    let mut count = 0;
    for row in actual {
        let row = row?;
        require(
            previous_actual.is_none_or(|key| key < row.key),
            "duplicated key or unordered result",
        )?;
        previous_actual = Some(row.key);
        loop {
            let value = wanted
                .next()
                .transpose()?
                .ok_or("wrong row count or membership")?;
            require(
                previous_wanted.is_none_or(|key| key < value.key),
                "duplicated key or unordered reference",
            )?;
            previous_wanted = Some(value.key);
            if subset && value.key < row.key {
                continue;
            }
            require(row == value, "wrong row membership or value")?;
            break;
        }
        count += 1;
    }
    if !subset {
        require(wanted.next().transpose()?.is_none(), "wrong row count")?;
    }
    require(count == expected, "wrong row count")?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::super::rows::Cell as Value;
    use super::*;

    #[test]
    fn external_merge_preserves_values_rejects_duplicates_and_cleans_failed_spills() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let layout = Layout::new(&[
            "l_orderkey".into(),
            "l_linenumber".into(),
            "l_comment".into(),
            "l_quantity".into(),
        ])?;
        let rows = (0..1000)
            .map(|i| {
                layout.row(vec![
                    Value::Int(i),
                    Value::Int(1),
                    Value::Text(format!("row {i}  ")),
                    Value::Decimal(1700),
                ])
            })
            .collect::<Result<Vec<_>>>()?;
        let batches = || {
            rows.rchunks(7).map(|batch| {
                let mut batch = batch.to_vec();
                batch.reverse();
                layout.batch(&batch)
            })
        };
        let sorted = Sorted::new(&layout, batches(), directory.path(), None, 4096)?;
        assert_eq!(
            compare(sorted, rows.iter().cloned().map(Ok), 1000, false)?,
            1000
        );
        assert!(fs::read_dir(directory.path())?.next().is_none());
        assert!(Sorted::new(&layout, batches(), directory.path(), Some(1), 4096).is_err());
        assert!(fs::read_dir(directory.path())?.next().is_none());
        assert!(
            compare(
                [rows[0].clone(), rows[0].clone()].into_iter().map(Ok),
                rows.iter().cloned().map(Ok),
                2,
                true
            )
            .is_err()
        );
        assert_eq!(
            compare(
                [rows[3].clone(), rows[7].clone()].into_iter().map(Ok),
                rows.iter().cloned().map(Ok),
                2,
                true
            )?,
            2
        );
        let mut bad = rows[3].clone();
        bad.values[3] = Value::Decimal(1701);
        assert!(
            compare(
                [bad].into_iter().map(Ok),
                rows.iter().cloned().map(Ok),
                1,
                true
            )
            .is_err()
        );
        Ok(())
    }
}

#[cfg(test)]
mod spill_tests {
    use super::*;
    #[test]
    fn bounded_external_sort_merges_six_hundred_thousand_keys() -> Result<()> {
        use super::super::rows::Cell;
        let root = tempfile::tempdir()?;
        let layout = Layout::new(&["l_orderkey".into(), "l_linenumber".into()])?;
        let row = |n| Row {
            key: [n, 1],
            values: vec![Cell::Int(n), Cell::Int(1)],
        };
        let mut spool = Spool::new(
            &layout,
            root.path(),
            Some(32 * 1024 * 1024),
            8 * 1024 * 1024,
        )?;
        for n in (0..600000).rev() {
            spool.push(row(n))?;
        }
        assert_eq!(
            compare(
                spool.finish()?,
                (0..600000).map(|n| Ok(row(n))),
                600000,
                false
            )?,
            600000
        );
        assert_eq!(fs::read_dir(root.path())?.count(), 0);
        Ok(())
    }
}
