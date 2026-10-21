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
        require(memory >= 1024, "oracle sort memory allowance is too small")?;
        let scratch = tempfile::Builder::new()
            .prefix("oracle-sort-")
            .tempdir_in(parent)?;
        let used = Rc::new(Cell::new(0));
        let mut paths = Vec::new();
        let mut rows = Vec::new();
        let mut bytes = 0;
        let mut serial = 0;
        let mut flush = |rows: &mut Vec<Row>, paths: &mut Vec<PathBuf>| -> Result<()> {
            rows.sort_unstable();
            let path = scratch.path().join(format!("run-{serial}.arrow"));
            serial += 1;
            write_run(layout, &path, rows.drain(..).map(Ok), &used, max)?;
            paths.push(path);
            Ok(())
        };
        for batch in batches {
            for row in layout.rows(batch?)? {
                bytes += row.bytes();
                rows.push(row);
                if bytes >= memory / 4 {
                    flush(&mut rows, &mut paths)?;
                    bytes = 0;
                }
            }
        }
        if !rows.is_empty() {
            flush(&mut rows, &mut paths)?;
        }
        // Bound both open files and decoded batches during every merge pass.
        while paths.len() > 8 {
            let mut merged = Vec::new();
            for chunk in paths.chunks(8) {
                let path = scratch.path().join(format!("run-{serial}.arrow"));
                serial += 1;
                write_run(layout, &path, Merge::open(chunk, layout)?, &used, max)?;
                for old in chunk {
                    used.set(used.get() - fs::metadata(old)?.len());
                    fs::remove_file(old)?;
                }
                merged.push(path);
            }
            paths = merged;
        }
        let merge = Merge::open(&paths, layout)?;
        Ok(Self {
            merge,
            _scratch: scratch,
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
