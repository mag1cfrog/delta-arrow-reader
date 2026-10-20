//! Synthetic benchmark for narrow and wide Parquet row-filter projections.
//!
//! Run with `cargo bench --bench row_filter`. Each measurement runs in a fresh child process so
//! Linux peak RSS is comparable across projection shapes. The timer wraps construction of the
//! Parquet reader, where synchronous row-filter predicates are decoded and evaluated. Output rows
//! are read only after both measurements have been captured.

use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, BooleanArray, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::file::properties::WriterProperties;

const DEFAULT_REPETITIONS: usize = 3;
const MAX_REPETITIONS: usize = 128;
const BENCHMARK_SHAPE: FixtureShape = FixtureShape {
    row_groups: 4,
    rows_per_group: 4_096,
    payload_columns: 64,
    match_every: 1_024,
};

#[derive(Debug, Clone, Copy)]
struct FixtureShape {
    row_groups: usize,
    rows_per_group: usize,
    payload_columns: usize,
    match_every: usize,
}

impl FixtureShape {
    fn row_count(self) -> usize {
        self.row_groups.saturating_mul(self.rows_per_group)
    }

    fn expected_ids(self) -> Vec<i32> {
        (0..self.row_count())
            .step_by(self.match_every)
            .filter_map(|row| i32::try_from(row).ok())
            .collect()
    }
}

#[derive(Debug)]
struct Config {
    repetitions: usize,
    temp_dir: PathBuf,
    retain_fixture: bool,
    child: Option<ChildConfig>,
}

#[derive(Debug)]
struct ChildConfig {
    case: ProjectionCase,
    fixture: PathBuf,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, Box<dyn Error>> {
        let mut repetitions = DEFAULT_REPETITIONS;
        let mut temp_dir = env::temp_dir();
        let mut retain_fixture = false;
        let mut child_case = None;
        let mut fixture = None;
        let mut args = args.into_iter();

        while let Some(argument) = args.next() {
            match argument.as_str() {
                "--repetitions" => {
                    repetitions = required_arg(&mut args, &argument)?.parse()?;
                }
                "--temp-dir" => temp_dir = required_arg(&mut args, &argument)?.into(),
                "--retain-fixture" => retain_fixture = true,
                "--child-case" => {
                    child_case = Some(ProjectionCase::parse(&required_arg(&mut args, &argument)?)?)
                }
                "--fixture" => fixture = Some(required_arg(&mut args, &argument)?.into()),
                "--bench" => {}
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => return Err(invalid(format!("unknown argument: {other}")).into()),
            }
        }

        if !(1..=MAX_REPETITIONS).contains(&repetitions) {
            return Err(invalid("repetitions must be between 1 and 128").into());
        }
        let child = match (child_case, fixture) {
            (Some(case), Some(fixture)) => Some(ChildConfig { case, fixture }),
            (None, None) => None,
            _ => {
                return Err(invalid("--child-case and --fixture must be provided together").into());
            }
        };
        Ok(Self {
            repetitions,
            temp_dir,
            retain_fixture,
            child,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ProjectionCase {
    Narrow,
    Wide,
}

impl ProjectionCase {
    fn parse(value: &str) -> Result<Self, io::Error> {
        match value {
            "narrow" => Ok(Self::Narrow),
            "wide" => Ok(Self::Wide),
            other => Err(invalid(format!("unknown projection case: {other}"))),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Narrow => "narrow",
            Self::Wide => "wide",
        }
    }

    fn predicate_roots(self, payload_columns: usize) -> Vec<usize> {
        match self {
            Self::Narrow => vec![1, 2, 3],
            Self::Wide => (1..4 + payload_columns).collect(),
        }
    }
}

#[derive(Debug)]
struct Fixture {
    path: PathBuf,
    shape: FixtureShape,
    retain: bool,
}

impl Fixture {
    fn create(temp_root: &Path, shape: FixtureShape, retain: bool) -> Result<Self, Box<dyn Error>> {
        if shape.row_groups == 0
            || shape.rows_per_group == 0
            || shape.payload_columns == 0
            || shape.match_every == 0
        {
            return Err(invalid("fixture dimensions must be positive").into());
        }
        i32::try_from(shape.row_count())?;

        let path = temp_root.join(format!(
            "delta-arrow-reader-row-filter-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        fs::create_dir_all(&path)?;
        let schema = benchmark_schema(shape.payload_columns);
        let data_path = path.join("part.parquet");
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(shape.rows_per_group))
            .build();
        let mut writer = ArrowWriter::try_new(
            File::create(&data_path)?,
            Arc::clone(&schema),
            Some(properties),
        )?;
        for row_group in 0..shape.row_groups {
            writer.write(&benchmark_batch(
                Arc::clone(&schema),
                row_group.saturating_mul(shape.rows_per_group),
                shape,
            )?)?;
        }
        writer.close()?;

        Ok(Self {
            path,
            shape,
            retain,
        })
    }

    fn data_path(&self) -> PathBuf {
        self.path.join("part.parquet")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Measurement {
    case: ProjectionCase,
    decode_micros: u64,
    peak_rss_bytes: Option<u64>,
    row_count: usize,
    id_checksum: i64,
}

impl Measurement {
    fn encode(&self) -> String {
        format!(
            "{},{},{},{},{}",
            self.case.name(),
            self.decode_micros,
            optional(self.peak_rss_bytes),
            self.row_count,
            self.id_checksum
        )
    }

    fn parse(value: &str) -> Result<Self, Box<dyn Error>> {
        let mut fields = value.trim().split(',');
        let case = ProjectionCase::parse(fields.next().ok_or_else(|| invalid("missing case"))?)?;
        let decode_micros = fields
            .next()
            .ok_or_else(|| invalid("missing decode time"))?
            .parse()?;
        let peak_rss_bytes = match fields.next().ok_or_else(|| invalid("missing peak RSS"))? {
            "" => None,
            value => Some(value.parse()?),
        };
        let row_count = fields
            .next()
            .ok_or_else(|| invalid("missing row count"))?
            .parse()?;
        let id_checksum = fields
            .next()
            .ok_or_else(|| invalid("missing ID checksum"))?
            .parse()?;
        if fields.next().is_some() {
            return Err(invalid("unexpected measurement fields").into());
        }
        Ok(Self {
            case,
            decode_micros,
            peak_rss_bytes,
            row_count,
            id_checksum,
        })
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::parse(env::args().skip(1))?;
    if let Some(child) = config.child {
        let measurement = measure_case(&child.fixture, BENCHMARK_SHAPE, child.case)?;
        println!("{}", measurement.encode());
        return Ok(());
    }

    let fixture = Fixture::create(&config.temp_dir, BENCHMARK_SHAPE, config.retain_fixture)?;
    if config.retain_fixture {
        eprintln!("retained row-filter fixture: {}", fixture.path.display());
    }
    run_parent(&fixture, config.repetitions)
}

fn run_parent(fixture: &Fixture, repetitions: usize) -> Result<(), Box<dyn Error>> {
    let executable = env::current_exe()?;
    let mut measurements = Vec::with_capacity(repetitions.saturating_mul(2));
    for repetition in 0..repetitions {
        let cases = if repetition % 2 == 0 {
            [ProjectionCase::Narrow, ProjectionCase::Wide]
        } else {
            [ProjectionCase::Wide, ProjectionCase::Narrow]
        };
        let mut pair = Vec::with_capacity(2);
        for case in cases {
            let output = Command::new(&executable)
                .arg("--child-case")
                .arg(case.name())
                .arg("--fixture")
                .arg(fixture.data_path())
                .output()?;
            if !output.status.success() {
                return Err(io::Error::other(format!(
                    "{} child failed: {}",
                    case.name(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ))
                .into());
            }
            let measurement = Measurement::parse(&String::from_utf8(output.stdout)?)?;
            if measurement.case != case {
                return Err(invalid("child returned the wrong projection case").into());
            }
            pair.push(measurement.clone());
            measurements.push((repetition + 1, measurement));
        }
        if pair[0].row_count != pair[1].row_count || pair[0].id_checksum != pair[1].id_checksum {
            return Err(
                io::Error::other("narrow and wide projections selected different rows").into(),
            );
        }
    }

    measurements.sort_by_key(|(repetition, measurement)| (measurement.case, *repetition));
    println!(
        "predicate_projection,predicate_columns,repetition,qualifying_rows,predicate_decode_micros,predicate_decode_peak_rss_bytes,id_checksum"
    );
    for (repetition, measurement) in measurements {
        println!(
            "{},{},{},{},{},{},{}",
            measurement.case.name(),
            measurement
                .case
                .predicate_roots(fixture.shape.payload_columns)
                .len(),
            repetition,
            measurement.row_count,
            measurement.decode_micros,
            optional(measurement.peak_rss_bytes),
            measurement.id_checksum
        );
    }
    Ok(())
}

fn measure_case(
    fixture_path: &Path,
    shape: FixtureShape,
    case: ProjectionCase,
) -> Result<Measurement, Box<dyn Error>> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(fixture_path)?)?;
    let predicate_projection = ProjectionMask::roots(
        builder.parquet_schema(),
        case.predicate_roots(shape.payload_columns),
    );
    let output_projection = ProjectionMask::roots(builder.parquet_schema(), [0]);
    let predicate = ArrowPredicateFn::new(predicate_projection, evaluate_benchmark_predicate);
    let started = Instant::now();
    let reader = builder
        .with_projection(output_projection)
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .build()?;
    let decode_micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let peak_rss_bytes = process_peak_rss_bytes();

    let mut ids = Vec::<i32>::new();
    for batch in reader {
        let batch = batch?;
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| invalid("row_id was not Int32"))?;
        ids.extend_from_slice(values.values());
    }
    let expected_ids = shape.expected_ids();
    if ids != expected_ids {
        return Err(io::Error::other(format!(
            "{} projection returned unexpected IDs",
            case.name()
        ))
        .into());
    }
    Ok(Measurement {
        case,
        decode_micros,
        peak_rss_bytes,
        row_count: ids.len(),
        id_checksum: ids.iter().map(|value| i64::from(*value)).sum(),
    })
}

fn evaluate_benchmark_predicate(batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
    let string_column = |index| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ArrowError::CastError("predicate column was not Utf8".to_owned()))
    };
    let partition_keys = string_column(0)?;
    let entity_ids = string_column(1)?;
    let event_ids = string_column(2)?;
    Ok(BooleanArray::from_iter((0..batch.num_rows()).map(|row| {
        Some(
            partition_keys.value(row) == "fixed-partition"
                && entity_ids.value(row) == "fixed-entity"
                && event_ids.value(row) == "fixed-event",
        )
    })))
}

fn benchmark_schema(payload_columns: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("partition_key", DataType::Utf8, false),
        Field::new("entity_id", DataType::Utf8, false),
        Field::new("event_id", DataType::Utf8, false),
    ];
    fields.extend(
        (0..payload_columns)
            .map(|index| Field::new(format!("payload_{index:03}"), DataType::Utf8, false)),
    );
    Arc::new(Schema::new(fields))
}

fn benchmark_batch(
    schema: SchemaRef,
    first_row: usize,
    shape: FixtureShape,
) -> Result<RecordBatch, Box<dyn Error>> {
    let rows = first_row..first_row.saturating_add(shape.rows_per_group);
    let mut columns = Vec::with_capacity(4 + shape.payload_columns);
    columns.push(Arc::new(Int32Array::from_iter_values(
        rows.clone()
            .map(i32::try_from)
            .collect::<Result<Vec<_>, _>>()?,
    )) as ArrayRef);
    columns.push(Arc::new(StringArray::from_iter_values(
        rows.clone().map(|_| "fixed-partition"),
    )));
    columns.push(Arc::new(StringArray::from_iter_values(
        rows.clone().map(|_| "fixed-entity"),
    )));
    columns.push(Arc::new(StringArray::from_iter_values(rows.clone().map(
        |row| {
            if row % shape.match_every == 0 {
                "fixed-event"
            } else {
                "other-event"
            }
        },
    ))));
    for column in 0..shape.payload_columns {
        columns.push(Arc::new(StringArray::from_iter_values(rows.clone().map(
            |row| format!("payload-{column:03}-{row:08}-abcdefghijklmnopqrstuvwxyz0123456789"),
        ))));
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn required_arg(
    args: &mut impl Iterator<Item = String>,
    argument: &str,
) -> Result<String, io::Error> {
    args.next()
        .ok_or_else(|| invalid(format!("missing value for {argument}")))
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn print_help() {
    println!(
        "cargo bench --bench row_filter -- [--repetitions N] [--temp-dir PATH] [--retain-fixture]"
    );
}

fn optional(value: Option<u64>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

fn process_peak_rss_bytes() -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    process_status_memory_kib(&status, "VmHWM").map(|kib| kib.saturating_mul(1_024))
}

fn process_status_memory_kib(status: &str, key: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name != key {
            return None;
        }
        let mut fields = value.split_whitespace();
        let kib = fields.next()?.parse::<u64>().ok()?;
        fields.next().is_none_or(|unit| unit == "kB").then_some(kib)
    })
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn narrow_and_wide_cases_select_the_same_rows() -> Result<(), Box<dyn Error>> {
        let shape = FixtureShape {
            row_groups: 2,
            rows_per_group: 32,
            payload_columns: 4,
            match_every: 16,
        };
        let fixture = Fixture::create(&env::temp_dir(), shape, false)?;
        let narrow = measure_case(&fixture.data_path(), shape, ProjectionCase::Narrow)?;
        let wide = measure_case(&fixture.data_path(), shape, ProjectionCase::Wide)?;

        assert_eq!(narrow.row_count, 4);
        assert_eq!(narrow.row_count, wide.row_count);
        assert_eq!(narrow.id_checksum, wide.id_checksum);
        assert_eq!(Measurement::parse(&narrow.encode())?, narrow);
        Ok(())
    }

    #[test]
    fn peak_memory_parser_accepts_linux_status_units() {
        assert_eq!(
            process_status_memory_kib("VmPeak:\t12 kB\nVmHWM:\t34 kB\n", "VmHWM"),
            Some(34)
        );
        assert_eq!(process_status_memory_kib("VmHWM:\t34 MB\n", "VmHWM"), None);
    }
}
