//! Synthetic fixtures and controlled transport for Parquet range-planning benchmarks.

use std::{
    collections::BTreeMap,
    error::Error,
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{ArrayRef, Int32Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use delta_arrow_reader::DeltaStorageOptions;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use serde_json::json;

#[path = "range_planning/controlled_http.rs"]
mod controlled_http;

use controlled_http::ControlledHttpServer;

const DATA_FILE: &str = "part.parquet";
const MATCH_VALUE: &str = "match";
const OTHER_VALUE: &str = "other";
const PAYLOAD_FILLER: &str = concat!(
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
    "abcdefghijklmnopqrstuvwxyz0123456789",
);

#[derive(Debug, Clone, Copy)]
struct FixtureShape {
    row_groups: usize,
    rows_per_group: usize,
    rows_per_page: usize,
    payload_columns: usize,
}

impl FixtureShape {
    fn validate(self) -> Result<(), io::Error> {
        if self.row_groups < 2
            || self.rows_per_group == 0
            || self.rows_per_page == 0
            || self.payload_columns < 4
            || !self.rows_per_group.is_multiple_of(self.rows_per_page)
        {
            return Err(invalid("invalid range-planning fixture dimensions"));
        }
        i32::try_from(self.row_count()).map_err(io::Error::other)?;
        Ok(())
    }

    const fn row_count(self) -> usize {
        self.row_groups.saturating_mul(self.rows_per_group)
    }

    fn expected_row_ids(self) -> Vec<i32> {
        (0..self.row_count())
            .filter(|row| row % self.rows_per_group < self.rows_per_page)
            .filter_map(|row| i32::try_from(row).ok())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProjectionDensity {
    Dense,
    Sparse,
}

impl ProjectionDensity {
    fn payload_indices(self, payload_columns: usize) -> impl Iterator<Item = usize> {
        let step = match self {
            Self::Dense => 2,
            Self::Sparse => 4,
        };
        (0..payload_columns).step_by(step)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TransportProfile {
    name: &'static str,
    request_latency: Duration,
    shared_throughput_bytes_per_second: u64,
}

impl TransportProfile {
    const LOW_LATENCY_LOW_THROUGHPUT: Self = Self {
        name: "low_latency_low_throughput",
        request_latency: Duration::from_millis(1),
        shared_throughput_bytes_per_second: 4 * 1024 * 1024,
    };
    const BALANCED: Self = Self {
        name: "balanced",
        request_latency: Duration::from_millis(8),
        shared_throughput_bytes_per_second: 64 * 1024 * 1024,
    };
    const HIGH_LATENCY_HIGH_THROUGHPUT: Self = Self {
        name: "high_latency_high_throughput",
        request_latency: Duration::from_millis(20),
        shared_throughput_bytes_per_second: 512 * 1024 * 1024,
    };

    const ALL: [Self; 3] = [
        Self::LOW_LATENCY_LOW_THROUGHPUT,
        Self::BALANCED,
        Self::HIGH_LATENCY_HIGH_THROUGHPUT,
    ];
}

#[derive(Debug)]
struct Fixture {
    path: PathBuf,
    table_uri: String,
    storage_options: DeltaStorageOptions,
    server: ControlledHttpServer,
    data_file_bytes: u64,
    retain: bool,
}

impl Fixture {
    fn create(
        temp_root: &Path,
        shape: FixtureShape,
        profile: TransportProfile,
        retain: bool,
    ) -> Result<Self, Box<dyn Error>> {
        shape.validate()?;
        let path = temp_root.join(format!(
            "delta-arrow-reader-range-planning-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos(),
        ));
        fs::create_dir_all(path.join("_delta_log"))?;

        let schema = benchmark_schema(shape.payload_columns);
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(shape.rows_per_group))
            .set_write_batch_size(shape.rows_per_page)
            .set_data_page_row_count_limit(shape.rows_per_page)
            .set_dictionary_enabled(false)
            .set_compression(Compression::UNCOMPRESSED)
            .set_offset_index_disabled(false)
            .build();
        let data_path = path.join(DATA_FILE);
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
        let data_file_bytes = fs::metadata(&data_path)?.len();
        write_delta_log(&path, shape, data_file_bytes)?;

        let server = ControlledHttpServer::start(path.clone(), profile)?;
        let table_uri = server.url().to_owned();
        Ok(Self {
            path,
            table_uri,
            storage_options: BTreeMap::from([("allow_http".to_owned(), "true".to_owned())]),
            server,
            data_file_bytes,
            retain,
        })
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.stop();
        if !self.retain {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn benchmark_schema(payload_columns: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("row_id", DataType::Int32, false),
        Field::new("event_id", DataType::Utf8, false),
    ];
    fields.extend(
        (0..payload_columns).map(|index| Field::new(payload_name(index), DataType::Utf8, true)),
    );
    Arc::new(Schema::new(fields))
}

fn benchmark_batch(
    schema: SchemaRef,
    first_row: usize,
    shape: FixtureShape,
) -> Result<RecordBatch, Box<dyn Error>> {
    let rows = first_row..first_row.saturating_add(shape.rows_per_group);
    let row_ids = rows
        .clone()
        .map(i32::try_from)
        .collect::<Result<Vec<_>, _>>()?;
    let mut columns = Vec::with_capacity(shape.payload_columns.saturating_add(2));
    columns.push(Arc::new(Int32Array::from(row_ids.clone())) as ArrayRef);
    columns.push(Arc::new(StringArray::from_iter_values(rows.map(|row| {
        if row % shape.rows_per_group < shape.rows_per_page {
            MATCH_VALUE
        } else {
            OTHER_VALUE
        }
    }))) as ArrayRef);
    for payload_index in 0..shape.payload_columns {
        columns.push(Arc::new(StringArray::from_iter(
            row_ids
                .iter()
                .map(|row_id| payload_value(*row_id, payload_index)),
        )));
    }
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn payload_value(row_id: i32, payload_index: usize) -> Option<String> {
    let row_id_usize = usize::try_from(row_id).ok()?;
    (!(row_id_usize + payload_index).is_multiple_of(17))
        .then(|| format!("payload-{payload_index:03}-{row_id:08}-{PAYLOAD_FILLER}"))
}

fn payload_name(index: usize) -> String {
    format!("payload_{index:03}")
}

fn write_delta_log(root: &Path, shape: FixtureShape, data_file_bytes: u64) -> io::Result<()> {
    let mut fields = vec![
        json!({"name": "row_id", "type": "integer", "nullable": false, "metadata": {}}),
        json!({"name": "event_id", "type": "string", "nullable": false, "metadata": {}}),
    ];
    fields.extend((0..shape.payload_columns).map(|index| {
        json!({"name": payload_name(index), "type": "string", "nullable": true, "metadata": {}})
    }));
    let protocol = json!({"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}});
    let metadata = json!({
        "metaData": {
            "id": "delta-arrow-reader-range-planning-benchmark",
            "format": {"provider": "parquet", "options": {}},
            "schemaString": json!({"type": "struct", "fields": fields}).to_string(),
            "partitionColumns": [],
            "configuration": {},
            "createdTime": 1_587_968_585_495_i64,
        }
    });
    let add = json!({
        "add": {
            "path": DATA_FILE,
            "partitionValues": {},
            "size": data_file_bytes,
            "modificationTime": 1_587_968_586_000_i64,
            "dataChange": true,
        }
    });
    fs::write(
        root.join("_delta_log/00000000000000000000.json"),
        format!("{protocol}\n{metadata}\n{add}\n"),
    )
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use delta_arrow_reader::DeltaTableBuilder;
    use parquet::file::reader::{FileReader, SerializedFileReader};

    use super::*;

    type TestResult = Result<(), Box<dyn Error>>;

    const TEST_SHAPE: FixtureShape = FixtureShape {
        row_groups: 2,
        rows_per_group: 256,
        rows_per_page: 64,
        payload_columns: 8,
    };

    const TEST_PROFILE: TransportProfile = TransportProfile {
        name: "test",
        request_latency: Duration::ZERO,
        shared_throughput_bytes_per_second: u64::MAX,
    };

    #[test]
    fn fixture_has_indexed_row_groups_and_interleaved_projections() -> TestResult {
        let fixture = Fixture::create(&std::env::temp_dir(), TEST_SHAPE, TEST_PROFILE, false)?;
        let reader = SerializedFileReader::new(File::open(fixture.path.join(DATA_FILE))?)?;
        assert_eq!(reader.metadata().num_row_groups(), TEST_SHAPE.row_groups);
        assert!(reader.metadata().row_groups().iter().all(|row_group| {
            row_group
                .columns()
                .iter()
                .all(|column| column.offset_index_offset().is_some())
        }));
        assert_eq!(
            ProjectionDensity::Dense
                .payload_indices(TEST_SHAPE.payload_columns)
                .collect::<Vec<_>>(),
            vec![0, 2, 4, 6]
        );
        assert_eq!(
            ProjectionDensity::Sparse
                .payload_indices(TEST_SHAPE.payload_columns)
                .collect::<Vec<_>>(),
            vec![0, 4]
        );
        assert_eq!(
            TEST_SHAPE.expected_row_ids().len(),
            TEST_SHAPE.row_groups * TEST_SHAPE.rows_per_page
        );
        Ok(())
    }

    #[test]
    fn controlled_http_fixture_loads_as_a_delta_table() -> TestResult {
        let fixture = Fixture::create(&std::env::temp_dir(), TEST_SHAPE, TEST_PROFILE, false)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let table = runtime.block_on(
            DeltaTableBuilder::new(&fixture.table_uri)
                .with_storage_options(fixture.storage_options.clone())
                .load_table(),
        )?;
        assert_eq!(table.version(), 0);
        assert_eq!(
            table.schema().fields().len(),
            TEST_SHAPE.payload_columns + 2
        );
        assert!(fixture.data_file_bytes > 0);
        Ok(())
    }

    #[test]
    fn transport_profile_names_are_stable() {
        assert_eq!(
            TransportProfile::ALL.map(|profile| profile.name),
            [
                "low_latency_low_throughput",
                "balanced",
                "high_latency_high_throughput"
            ]
        );
    }
}
