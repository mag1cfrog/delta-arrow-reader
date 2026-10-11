//! Manual #452 query driver; ordinary and experimental modes use the same plan.

#[path = "../delta_length.rs"]
mod delta_length;
#[path = "../delta_length_reader.rs"]
mod delta_length_reader;

// Satisfy the private planner's import without changing the production crate.
// Only its explicit-concurrency cost helpers are used by this experiment.
mod reader {
    pub mod options {
        pub const MAX_CONCURRENT_PARQUET_RANGE_READS: usize = 10;
    }
}

use arrow::{
    array::{BooleanArray, Int32Array},
    datatypes::{DataType, Schema, SchemaRef},
    ipc::writer::StreamWriter,
    record_batch::RecordBatch,
};
use delta_kernel::{Engine, scan::state::ScanFile, snapshot::Snapshot};
use delta_kernel_default_engine::DefaultEngineBuilder;
use delta_length_reader::{CostModel, Io, Reader};
use futures_util::TryStreamExt;
use object_store::{ObjectStore, aws::AmazonS3Builder, path::Path as ObjectPath};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions, RowSelection},
        async_reader::{AsyncFileReader, ParquetObjectReader, ParquetRecordBatchStreamBuilder},
    },
    file::metadata::PageIndexPolicy,
};
use serde_json::json;
use std::{
    fs::{self, File},
    io::Read,
    path::Path,
    sync::Arc,
    time::Instant,
};
use url::Url;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn scan_files(snapshot: &Arc<Snapshot>, engine: &dyn Engine) -> Result<Vec<ScanFile>> {
    let mut files = Vec::new();
    for metadata in snapshot
        .clone()
        .scan_builder()
        .build()?
        .scan_metadata(engine)?
    {
        files = metadata?.visit_scan_files(files, |files, file| files.push(file))?;
    }
    files.sort_by(|a: &ScanFile, b| a.path.cmp(&b.path));
    if files.is_empty() || files.len() > 8 {
        return Err("experiment requires 1-8 fixture files".into());
    }
    Ok(files)
}

async fn query(
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
    io: Arc<Io>,
    mode: &str,
    selection: &str,
    views: bool,
    validate: bool,
) -> Result<(Vec<RecordBatch>, usize, SchemaRef)> {
    let files = scan_files(&snapshot, engine)?;
    let mut output = Vec::new();
    let mut output_rows = 0;
    let mut output_schema = None;
    for file in files {
        if file.transform.is_some() || !file.partition_values.is_empty() {
            return Err("experiment requires flat unpartitioned fixtures".into());
        }
        let path = ObjectPath::from_url_path(snapshot.table_root().join(&file.path)?.path())?;
        let mut remote = ParquetObjectReader::new(io.store.clone(), path.clone())
            .with_file_size(file.size.try_into()?);
        let options = ArrowReaderOptions::new().with_offset_index_policy(PageIndexPolicy::Required);
        let metadata = remote.get_metadata(Some(&options)).await?;
        let rows: usize = metadata.file_metadata().num_rows().try_into()?;
        if rows > 1048576 {
            return Err("experiment file exceeds row bound".into());
        }
        let arrow_metadata = ArrowReaderMetadata::try_new(metadata.clone(), options)?;
        let schema = arrow_metadata.schema();
        if schema.fields().len() != 11
            || schema.field(0).name() != "row_id"
            || schema.field(1).name() != "grouped"
            || schema.field(2).name() != "scattered"
        {
            return Err("not a delta-length network fixture".into());
        }
        let live = file
            .dv_info
            .get_selection_vector(engine, snapshot.table_root())?;
        let mut selected = vec![true; rows];
        if selection != "dense" {
            let column = if selection == "grouped" { 1 } else { 2 };
            let reader = Reader::new(io.clone(), path.clone(), metadata.clone(), &[], false)?;
            let mut stream =
                ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata.clone())
                    .with_projection(ProjectionMask::roots(
                        metadata.file_metadata().schema_descr(),
                        [column],
                    ))
                    .with_batch_size(8192)
                    .build()?;
            let mut offset = 0;
            while let Some(batch) = stream.try_next().await? {
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .ok_or("predicate type")?;
                for (i, value) in values.iter().enumerate() {
                    selected[offset + i] = value == Some(1);
                }
                offset += batch.num_rows();
            }
            if offset != rows {
                return Err("predicate row count".into());
            }
        }
        if let Some(live) = live {
            if live.len() > rows {
                return Err("DV exceeds file rows".into());
            }
            for (keep, live) in selected.iter_mut().zip(live) {
                *keep &= live;
            }
        }
        let options = if views {
            ArrowReaderOptions::new().with_schema(Arc::new(Schema::new(
                schema
                    .fields()
                    .iter()
                    .map(|field| {
                        let dtype = match field.data_type() {
                            DataType::Utf8 => DataType::Utf8View,
                            DataType::Binary => DataType::BinaryView,
                            dtype => dtype.clone(),
                        };
                        field.as_ref().clone().with_data_type(dtype)
                    })
                    .collect::<Vec<_>>(),
            )))
        } else {
            ArrowReaderOptions::new()
        };
        let arrow_metadata = ArrowReaderMetadata::try_new(metadata.clone(), options)?;
        let columns: Vec<_> = std::iter::once(0).chain(3..schema.fields().len()).collect();
        output_schema = Some(Arc::new(arrow_metadata.schema().project(&columns)?));
        let reader = Reader::new(
            io.clone(),
            path,
            metadata.clone(),
            &selected,
            mode == "experimental",
        )?;
        let filter = BooleanArray::from(selected);
        let mut stream = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, arrow_metadata)
            .with_projection(ProjectionMask::roots(
                metadata.file_metadata().schema_descr(),
                columns,
            ))
            .with_row_selection(RowSelection::from_filters(&[filter]))
            .with_batch_size(8192)
            .build()?;
        while let Some(batch) = stream.try_next().await? {
            output_rows += batch.num_rows();
            if validate {
                output.push(batch);
            }
        }
    }
    Ok((
        output,
        output_rows,
        output_schema.ok_or("missing output schema")?,
    ))
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if !(7..=8).contains(&args.len())
        || !matches!(args[2].as_str(), "ordinary" | "experimental")
        || !matches!(args[3].as_str(), "grouped" | "scattered" | "dense")
        || !matches!(args[4].as_str(), "arrays" | "views")
        || !matches!(args[5].as_str(), "validate" | "timing")
    {
        return Err("TABLE_URI ordinary|experimental grouped|scattered|dense arrays|views validate|timing [auto|COST_PROFILE_JSON] NEW_OUTPUT_DIRECTORY".into());
    }
    let automatic = args.len() == 8 && args[6] == "auto";
    let profile = (args.len() == 8 && !automatic)
        .then(|| fs::read(&args[6]))
        .transpose()?;
    let mut profile = profile.as_deref().map(serde_json::from_slice).transpose()?;
    let mut cost_model = profile.as_ref().map(CostModel::new).transpose()?;
    // The shared resource-limit launcher expects the output directory last.
    let output = Path::new(args.last().unwrap());
    fs::create_dir(output)?;
    let started = Instant::now();
    let url = Url::parse(&args[1])?;
    let store: Arc<dyn ObjectStore> = if url.scheme() == "s3" {
        Arc::new(
            AmazonS3Builder::from_env()
                .with_bucket_name(url.host_str().ok_or("bucket")?)
                .build()?,
        )
    } else if url.scheme() == "file" {
        object_store::parse_url(&url)?.0.into()
    } else {
        return Err("fixture URI must be file:// or s3://".into());
    };
    let engine = DefaultEngineBuilder::new(store.clone()).build();
    let snapshot = Snapshot::builder_for(&args[1])
        .at_version(0)
        .build(&engine)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()?;
    if automatic {
        if url.scheme() != "s3" {
            return Err("automatic calibration requires s3://".into());
        }
        let files = scan_files(&snapshot, &engine)?
            .into_iter()
            .map(|file| {
                Ok((
                    ObjectPath::from_url_path(snapshot.table_root().join(&file.path)?.path())?,
                    file.size.try_into()?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let (measured, record) =
            runtime.block_on(delta_length_reader::calibrate(store.as_ref(), &files));
        cost_model = measured;
        profile = Some(record);
    }
    let initialization = started.elapsed().as_secs_f64();
    fs::write(
        output.join("initialized.json"),
        serde_json::to_vec(&json!({"seconds":initialization,"cost_profile":profile}))?,
    )?;
    // The supervisor records initialization I/O, then resets the proxy before releasing this gate.
    std::io::stdin().read_exact(&mut [0])?;
    let io = Arc::new(Io::new(store, cost_model));
    let started = Instant::now();
    let (batches, rows, schema) = runtime.block_on(query(
        snapshot,
        &engine,
        io.clone(),
        &args[2],
        &args[3],
        args[4] == "views",
        args[5] == "validate",
    ))?;
    let seconds = started.elapsed().as_secs_f64();
    if args[5] == "validate" {
        let mut writer =
            StreamWriter::try_new(File::create_new(output.join("result.arrow"))?, &schema)?;
        for batch in batches {
            writer.write(&batch)?;
        }
        writer.finish()?;
    }
    let record = json!({"initialization_seconds":initialization,"query_seconds":seconds,"output_rows":rows,
        "mode":args[2],"selection":args[3],"array_type":args[4],"metrics":io.metrics.snapshot(),
        "projection":["row_id","payload_0","payload_1","payload_2","payload_3","payload_4","payload_5","payload_6","payload_7"],
        "predicate":if args[3] == "dense" { "true".to_string() } else { format!("{} = 1", args[3]) },
        "snapshot_version":0,"deletion_vectors":true,
        "request_concurrency":32,"threads":8,"cost_profile":profile});
    fs::write(
        output.join("record.json"),
        serde_json::to_vec_pretty(&record)?,
    )?;
    println!("{record}");
    Ok(())
}
