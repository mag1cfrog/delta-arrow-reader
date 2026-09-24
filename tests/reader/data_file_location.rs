//! Public API regression for a foreign URL aliasing a readable local Parquet file.

use std::{error::Error, fs};

use delta_arrow_reader::{DeltaReaderError, DeltaScanExecutionOptions, DeltaTableBuilder};
use futures_util::TryStreamExt;
use serde_json::Value;
use url::Url;

use super::support::RealParquetDeltaTable;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

fn fixture(name: &str, foreign: bool) -> TestResult<RealParquetDeltaTable> {
    let table = RealParquetDeltaTable::new_default(name)?;
    let file = fs::canonicalize(table.path().join(table.data_file_path()))?;
    let local = Url::from_file_path(file).map_err(|()| "invalid file URL")?;
    let location = if foreign {
        format!("s3://secret-bucket{}?secret-token", local.path())
    } else {
        local.to_string()
    };
    let log = table.path().join("_delta_log/00000000000000000001.json");
    let mut actions = fs::read_to_string(&log)?
        .lines()
        .map(serde_json::from_str::<Value>)
        .collect::<Result<Vec<_>, _>>()?;
    let add = actions
        .iter_mut()
        .find_map(|action| action.get_mut("add"))
        .ok_or("missing add action")?;
    add["path"] = Value::String(location);
    fs::write(
        log,
        actions
            .iter()
            .map(Value::to_string)
            .collect::<Vec<_>>()
            .join("\n"),
    )?;
    Ok(table)
}

#[tokio::test]
async fn data_file_location_streaming_rejects_foreign_url_with_readable_local_key() -> TestResult {
    for buffered in [false, true] {
        for foreign in [false, true] {
            let fixture = fixture(&format!("streaming-location-{buffered}-{foreign}"), foreign)?;
            let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
                .load_table()
                .await?;
            let options = DeltaScanExecutionOptions::new()
                .with_parquet_full_file_read_threshold_bytes(
                    buffered.then_some(usize::try_from(fixture.data_file_size())?),
                )?;
            let scan = table.scan().with_execution_options(options).build().await?;
            let stream = scan.into_stream();
            let metrics = stream.metrics();
            let result = stream.try_collect::<Vec<_>>().await;
            if foreign {
                let error = result.err().ok_or("foreign URL returned local data")?;
                assert!(matches!(
                    error,
                    DeltaReaderError::DataFileRead {
                        reason: "data_file_store_mismatch",
                        ..
                    }
                ));
                assert!(!format!("{error} {error:?}").contains("secret"));
                let metrics = metrics.snapshot();
                assert_eq!(metrics.parquet_data_file_full_get_operations, Some(0));
                assert_eq!(metrics.parquet_data_file_range_get_operations, Some(0));
                assert_eq!(metrics.scheduler_rows_emitted, 0);
            } else {
                assert_eq!(
                    result?.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    3
                );
            }
        }
    }
    Ok(())
}

#[cfg(feature = "datafusion")]
#[tokio::test]
async fn data_file_location_datafusion_rejects_foreign_url_with_readable_local_key() -> TestResult {
    use datafusion::prelude::SessionContext;
    use delta_arrow_reader::datafusion::{ScanOptions, register_table};

    for foreign in [false, true] {
        let fixture = fixture(&format!("datafusion-location-{foreign}"), foreign)?;
        let table = DeltaTableBuilder::new(fixture.path().to_string_lossy())
            .load_table()
            .await?;
        let context = SessionContext::new();
        register_table(&context, "orders", table, ScanOptions::default())?;
        let result = context.sql("SELECT id FROM orders").await?.collect().await;
        if foreign {
            let error = result
                .err()
                .ok_or("foreign URL returned local data through DataFusion")?;
            assert!(error.to_string().contains("data_file_store_mismatch"));
            assert!(!format!("{error} {error:?}").contains("secret"));
        } else {
            assert_eq!(
                result?.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                3
            );
        }
    }
    Ok(())
}
