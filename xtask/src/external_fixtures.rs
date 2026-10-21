//! Inventory and validate the independent Spark writer's fixture corpus.

use std::{
    fs,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
};

use arrow_array::Int32Array;
use arrow_ipc::reader::FileReader;
use arrow_schema::DataType;
use arrow_select::concat::concat_batches;
use parquet::file::reader::{FileReader as _, SerializedFileReader};
use serde_json::{Map, Value, json};

use crate::Result;

const BUDGET: u64 = 256 * 1024;

pub(crate) fn run(directory: &Path) -> Result<()> {
    let destination = directory.join("manifest.json");
    if destination.exists() {
        return Err("fixture manifest already exists".into());
    }
    let writer_path = directory.join("writer.json");
    let writer: Value = serde_json::from_slice(&fs::read(&writer_path)?)?;
    let manifest = inventory(directory, &writer)?;
    let rendered = serde_json::to_string_pretty(&manifest)? + "\n";
    let actual = files(directory)?
        .into_iter()
        .filter(|path| path != &writer_path)
        .try_fold(rendered.len() as u64, |total, path| {
            Ok::<_, std::io::Error>(total + fs::metadata(path)?.len())
        })?;
    if actual > BUDGET {
        return Err(format!("fixture corpus exceeds {BUDGET} bytes: {actual}").into());
    }
    File::options()
        .write(true)
        .create_new(true)
        .open(destination)?
        .write_all(rendered.as_bytes())?;
    fs::remove_file(writer_path)?;
    println!("Generated 3 fixtures, {actual} bytes including manifest.");
    Ok(())
}

fn inventory(directory: &Path, writer: &Value) -> Result<Value> {
    let sources = writer["fixtures"]
        .as_array()
        .ok_or("missing writer fixtures")?;
    if sources
        .iter()
        .map(|item| item["name"].as_str())
        .collect::<Vec<_>>()
        != [
            Some("partitioned"),
            Some("nested_mapping"),
            Some("deletion_vectors"),
        ]
    {
        return Err("unexpected writer fixtures".into());
    }
    let mut fixtures = Vec::new();
    let mut data_bytes = 0;
    for source in sources {
        let name = source["name"].as_str().ok_or("missing fixture name")?;
        let root = directory.join(name);
        let table = root.join("table");
        let oracle = FileReader::try_new(File::open(root.join("expected.arrow"))?, None)?;
        let schema = oracle.schema();
        let expected = concat_batches(
            &schema,
            &oracle.collect::<std::result::Result<Vec<_>, _>>()?,
        )?;
        let mut actions = Vec::<Value>::new();
        for log in files(&table.join("_delta_log"))? {
            if log.extension().is_some_and(|extension| extension == "json") {
                for line in fs::read_to_string(log)?.lines() {
                    actions.push(serde_json::from_str(line)?);
                }
            }
        }
        let protocol = actions
            .iter()
            .rev()
            .find_map(|action| action.get("protocol"))
            .ok_or("missing protocol")?;
        let metadata = actions
            .iter()
            .rev()
            .find_map(|action| action.get("metaData"))
            .ok_or("missing metadata")?;
        let descriptors = actions
            .iter()
            .filter_map(|action| action["add"].get("deletionVector"))
            .collect::<Vec<_>>();
        let mut groups = Map::new();
        let mut physical_rows = 0_i64;
        for path in files(&table)? {
            if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                let parquet = SerializedFileReader::new(File::open(&path)?)?;
                groups.insert(relative(&table, &path)?, json!(parquet.num_row_groups()));
                physical_rows += parquet.metadata().file_metadata().num_rows();
            }
        }
        let valid = match name {
            "partitioned" => {
                groups.len() >= 3
                    && groups.values().any(|n| n.as_u64().is_some_and(|n| n > 1))
                    && expected.num_rows() == 360
            }
            "nested_mapping" => {
                let profile = schema.field_with_name("profile")?;
                metadata["configuration"]["delta.columnMapping.mode"] == "name"
                    && matches!(profile.data_type(), DataType::Struct(fields)
                        if fields.iter().map(|f| f.name().as_str()).collect::<Vec<_>>() == ["score", "city"])
                    && expected.num_rows() == 6
            }
            "deletion_vectors" => {
                let ids = expected
                    .column_by_name("id")
                    .and_then(|array| array.as_any().downcast_ref::<Int32Array>())
                    .ok_or("missing integer oracle IDs")?;
                !descriptors.is_empty()
                    && descriptors
                        .iter()
                        .map(|d| d["cardinality"].as_u64().unwrap_or(0))
                        .sum::<u64>()
                        == 3
                    && physical_rows == 12
                    && expected.num_rows() == 9
                    && ids.iter().collect::<Vec<_>>() == [1, 3, 4, 6, 7, 8, 9, 10, 11].map(Some)
            }
            _ => false,
        };
        if !valid {
            return Err(format!(
                "fixture lacks the required physical features or oracle rows: {name}"
            )
            .into());
        }
        // Hadoop checksum sidecars are not part of the Delta table format.
        for path in files(&table)? {
            if path.extension().is_some_and(|extension| extension == "crc") {
                fs::remove_file(path)?;
            }
        }
        let mut entries = Map::new();
        for path in files(&root)? {
            let bytes = fs::metadata(&path)?.len();
            data_bytes += bytes;
            entries.insert(relative(&root, &path)?, json!({"bytes": bytes}));
        }
        fixtures.push(json!({
            "name": name, "snapshot_version": source["snapshot_version"], "schema": source["schema"],
            "protocol": protocol, "partition_columns": metadata["partitionColumns"],
            "expected_schema_and_rows": "expected.arrow", "expected_row_count": expected.num_rows(),
            "physical_row_count": physical_rows, "row_groups_per_file": groups,
            "deletion_vectors": descriptors, "files": entries,
        }));
    }
    Ok(json!({
        "provenance": "Original synthetic data written and read by Apache Spark / Delta Lake; no reader fixture helper is used.",
        "license": "Apache-2.0 (same as this repository)", "generator": "../generate.py",
        "generation_command": "JAVA_HOME=/path/to/java-21 SPARK_LOCAL_IP=127.0.0.1 uv run --python 3.13.12 --script crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/generate.py NEW_OUTPUT_DIRECTORY",
        "tools": writer["tools"], "python": writer["python"], "java": writer["java"],
        "size_budget_bytes": BUDGET, "data_bytes": data_bytes, "fixtures": fixtures,
    }))
}

fn relative(root: &Path, path: &Path) -> Result<String> {
    Ok(path
        .strip_prefix(root)?
        .to_str()
        .ok_or("non-UTF-8 fixture filename")?
        .replace('\\', "/"))
}

fn files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if kind.is_dir() {
            paths.extend(files(&entry.path())?);
        } else if kind.is_file() {
            paths.push(entry.path());
        } else {
            return Err("fixture corpus must contain only ordinary files and directories".into());
        }
    }
    paths.sort();
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn copy_corpus() -> Result<(tempfile::TempDir, Value)> {
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus");
        let temporary = tempfile::tempdir()?;
        for path in files(&source)? {
            if path.file_name().is_some_and(|name| name == "manifest.json") {
                continue;
            }
            let destination = temporary.path().join(path.strip_prefix(&source)?);
            fs::create_dir_all(destination.parent().ok_or("missing fixture parent")?)?;
            fs::copy(path, destination)?;
        }
        let manifest: Value = serde_json::from_slice(&fs::read(source.join("manifest.json"))?)?;
        fs::write(
            temporary.path().join("writer.json"),
            serde_json::to_vec(&manifest)?,
        )?;
        Ok((temporary, manifest))
    }

    #[test]
    fn finalization_preserves_spark_results_and_refuses_overwrite() -> Result<()> {
        let (temporary, expected) = copy_corpus()?;
        let sidecar = temporary.path().join("partitioned/table/ignored.crc");
        fs::write(&sidecar, b"Hadoop checksum")?;
        run(temporary.path())?;
        let path = temporary.path().join("manifest.json");
        let bytes = fs::read(&path)?;
        let actual: Value = serde_json::from_slice(&bytes)?;
        assert_eq!(actual["fixtures"], expected["fixtures"]);
        assert_eq!(actual["data_bytes"], expected["data_bytes"]);
        assert!(!sidecar.exists());
        assert!(!temporary.path().join("writer.json").exists());
        assert!(run(temporary.path()).is_err());
        assert_eq!(fs::read(path)?, bytes);
        Ok(())
    }

    #[test]
    fn invalid_or_oversized_corpus_never_gets_a_manifest() -> Result<()> {
        for oversized in [false, true] {
            let (temporary, _) = copy_corpus()?;
            if oversized {
                fs::write(
                    temporary.path().join("unexpected.dat"),
                    vec![0; BUDGET as usize],
                )?;
            } else {
                fs::copy(
                    temporary.path().join("nested_mapping/expected.arrow"),
                    temporary.path().join("deletion_vectors/expected.arrow"),
                )?;
            }
            assert!(run(temporary.path()).is_err());
            assert!(!temporary.path().join("manifest.json").exists());
        }
        Ok(())
    }
}
