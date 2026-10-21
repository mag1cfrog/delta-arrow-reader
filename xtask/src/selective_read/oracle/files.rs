use std::{
    collections::HashMap,
    fs::{self, File},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use serde_json::{Value, json};
use uuid::Uuid;

use super::{
    Result, require,
    rules::{integer, key},
};
use crate::{
    artifacts::reader_sha256,
    selective_read::{array, storage::read_json, text},
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Identity([u64; 7]);
pub(super) fn identity(path: &Path) -> Result<Identity> {
    from_metadata(&fs::metadata(path)?)
}
fn from_metadata(m: &fs::Metadata) -> Result<Identity> {
    require(m.is_file(), "expected a regular fixture file")?;
    Ok(Identity([
        m.dev(),
        m.ino(),
        m.len(),
        m.mtime() as u64,
        m.mtime_nsec() as u64,
        m.ctime() as u64,
        m.ctime_nsec() as u64,
    ]))
}
#[derive(Default)]
pub(super) struct Digests(HashMap<Identity, String>);
impl Digests {
    pub(super) fn digest(&mut self, path: &Path) -> Result<String> {
        let mut file = File::open(path)?;
        let before = from_metadata(&file.metadata()?)?;
        let hash = match self.0.get(&before) {
            Some(hash) => hash.clone(),
            None => reader_sha256(&mut file)?,
        };
        require(
            from_metadata(&file.metadata()?)? == before && identity(path)? == before,
            "file changed during digest",
        )?;
        self.0.insert(before, hash.clone());
        Ok(hash)
    }
    pub(super) fn verify(&mut self, root: &Path, item: &Value) -> Result<PathBuf> {
        let path = inside(root, text(&item["path"])?)?;
        require(
            fs::metadata(&path)?.len() == integer(&item["bytes"])?
                && self.digest(&path)? == item["sha256"],
            "object size/checksum changed",
        )?;
        Ok(path)
    }
}
pub(super) fn inside(root: &Path, relative: &str) -> Result<PathBuf> {
    require(
        !Path::new(relative).is_absolute(),
        "expected relative object path",
    )?;
    let root = root.canonicalize()?;
    let path = root.join(relative).canonicalize()?;
    require(
        path.starts_with(root),
        "object path escapes fixture directory",
    )?;
    Ok(path)
}
pub(super) fn path(root: &Path, table: &Value, item: &Value) -> Result<PathBuf> {
    let group = inside(root, text(&table["path"])?)?;
    inside(&group, text(&item["path"])?)
}
fn logs(group: &Value) -> Result<Vec<Value>> {
    if let Some(logs) = group.get("delta_logs") {
        Ok(array(logs)?.clone())
    } else {
        Ok(group
            .get("delta_log")
            .filter(|v| !v.is_null())
            .cloned()
            .into_iter()
            .collect())
    }
}
pub(super) fn objects(
    digests: &mut Digests,
    root: &Path,
    manifest: &Value,
    table: &Value,
    source: Option<&Value>,
) -> Result<Vec<Value>> {
    let mut verified = Vec::new();
    for group in source.into_iter().chain([table]) {
        let files = array(&group["files"])?;
        for item in files
            .iter()
            .cloned()
            .chain(logs(group)?)
            .chain(
                files
                    .iter()
                    .filter_map(|f| f.get("deletion_vector").cloned()),
            )
            .chain(files.iter().filter_map(|f| f.get("geometry").cloned()))
        {
            let descriptor = json!({"path":Path::new(text(&group["path"])?).join(text(&item["path"])?),"bytes":item["bytes"],"sha256":item["sha256"]});
            digests.verify(root, &descriptor)?;
            verified.push(descriptor);
        }
        for item in files {
            if let Some(geometry) = item.get("geometry") {
                require(
                    geometry["path"] == format!("{}.geometry.json", text(&item["path"])?),
                    "invalid geometry sidecar path",
                )?;
                let details = read_json(&path(root, group, geometry)?)?;
                let groups: Vec<_> = array(&details["row_groups"])?
                    .iter()
                    .map(|g| json!({"first_row":g["first_row"],"rows":g["rows"]}))
                    .collect();
                require(
                    details["parquet_sha256"] == item["sha256"]
                        && json!(groups) == item["row_groups"],
                    "geometry sidecar differs from Parquet identity or groups",
                )?;
                let names: Vec<_> = array(&group["schema"]["fields"])?
                    .iter()
                    .map(|f| &f["name"])
                    .collect();
                for group in array(&details["row_groups"])? {
                    require(
                        array(&group["columns"])?
                            .iter()
                            .map(|c| &c["column"])
                            .collect::<Vec<_>>()
                            == names,
                        "geometry schema differs",
                    )?;
                }
            }
        }
    }
    let logs = logs(table)?;
    let version = integer(&table["snapshot_version"])?;
    require(
        logs.len() as u64 == version + 1 && logs.last() == Some(&table["delta_log"]),
        "wrong snapshot logs",
    )?;
    let mut inventory = std::collections::BTreeMap::new();
    let mut metadata = Value::Null;
    for (v, log) in logs.iter().enumerate() {
        require(
            log["path"] == format!("_delta_log/{v:020}.json"),
            "wrong snapshot log path",
        )?;
        let actions = fs::read_to_string(path(root, table, log)?)?
            .lines()
            .map(serde_json::from_str)
            .collect::<std::result::Result<Vec<Value>, _>>()?;
        let protocol = if v == 0 {
            json!({"minReaderVersion":1,"minWriterVersion":2})
        } else {
            json!({"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors","invariants","appendOnly"]})
        };
        require(
            actions
                .iter()
                .filter_map(|a| a.get("protocol"))
                .collect::<Vec<_>>()
                == vec![&protocol],
            "unexpected Delta protocol",
        )?;
        for action in actions {
            if let Some(current) = action.get("metaData") {
                if v > 0 {
                    require(
                        metadata.is_object() && metadata["configuration"].is_object(),
                        "missing base table metadata",
                    )?;
                    let mut expected = metadata.clone();
                    expected["configuration"]["delta.enableDeletionVectors"] = json!("true");
                    require(current == &expected, "DV metadata changed unrelated fields")?;
                }
                metadata = current.clone();
            }
            if let Some(removed) = action.get("remove") {
                require(
                    v == 1
                        && removed.get("deletionVector").is_none()
                        && inventory.remove(text(&removed["path"])?).is_some(),
                    "unexpected removed file identity",
                )?;
            }
            if let Some(added) = action.get("add") {
                require(
                    (v > 0 || added.get("deletionVector").is_none())
                        && inventory
                            .insert(text(&added["path"])?.to_owned(), added.clone())
                            .is_none(),
                    "duplicate Add or DV in base snapshot",
                )?;
            }
        }
    }
    let files = array(&table["files"])?;
    require(
        inventory.len() == files.len()
            && files.iter().all(|f| {
                f["path"]
                    .as_str()
                    .is_some_and(|p| inventory.contains_key(p))
            }),
        "Delta file inventory differs from manifest",
    )?;
    let profile = if manifest["profile"] == "large" || manifest.get("profile").is_none() {
        format!(
            "large-sf{}",
            table["scale_factor"].as_f64().ok_or("invalid scale")? as u64
        )
    } else {
        text(&manifest["profile"])?.to_owned()
    };
    let table_uuid = Uuid::new_v5(
        &Uuid::NAMESPACE_URL,
        format!(
            "https://github.com/mag1cfrog/delta-arrow-reader/selective-read-v1/{profile}/{}",
            text(&table["id"])?
        )
        .as_bytes(),
    );
    if version > 0 {
        require(
            metadata["id"] == table_uuid.to_string()
                && serde_json::from_str::<Value>(text(&metadata["schemaString"])?)?
                    == table["schema"]
                && metadata["configuration"]["delta.enableDeletionVectors"] == "true",
            "invalid DV table metadata",
        )?;
    }
    for item in files {
        let action = &inventory[text(&item["path"])?];
        let dv = &item["deletion_vector"];
        require(
            action["deletionVector"] == dv["descriptor"]
                && action["partitionValues"]
                    .as_object()
                    .is_none_or(|v| v.is_empty()),
            "unexpected DV or partitions",
        )?;
        require(
            action["size"] == item["bytes"]
                && serde_json::from_str::<Value>(text(&action["stats"])?)? == item["delta_stats"]
                && item["delta_stats"]["numRecords"] == item["rows"],
            "Delta Add differs from manifest",
        )?;
        if dv.is_null() {
            continue;
        }
        let ordinals = array(&dv["physical_ordinals"])?;
        let logical = array(&dv["logical_ids"])?;
        let descriptor = &dv["descriptor"];
        let indexes = ordinals.iter().map(integer).collect::<Result<Vec<_>>>()?;
        require(
            table["deletion_vectors"] == true
                && item["delta_stats"]["tightBounds"] == false
                && !indexes.is_empty()
                && indexes
                    .iter()
                    .all(|i| *i < item["rows"].as_u64().unwrap_or(0))
                && indexes.windows(2).all(|p| p[0] < p[1])
                && indexes.len() == logical.len()
                && indexes.len() as u64 == integer(&descriptor["cardinality"])?,
            "invalid deletion coordinates",
        )?;
        for k in logical {
            key(k)?;
        }
        let id = Uuid::new_v5(&table_uuid, text(&item["path"])?.as_bytes());
        require(
            dv["path"] == format!("deletion_vector_{id}.bin")
                && descriptor["storageType"] == "u"
                && descriptor["pathOrInlineDv"] == z85::encode(id.as_bytes())
                && descriptor["offset"] == 1,
            "invalid deterministic DV path/offset",
        )?;
        let bytes = fs::read(path(root, table, dv)?)?;
        let size = integer(&descriptor["sizeInBytes"])?;
        require(
            bytes.len() >= 13 && bytes.len() as u64 == size + 9,
            "invalid DV envelope size",
        )?;
        let mut crc = flate2::Crc::new();
        crc.update(&bytes[5..bytes.len() - 4]);
        require(
            bytes[0] == 1
                && u32::from_be_bytes(bytes[1..5].try_into()?) as u64 == size
                && u32::from_le_bytes(bytes[5..9].try_into()?) == 1681511377
                && crc.sum() == u32::from_be_bytes(bytes[bytes.len() - 4..].try_into()?),
            "invalid DV envelope/checksum",
        )?;
    }
    Ok(verified)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn digest_cache_tracks_hardlinks_writes_and_replacements_and_paths_cannot_escape() -> Result<()>
    {
        let root = tempfile::tempdir()?;
        let path = root.path().join("data");
        fs::write(&path, b"first")?;
        let mut hashes = Digests::default();
        let link = root.path().join("link");
        fs::hard_link(&path, &link)?;
        let old = hashes.digest(&path)?;
        assert_eq!(hashes.digest(&link)?, old);
        assert_eq!(hashes.0.len(), 1);
        fs::write(&path, b"later")?;
        assert_ne!(hashes.digest(&path)?, old);
        fs::remove_file(&path)?;
        fs::write(&path, b"first")?;
        assert_eq!(hashes.digest(&path)?, old);
        assert!(inside(root.path(), "..").is_err());
        std::os::unix::fs::symlink("/", root.path().join("escape"))?;
        assert!(inside(root.path(), "escape").is_err());
        Ok(())
    }
}
