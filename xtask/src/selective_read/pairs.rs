use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::CString,
    fs,
    mem::MaybeUninit,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::{array, field, fixture_tables, require, storage, text, workload};
use crate::{
    Result, artifacts::reader_sha256, command::wait_with_timeout, selective_read_build::digest,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    path: PathBuf,
    manifest_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    fixtures: Vec<Fixture>,
    binary: PathBuf,
    output: PathBuf,
    disk_gib: u64,
    seconds: u32,
    layout: Option<String>,
    contract_sha256: String,
    driver_sha256: String,
}

#[derive(clap::Args)]
pub(crate) struct Args {
    #[arg(long, required = true)]
    fixtures: Vec<PathBuf>,
    #[arg(long)]
    binary: PathBuf,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    disk_limit_gib: u64,
    #[arg(long, default_value_t = 1800)]
    elapsed_limit_seconds: u32,
    #[arg(long, value_parser = ["localized", "scattered"])]
    layout: Option<String>,
    #[arg(long, default_value = "benches/selective_read")]
    harness: PathBuf,
}

pub(super) fn prepare(args: Args) -> Result<Value> {
    let protocols = fs::canonicalize(args.harness.join("../../docs/content/benchmarks"))?;
    let fixtures = args
        .fixtures
        .iter()
        .map(|root| {
            let path = fs::canonicalize(root)?;
            let bytes = fs::read(path.join("manifest.json"))?;
            let manifest: Value = serde_json::from_slice(&bytes)?;
            for table in fixture_tables(&manifest)? {
                workload::query(text(&table["id"])?, &manifest, &protocols)?;
            }
            Ok(Fixture {
                path,
                manifest_sha256: reader_sha256(bytes.as_slice())?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    run(Request {
        fixtures,
        binary: args.binary,
        output: args.output,
        disk_gib: args.disk_limit_gib,
        seconds: args.elapsed_limit_seconds,
        layout: args.layout,
        contract_sha256: digest(&protocols.join("selective-read-production-shapes.md"))?,
        driver_sha256: reader_sha256(include_bytes!("pairs.rs").as_slice())?,
    })
}

fn objects(table: &Value) -> Result<Vec<&Value>> {
    let files = array(&table["files"])?;
    let mut objects: Vec<_> = files.iter().collect();
    if !table["delta_log"].is_null() {
        objects.push(&table["delta_log"]);
    }
    objects.extend(files.iter().filter_map(|file| file.get("geometry")));
    Ok(objects)
}

fn relative(table: &Value, item: &Value) -> Result<PathBuf> {
    let path = Path::new(text(&table["path"])?).join(text(&item["path"])?);
    require(
        !path.as_os_str().is_empty()
            && path
                .components()
                .all(|part| matches!(part, Component::Normal(_))),
        "expected a relative fixture object path",
    )?;
    Ok(path)
}

fn source(root: &Path, relative: &Path, item: &Value) -> Result<PathBuf> {
    let path = fs::canonicalize(root.join(relative))?;
    require(
        path.starts_with(root) && path.is_file(),
        "object path escapes fixture directory",
    )?;
    require(
        item["bytes"] == fs::metadata(&path)?.len(),
        "fixture object size changed",
    )?;
    Ok(path)
}

fn retained(root: &Path, files: &mut BTreeMap<(u64, u64), u64>) -> Result<()> {
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            retained(&entry.path(), files)?;
        } else {
            let meta = fs::metadata(entry.path())?;
            if meta.is_file() {
                files.insert(
                    (meta.dev(), meta.ino()),
                    meta.len()
                        .max(meta.blocks().checked_mul(512).ok_or("size overflow")?),
                );
            }
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct Capacity {
    retained_input_bytes: u64,
    copied_input_bytes: u64,
    metadata_reserve_bytes: u64,
    conservative_bytes: u64,
    additional_disk_bytes: u64,
    immutable_hardlinks: bool,
}

impl Capacity {
    fn check(&self, limit: u64, free: u64) -> Result<()> {
        require(
            self.conservative_bytes <= limit && self.additional_disk_bytes <= free,
            "production DV pair exceeds its disk ceiling",
        )
    }
}

fn capacity(
    roots: &[PathBuf],
    transfers: &[(PathBuf, Value)],
    destination_device: u64,
) -> Result<Capacity> {
    let mut files = BTreeMap::new();
    for root in roots {
        retained(root, &mut files)?;
    }
    let resident = files.values().try_fold(0u64, |total, size| {
        total.checked_add(*size).ok_or("size overflow")
    })?;
    let mut copied = 0u64;
    for (root, table) in transfers {
        for item in objects(table)? {
            let meta = fs::metadata(source(root, &relative(table, item)?, item)?)?;
            if meta.dev() != destination_device {
                copied = copied.checked_add(meta.len()).ok_or("size overflow")?;
            }
        }
    }
    let reserve = resident / 4 + 256 * 1024 * 1024;
    let additional = copied.checked_add(reserve).ok_or("size overflow")?;
    Ok(Capacity {
        retained_input_bytes: resident,
        copied_input_bytes: copied,
        metadata_reserve_bytes: reserve,
        conservative_bytes: resident.checked_add(additional).ok_or("size overflow")?,
        additional_disk_bytes: additional,
        immutable_hardlinks: copied == 0,
    })
}

pub(super) fn free_space(path: &Path) -> Result<u64> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut space = MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: path is NUL terminated and space is writable; success initializes it.
    if unsafe { libc::statvfs(path.as_ptr(), space.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    // SAFETY: statvfs succeeded.
    let space = unsafe { space.assume_init() };
    Ok(space
        .f_bavail
        .checked_mul(space.f_frsize)
        .ok_or("size overflow")?)
}

fn copy(root: &Path, table: &Value, output: &Path, remaining: &mut u64) -> Result<()> {
    for item in objects(table)? {
        let relative = relative(table, item)?;
        let path = source(root, &relative, item)?;
        require(digest(&path)? == item["sha256"], "object checksum changed")?;
        let target = output.join(relative);
        fs::create_dir_all(target.parent().ok_or("missing object parent")?)?;
        if let Err(error) = fs::hard_link(&path, &target) {
            if error.raw_os_error() != Some(libc::EXDEV) {
                return Err(error.into());
            }
            let size = fs::metadata(&path)?.len();
            *remaining = remaining
                .checked_sub(size)
                .ok_or("unbudgeted cross-filesystem copy")?;
            let mut input = fs::File::open(&path)?;
            let mut destination = fs::File::create_new(&target)?;
            require(
                std::io::copy(&mut input, &mut destination)? == size,
                "copy size changed",
            )?;
            require(
                digest(&target)? == item["sha256"],
                "copied object checksum changed",
            )?;
        }
    }
    Ok(())
}

pub(super) fn run(request: Request) -> Result<Value> {
    require(
        (1..=192).contains(&request.disk_gib) && request.seconds > 0,
        "finite production pairing ceilings required",
    )?;
    require(
        matches!(
            request.layout.as_deref(),
            None | Some("localized" | "scattered")
        ),
        "unknown pairing layout",
    )?;
    let binary = fs::canonicalize(&request.binary)?;
    let binary_hash = digest(&binary)?;
    let mut roots = Vec::new();
    let mut parents = Vec::new();
    let mut bases = BTreeMap::new();
    let mut original: Option<(PathBuf, Value)> = None;
    let mut modes = BTreeSet::new();
    let mut source_hashes = BTreeSet::new();
    let mut definitions = BTreeMap::new();
    for fixture in &request.fixtures {
        let root = fs::canonicalize(&fixture.path)?;
        let manifest_path = root.join("manifest.json");
        let bytes = fs::read(&manifest_path)?;
        require(
            reader_sha256(bytes.as_slice())? == fixture.manifest_sha256,
            "validated fixture manifest changed",
        )?;
        let manifest: Value = serde_json::from_slice(&bytes)?;
        require(
            manifest["protocol"] == "selective-read-production-fixtures-v1"
                && manifest["status"] == "complete"
                && manifest["writer"]["status"] == "complete"
                && manifest["contract_sha256"] == request.contract_sha256,
            "expected original production fixtures",
        )?;
        modes.insert(text(&manifest["mode"])?.to_owned());
        source_hashes.insert(text(&manifest["source_parent_manifest_sha256"])?.to_owned());
        parents.push(json!({"path":root,"manifest_sha256":fixture.manifest_sha256,"writer":field(&manifest["writer"],"generator")?}));
        for table in array(&manifest["writer"]["tables"])? {
            if request
                .layout
                .as_ref()
                .is_some_and(|layout| table["layout"] != *layout)
            {
                continue;
            }
            require(table["deletion_vectors"] == false, "non-base table")?;
            let id = text(&table["id"])?.to_owned();
            require(
                bases.insert(id, (root.clone(), table.clone())).is_none(),
                "duplicate base table",
            )?;
        }
        let shape = text(&manifest["shape"])?;
        require(matches!(shape, "q2" | "q4"), "unknown production shape")?;
        let definition = field(&manifest, "shape_definition")?;
        require(
            definitions
                .insert(shape.to_owned(), definition.clone())
                .is_none_or(|old| old == *definition),
            "paired layout definitions differ",
        )?;
        let current = array(&manifest["sources"])?
            .first()
            .ok_or("missing source inventory")?;
        require(
            original.as_ref().is_none_or(|(_, value)| value == current),
            "source inventories differ",
        )?;
        original = Some((root.clone(), current.clone()));
        roots.push(root);
    }
    let expected: BTreeSet<_> = definitions
        .keys()
        .flat_map(|shape| {
            ["localized", "scattered"]
                .into_iter()
                .filter(|layout| {
                    request
                        .layout
                        .as_deref()
                        .is_none_or(|selected| selected == *layout)
                })
                .map(move |layout| format!("production.{shape}.{layout}"))
        })
        .collect();
    require(
        !bases.is_empty()
            && bases.keys().cloned().collect::<BTreeSet<_>>() == expected
            && modes.len() == 1
            && source_hashes.len() == 1,
        "pair the requested layouts of each included shape from one source and scope",
    )?;
    let original = original.ok_or("missing source inventory")?;
    let transfers: Vec<_> = std::iter::once(original.clone())
        .chain(bases.values().cloned())
        .collect();
    let parent = request.output.parent().ok_or("missing output parent")?;
    let parent = fs::canonicalize(parent)?;
    let phase = capacity(&roots, &transfers, fs::metadata(&parent)?.dev())?;
    let limit = request.disk_gib * 1024u64.pow(3);
    phase.check(limit, free_space(&parent)?)?;
    fs::create_dir(&request.output)?;
    let output = fs::canonicalize(&request.output)?;
    let mut attempt = serde_json::to_value(&phase)?;
    attempt["status"] = json!("preparing");
    attempt["disk_limit_bytes"] = json!(limit);
    attempt["parents"] = json!(parents);
    storage::save(&output.join("attempt.json"), &attempt)?;
    let mut remaining = phase.copied_input_bytes;
    copy(&original.0, &original.1, &output, &mut remaining)?;
    let mut tables = Vec::new();
    for (case, (root, table)) in &bases {
        copy(root, table, &output, &mut remaining)?;
        let shape = case.split('.').nth(1).ok_or("invalid case ID")?;
        let mut table = table.clone();
        table["queries"] = json!({case:field(&definitions[shape],"canonical_sql")?});
        tables.push(table);
    }
    let mode = modes.first().ok_or("missing fixture mode")?;
    let mut payload = json!({"format":"selective-read-production-pairs-request-v1",
        "contract_sha256":request.contract_sha256,"mode":mode,"tables":tables,"output_limit_bytes":limit});
    if let Some(layout) = &request.layout {
        payload["layout"] = json!(layout);
    }
    let payload_path = output.join("dv-request.json");
    storage::save(&payload_path, &payload)?;
    let payload_hash = digest(&payload_path)?;
    let mut child = Command::new(&binary)
        .arg("production-pairs")
        .arg(&payload_path)
        .arg(&output)
        .arg(request.seconds.to_string())
        .stdout(Stdio::from(fs::File::create_new(
            output.join("writer.stdout.log"),
        )?))
        .stderr(Stdio::from(fs::File::create_new(
            output.join("writer.stderr.log"),
        )?))
        .spawn()?;
    require(
        wait_with_timeout(
            &mut child,
            Duration::from_secs(u64::from(request.seconds) + 30),
        )?
        .success(),
        "DV writer failed; inspect writer.stderr.log",
    )?;
    let result_path = output.join("dv-result.json");
    let result = storage::read_json(&result_path)?;
    require(
        result["status"] == "complete"
            && result["request_sha256"] == payload_hash
            && digest(&payload_path)? == payload_hash
            && result["generator"]["executable_sha256"] == binary_hash
            && digest(&binary)? == binary_hash,
        "DV writer identity changed",
    )?;
    let tables: Vec<_> = array(&result["base_tables"])?
        .iter()
        .chain(array(&result["tables"])?)
        .collect();
    let mut manifest = json!({"protocol":"selective-read-production-pairs-v1","status":"complete","mode":mode,
        "contract_sha256":request.contract_sha256,"source_parent_manifest_sha256":source_hashes.first(),
        "parents":parents,"sources":[original.1],"shape_definitions":definitions,
        "production_dv":{"base_case_ids":bases.keys().collect::<Vec<_>>(),"extra_logical_keys":field(&result,"extra_logical_keys")?},
        "writer":{"status":"complete","generator":result["generator"],"tables":tables},
        "dv_result_sha256":digest(&result_path)?,"driver_sha256":request.driver_sha256,
        "controller_sha256":digest(&std::env::current_exe()?)?,"native_campaign_ready":false,"publication_ready":false});
    if let Some(layout) = &request.layout {
        manifest["production_dv"]["layout"] = json!(layout);
    }
    storage::save(&output.join("manifest.json"), &manifest)?;
    Ok(manifest)
}

#[cfg(test)]
mod tests;
