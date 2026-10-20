use std::{
    collections::BTreeMap,
    env, fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::Command,
};

use clap::Args;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    Result,
    artifacts::{reader_sha256, write_new_file},
    command::{command_output, remove_cargo_package_environment, run_command},
    repository::{cargo_workspace_root, files},
};

const TARGET: &str = "x86_64-unknown-linux-gnu";
const TOOLCHAIN: &str = "+1.98.1";
const RUSTFLAGS: &str = "-C target-cpu=x86-64";
const DELTA_RS_COMMIT: &str = "41f1ce23377f088298a3ed196ad6c8e40cb9bfed";
const RUNNERS: &str = "benches/selective_read/runners";

mod python;

#[derive(Args)]
pub(crate) struct Arguments {
    #[arg(long, value_parser = ["dar", "delta-rs", "duckdb", "polars", "spark"])]
    reader: String,
    /// New directory for the executable, lockfile, and build provenance.
    #[arg(long)]
    output: PathBuf,
    /// CPython 3.14.6 for a Python reader; defaults to python3 on PATH.
    #[arg(long)]
    python: Option<PathBuf>,
    /// Reuse pinned Python reader artifacts without downloading them.
    #[arg(long)]
    artifacts: Option<PathBuf>,
    /// Pinned Temurin JDK to copy into a Spark build.
    #[arg(long, required_if_eq("reader", "spark"))]
    java_home: Option<PathBuf>,
}

#[derive(Deserialize)]
struct Metadata {
    packages: Vec<Package>,
    resolve: Resolve,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    name: String,
    version: String,
    source: Option<String>,
    manifest_path: PathBuf,
}

#[derive(Deserialize)]
struct Resolve {
    nodes: Vec<Node>,
}

#[derive(Deserialize)]
struct Node {
    id: String,
    features: Vec<String>,
}

fn file_identity(metadata: &fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}

pub(crate) fn digest(path: &Path) -> Result<String> {
    let mut file = fs::File::open(path)?;
    let before = file_identity(&file.metadata()?);
    let hash = reader_sha256(&mut file)?;
    if file_identity(&file.metadata()?) != before || file_identity(&fs::metadata(path)?) != before {
        return Err(format!("file changed during checksum: {}", path.display()).into());
    }
    Ok(hash)
}

fn sources(root: &Path) -> Result<BTreeMap<String, String>> {
    let mut paths: Vec<_> = [
        "Cargo.toml",
        "Cargo.lock",
        "README.md",
        ".cargo/config.toml",
        "xtask/Cargo.toml",
        "crates/delta-arrow-reader-core/Cargo.toml",
        "benches/selective_read/oracle.py",
        "benches/selective_read/rust-toolchain.toml",
        "docs/content/benchmarks/selective-read-protocol.md",
        "docs/content/benchmarks/selective-read-large-workloads.md",
        "docs/content/benchmarks/selective-read-sampling.md",
        "docs/content/benchmarks/selective-read-production-workloads.md",
        "docs/content/benchmarks/selective-read-spark-matrix.md",
    ]
    .into_iter()
    .map(|path| root.join(path))
    .collect();
    for base in ["crates/delta-arrow-reader-core/src", RUNNERS, "xtask/src"] {
        paths.extend(files(&root.join(base))?.into_iter().filter(|path| {
            matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("rs" | "toml" | "lock" | "py")
            )
        }));
    }
    paths
        .into_iter()
        .map(|path| {
            Ok((
                path.strip_prefix(root)?
                    .to_str()
                    .ok_or("non-UTF-8 source path")?
                    .to_owned(),
                digest(&path)?,
            ))
        })
        .collect()
}

fn cargo(root: &Path, target_dir: &Path) -> Command {
    let mut command = Command::new("cargo");
    command
        .arg(TOOLCHAIN)
        .current_dir(root)
        .env("RUSTFLAGS", RUSTFLAGS)
        .env("CARGO_INCREMENTAL", "0")
        .env("CARGO_TARGET_DIR", target_dir)
        .env_remove("CARGO_ENCODED_RUSTFLAGS");
    remove_cargo_package_environment(&mut command);
    command
}

impl Arguments {
    pub(crate) fn run(self) -> Result<()> {
        let root = cargo_workspace_root(&env::current_dir()?)?;
        if matches!(self.reader.as_str(), "duckdb" | "polars" | "spark") {
            return python::prepare(&root, self);
        }
        if self.python.is_some() || self.artifacts.is_some() || self.java_home.is_some() {
            return Err("Python reader options require duckdb, polars, or spark".into());
        }
        fs::create_dir(&self.output)?;
        let output = fs::canonicalize(&self.output)?;
        let manifest = root.join(RUNNERS).join(&self.reader).join("Cargo.toml");
        let executable = format!("selective-read-{}", self.reader);
        let target_dir = root
            .join("target/selective-read-runners")
            .join(&self.reader);
        let mut build = cargo(&root, &target_dir);
        build
            .args([
                "build",
                "--release",
                "--locked",
                "--target",
                TARGET,
                "-j8",
                "--manifest-path",
            ])
            .arg(&manifest);
        let command: Vec<_> = std::iter::once(build.get_program())
            .chain(build.get_args())
            .map(|arg| {
                arg.to_str()
                    .ok_or("non-UTF-8 build argument")
                    .map(str::to_owned)
            })
            .collect::<std::result::Result<_, _>>()?;
        let before = sources(&root)?;
        let builder_hash = digest(&env::current_exe()?)?;
        run_command(&mut build)?;
        let metadata = command_output(
            cargo(&root, &target_dir)
                .args([
                    "metadata",
                    "--locked",
                    "--format-version=1",
                    "--filter-platform",
                    TARGET,
                    "--manifest-path",
                ])
                .arg(&manifest),
        )?;
        let tree = command_output(
            cargo(&root, &target_dir)
                .args(["tree", "--locked", "--target", TARGET, "--manifest-path"])
                .arg(&manifest)
                .args(["--edges", "normal,build,features"]),
        )?;
        if sources(&root)? != before {
            return Err("source files changed during build; use a new output directory".into());
        }
        let resolved: Metadata = serde_json::from_str(&metadata)?;
        let mut published_vcs = BTreeMap::<String, Value>::new();
        for package in &resolved.packages {
            if package.name.starts_with("deltalake") {
                let vcs = package.manifest_path.with_file_name(".cargo_vcs_info.json");
                if vcs.try_exists()? {
                    published_vcs.insert(
                        package.name.clone(),
                        serde_json::from_slice(&fs::read(vcs)?)?,
                    );
                }
            }
        }
        if self.reader == "delta-rs"
            && published_vcs
                .get("deltalake")
                .and_then(|vcs| vcs["git"]["sha1"].as_str())
                != Some(DELTA_RS_COMMIT)
        {
            return Err("delta-rs release source differs from the protocol pin".into());
        }
        let nodes: BTreeMap<_, _> = resolved
            .resolve
            .nodes
            .into_iter()
            .map(|node| (node.id, node.features))
            .collect();
        let dependencies: Vec<_> = resolved
            .packages
            .iter()
            .filter_map(|package| {
                nodes.get(&package.id).map(|features| {
                    json!({
                        "name": package.name, "version": package.version,
                        "source": package.source, "features": features,
                    })
                })
            })
            .collect();
        fs::copy(
            target_dir.join(TARGET).join("release").join(&executable),
            output.join(&executable),
        )?;
        fs::copy(
            manifest.with_file_name("Cargo.lock"),
            output.join("Cargo.lock"),
        )?;
        write_new_file(&output.join("metadata.json"), metadata.as_bytes())?;
        write_new_file(&output.join("cargo-tree.txt"), tree.as_bytes())?;
        let commit = command_output(
            Command::new("git")
                .current_dir(&root)
                .args(["rev-parse", "HEAD"]),
        )?;
        let linker = command_output(Command::new("cc").arg("--version"))?;
        let record = json!({
            "format": "selective-read-build-v1",
            "reader_id": if self.reader == "dar" { "delta-arrow-reader" } else { "delta-rs" },
            "harness_git_commit": commit.trim(),
            "reader_git_commit": if self.reader == "dar" { commit.trim() } else { DELTA_RS_COMMIT },
            "published_vcs": published_vcs,
            "git_status": command_output(Command::new("git").current_dir(&root).args(["status", "--porcelain"]))?,
            "source_sha256": before,
            "builder_executable_sha256": builder_hash,
            "command": command,
            "rustflags": RUSTFLAGS,
            "target": TARGET,
            "rustc": command_output(Command::new("rustc").args([TOOLCHAIN, "-vV"]))?,
            "cargo": command_output(cargo(&root, &target_dir).arg("--version"))?.trim(),
            "linker": linker.lines().next().ok_or("missing linker version")?,
            "executable_sha256": digest(&output.join(&executable))?,
            "lockfile_sha256": digest(&output.join("Cargo.lock"))?,
            "metadata_sha256": digest(&output.join("metadata.json"))?,
            "cargo_tree_sha256": digest(&output.join("cargo-tree.txt"))?,
            "dependencies": dependencies,
        });
        write_new_file(
            &output.join("build.json"),
            (serde_json::to_string_pretty(&record)? + "\n").as_bytes(),
        )?;
        println!("{}", output.join(executable).display());
        Ok(())
    }
}
