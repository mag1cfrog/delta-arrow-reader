use super::*;
use flate2::{Compression, write::GzEncoder};
use std::io::Write;

fn fixture(reader: &str) -> Result<tempfile::TempDir> {
    let temporary = super::fixture()?;
    let root = temporary.path();
    for path in [
        "run.py",
        "python_common.py",
        "native.py",
        "check.py",
        "capabilities.py",
    ] {
        write(
            root,
            &format!("benches/selective_read/runners/{path}"),
            "# fixture\n",
        )?;
    }
    write(
        root,
        "benches/selective_read/runners/native.py",
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../benches/selective_read/runners/native.py"
        )),
    )?;
    write(
        root,
        &format!("benches/selective_read/runners/{reader}/runner.py"),
        "# adapter\n",
    )?;
    write(root, "saved/package.whl", "pinned wheel")?;
    let wheel = json!({"name":"package","version":"1","filename":"package.whl",
        "url":"https://invalid.example/wheel","sha256":hash(&root.join("saved/package.whl"))?});
    let mut lock = json!({"python":"3.14.6","wheels":[wheel]});
    lock[format!("{reader}_source")] = json!("reader source");
    if reader == "duckdb" {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(b"pinned extension")?;
        fs::write(root.join("saved/extension.gz"), encoder.finish()?)?;
        write(root, "extension", "pinned extension")?;
        lock["extensions"] = json!([{"name":"extension","filename":"extension.gz",
            "sha256":hash(&root.join("saved/extension.gz"))?,"binary_sha256":hash(&root.join("extension"))?}]);
    }
    if reader == "spark" {
        let mut archive = tar::Builder::new(GzEncoder::new(Vec::new(), Compression::default()));
        for name in [
            "pyspark-4.1.1/pyspark/__init__.py",
            "pyspark-4.1.1/pyspark.egg-info/PKG-INFO",
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o644);
            header.set_size(4);
            header.set_cksum();
            archive.append_data(&mut header, name, b"data".as_slice())?;
        }
        fs::write(
            root.join("saved/spark.tar.gz"),
            archive.into_inner()?.finish()?,
        )?;
        lock["spark_distribution"] =
            json!({"filename":"spark.tar.gz","sha256":hash(&root.join("saved/spark.tar.gz"))?});
        lock["spark_version"] = json!("4.1.1");
        lock["delta_source"] = json!("delta source");
        lock["java_runtime_version"] = json!("21.0.8+9-LTS");
        lock["java_implementor"] = json!("Eclipse Adoptium");
        write(
            root,
            "java/release",
            "JAVA_RUNTIME_VERSION=\"21.0.8+9-LTS\"\nIMPLEMENTOR=\"Eclipse Adoptium\"\n",
        )?;
        write(root, "java/lib/java", "JDK")?;
        fs::create_dir(root.join("java/bin"))?;
        std::os::unix::fs::symlink("../lib/java", root.join("java/bin/java"))?;
        write(root, "saved/delta.jar", "jar")?;
        lock["jars"] =
            json!([{"filename":"delta.jar","sha256":hash(&root.join("saved/delta.jar"))?}]);
    }
    write(
        root,
        &format!("benches/selective_read/runners/{reader}/lock.json"),
        &lock.to_string(),
    )?;
    for (name, body) in [
        (
            "python",
            r#"
if [ "$2" = -c ]; then
    if [ "${MOCK_EARLY_MUTATE-no}" = yes ]; then printf 'changed\n' >> README.md; fi
    printf '["CPython","%s","linux","x86_64"]\n' "${MOCK_VERSION-3.14.6}"
elif [ "$2" = -m ]; then
    test "$3 $4 $5" = 'venv --without-pip --copies'
    mkdir -p "$6/bin"
    cp "$0" "$6/bin/python"
else
    test "$3" = --describe-build
    printf '{"packages":{"package":{"version":"1"}}}\n'
fi
"#,
        ),
        (
            "uv",
            r#"
if [ "$1" = --version ]; then
    printf 'uv fixture\n'
else
    test "$1 $2" = 'pip sync'
    printf '%s\n' "$*" > uv-command
    if [ "${MOCK_MUTATE-no}" = yes ]; then printf 'changed\n' >> README.md; fi
fi
"#,
        ),
    ] {
        write(
            root,
            &format!("bin/{name}"),
            &format!("#!/bin/sh\nset -eu\n{body}"),
        )?;
        fs::set_permissions(
            root.join("bin").join(name),
            fs::Permissions::from_mode(0o755),
        )?;
    }
    Ok(temporary)
}

fn run(root: &Path, reader: &str, extra: &[(&str, &str)]) -> Result<Output> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .current_dir(root)
        .args([
            "build-selective-read",
            "--reader",
            reader,
            "--output",
            "output",
            "--python",
            "bin/python",
            "--artifacts",
            "saved",
        ])
        .env(
            "PATH",
            format!("{}:/usr/bin:/bin", root.join("bin").display()),
        )
        .env("CARGO", root.join("bin/cargo"))
        .envs(extra.iter().copied());
    if reader == "spark" {
        command.args(["--java-home", "java"]);
    }
    Ok(command.output()?)
}

#[test]
fn prepares_all_python_readers_offline_with_frozen_artifacts_and_build_provenance() -> Result {
    for reader in ["duckdb", "polars", "spark"] {
        let temporary = fixture(reader)?;
        let root = temporary.path();
        let result = run(root, reader, &[])?;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let output = root.join("output");
        let executable = output.join(format!("selective-read-{reader}"));
        let record: Value = serde_json::from_slice(&fs::read(output.join("build.json"))?)?;
        assert_eq!(record["reader_id"], reader);
        assert_eq!(
            record["bundled_sha256"]["selective-read-controller"],
            hash(Path::new(env!("CARGO_BIN_EXE_xtask")))?
        );
        assert_eq!(record["runtime"]["packages"]["package"]["version"], "1");
        assert_eq!(record["executable_sha256"], hash(&executable)?);
        assert_eq!(
            record["builder_executable_sha256"],
            hash(Path::new(env!("CARGO_BIN_EXE_xtask")))?
        );
        assert_eq!(record["lockfile_sha256"], hash(&output.join("lock.json"))?);
        assert_eq!(record["reader_git_commit"], "reader source");
        let command = record["command"].as_array().unwrap();
        assert!(
            command.contains(&json!("--require-hashes")) && command.contains(&json!("--no-index"))
        );
        assert_eq!(command.contains(&json!("--no-cache")), reader == "spark");
        for (name, checksum) in record["bundled_sha256"].as_object().unwrap() {
            assert_eq!(*checksum, hash(&output.join(name))?);
        }
        for name in [
            "oracle.py",
            "protocol.md",
            "large-workloads.md",
            "sampling.md",
            "production-workloads.md",
            "spark-matrix.md",
        ] {
            assert!(!output.join(name).exists());
            assert!(record["bundled_sha256"].get(name).is_none());
        }
        let probe = Command::new("python3").args(["-I", "-c",
            "import json,sys; sys.path.insert(0,sys.argv[1]); import native; print(json.dumps(native.call('reader-contract', {'action':'comparison','value':{'comparison_revision':2,'protocol_sha256':'old docs'}})))"])
            .arg(&output).current_dir(root).env("SELECTIVE_READ_XTASK", "/missing/controller").output()?;
        assert!(
            probe.status.success(),
            "{}",
            String::from_utf8_lossy(&probe.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&probe.stdout)?,
            json!({"comparison_revision":2,"protocol_sha256":"old docs"})
        );
        for (name, checksum) in record["source_sha256"].as_object().unwrap() {
            assert_eq!(*checksum, hash(&root.join(name))?);
        }
        let shebang = fs::read_to_string(&executable)?;
        assert!(shebang.starts_with(&format!(
            "#!{} {}\n",
            output.join("venv/bin/python").display(),
            if reader == "spark" { "-IB" } else { "-I" }
        )));
        if reader == "duckdb" {
            assert_eq!(
                fs::read(output.join("extension.duckdb_extension"))?,
                b"pinned extension"
            );
        }
        if reader == "spark" {
            assert!(!output.join("spark/pyspark.egg-info").exists());
            assert!(output.join("spark/pyspark/__init__.py").is_file());
            assert_eq!(fs::read(output.join("jars/delta.jar"))?, b"jar");
            assert!(output.join("java/bin/java").is_symlink());
            assert_eq!(fs::read(output.join("java/bin/java"))?, b"JDK");
            assert_eq!(record["pilot_only"], false);
            assert_eq!(record["delta_git_commit"], "delta source");
            assert!(record["bundled_sha256"].get("lock.json").is_some());
        }
        assert!(!run(root, reader, &[])?.status.success());
        assert_eq!(fs::read_to_string(executable)?, shebang);
    }
    Ok(())
}

#[test]
fn rejects_changed_artifacts_interpreters_jdks_extensions_and_sources() -> Result {
    for failure in [
        "wheel",
        "extension",
        "python",
        "java",
        "sources",
        "early-sources",
        "path",
        "archive",
    ] {
        let reader = if matches!(failure, "java" | "archive") {
            "spark"
        } else {
            "duckdb"
        };
        let temporary = fixture(reader)?;
        let root = temporary.path();
        let mut extra = Vec::new();
        match failure {
            "wheel" => write(root, "saved/package.whl", "changed")?,
            "python" => extra.push(("MOCK_VERSION", "3.13.0")),
            "java" => write(root, "java/release", "wrong JDK")?,
            "sources" => extra.push(("MOCK_MUTATE", "yes")),
            "early-sources" => extra.push(("MOCK_EARLY_MUTATE", "yes")),
            field => {
                let path = root.join(format!("benches/selective_read/runners/{reader}/lock.json"));
                let mut lock: Value = serde_json::from_slice(&fs::read(&path)?)?;
                if field == "archive" {
                    let mut archive =
                        tar::Builder::new(GzEncoder::new(Vec::new(), Compression::default()));
                    let mut header = tar::Header::new_gnu();
                    header.set_mode(0o777);
                    header.set_size(0);
                    header.set_entry_type(tar::EntryType::Symlink);
                    header.set_link_name("../../outside")?;
                    header.set_cksum();
                    archive.append_data(&mut header, "pyspark-4.1.1/link", std::io::empty())?;
                    fs::write(
                        root.join("saved/spark.tar.gz"),
                        archive.into_inner()?.finish()?,
                    )?;
                    lock["spark_distribution"]["sha256"] =
                        json!(hash(&root.join("saved/spark.tar.gz"))?);
                } else if field == "extension" {
                    lock["extensions"][0]["binary_sha256"] = json!("wrong");
                } else {
                    lock["wheels"][0]["filename"] = json!("../escape");
                }
                fs::write(path, serde_json::to_vec(&lock)?)?;
            }
        }
        let result = run(root, reader, &extra)?;
        assert!(!result.status.success(), "{failure}");
        assert!(!root.join("output/build.json").exists(), "{failure}");
        if matches!(failure, "python" | "java") {
            assert!(!root.join("output").exists(), "{failure}");
        }
    }
    Ok(())
}
