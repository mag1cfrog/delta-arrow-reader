use super::*;
use flate2::read::GzDecoder;
use std::{io, os::unix::fs::PermissionsExt, path::Component};

fn text(value: &Value) -> Result<&str> {
    value
        .as_str()
        .ok_or_else(|| "invalid Python reader lock string".into())
}

fn items<'a>(lock: &'a Value, key: &str) -> Result<&'a [Value]> {
    match lock.get(key) {
        None => Ok(&[]),
        Some(value) => value
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| "invalid Python reader artifact list".into()),
    }
}

fn name(value: &Value) -> Result<&str> {
    let value = text(value)?;
    if value.is_empty()
        || !matches!(
            Path::new(value).components().collect::<Vec<_>>().as_slice(),
            [Component::Normal(_)]
        )
    {
        return Err("artifact names must be single filenames".into());
    }
    Ok(value)
}

fn unpack_spark(archive: &Path, output: &Path, prefix: &str) -> Result<()> {
    fs::create_dir(output)?;
    let mut archive = tar::Archive::new(GzDecoder::new(fs::File::open(archive)?));
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let kind = entry.header().entry_type();
        if !(kind.is_file() || kind.is_dir())
            || !path.starts_with(prefix)
            || !path
                .components()
                .all(|part| matches!(part, Component::Normal(_)))
            || !entry.unpack_in(output)?
        {
            return Err("unexpected Spark archive member".into());
        }
    }
    Ok(())
}

pub(super) fn prepare(root: &Path, args: Arguments) -> Result<()> {
    let reader = &args.reader;
    let spark = reader == "spark";
    if !spark && args.java_home.is_some() {
        return Err("--java-home requires the Spark reader".into());
    }
    let adapter = root.join(RUNNERS).join(reader);
    let hashes = || -> Result<BTreeMap<String, String>> {
        let mut hashes = sources(root)?;
        hashes.insert(
            format!("{RUNNERS}/{reader}/lock.json"),
            digest(&adapter.join("lock.json"))?,
        );
        Ok(hashes)
    };
    let before = hashes()?;
    let lock: Value = serde_json::from_slice(&fs::read(adapter.join("lock.json"))?)?;
    let python = args.python.as_deref().unwrap_or(Path::new("python3"));
    let platform: Value = serde_json::from_str(&command_output(Command::new(python).args([
        "-I", "-c",
        "import json, platform, sys; print(json.dumps([platform.python_implementation(), platform.python_version(), sys.platform, platform.machine()]))",
    ]))?)?;
    if platform != json!(["CPython", text(&lock["python"])?, "linux", "x86_64"]) {
        return Err("use CPython 3.14.6 on Linux x86-64".into());
    }
    if spark {
        let release = fs::read_to_string(
            args.java_home
                .as_ref()
                .ok_or("Spark requires --java-home")?
                .join("release"),
        )?;
        for (field, key) in [
            ("JAVA_RUNTIME_VERSION", "java_runtime_version"),
            ("IMPLEMENTOR", "java_implementor"),
        ] {
            let expected = format!("{field}=\"{}\"", text(&lock[key])?);
            if !release.lines().any(|line| line == expected) {
                return Err("use the pinned Temurin JDK".into());
            }
        }
    }
    let bundled: BTreeMap<String, PathBuf> = [
        ("lock.json", adapter.join("lock.json")),
        ("run.py", root.join(RUNNERS).join("run.py")),
        (
            "python_common.py",
            root.join(RUNNERS).join("python_common.py"),
        ),
    ]
    .into_iter()
    .map(|(name, path)| (name.to_owned(), path))
    .collect();
    let builder_hash = digest(&env::current_exe()?)?;
    fs::create_dir(&args.output)?;
    let output = fs::canonicalize(&args.output)?;
    let downloads = output.join("artifacts");
    fs::create_dir(&downloads)?;
    let mut artifacts: Vec<_> = if spark {
        vec![&lock["spark_distribution"]]
    } else {
        Vec::new()
    };
    for key in if spark {
        ["wheels", "jars", "source_artifacts"]
    } else {
        ["wheels", "extensions", "source_artifacts"]
    } {
        artifacts.extend(items(&lock, key)?);
    }
    for item in artifacts {
        let filename = name(&item["filename"])?;
        let dest = downloads.join(filename);
        let mut target = fs::File::create_new(&dest)?;
        if let Some(saved) = &args.artifacts {
            io::copy(&mut fs::File::open(saved.join(filename))?, &mut target)?;
        } else {
            run_command(
                Command::new("curl")
                    .args([
                        "--disable",
                        "--fail",
                        "--silent",
                        "--show-error",
                        "--location",
                        "--connect-timeout",
                        "60",
                        "--speed-limit",
                        "1",
                        "--speed-time",
                        "60",
                        "--user-agent",
                        "selective-read-benchmark/1",
                    ])
                    .arg("--output")
                    .arg(&dest)
                    .arg("--")
                    .arg(text(&item["url"])?),
            )?;
        }
        if digest(&dest)? != item["sha256"] {
            return Err(format!("artifact checksum mismatch: {filename}").into());
        }
    }
    if spark {
        let prefix = format!("pyspark-{}", text(&lock["spark_version"])?);
        let unpack = output.join("unpack");
        unpack_spark(
            &downloads.join(name(&lock["spark_distribution"]["filename"])?),
            &unpack,
            &prefix,
        )?;
        fs::rename(unpack.join(prefix), output.join("spark"))?;
        fs::remove_dir(unpack)?;
        for entry in fs::read_dir(output.join("spark"))? {
            let path = entry?.path();
            if path.extension().is_some_and(|value| value == "egg-info") {
                fs::remove_dir_all(path)?;
            }
        }
        run_command(
            Command::new("cp")
                .args(["-a", "--"])
                .arg(args.java_home.as_ref().ok_or("missing JDK")?)
                .arg(output.join("java")),
        )?;
        fs::create_dir(output.join("jars"))?;
        for item in items(&lock, "jars")? {
            let filename = name(&item["filename"])?;
            fs::copy(downloads.join(filename), output.join("jars").join(filename))?;
        }
        for name in ["conf", "spill"] {
            fs::create_dir(output.join(name))?;
        }
    }
    let requirements: String = items(&lock, "wheels")?
        .iter()
        .map(|wheel| {
            Ok(format!(
                "{}=={} --hash=sha256:{}\n",
                text(&wheel["name"])?,
                text(&wheel["version"])?,
                text(&wheel["sha256"])?
            ))
        })
        .collect::<Result<_>>()?;
    let requirements_path = output.join("requirements.lock");
    write_new_file(&requirements_path, requirements.as_bytes())?;
    run_command(
        Command::new(python)
            .args(["-I", "-m", "venv", "--without-pip", "--copies"])
            .arg(output.join("venv")),
    )?;
    let python = output.join("venv/bin/python");
    let mut command: Vec<String> = vec![
        "uv".into(),
        "pip".into(),
        "sync".into(),
        "--python".into(),
        python.to_string_lossy().into_owned(),
        "--no-index".into(),
        "--find-links".into(),
        downloads.to_string_lossy().into_owned(),
        "--require-hashes".into(),
    ];
    if spark {
        command.extend(["--no-cache".into(), "--link-mode=copy".into()]);
    }
    command.push(requirements_path.to_string_lossy().into_owned());
    run_command(Command::new(&command[0]).args(&command[1..]))?;
    for item in items(&lock, "extensions")? {
        let dest = output.join(format!("{}.duckdb_extension", name(&item["name"])?));
        let mut source = GzDecoder::new(fs::File::open(downloads.join(name(&item["filename"])?))?);
        io::copy(&mut source, &mut fs::File::create_new(&dest)?)?;
        if digest(&dest)? != item["binary_sha256"] {
            return Err(format!("extension checksum mismatch: {}", dest.display()).into());
        }
    }
    for (name, source) in &bundled {
        fs::copy(source, output.join(name))?;
    }
    let executable = output.join(format!("selective-read-{reader}"));
    write_new_file(
        &executable,
        format!(
            "#!{} {}\n{}",
            python.display(),
            if spark { "-IB" } else { "-I" },
            fs::read_to_string(adapter.join("runner.py"))?
        )
        .as_bytes(),
    )?;
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755))?;
    let runtime: Value = serde_json::from_str(&command_output(
        Command::new(&executable).arg("--describe-build"),
    )?)?;
    if hashes()? != before {
        return Err("source files changed during preparation; use a new output directory".into());
    }
    let mut bundled_hashes = BTreeMap::new();
    for name in bundled
        .keys()
        .filter(|name| spark || name.as_str() != "lock.json")
    {
        bundled_hashes.insert(name, digest(&output.join(name))?);
    }
    let mut record = json!({"format":"selective-read-build-v1","reader_id":reader,
        "harness_git_commit":command_output(Command::new("git").args(["rev-parse","HEAD"]).current_dir(root))?.trim(),
        "reader_git_commit":text(&lock[format!("{reader}_source")])?,
        "git_status":command_output(Command::new("git").args(["status","--porcelain"]).current_dir(root))?,
        "source_sha256":before,"command":command,"uv_version":command_output(Command::new("uv").arg("--version"))?.trim(),
        "executable_sha256":digest(&executable)?,"lockfile_sha256":digest(&adapter.join("lock.json"))?,
        "bundled_sha256":bundled_hashes,"runtime":runtime,"builder_executable_sha256":builder_hash});
    if spark {
        record["pilot_only"] = json!(false);
        record["delta_git_commit"] = json!(text(&lock["delta_source"])?);
    }
    write_new_file(
        &output.join("build.json"),
        &serde_json::to_vec_pretty(&record)?,
    )?;
    println!("{}", executable.display());
    Ok(())
}
