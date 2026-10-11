use super::*;
use serde_json::{Value, json};

type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Fixture {
    root: tempfile::TempDir,
    identity: BuildIdentity,
    dependencies: Vec<String>,
    inputs: PathBuf,
    output: PathBuf,
    notices: PathBuf,
}

impl Fixture {
    fn new() -> TestResult<Self> {
        let root = tempfile::tempdir()?;
        let inputs = root.path().join("inputs");
        let output = root.path().join("output");
        let notices = root.path().join("notices.txt");
        fs::write(&notices, "Test dependency license and copyright.\n")?;
        let identity = BuildIdentity {
            version: "1.2.3".into(),
            source_commit: "a".repeat(40),
            rust_toolchain: "1.94.0".into(),
        };
        let dependencies = vec!["libc.so.6".into(), "libgcc_s.so.1".into()];
        let fixture = Self {
            root,
            identity,
            dependencies,
            inputs,
            output,
            notices,
        };
        for architecture in Architecture::ALL {
            let binary = fixture.binary(architecture);
            fs::write(&binary, format!("test {} binary\n", architecture.name()))?;
            package(
                &binary,
                &fixture.notices,
                &fixture.inputs.join(architecture.name()),
                &fixture.identity,
                architecture,
                &fixture.dependencies,
            )?;
        }
        Ok(fixture)
    }

    fn binary(&self, architecture: Architecture) -> PathBuf {
        self.root
            .path()
            .join(format!("dar-{}", architecture.name()))
    }

    fn archive(&self) -> PathBuf {
        self.inputs
            .join("amd64")
            .join(archive_name(&self.identity.version, Architecture::Amd64))
    }

    fn describe(&self) -> Result<ArtifactTarget> {
        describe_archive(
            &self.inputs.join("amd64"),
            &self.identity,
            Architecture::Amd64,
            &self.dependencies,
        )
    }

    fn assemble(&self) -> Result<ArtifactManifest> {
        assemble(&self.inputs, &self.output, &self.identity)
    }
}

#[test]
fn complete_set_preserves_bytes_and_matches_checksums() -> TestResult {
    let fixture = Fixture::new()?;
    let manifest = fixture.assemble()?;
    verify(&fixture.output, &fixture.identity)?;
    assert_eq!(manifest.schema_version, 1);
    assert_eq!(manifest.version, fixture.identity.version);
    assert_eq!(manifest.source_commit, fixture.identity.source_commit);
    assert_eq!(manifest.rust_toolchain, fixture.identity.rust_toolchain);
    assert_eq!(
        manifest
            .targets
            .iter()
            .map(|target| target.architecture)
            .collect::<Vec<_>>(),
        Architecture::ALL
    );
    for target in manifest.targets {
        assert_eq!(target.os, "linux");
        assert_eq!(target.rust_target, target.architecture.rust_target());
        assert_eq!(target.libc, "glibc");
        assert_eq!(target.min_glibc, "2.36");
        assert_eq!(target.runtime_dependencies, fixture.dependencies);
        assert_eq!(
            target.archive,
            archive_name(&fixture.identity.version, target.architecture)
        );
        let path = fixture.output.join(&target.archive);
        assert_eq!(
            fs::read(&path)?,
            fs::read(
                fixture
                    .inputs
                    .join(target.architecture.name())
                    .join(&target.archive)
            )?
        );
        assert_eq!(file_sha256(&path)?, target.archive_sha256);
        let mut archive = Archive::new(MultiGzDecoder::new(File::open(path)?));
        let mut members = archive.entries()?;
        for (name, mode) in MEMBERS {
            let mut member = members.next().ok_or("missing member")??;
            assert_eq!(member.path_bytes(), name.as_bytes());
            assert_eq!(member.header().mode()?, mode);
            assert_eq!(member.header().mtime()?, 0);
            assert_eq!(member.header().uid()?, 0);
            assert_eq!(member.header().gid()?, 0);
            let mut data = Vec::new();
            member.read_to_end(&mut data)?;
            let expected = match name {
                "dar" => fs::read(fixture.binary(target.architecture))?,
                "LICENSE" => LICENSE.to_vec(),
                _ => [NOTICE, b"\n", &fs::read(&fixture.notices)?].concat(),
            };
            assert_eq!(data, expected);
        }
        assert!(members.next().is_none());
        assert_eq!(
            target.dar_sha256,
            file_sha256(&fixture.binary(target.architecture))?
        );
    }
    #[cfg(unix)]
    {
        let result = std::process::Command::new("sha256sum")
            .args(["--check", "SHA256SUMS"])
            .current_dir(&fixture.output)
            .output()?;
        assert!(result.status.success(), "{:?}", result);
    }
    Ok(())
}

#[test]
fn archives_are_reproducible_and_ignore_input_permissions_and_timestamps() -> TestResult {
    let fixture = Fixture::new()?;
    let binary = fixture.binary(Architecture::Amd64);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&binary, fs::Permissions::from_mode(0o700))?;
    }
    File::options()
        .write(true)
        .open(&binary)?
        .set_times(fs::FileTimes::new().set_modified(
            std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(123456),
        ))?;
    let other = fixture.root.path().join("other");
    package(
        &binary,
        &fixture.notices,
        &other,
        &fixture.identity,
        Architecture::Amd64,
        &fixture.dependencies,
    )?;
    for entry in fs::read_dir(&other)? {
        let entry = entry?;
        assert_eq!(
            fs::read(entry.path())?,
            fs::read(fixture.inputs.join("amd64").join(entry.file_name()))?
        );
    }
    Ok(())
}

#[test]
fn binary_larger_than_io_buffers_preserves_its_hash() -> TestResult {
    let fixture = Fixture::new()?;
    let payload: Vec<u8> = (0..=255).cycle().take(256 * 8193).collect();
    let binary = fixture.root.path().join("large-dar");
    fs::write(&binary, &payload)?;
    let manifest = package(
        &binary,
        &fixture.notices,
        &fixture.root.path().join("large-package"),
        &fixture.identity,
        Architecture::Amd64,
        &fixture.dependencies,
    )?;
    assert_eq!(
        manifest.targets[0].dar_sha256,
        format!("{:x}", Sha256::digest(&payload))
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn extraction_checks_identity_preserves_contents_and_never_overwrites() -> TestResult {
    use std::os::unix::fs::PermissionsExt;
    let mut fixture = Fixture::new()?;
    for architecture in Architecture::ALL {
        let destination = fixture
            .root
            .path()
            .join(format!("extracted-{}", architecture.name()));
        let source = fixture.inputs.join(architecture.name());
        fixture.identity.source_commit = "b".repeat(40);
        assert!(extract(&source, &destination, &fixture.identity, architecture).is_err());
        assert!(!destination.exists());
        fixture.identity.source_commit = "a".repeat(40);
        extract(&source, &destination, &fixture.identity, architecture)?;
        check_files(&destination, &MEMBERS.map(|(name, _)| name.into()))?;
        assert_eq!(
            fs::read(destination.join("dar"))?,
            fs::read(fixture.binary(architecture))?
        );
        assert_eq!(fs::read(destination.join("LICENSE"))?, LICENSE);
        assert_eq!(
            fs::read(destination.join("NOTICE"))?,
            [NOTICE, b"\n", &fs::read(&fixture.notices)?].concat()
        );
        for (name, mode) in MEMBERS {
            assert_eq!(
                fs::metadata(destination.join(name))?.permissions().mode() & 0o7777,
                mode
            );
        }
        assert!(extract(&source, &destination, &fixture.identity, architecture).is_err());
    }
    Ok(())
}

#[test]
fn outputs_are_never_overwritten() -> TestResult {
    let fixture = Fixture::new()?;
    fixture.assemble()?;
    let original = fs::read(fixture.output.join(MANIFEST))?;
    assert!(fixture.assemble().is_err());
    assert_eq!(fs::read(fixture.output.join(MANIFEST))?, original);
    let directory = fixture.inputs.join("amd64");
    let original = fs::read(directory.join(MANIFEST))?;
    assert!(
        package(
            &fixture.binary(Architecture::Amd64),
            &fixture.notices,
            &directory,
            &fixture.identity,
            Architecture::Amd64,
            &fixture.dependencies
        )
        .is_err()
    );
    assert_eq!(fs::read(directory.join(MANIFEST))?, original);
    Ok(())
}

#[test]
fn missing_or_extra_architecture_fails_before_creating_output() -> TestResult {
    let fixture = Fixture::new()?;
    let arm64 = fixture.inputs.join("arm64");
    let saved = fixture.root.path().join("saved");
    fs::rename(&arm64, &saved)?;
    assert!(fixture.assemble().is_err());
    fs::rename(&saved, &arm64)?;
    fs::create_dir(fixture.inputs.join("extra"))?;
    assert!(fixture.assemble().is_err());
    assert!(!fixture.output.exists());
    Ok(())
}

#[test]
fn manifest_rejects_changed_identity_metadata_and_dependency_shapes() -> TestResult {
    let fixture = Fixture::new()?;
    let path = fixture.inputs.join("amd64").join(MANIFEST);
    let original: Value = serde_json::from_slice(&fs::read(&path)?)?;
    for (pointer, value) in [
        ("/schema_version", json!(true)),
        ("/schema_version", json!(1.0)),
        ("/source_commit", json!("b".repeat(40))),
        ("/version", json!("1.2.4")),
        ("/rust_toolchain", json!("1.95.0")),
        ("/targets/0/architecture", json!("arm64")),
        ("/targets/0/rust_target", json!("aarch64-unknown-linux-gnu")),
        ("/targets/0/min_glibc", json!("2.39")),
        ("/targets/0/archive", json!("../dar.tar.gz")),
        ("/targets/0/archive_sha256", json!("0".repeat(64))),
        ("/targets/0/dar_sha256", json!("0".repeat(64))),
        ("/targets/0/runtime_dependencies", json!([])),
        ("/targets/0/runtime_dependencies", json!("libc.so.6")),
        (
            "/targets/0/runtime_dependencies",
            json!(["libc.so.6", "libc.so.6"]),
        ),
        (
            "/targets/0/runtime_dependencies",
            json!(["libgcc_s.so.1", "libc.so.6"]),
        ),
        ("/targets/0/runtime_dependencies", json!([null])),
        ("/targets/0/runtime_dependencies", json!(["../libc.so.6"])),
    ] {
        let mut manifest = original.clone();
        *manifest
            .pointer_mut(pointer)
            .ok_or("missing manifest field")? = value;
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        assert!(
            fixture.assemble().is_err(),
            "accepted {pointer}: {manifest}"
        );
        assert!(!fixture.output.exists());
    }
    for target in [false, true] {
        let mut manifest = original.clone();
        let object = if target {
            &mut manifest["targets"][0]
        } else {
            &mut manifest
        };
        object["extra"] = json!("unexpected");
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        assert!(fixture.assemble().is_err());
    }
    Ok(())
}

#[test]
fn manifest_rejects_malformed_shapes_and_duplicate_fields() -> TestResult {
    let fixture = Fixture::new()?;
    let path = fixture.inputs.join("amd64").join(MANIFEST);
    let original = fs::read_to_string(&path)?;
    for text in [
        "{".into(),
        "[]".into(),
        "null".into(),
        "{}".into(),
        original.replace(
            "\"schema_version\": 1",
            "\"schema_version\": 1, \"schema_version\": 1",
        ),
        original.replace("\"os\": \"linux\"", "\"os\": \"linux\", \"os\": \"linux\""),
    ] {
        fs::write(&path, text)?;
        assert!(fixture.assemble().is_err());
    }
    for targets in [json!([]), json!({}), json!([null])] {
        let mut manifest: Value = serde_json::from_str(&original)?;
        manifest["targets"] = targets;
        fs::write(&path, serde_json::to_vec(&manifest)?)?;
        assert!(fixture.assemble().is_err());
    }
    Ok(())
}

#[test]
fn incomplete_or_unexpected_public_files_are_rejected() -> TestResult {
    let fixture = Fixture::new()?;
    fixture.assemble()?;
    for entry in fs::read_dir(&fixture.output)? {
        let entry = entry?;
        let saved = fixture.root.path().join("saved");
        fs::rename(entry.path(), &saved)?;
        assert!(verify(&fixture.output, &fixture.identity).is_err());
        fs::rename(&saved, entry.path())?;
    }
    fs::write(fixture.output.join("debug-symbols"), "extra")?;
    assert!(verify(&fixture.output, &fixture.identity).is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn symlink_artifacts_and_directories_are_rejected() -> TestResult {
    let fixture = Fixture::new()?;
    for path in [
        fixture.inputs.join("amd64").join(MANIFEST),
        fixture.archive(),
        fixture.inputs.join("amd64"),
        fixture.inputs.clone(),
    ] {
        let saved = fixture.root.path().join("saved");
        fs::rename(&path, &saved)?;
        std::os::unix::fs::symlink(&saved, &path)?;
        assert!(fixture.assemble().is_err());
        assert!(!fixture.output.exists());
        fs::remove_file(&path)?;
        fs::rename(&saved, &path)?;
    }
    Ok(())
}

fn compress(data: &[u8]) -> io::Result<Vec<u8>> {
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::fast());
    encoder.write_all(data)?;
    encoder.finish()
}

#[test]
fn gzip_footers_tar_end_blocks_and_hidden_payloads_are_checked() -> TestResult {
    let fixture = Fixture::new()?;
    let path = fixture.archive();
    let original = fs::read(&path)?;
    let mut bad_crc = original.clone();
    bad_crc[original.len() - 8] ^= 1;
    let mut bad_size = original.clone();
    bad_size[original.len() - 4] ^= 1;
    let mut uncompressed = Vec::new();
    MultiGzDecoder::new(&original[..]).read_to_end(&mut uncompressed)?;
    let mut archive = Archive::new(&uncompressed[..]);
    let mut content_end = 0;
    for member in archive.entries()? {
        let member = member?;
        content_end = member.raw_file_position() + member.size();
    }
    for data in [
        b"not gzip".to_vec(),
        original[..original.len() / 2].to_vec(),
        bad_crc,
        bad_size,
        original[..original.len() - 8].to_vec(),
        [&original[..], b"extra"].concat(),
        [&original[..], &compress(b"hidden tar payload")?].concat(),
        compress(&uncompressed[..content_end as usize])?,
        compress(&uncompressed[..uncompressed.len() - 1])?,
    ] {
        fs::write(&path, data)?;
        assert!(fixture.describe().is_err());
    }
    Ok(())
}

#[test]
fn checksum_file_must_match_the_exact_complete_set() -> TestResult {
    let fixture = Fixture::new()?;
    fixture.assemble()?;
    let path = fixture.output.join("SHA256SUMS");
    let original = fs::read_to_string(&path)?;
    for text in [
        String::new(),
        original.to_uppercase(),
        format!("{original}{}  SHA256SUMS\n", "0".repeat(64)),
    ] {
        fs::write(&path, text)?;
        assert!(verify(&fixture.output, &fixture.identity).is_err());
    }
    Ok(())
}

fn read_members(path: &Path) -> TestResult<Vec<(Header, Vec<u8>)>> {
    let mut archive = Archive::new(MultiGzDecoder::new(File::open(path)?));
    let mut members = Vec::new();
    for member in archive.entries()? {
        let mut member = member?;
        let header = member.header().clone();
        let mut data = Vec::new();
        member.read_to_end(&mut data)?;
        members.push((header, data));
    }
    Ok(members)
}

fn write_members(path: &Path, members: &[(Header, Vec<u8>)]) -> TestResult {
    let mut archive =
        Builder::new(GzBuilder::new().write(File::create(path)?, Compression::fast()));
    for (header, data) in members {
        archive.append(header, &data[..])?;
    }
    archive.into_inner()?.finish()?;
    Ok(())
}

#[test]
fn unsafe_incomplete_reordered_or_extra_members_are_rejected() -> TestResult {
    let fixture = Fixture::new()?;
    let path = fixture.archive();
    let originals = read_members(&path)?;
    for name in ["../outside", "/absolute/dar", "other"] {
        let mut members = originals.clone();
        members[0].0.as_mut_bytes()[..100].fill(0);
        members[0].0.as_mut_bytes()[..name.len()].copy_from_slice(name.as_bytes());
        members[0].0.set_cksum();
        write_members(&path, &members)?;
        assert!(fixture.describe().is_err());
    }
    for kind in [b'1', b'2', b'5', b'6', b'x', b'g', b'L', 0] {
        let mut members = originals.clone();
        members[0].0.as_old_mut().linkflag[0] = kind;
        members[0].0.set_cksum();
        write_members(&path, &members)?;
        assert!(fixture.describe().is_err(), "accepted entry type {kind}");
    }
    for mode in [0o644, 0o4755] {
        let mut members = originals.clone();
        members[0].0.set_mode(mode);
        members[0].0.set_cksum();
        write_members(&path, &members)?;
        assert!(fixture.describe().is_err());
    }
    let mut empty = originals.clone();
    empty[0].0.set_size(0);
    empty[0].0.set_cksum();
    empty[0].1.clear();
    let mut extra = originals.clone();
    extra.push(originals[0].clone());
    let mut reversed = originals.clone();
    reversed.reverse();
    for members in [empty, originals[..2].to_vec(), extra, reversed] {
        write_members(&path, &members)?;
        assert!(fixture.describe().is_err());
    }
    Ok(())
}

#[test]
fn repository_license_and_dependency_notices_are_required() -> TestResult {
    let fixture = Fixture::new()?;
    let path = fixture.archive();
    let originals = read_members(&path)?;
    for (index, data) in [
        (1, b"wrong license".to_vec()),
        (2, fs::read(&fixture.notices)?),
        (2, [NOTICE, b"\n"].concat()),
        (2, [NOTICE, b"\n \t\n\r\x0b\x0c"].concat()),
    ] {
        let mut members = originals.clone();
        members[index].0.set_size(data.len() as u64);
        members[index].0.set_cksum();
        members[index].1 = data;
        write_members(&path, &members)?;
        assert!(fixture.describe().is_err());
    }
    Ok(())
}

#[test]
fn identity_rejects_unstable_versions_and_noncanonical_commits() {
    let mut identity = BuildIdentity {
        version: "1.2.3".into(),
        source_commit: "a".repeat(40),
        rust_toolchain: "1.94.0".into(),
    };
    for version in [
        "../1.2.3",
        "v1.2.3",
        "01.2.3",
        "1.2.3-rc.1",
        "1.2",
        "1.2.3\n",
    ] {
        identity.version = version.into();
        assert!(identity.validate().is_err());
    }
    identity.version = "1.2.3".into();
    for source in [
        "main".into(),
        "a".repeat(39),
        "A".repeat(40),
        "g".repeat(40),
    ] {
        identity.source_commit = source;
        assert!(identity.validate().is_err());
    }
    identity.source_commit = "a".repeat(40);
    for toolchain in ["stable", "1.94", "1.94.0\n"] {
        identity.rust_toolchain = toolchain.into();
        assert!(identity.validate().is_err());
    }
}
