use std::io::Read;

use super::*;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    directory: tempfile::TempDir,
    repository: PathBuf,
    source: PathBuf,
    destination: PathBuf,
}

impl Fixture {
    fn new() -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let repository = directory.path().join("source tree");
        for (relative, contents) in [
            (
                "Cargo.toml",
                "[workspace]\nmembers = [\"crates/delta-arrow-reader-python\", \"crates/delta-arrow-reader-cli\", \"xtask\"]\n[workspace.package]\nversion = \"1.2.3\"\nrust-version = \"1.94\"\n[workspace.metadata.cli-release]\ncargo-about-version = \"0.9.2\"\n",
            ),
            ("Cargo.lock", "original workspace lockfile"),
            ("LICENSE", "Apache-2.0"),
            ("NOTICE", "Project notices"),
            (
                ".cargo/config.toml",
                "[alias]\nxtask = \"run --locked --package xtask --\"\n",
            ),
            ("crates/delta-arrow-reader-cli/Cargo.toml", "CLI manifest"),
            ("crates/delta-arrow-reader-cli/Dockerfile", "FROM fixture"),
            ("crates/delta-arrow-reader-cli/src/main.rs", "fn main() {}"),
            (
                "crates/delta-arrow-reader-cli/release/about.toml",
                "accepted = [\"Apache-2.0\"]",
            ),
            ("xtask/Cargo.toml", "xtask manifest"),
            ("xtask/src/main.rs", "fn main() {}"),
            ("target/release/dar", "generated executable"),
            (
                "crates/delta-arrow-reader-cli/target/debug/dar",
                "generated executable",
            ),
            (".git/config", "checkout state"),
        ] {
            let path = repository.join(relative);
            fs::create_dir_all(path.parent().unwrap())?;
            fs::write(path, contents)?;
        }
        let source = directory.path().join("input.tar.gz");
        let destination = directory
            .path()
            .join("output directory/delta_arrow_reader-1.2.3.tar.gz");
        Ok(Self {
            directory,
            repository,
            source,
            destination,
        })
    }

    fn archive(&self, missing: Option<&str>, extra: Option<&str>) -> TestResult {
        let compressed =
            GzBuilder::new().write(File::create(&self.source)?, Compression::default());
        let mut archive = Builder::new(compressed);
        let entries = [
            "Cargo.toml",
            "Cargo.lock",
            "pyproject.toml",
            "PKG-INFO",
            "build_backend.py",
            "crates/delta-arrow-reader-python/src/lib.rs",
        ];
        for relative in entries
            .into_iter()
            .filter(|path| Some(*path) != missing)
            .chain(extra)
        {
            let mut header = tar::Header::new_gnu();
            header.set_size(8);
            header.set_mode(0o644);
            header.set_cksum();
            archive.append_data(
                &mut header,
                format!("delta_arrow_reader-1.2.3/{relative}"),
                b"retained".as_slice(),
            )?;
        }
        archive.into_inner()?.finish()?;
        Ok(())
    }
}

#[test]
fn preserves_metadata_and_adds_build_sources_without_generated_files() -> TestResult {
    let fixture = Fixture::new()?;
    let long_path = format!(
        "crates/delta-arrow-reader-python/{}source.rs",
        "long_path/".repeat(30)
    );
    fixture.archive(None, Some(&long_path))?;
    complete_workspace_sdist(&fixture.repository, &fixture.source, &fixture.destination)?;
    let mut archive = Archive::new(GzDecoder::new(File::open(&fixture.destination)?));
    let mut files = BTreeMap::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry
            .path()?
            .strip_prefix("delta_arrow_reader-1.2.3")?
            .to_owned();
        let mut contents = String::new();
        entry.read_to_string(&mut contents)?;
        assert!(files.insert(path, contents).is_none());
    }
    for relative in [
        "Cargo.toml",
        "Cargo.lock",
        "crates/delta-arrow-reader-cli/src/main.rs",
        "xtask/src/main.rs",
    ] {
        assert_eq!(
            files[Path::new(relative)],
            fs::read_to_string(fixture.repository.join(relative))?
        );
    }
    assert_eq!(files[Path::new("PKG-INFO")], "retained");
    assert_eq!(files[Path::new(&long_path)], "retained");
    assert_eq!(
        files[Path::new("crates/delta-arrow-reader-python/src/lib.rs")],
        "retained"
    );
    assert!(files.keys().all(|path| {
        !path
            .components()
            .any(|component| matches!(component.as_os_str().to_str(), Some("target" | ".git")))
    }));
    Ok(())
}

#[test]
fn rejects_incomplete_or_ambiguous_input_without_replacing_output() -> TestResult {
    let fixture = Fixture::new()?;
    fs::create_dir_all(fixture.destination.parent().unwrap())?;
    fs::write(&fixture.destination, "previous completed archive")?;
    for (missing, extra) in [(Some("build_backend.py"), None), (None, Some("PKG-INFO"))] {
        fixture.archive(missing, extra)?;
        assert!(
            complete_workspace_sdist(&fixture.repository, &fixture.source, &fixture.destination)
                .is_err()
        );
        assert_eq!(
            fs::read_to_string(&fixture.destination)?,
            "previous completed archive"
        );
    }
    fixture.archive(None, None)?;
    let mut truncated = fs::read(&fixture.source)?;
    truncated.truncate(truncated.len() - 4);
    fs::write(&fixture.source, truncated)?;
    assert!(
        complete_workspace_sdist(&fixture.repository, &fixture.source, &fixture.destination)
            .is_err()
    );
    assert_eq!(
        fs::read_to_string(&fixture.destination)?,
        "previous completed archive"
    );
    fixture.archive(None, None)?;
    fs::remove_file(fixture.repository.join("xtask/src/main.rs"))?;
    fs::remove_dir(fixture.repository.join("xtask/src"))?;
    assert!(
        complete_workspace_sdist(&fixture.repository, &fixture.source, &fixture.destination)
            .is_err()
    );
    assert_eq!(
        fs::read_to_string(&fixture.destination)?,
        "previous completed archive"
    );
    // Only the input, source tree, and completed output survive a failed archive build.
    assert_eq!(fs::read_dir(fixture.directory.path())?.count(), 3);
    assert_eq!(
        fs::read_dir(fixture.destination.parent().unwrap())?.count(),
        1
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejects_symlinked_build_sources() -> TestResult {
    let fixture = Fixture::new()?;
    fixture.archive(None, None)?;
    let source = fixture.repository.join("xtask/src/main.rs");
    fs::remove_file(&source)?;
    std::os::unix::fs::symlink(fixture.repository.join("NOTICE"), &source)?;
    assert!(
        complete_workspace_sdist(&fixture.repository, &fixture.source, &fixture.destination)
            .is_err()
    );
    assert!(!fixture.destination.exists());
    Ok(())
}
