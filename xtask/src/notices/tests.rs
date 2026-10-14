use super::*;

type TestResult = std::result::Result<(), Box<dyn std::error::Error>>;

struct Fixture {
    root: tempfile::TempDir,
    report: LicenseReport,
    rust_docs: PathBuf,
    extra: ExtraNotice,
}

impl Fixture {
    fn new() -> std::result::Result<Self, Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let dependency = root.path().join("dependency");
        fs::create_dir(&dependency)?;
        fs::write(
            dependency.join("NOTICE"),
            "Original copyright attribution\r\n",
        )?;
        fs::write(
            dependency.join(".cargo_vcs_info.json"),
            format!(r#"{{"git":{{"sha1":"{}"}}}}"#, "a".repeat(40)),
        )?;
        let package = DependencyPackage {
            id: "registry+https://github.com/rust-lang/crates.io-index#dependency@1.0.0".into(),
            name: "dependency".into(),
            version: "1.0.0".into(),
            manifest_path: dependency.join("Cargo.toml"),
            license_file: None,
            source: Some("registry+https://github.com/rust-lang/crates.io-index".into()),
        };
        let report = LicenseReport {
            licenses: vec![License {
                id: "MIT".into(),
                text: "Full license text\n".into(),
                used_by: vec![LicenseUse {
                    package: DependencyIdentity {
                        id: package.id.clone(),
                        name: package.name.clone(),
                        version: package.version.clone(),
                    },
                }],
            }],
            crates: vec![ResolvedCrate { package }],
        };
        let rust_docs = root.path().join("rust-docs");
        fs::create_dir_all(rust_docs.join("licenses"))?;
        fs::write(
            rust_docs.join("COPYRIGHT-library.html"),
            "<pre>Rust library attribution &amp; terms</pre>\n",
        )?;
        for name in ["Apache-2.0", "MIT"] {
            fs::write(
                rust_docs.join(format!("licenses/{name}.txt")),
                format!("{name} terms\n"),
            )?;
        }
        let extra = ExtraNotice {
            package: "dependency".into(),
            version: "1.0.0".into(),
            url: format!(
                "https://raw.githubusercontent.com/owner/project/{}/LICENSE",
                "a".repeat(40)
            ),
            sha256: format!("{:x}", Sha256::digest(b"Original license\n")),
            crate_checksum: None,
        };
        Ok(Self {
            root,
            report,
            rust_docs,
            extra,
        })
    }

    fn render(&self) -> Result<String> {
        render_notices(&self.report, &self.rust_docs, &ExtraNotices::new(), "0.9.2")
    }
}

#[test]
fn preserves_original_text_and_nested_files() -> TestResult {
    let mut fixture = Fixture::new()?;
    let dependency = fixture.root.path().join("dependency");
    fs::create_dir_all(dependency.join("vendor/licenses"))?;
    fs::write(dependency.join("vendor/licenses/BSD.txt"), "Vendor terms\n")?;
    fs::write(
        dependency.join("special-terms"),
        "Explicit license_file terms\n",
    )?;
    fs::write(
        dependency.join("secret-data"),
        "Do not distribute this file\n",
    )?;
    fixture.report.crates[0].package.license_file = Some("special-terms".into());
    let result = fixture.render()?;
    for expected in [
        "Original copyright attribution\r\n",
        "Full license text\n",
        "vendor/licenses/BSD.txt\n\nVendor terms\n",
        "special-terms\n\nExplicit license_file terms\n",
        "<pre>Rust library attribution &amp; terms</pre>\n",
        "Apache-2.0 terms\n",
        "MIT terms\n",
    ] {
        assert!(result.contains(expected), "missing {expected:?}");
    }
    assert!(!result.contains("Do not distribute"));
    assert!(!result.contains(fixture.root.path().to_string_lossy().as_ref()));
    assert_eq!(result, fixture.render()?);
    Ok(())
}

#[test]
fn missing_original_files_require_a_verified_supplement() -> TestResult {
    let fixture = Fixture::new()?;
    fs::remove_file(fixture.root.path().join("dependency/NOTICE"))?;
    assert!(fixture.render().is_err());
    let extra = collect_extra_notices(&fixture.report.crates, &[], &[fixture.extra], |_| {
        Ok("Original license\n".into())
    })?;
    let text = render_notices(&fixture.report, &fixture.rust_docs, &extra, "0.9.2")?;
    assert!(text.contains("Original license\n"));
    Ok(())
}

#[test]
fn rejects_incomplete_or_inconsistent_license_reports() -> TestResult {
    let fixture = Fixture::new()?;
    let mut reports = vec![];
    let mut report = fixture.report.clone();
    report.crates.clear();
    reports.push(report);
    let mut report = fixture.report.clone();
    report.licenses.clear();
    reports.push(report);
    let mut report = fixture.report.clone();
    report.licenses[0].text = " ".into();
    reports.push(report);
    let mut report = fixture.report.clone();
    report.licenses[0].used_by.clear();
    reports.push(report);
    let mut report = fixture.report.clone();
    report.crates[0].package.id = "uncovered".into();
    reports.push(report);
    let mut report = fixture.report.clone();
    report.crates.push(report.crates[0].clone());
    reports.push(report);
    let mut report = fixture.report.clone();
    report.crates[0].package.source = None;
    reports.push(report);
    for report in reports {
        assert!(
            render_notices(&report, &fixture.rust_docs, &ExtraNotices::new(), "0.9.2").is_err()
        );
    }
    Ok(())
}

#[test]
fn rejects_missing_empty_or_non_file_notices() -> TestResult {
    let mut fixture = Fixture::new()?;
    for name in ["COPYRIGHT-library.html", "licenses/MIT.txt"] {
        let path = fixture.rust_docs.join(name);
        let original = fs::read(&path)?;
        fs::remove_file(&path)?;
        assert!(fixture.render().is_err());
        fs::write(&path, original)?;
    }
    let path = fixture.root.path().join("dependency/NOTICE");
    fs::write(&path, " ")?;
    assert!(fixture.render().is_err());
    fs::write(&path, [0xff])?;
    assert!(fixture.render().is_err());
    fs::remove_file(&path)?;
    fs::create_dir(&path)?;
    fixture.report.crates[0].package.license_file = Some("NOTICE".into());
    assert!(fixture.render().is_err());
    fixture.report.crates[0].package.license_file = Some("../rust-docs/licenses/MIT.txt".into());
    assert!(fixture.render().is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn rejects_symlinks_anywhere_in_dependency_source() -> TestResult {
    let fixture = Fixture::new()?;
    let linked = fixture.root.path().join("dependency/linked-directory");
    std::os::unix::fs::symlink(&fixture.rust_docs, &linked)?;
    assert!(fixture.render().is_err());
    fs::remove_file(&linked)?;
    let path = fixture.root.path().join("dependency/NOTICE");
    fs::remove_file(&path)?;
    std::os::unix::fs::symlink(fixture.rust_docs.join("licenses/MIT.txt"), &path)?;
    assert!(fixture.render().is_err());
    Ok(())
}

#[test]
fn downloads_require_matching_content_version_and_source() -> TestResult {
    let fixture = Fixture::new()?;
    for text in ["Changed text", " "] {
        assert!(
            collect_extra_notices(
                &fixture.report.crates,
                &[],
                std::slice::from_ref(&fixture.extra),
                |_| Ok(text.into())
            )
            .is_err()
        );
    }
    let mut empty = fixture.extra.clone();
    empty.sha256 = format!("{:x}", Sha256::digest(b" "));
    assert!(
        collect_extra_notices(&fixture.report.crates, &[], &[empty], |_| Ok(" ".into())).is_err()
    );
    let mut entries = vec![];
    let mut entry = fixture.extra.clone();
    entry.version = "2.0.0".into();
    entries.push(entry);
    for url in [
        fixture.extra.url.replace(&"a".repeat(40), &"b".repeat(40)),
        fixture.extra.url.replace(&"a".repeat(40), "main"),
        fixture.extra.url.replace("https:", "http:"),
        format!("{}?ref=main", fixture.extra.url),
    ] {
        entries.push(ExtraNotice {
            url,
            ..fixture.extra.clone()
        });
    }
    for entry in entries {
        let mut called = false;
        let result = collect_extra_notices(&fixture.report.crates, &[], &[entry], |_| {
            called = true;
            Ok("Original license\n".into())
        });
        assert!(result.is_err());
        assert!(!called);
    }
    let mut called = false;
    let result = collect_extra_notices(&[], &[], &[fixture.extra], |_| {
        called = true;
        Ok(String::new())
    })?;
    assert!(result.is_empty());
    assert!(!called);
    Ok(())
}

#[test]
fn archives_without_vcs_metadata_require_an_exact_registry_pin() -> TestResult {
    let fixture = Fixture::new()?;
    fs::remove_file(fixture.root.path().join("dependency/.cargo_vcs_info.json"))?;
    fs::remove_file(fixture.root.path().join("dependency/NOTICE"))?;
    assert!(
        collect_extra_notices(
            &fixture.report.crates,
            &[],
            std::slice::from_ref(&fixture.extra),
            |_| panic!("missing source identity must fail before downloading")
        )
        .is_err()
    );
    let package = &fixture.report.crates[0].package;
    let checksum = "c".repeat(64);
    let locked = || LockedDependency {
        name: package.name.clone(),
        version: package.version.clone(),
        source: package.source.clone(),
        checksum: Some(checksum.clone()),
    };
    let entry = ExtraNotice {
        crate_checksum: Some(checksum.clone()),
        ..fixture.extra.clone()
    };
    for field in ["name", "version", "source", "checksum"] {
        let mut wrong = locked();
        match field {
            "name" => wrong.name = "another-crate".into(),
            "version" => wrong.version = "2.0.0".into(),
            "source" => wrong.source = None,
            "checksum" => wrong.checksum = Some("d".repeat(64)),
            _ => unreachable!(),
        }
        assert!(
            collect_extra_notices(
                &fixture.report.crates,
                &[wrong],
                std::slice::from_ref(&entry),
                |_| panic!("{field} mismatch must fail before downloading")
            )
            .is_err()
        );
    }
    let extra = collect_extra_notices(&fixture.report.crates, &[locked()], &[entry], |_| {
        Ok("Original license\n".into())
    })?;
    let text = render_notices(&fixture.report, &fixture.rust_docs, &extra, "0.9.2")?;
    assert!(text.contains("Original license\n"));
    Ok(())
}

#[test]
fn does_not_overwrite_an_existing_output() -> TestResult {
    let fixture = Fixture::new()?;
    let path = fixture.root.path().join("notices.txt");
    write_notices(&path, "Previously validated notices\n")?;
    assert!(write_notices(&path, "Replacement notices\n").is_err());
    assert_eq!(fs::read_to_string(path)?, "Previously validated notices\n");
    Ok(())
}
