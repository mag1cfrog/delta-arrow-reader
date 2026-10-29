//! Python wheel contents, filenames, metadata, and archive validation.

#![allow(clippy::unwrap_used)]

use std::{fs, path::Path, process::Command};

mod support;
use support::{
    PLATFORMS, Result, WheelFixture, cli_wheel_filename, cli_wheel_files, record_contents,
    wheel_filename, wheel_files, write_wheel, write_zip, write_zip_with_permissions,
};

fn replace_text(contents: &mut Vec<u8>, from: &str, to: &str) {
    *contents = String::from_utf8(contents.clone())
        .unwrap()
        .replace(from, to)
        .into_bytes();
}

fn check(
    path: &Path,
    version: &str,
    platform: Option<&str>,
    expected: std::result::Result<(), &str>,
) -> Result<()> {
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .arg(if platform.is_some() {
            "check-python-wheel"
        } else {
            "check-python-artifacts"
        })
        .arg(path)
        .args(["--expected-version", version]);
    if let Some(platform) = platform {
        command.args(["--platform-tag", platform]);
    }
    let output = command.output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    match expected {
        Ok(()) => assert!(output.status.success(), "{command:?}: {stderr}"),
        Err(message) => {
            assert!(!output.status.success(), "accepted {command:?}");
            assert!(stderr.contains(message), "expected {message:?}: {stderr}");
        }
    }
    Ok(())
}

#[test]
fn accepts_stored_and_deflated_wheels_for_all_platforms() -> Result<()> {
    let fixture = WheelFixture::new("valid")?;
    for (_, tag) in PLATFORMS {
        let path = fixture.directory.join(wheel_filename(tag));
        for compressed in [false, true] {
            write_wheel(&path, &wheel_files(tag), compressed)?;
            check(&path, "1.2.3", Some(tag), Ok(()))?;
        }
        let mut files = wheel_files(tag);
        replace_text(&mut files[0].1, "Name:", "nAmE:");
        replace_text(&mut files[0].1, "\n", "\r\n");
        files[0].1.extend_from_slice(
            b"Description: first line\r\n continued\r\n\r\nName: text in the body\r\n",
        );
        write_wheel(&path, &files, true)?;
        check(&path, "1.2.3", Some(tag), Ok(()))?;
    }
    Ok(())
}

#[test]
fn rejects_wrong_wheel_filenames() -> Result<()> {
    let fixture = WheelFixture::new("filenames")?;
    let tag = PLATFORMS[0].1;
    for (old, new) in [
        ("delta_arrow_reader", "other_package"),
        ("1.2.3", "1.2.4"),
        ("cp310", "cp311"),
        ("abi3", "cp310"),
        (tag, "win_amd64"),
    ] {
        let path = fixture
            .directory
            .join(wheel_filename(tag).replace(old, new));
        write_wheel(&path, &wheel_files(tag), false)?;
        check(&path, "1.2.3", Some(tag), Err("expected wheel"))?;
    }
    Ok(())
}

#[test]
fn distributions_reject_shared_payloads_and_cli_dependencies() -> Result<()> {
    let fixture = WheelFixture::new("independent-distributions")?;
    for (_, tag) in PLATFORMS {
        let library_path = fixture.directory.join(wheel_filename(tag));
        let cli_path = fixture.directory.join(cli_wheel_filename(tag));
        let mut library = wheel_files(tag);
        let mut cli = cli_wheel_files(tag);
        write_wheel(&cli_path, &cli, true)?;
        check(&cli_path, "1.2.3", Some(tag), Ok(()))?;

        // Library and CLI cannot both own the command, even with a valid RECORD.
        let (name, bytes) = cli
            .iter()
            .find(|(name, _)| name.contains("/scripts/"))
            .unwrap();
        library.push((
            name.replace("delta_arrow_reader_cli-", "delta_arrow_reader-"),
            bytes.clone(),
        ));
        write_wheel(&library_path, &library, true)?;
        check(
            &library_path,
            "1.2.3",
            Some(tag),
            Err("unexpected wheel member"),
        )?;
        cli.push(("delta_arrow_reader/__init__.py".into(), b"# API".to_vec()));
        write_wheel(&cli_path, &cli, true)?;
        check(
            &cli_path,
            "1.2.3",
            Some(tag),
            Err("unexpected wheel member"),
        )?;

        let mut cli = cli_wheel_files(tag);
        cli[0]
            .1
            .extend_from_slice(b"Requires-Dist: pyarrow>=18.0.0\n");
        write_wheel(&cli_path, &cli, true)?;
        check(
            &cli_path,
            "1.2.3",
            Some(tag),
            Err("CLI wheel must not install Python dependencies"),
        )?;
    }
    Ok(())
}

#[test]
fn rejects_inconsistent_missing_duplicate_and_folded_headers() -> Result<()> {
    let fixture = WheelFixture::new("headers")?;
    let tag = PLATFORMS[0].1;
    let path = fixture.directory.join(wheel_filename(tag));
    for (member, original, replacement) in [
        (0, "Name: delta-arrow-reader\n", "Name: other-package\n"),
        (0, "Version: 1.2.3\n", "Version: 1.2.4\n"),
        (0, "Version: 1.2.3\n", "Version: 1.2.3\nVersion: 1.2.3\n"),
        (0, "Version: 1.2.3\n", "Version: 1.2.3\nversion: 1.2.3\n"),
        (0, "Version: 1.2.3\n", "Version: 1.2.3 \n"),
        (0, "Version: 1.2.3\n", "Version: 1.2.3\n extra\n"),
        (0, "Version: 1.2.3\n", "\nVersion: 1.2.3\n"),
        (1, "Root-Is-Purelib: false\n", "Root-Is-Purelib: true\n"),
        (
            1,
            "Tag: cp310-abi3-manylinux_2_28_x86_64\n",
            "Tag: cp310-abi3-win_amd64\n",
        ),
        (1, "Tag: cp310-abi3-manylinux_2_28_x86_64\n", ""),
        (
            1,
            "Tag: cp310-abi3-manylinux_2_28_x86_64\n",
            "Tag: cp310-abi3-manylinux_2_28_x86_64\nTag: cp310-abi3-win_amd64\n",
        ),
    ] {
        let mut files = wheel_files(tag);
        replace_text(&mut files[member].1, original, replacement);
        write_wheel(&path, &files, true)?;
        check(&path, "1.2.3", Some(tag), Err("expected one"))?;
    }
    Ok(())
}

#[test]
fn rejects_missing_foreign_and_duplicate_metadata_entries() -> Result<()> {
    let fixture = WheelFixture::new("entries")?;
    let tag = PLATFORMS[0].1;
    let path = fixture.directory.join(wheel_filename(tag));
    for member in 0..2 {
        for problem in ["missing", "foreign", "duplicate", "nested"] {
            let mut files = wheel_files(tag);
            match problem {
                "missing" => {
                    files.remove(member);
                }
                "foreign" => files.push((
                    files[member].0.replace("1.2.3", "1.2.4"),
                    files[member].1.clone(),
                )),
                "duplicate" => files.push(files[member].clone()),
                "nested" => files[member].0 = format!("nested/{}", files[member].0),
                _ => unreachable!(),
            }
            write_wheel(&path, &files, false)?;
            let message = match problem {
                "missing" => "missing required wheel file",
                "duplicate" => "duplicate or aliased ZIP member",
                _ => "unexpected wheel member",
            };
            check(&path, "1.2.3", Some(tag), Err(message))?;
        }
    }
    for name in [
        "delta_arrow_reader-1.2.3.dist-info/METADATA\0shadow",
        "delta_arrow_reader-1.2.3.dist-info\\METADATA",
    ] {
        let mut files = wheel_files(tag);
        files.push((name.into(), files[0].1.clone()));
        write_wheel(&path, &files, false)?;
        check(&path, "1.2.3", Some(tag), Err("ambiguous ZIP member name"))?;
    }
    Ok(())
}

#[test]
fn rejects_metadata_paths_that_installers_can_alias() -> Result<()> {
    let fixture = WheelFixture::new("metadata-aliases")?;
    let tag = PLATFORMS[0].1;
    let path = fixture.directory.join(wheel_filename(tag));
    for alias in [
        "delta_arrow_reader-1.2.3.dist-info/./METADATA",
        "delta_arrow_reader-1.2.3.dist-info//METADATA",
        "delta_arrow_reader-1.2.3.dist-info/nested/../METADATA",
        "delta_arrow_reader-1.2.3.dist-info/METADATA.",
        "delta_arrow_reader-1.2.3.dist-info/METADATA ",
        "delta_arrow_reader-1.2.3.dist-info/METADATA:",
        "delta_arrow_reader-1.2.3.dist-info/METADATA?",
        "delta_arrow_reader-1.2.3.dist-info/metadata",
        "DELTA_ARROW_READER-1.2.3.DIST-INFO/METADATA",
        "delta_arrow_reader-1.2.3.dist-info/./WHEEL",
        "delta_arrow_reader-1.2.3.dist-info/wheel",
    ] {
        let mut files = wheel_files(tag);
        files.push((alias.into(), b"shadow metadata".to_vec()));
        write_wheel(&path, &files, false)?;
        check(&path, "1.2.3", Some(tag), Err("Error"))?;
    }
    Ok(())
}

#[test]
fn rejects_invalid_archives_and_metadata_with_a_bad_crc() -> Result<()> {
    let fixture = WheelFixture::new("corrupt")?;
    let tag = PLATFORMS[0].1;
    let path = fixture.directory.join(wheel_filename(tag));
    fs::write(&path, "not a ZIP archive")?;
    check(&path, "1.2.3", Some(tag), Err("Error"))?;
    write_wheel(&path, &wheel_files(tag), false)?;
    let mut bytes = fs::read(&path)?;
    let position = bytes
        .windows(b"Name: delta-arrow-reader".len())
        .position(|window| window == b"Name: delta-arrow-reader")
        .unwrap();
    bytes[position] = b'X';
    fs::write(&path, bytes)?;
    check(&path, "1.2.3", Some(tag), Err("Invalid checksum"))?;

    write_wheel(&path, &wheel_files(tag), false)?;
    let mut bytes = fs::read(&path)?;
    let name = b"delta_arrow_reader-1.2.3.dist-info/METADATA";
    let position = bytes
        .windows(name.len())
        .position(|window| window == name)
        .unwrap();
    bytes[position] = b'x';
    fs::write(&path, bytes)?;
    check(
        &path,
        "1.2.3",
        Some(tag),
        Err("ZIP local header does not match"),
    )
}

#[test]
fn requires_both_distributions_from_each_platform() -> Result<()> {
    let fixture = WheelFixture::new("artifacts")?;
    let (directory, paths) = fixture.create_artifacts()?;
    check(&directory, "1.2.3", None, Ok(()))?;
    for path in &paths {
        let artifact = path.parent().unwrap();
        fs::rename(artifact, fixture.directory.join("removed"))?;
        check(&directory, "1.2.3", None, Err("expected artifacts"))?;
        fs::rename(fixture.directory.join("removed"), artifact)?;
    }
    let extra = directory.join("delta-arrow-reader-macos-x86_64-wheel");
    fs::create_dir(&extra)?;
    check(&directory, "1.2.3", None, Err("expected artifacts"))?;
    fs::remove_dir(extra)?;

    let path = &paths[0];
    let original = fs::read(path)?;
    fs::remove_file(path)?;
    check(
        &directory,
        "1.2.3",
        None,
        Err("expected Python and CLI wheels"),
    )?;
    fs::write(path, &original)?;
    let extra = path.with_file_name("extra.whl");
    fs::write(&extra, "extra")?;
    check(
        &directory,
        "1.2.3",
        None,
        Err("expected Python and CLI wheels"),
    )?;
    fs::remove_file(extra)?;
    let wrong_platform = path.with_file_name(paths[1].file_name().unwrap());
    fs::rename(path, &wrong_platform)?;
    check(
        &directory,
        "1.2.3",
        None,
        Err("expected Python and CLI wheels"),
    )?;
    fs::rename(wrong_platform, path)?;
    check(
        &directory,
        "1.2.4",
        None,
        Err("expected Python and CLI wheels"),
    )?;

    let mut files = wheel_files(PLATFORMS[0].1);
    replace_text(&mut files[0].1, "1.2.3", "1.2.4");
    write_wheel(path, &files, true)?;
    check(
        &directory,
        "1.2.3",
        None,
        Err("METADATA: expected one Version"),
    )
}

#[test]
fn requires_all_installed_files_and_rejects_duplicates() -> Result<()> {
    let fixture = WheelFixture::new("payload-files")?;
    for (_, tag) in PLATFORMS {
        let path = fixture.directory.join(wheel_filename(tag));
        let complete = wheel_files(tag);
        for index in 2..complete.len() {
            let mut files = complete.clone();
            files.remove(index);
            write_wheel(&path, &files, true)?;
            check(
                &path,
                "1.2.3",
                Some(tag),
                Err("missing required wheel file"),
            )?;

            let mut files = complete.clone();
            files.push(files[index].clone());
            write_wheel(&path, &files, true)?;
            check(
                &path,
                "1.2.3",
                Some(tag),
                Err("duplicate or aliased ZIP member"),
            )?;
        }
        write_zip(&path, &complete, false)?;
        check(
            &path,
            "1.2.3",
            Some(tag),
            Err("missing required wheel file"),
        )?;
    }
    Ok(())
}

#[test]
fn rejects_wrong_cli_names_and_installation_paths() -> Result<()> {
    let fixture = WheelFixture::new("cli-layout")?;
    for (_, tag) in PLATFORMS {
        let path = fixture.directory.join(cli_wheel_filename(tag));
        for name in [
            "delta_arrow_reader_cli-1.2.3.data/scripts/delta-arrow-reader",
            "delta_arrow_reader_cli-1.2.3.data/scripts/dar.py",
            "delta_arrow_reader_cli-1.2.3.data/scripts/nested/dar",
            "delta_arrow_reader.data/scripts/dar",
            "delta_arrow_reader_cli-1.2.4.data/scripts/dar",
            "delta_arrow_reader_cli-1.2.3.data/purelib/delta_arrow_reader/__init__.py",
            "delta_arrow_reader/dar",
            "dar",
            "/home/builder/dar",
            "C:\\build\\dar.exe",
            if tag == "win_amd64" {
                "delta_arrow_reader_cli-1.2.3.data/scripts/dar"
            } else {
                "delta_arrow_reader_cli-1.2.3.data/scripts/dar.exe"
            },
        ] {
            let mut files = cli_wheel_files(tag);
            let cli = files
                .iter_mut()
                .find(|(name, _)| name.contains("/scripts/"))
                .unwrap();
            cli.0 = name.into();
            write_wheel(&path, &files, true)?;
            check(&path, "1.2.3", Some(tag), Err("Error"))?;
        }
        for extra in [
            "delta_arrow_reader_cli-1.2.3.data/scripts/unrelated",
            "delta_arrow_reader_cli-1.2.3.dist-info/entry_points.txt",
            "delta_arrow_reader.libs/another-program.exe",
        ] {
            let mut files = cli_wheel_files(tag);
            files.push((extra.into(), b"extra program".to_vec()));
            write_wheel(&path, &files, false)?;
            check(&path, "1.2.3", Some(tag), Err("unexpected wheel member"))?;
        }
        let mut files = cli_wheel_files(tag);
        let (name, contents) = files
            .iter()
            .find(|(name, _)| name.contains("/scripts/"))
            .unwrap();
        files.push((name.to_uppercase(), contents.clone()));
        write_wheel(&path, &files, true)?;
        check(
            &path,
            "1.2.3",
            Some(tag),
            Err("duplicate or aliased ZIP member"),
        )?;
    }
    Ok(())
}

#[test]
fn requires_native_payloads_and_unix_cli_permissions() -> Result<()> {
    let fixture = WheelFixture::new("native-payload")?;
    for (_, tag) in PLATFORMS {
        let path = fixture.directory.join(wheel_filename(tag));
        for contents in [b"#!python\npass\n".as_slice(), b"wrong binary format", b"M"] {
            let mut files = wheel_files(tag);
            files
                .iter_mut()
                .find(|(name, _)| name.contains("/delta_arrow_reader."))
                .unwrap()
                .1 = contents.to_vec();
            write_wheel(&path, &files, false)?;
            check(&path, "1.2.3", Some(tag), Err("Error"))?;
        }
        let path = fixture.directory.join(cli_wheel_filename(tag));
        let mut files = cli_wheel_files(tag);
        files.push((
            "delta_arrow_reader_cli-1.2.3.dist-info/RECORD".into(),
            record_contents(&files)?,
        ));
        for (mode, message) in [
            (0o755, "CLI must declare a regular file type for pip"),
            (0o644, "CLI must be executable"),
            (0o744, "CLI must be executable"),
            (0o120755, "must be a regular file"),
            (0o040755, "must be a regular file"),
        ] {
            write_zip_with_permissions(&path, &files, false, |name| {
                if name.contains("/scripts/") {
                    mode
                } else {
                    0o644
                }
            })?;
            let expected = if tag == "win_amd64" && mode < 0o1000 {
                Ok(())
            } else {
                Err(message)
            };
            check(&path, "1.2.3", Some(tag), expected)?;
        }
        let path = fixture.directory.join(wheel_filename(tag));
        for suffix in [
            "__init__.pyi",
            "THIRD_PARTY_NOTICES.txt",
            "licenses/LICENSE",
        ] {
            let mut files = wheel_files(tag);
            files
                .iter_mut()
                .find(|(name, _)| name.ends_with(suffix))
                .unwrap()
                .1
                .clear();
            write_wheel(&path, &files, false)?;
            check(&path, "1.2.3", Some(tag), Err("empty wheel file"))?;
        }
    }
    Ok(())
}

#[test]
fn accepts_repair_libraries_sboms_directories_and_quoted_record_paths() -> Result<()> {
    let fixture = WheelFixture::new("repair-layout")?;
    for (_, tag) in PLATFORMS {
        let path = fixture.directory.join(wheel_filename(tag));
        let mut files = wheel_files(tag);
        let libraries = match tag {
            "win_amd64" => vec![
                "delta_arrow_reader.libs/dependency.dll",
                "delta_arrow_reader-1.2.3.dist-info/DELVEWHEEL",
            ],
            "macosx_12_0_arm64" => vec![
                "delta_arrow_reader.dylibs/libdependency.dylib",
                "delta_arrow_reader/.dylibs/libother.dylib",
            ],
            _ => vec![
                "delta_arrow_reader.libs/libdependency.so.1",
                "delta_arrow_reader.libs/libother.so",
            ],
        };
        for name in libraries {
            files.push((name.into(), b"repair fixture".to_vec()));
        }
        files.push((
            "delta_arrow_reader-1.2.3.dist-info/sboms/extension,cli.json".into(),
            b"{}".to_vec(),
        ));
        files.push(("delta_arrow_reader/".into(), Vec::new()));
        files.push(("delta_arrow_reader-1.2.3.data/data/".into(), Vec::new()));
        write_wheel(&path, &files, true)?;
        check(&path, "1.2.3", Some(tag), Ok(()))?;

        files.push(("unrelated/".into(), Vec::new()));
        write_wheel(&path, &files, true)?;
        check(&path, "1.2.3", Some(tag), Err("unexpected wheel directory"))?;
        files.pop();
        files.last_mut().unwrap().1.push(0);
        write_wheel(&path, &files, true)?;
        check(&path, "1.2.3", Some(tag), Err("nonempty ZIP directory"))?;
    }
    Ok(())
}

#[test]
fn record_must_cover_every_file_once_with_exact_hashes_and_sizes() -> Result<()> {
    let fixture = WheelFixture::new("record-integrity")?;
    let tag = PLATFORMS[0].1;
    let path = fixture.directory.join(wheel_filename(tag));
    let files = wheel_files(tag);
    let record = String::from_utf8(record_contents(&files)?)?;
    let first = record.lines().next().unwrap();
    let record_path = "delta_arrow_reader-1.2.3.dist-info/RECORD";
    for (contents, message) in [
        (
            record.replacen("sha256=", "md5=", 1),
            "incorrect hash or size",
        ),
        (
            record.replacen("sha256=", "sha256=wrong", 1),
            "incorrect hash or size",
        ),
        (
            record.replacen(first, &format!("{},sha256=,0", files[0].0), 1),
            "incorrect hash or size",
        ),
        (
            record.replacen(
                first,
                &format!("{},{}", first.rsplit_once(',').unwrap().0, 999),
                1,
            ),
            "incorrect hash or size",
        ),
        (
            record.replacen(first, &format!("{},,", files[0].0), 1),
            "incorrect hash or size",
        ),
        (
            record.replacen(&format!("{first}\n"), "", 1),
            "missing files",
        ),
        (format!("{record}{first}\n"), "duplicate or unknown path"),
        (
            format!("{record}missing-file,,\n"),
            "duplicate or unknown path",
        ),
        (
            record.replacen(&files[0].0, &format!("/{}", files[0].0), 1),
            "duplicate or unknown path",
        ),
        (
            record.replace(&format!("{record_path},,\n"), ""),
            "missing files",
        ),
        (
            record.replace(
                &format!("{record_path},,"),
                &format!("{record_path},sha256=unexpected,1"),
            ),
            "incorrect hash or size",
        ),
        (
            record.replacen(first, "too,few", 1),
            "expected path, hash, and size",
        ),
    ] {
        let mut files = files.clone();
        files.push((record_path.into(), contents.into_bytes()));
        write_wheel(&path, &files, true)?;
        check(&path, "1.2.3", Some(tag), Err(message))?;
    }
    // ZIP CRC remains valid, but RECORD must catch a payload replaced after hashing.
    let mut files = files;
    files.push((record_path.into(), record.into_bytes()));
    files
        .iter_mut()
        .find(|(name, _)| name.ends_with(".abi3.so"))
        .unwrap()
        .1
        .push(1);
    write_wheel(&path, &files, false)?;
    check(&path, "1.2.3", Some(tag), Err("incorrect hash or size"))
}

#[test]
fn rejects_corrupt_native_bytes_and_conflicting_zip_headers() -> Result<()> {
    let fixture = WheelFixture::new("native-corruption")?;
    let tag = PLATFORMS[0].1;
    let path = fixture.directory.join(cli_wheel_filename(tag));
    let cli = "delta_arrow_reader_cli-1.2.3.data/scripts/dar";
    write_wheel(&path, &cli_wheel_files(tag), false)?;
    let original = fs::read(&path)?;
    let archive = rawzip::ZipArchive::from_slice(&original)?;
    let mut entries = archive.entries();
    let (local, central) = loop {
        let entry = entries.next_entry()?.unwrap();
        if entry.file_path().as_bytes() == cli.as_bytes() {
            break (
                entry.local_header_offset() as usize,
                entry.central_directory_offset() as usize,
            );
        }
    };
    let extra_length = u16::from_le_bytes(original[local + 28..local + 30].try_into()?) as usize;
    for (offset, value, message) in [
        (
            local + 30 + cli.len() + extra_length + 5,
            b'X',
            "Invalid checksum",
        ),
        (local + 30, b'x', "ZIP local header does not match"),
        (local + 6, original[local + 6] | 1, "encrypted wheel member"),
        (
            central + 8,
            original[central + 8] | 1,
            "encrypted wheel member",
        ),
        (local + 8, 8, "ZIP local header does not match"),
    ] {
        let mut bytes = original.clone();
        bytes[offset] = value;
        fs::write(&path, bytes)?;
        check(&path, "1.2.3", Some(tag), Err(message))?;
    }
    Ok(())
}
