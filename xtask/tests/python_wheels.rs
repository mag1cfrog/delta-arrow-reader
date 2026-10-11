use std::{fs, path::Path, process::Command};

mod support;
use support::{PLATFORMS, Result, Wheels, filename, metadata, write_wheel};

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
            "check-python-wheels"
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
    let wheels = Wheels::new("valid")?;
    for (_, tag) in PLATFORMS {
        let path = wheels.0.join(filename(tag));
        for compressed in [false, true] {
            write_wheel(&path, &metadata(tag), compressed)?;
            check(&path, "1.2.3", Some(tag), Ok(()))?;
        }
        let mut files = metadata(tag);
        files[0].1 = files[0].1.replace("Name:", "nAmE:").replace('\n', "\r\n");
        files[0]
            .1
            .push_str("Description: first line\r\n continued\r\n\r\nName: text in the body\r\n");
        write_wheel(&path, &files, true)?;
        check(&path, "1.2.3", Some(tag), Ok(()))?;
    }
    Ok(())
}

#[test]
fn rejects_wrong_wheel_filenames() -> Result<()> {
    let wheels = Wheels::new("filenames")?;
    let tag = PLATFORMS[0].1;
    for (old, new) in [
        ("delta_arrow_reader", "other_package"),
        ("1.2.3", "1.2.4"),
        ("cp310", "cp311"),
        ("abi3", "cp310"),
        (tag, "win_amd64"),
    ] {
        let path = wheels.0.join(filename(tag).replace(old, new));
        write_wheel(&path, &metadata(tag), false)?;
        check(&path, "1.2.3", Some(tag), Err("expected wheel"))?;
    }
    Ok(())
}

#[test]
fn rejects_inconsistent_missing_duplicate_and_folded_headers() -> Result<()> {
    let wheels = Wheels::new("headers")?;
    let tag = PLATFORMS[0].1;
    let path = wheels.0.join(filename(tag));
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
        let mut files = metadata(tag);
        files[member].1 = files[member].1.replace(original, replacement);
        write_wheel(&path, &files, true)?;
        check(&path, "1.2.3", Some(tag), Err("expected one"))?;
    }
    Ok(())
}

#[test]
fn rejects_missing_foreign_and_duplicate_metadata_entries() -> Result<()> {
    let wheels = Wheels::new("entries")?;
    let tag = PLATFORMS[0].1;
    let path = wheels.0.join(filename(tag));
    for member in 0..2 {
        for problem in ["missing", "foreign", "duplicate", "nested"] {
            let mut files = metadata(tag);
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
            check(&path, "1.2.3", Some(tag), Err("expected exactly one"))?;
        }
    }
    for name in [
        "delta_arrow_reader-1.2.3.dist-info/METADATA\0shadow",
        "delta_arrow_reader-1.2.3.dist-info\\METADATA",
    ] {
        let mut files = metadata(tag);
        files.push((name.into(), files[0].1.clone()));
        write_wheel(&path, &files, false)?;
        check(&path, "1.2.3", Some(tag), Err("ambiguous ZIP member name"))?;
    }
    Ok(())
}

#[test]
fn rejects_invalid_archives_and_metadata_with_a_bad_crc() -> Result<()> {
    let wheels = Wheels::new("corrupt")?;
    let tag = PLATFORMS[0].1;
    let path = wheels.0.join(filename(tag));
    fs::write(&path, "not a ZIP archive")?;
    check(&path, "1.2.3", Some(tag), Err("Error"))?;
    write_wheel(&path, &metadata(tag), false)?;
    let mut bytes = fs::read(&path)?;
    let position = bytes
        .windows(b"Name: delta-arrow-reader".len())
        .position(|window| window == b"Name: delta-arrow-reader")
        .unwrap();
    bytes[position] = b'X';
    fs::write(&path, bytes)?;
    check(&path, "1.2.3", Some(tag), Err("InvalidChecksum"))?;

    write_wheel(&path, &metadata(tag), false)?;
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
fn requires_exactly_one_valid_wheel_from_each_platform() -> Result<()> {
    let wheels = Wheels::new("artifacts")?;
    let (directory, paths) = wheels.artifact_set()?;
    check(&directory, "1.2.3", None, Ok(()))?;
    for path in &paths {
        let artifact = path.parent().unwrap();
        fs::rename(artifact, wheels.0.join("removed"))?;
        check(&directory, "1.2.3", None, Err("expected artifacts"))?;
        fs::rename(wheels.0.join("removed"), artifact)?;
    }
    let extra = directory.join("unexpected-artifact");
    fs::create_dir(&extra)?;
    check(&directory, "1.2.3", None, Err("expected artifacts"))?;
    fs::remove_dir(extra)?;

    let path = &paths[0];
    let original = fs::read(path)?;
    fs::remove_file(path)?;
    check(&directory, "1.2.3", None, Err("expected one wheel"))?;
    fs::write(path, &original)?;
    let extra = path.with_file_name("extra.whl");
    fs::write(&extra, "extra")?;
    check(&directory, "1.2.3", None, Err("expected one wheel"))?;
    fs::remove_file(extra)?;
    let wrong_platform = path.with_file_name(paths[1].file_name().unwrap());
    fs::rename(path, &wrong_platform)?;
    check(&directory, "1.2.3", None, Err("expected wheel"))?;
    fs::rename(wrong_platform, path)?;
    check(&directory, "1.2.4", None, Err("expected wheel"))?;

    let mut files = metadata(PLATFORMS[0].1);
    files[0].1 = files[0].1.replace("1.2.3", "1.2.4");
    write_wheel(path, &files, true)?;
    check(
        &directory,
        "1.2.3",
        None,
        Err("METADATA: expected one Version"),
    )
}
