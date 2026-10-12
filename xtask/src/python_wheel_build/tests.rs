use super::*;
use crate::{Arguments, Task};
use clap::Parser;

fn arguments(args: &[&str]) -> BuildPythonWheelArgs {
    let parsed = Arguments::try_parse_from(
        [
            "xtask",
            "build-python-wheel",
            "--wheel-directory",
            "wheel output",
        ]
        .into_iter()
        .chain(args.iter().copied()),
    )
    .unwrap();
    let Task::BuildPythonWheel(args) = parsed.command else {
        panic!("expected wheel task")
    };
    args
}

#[test]
fn wheel_and_editable_profiles_follow_maturin_precedence() {
    let config = MaturinSettings {
        manifest_path: "Cargo.toml".into(),
        profile: Some("release".into()),
        editable_profile: Some("dev".into()),
    };
    assert_eq!(arguments(&[]).build_profile(&config).unwrap(), "release");
    assert_eq!(
        arguments(&["--editable"]).build_profile(&config).unwrap(),
        "dev"
    );
    assert_eq!(
        arguments(&["--editable", "--profile", "release"])
            .build_profile(&config)
            .unwrap(),
        "release"
    );
    let invalid = MaturinSettings {
        manifest_path: "Cargo.toml".into(),
        profile: Some("custom".into()),
        editable_profile: None,
    };
    assert!(arguments(&[]).build_profile(&invalid).is_err());
}

#[test]
fn cargo_options_keep_paths_as_arguments_and_resolve_from_the_project() {
    let options = arguments(&[
        "--target-dir",
        "target with spaces",
        "--jobs",
        "2",
        "--offline",
        "--frozen",
    ]);
    let project = Path::new("/source with spaces");
    assert_eq!(
        options.cargo_arguments(project),
        vec![
            OsString::from("--target-dir"),
            project.join("target with spaces").into_os_string(),
            "--jobs".into(),
            "2".into(),
            "--offline".into(),
            "--frozen".into(),
        ]
    );
    for args in [
        vec!["--profile", "custom"],
        vec!["--target", "first", "--target", "second"],
        vec!["--manifest-path", "other/Cargo.toml"],
        vec!["--bindings", "bin"],
        vec!["--prepare-only", "--wheel-directory", "output"],
    ] {
        assert!(
            Arguments::try_parse_from(["xtask", "build-python-wheel"].into_iter().chain(args))
                .is_err()
        );
    }
}

#[test]
fn wheel_copy_preserves_hard_linked_source() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("built.whl");
    let destination = temp.path().join("output.whl");
    fs::write(&source, b"complete wheel").unwrap();
    fs::hard_link(&source, &destination).unwrap();

    copy_wheel(&source, &destination).unwrap();

    assert_eq!(fs::read(&source).unwrap(), b"complete wheel");
    assert_eq!(fs::read(&destination).unwrap(), b"complete wheel");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
}

#[cfg(unix)]
#[test]
fn wheel_copy_preserves_source_in_symlinked_directory() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("output");
    fs::create_dir(&output).unwrap();
    let alias = temp.path().join("alias");
    std::os::unix::fs::symlink(&output, &alias).unwrap();
    let source = alias.join("built.whl");
    let destination = output.join("built.whl");
    fs::write(&source, b"complete wheel").unwrap();

    copy_wheel(&source, &destination).unwrap();

    assert_eq!(fs::read(&source).unwrap(), b"complete wheel");
    assert_eq!(fs::read(&destination).unwrap(), b"complete wheel");
    assert_eq!(fs::read_dir(output).unwrap().count(), 1);
}

#[test]
fn wheel_copy_replaces_output_only_after_success() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("built.whl");
    let destination = temp.path().join("output.whl");
    fs::write(&destination, b"previous wheel").unwrap();

    assert!(copy_wheel(&source, &destination).is_err());
    assert_eq!(fs::read(&destination).unwrap(), b"previous wheel");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);

    fs::write(&source, b"complete wheel").unwrap();
    let directory = temp.path().join("directory.whl");
    fs::create_dir(&directory).unwrap();
    fs::write(directory.join("keep"), b"untouched").unwrap();
    assert!(copy_wheel(&source, &directory).is_err());
    assert_eq!(fs::read(directory.join("keep")).unwrap(), b"untouched");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 3);

    copy_wheel(&source, &destination).unwrap();
    assert_eq!(fs::read(&source).unwrap(), b"complete wheel");
    assert_eq!(fs::read(&destination).unwrap(), b"complete wheel");
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 3);
}

#[test]
fn stale_data_is_removed_and_symlinks_are_rejected() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join(WHEEL_DATA);
    let script = data.join("scripts/delta-arrow-reader");
    fs::create_dir_all(script.parent().unwrap()).unwrap();
    fs::write(&script, "stale binary").unwrap();
    remove_generated_data(&data).unwrap();
    assert!(!data.exists());
    remove_generated_data(&data).unwrap();
    fs::write(&data, "not a directory").unwrap();
    assert!(remove_generated_data(&data).is_err());

    #[cfg(unix)]
    {
        fs::remove_file(&data).unwrap();
        let external = temp.path().join("external");
        fs::create_dir(&external).unwrap();
        fs::write(external.join("keep"), "untouched").unwrap();
        std::os::unix::fs::symlink(&external, &data).unwrap();
        assert!(remove_generated_data(&data).is_err());
        assert!(external.join("keep").is_file());
    }
}
