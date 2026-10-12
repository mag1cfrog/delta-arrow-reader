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
