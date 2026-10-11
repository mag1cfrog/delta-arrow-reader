use std::{env, error::Error, fs, path::PathBuf, process::Command};

use serde_json::Value;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const RELEASE_CONFIG: &str = include_str!("../../release-plz.toml");

#[test]
fn python_crate_uses_the_shared_release_without_separate_publication() -> Result<()> {
    let config: toml::Value = toml::from_str(RELEASE_CONFIG)?;
    let packages = config["package"].as_array().unwrap();
    let python = packages
        .iter()
        .find(|package| package["name"].as_str() == Some("delta-arrow-reader-python"))
        .unwrap();
    assert_eq!(python["git_only"].as_bool(), Some(true));
    for option in [
        "publish",
        "git_tag_enable",
        "git_release_enable",
        "changelog_update",
    ] {
        assert_eq!(python[option].as_bool(), Some(false), "{option}");
    }
    let cli = packages
        .iter()
        .find(|package| package["name"].as_str() == Some("delta-arrow-reader-cli"))
        .unwrap();
    assert_eq!(cli["release"].as_bool(), Some(false));
    let xtask = packages
        .iter()
        .find(|package| package["name"].as_str() == Some("xtask"))
        .unwrap();
    assert_eq!(xtask["release"].as_bool(), Some(false));
    for option in ["git_tag_name", "git_release_name"] {
        assert_eq!(config["workspace"][option].as_str(), Some("v{{ version }}"));
    }
    let manifest: toml::Value = toml::from_str(include_str!(
        "../../crates/delta-arrow-reader-python/Cargo.toml"
    ))?;
    assert_eq!(manifest["package"]["publish"].as_bool(), Some(false));
    Ok(())
}

struct ReleasePlanningWorkspace {
    directory: PathBuf,
}

impl ReleasePlanningWorkspace {
    fn new(name: &str, python_at_baseline: bool) -> Result<Self> {
        let directory = env::temp_dir().join(format!("release-plz-{name}-{}", std::process::id()));
        fs::create_dir(&directory)?;
        let workspace = Self { directory };
        let root_manifest: toml::Value = toml::from_str(include_str!("../../Cargo.toml"))?;
        let exclude = serde_json::to_string(&root_manifest["package"]["exclude"])?;
        workspace.write(
            "Cargo.toml",
            &format!(
                r#"[workspace]
members = ["xtask"]
default-members = ["."]
resolver = "3"

[workspace.package]
version = "0.6.2"

[package]
name = "delta-arrow-reader"
version.workspace = true
edition = "2024"
description = "Release configuration fixture"
license = "Apache-2.0"
repository = "https://github.com/mag1cfrog/delta-arrow-reader"
exclude = {exclude}
"#
            ),
        )?;
        workspace.write("src/lib.rs", "pub fn read() {}\n")?;
        if python_at_baseline {
            workspace.add_python()?;
        }
        workspace.write(
            "xtask/Cargo.toml",
            "[package]\nname = \"xtask\"\nversion = \"0.0.0\"\nedition = \"2024\"\npublish = false\n",
        )?;
        workspace.write("xtask/src/main.rs", "fn main() {}\n")?;
        workspace.write("release-plz.toml", RELEASE_CONFIG)?;
        workspace.run("git", &["init", "--initial-branch=main"])?;
        workspace.run(
            "git",
            &["config", "user.name", "Release configuration test"],
        )?;
        workspace.run(
            "git",
            &["config", "user.email", "release-test@example.invalid"],
        )?;
        workspace.run("cargo", &["generate-lockfile"])?;
        workspace.run("git", &["add", "."])?;
        workspace.run("git", &["commit", "-m", "chore: initial release"])?;
        workspace.run("git", &["tag", "v0.6.2"])?;
        workspace.run(
            "git",
            &["clone", "--quiet", "--no-hardlinks", ".", "../published"],
        )?;
        if !python_at_baseline {
            workspace.add_python()?;
            workspace.run("cargo", &["generate-lockfile"])?;
            workspace.run("git", &["add", "."])?;
            workspace.run(
                "git",
                &["commit", "-m", "feat(python): introduce Python bindings"],
            )?;
        }
        Ok(workspace)
    }

    fn add_python(&self) -> Result<()> {
        let manifest = fs::read_to_string(self.directory.join("project/Cargo.toml"))?;
        self.write(
            "Cargo.toml",
            &manifest.replace(
                "members = [\"xtask\"]",
                "members = [\"crates/delta-arrow-reader-python\", \"crates/delta-arrow-reader-cli\", \"xtask\"]",
            ),
        )?;
        self.write(
            "pyproject.toml",
            "[project]\nname = \"delta-arrow-reader\"\nrequires-python = \">=3.10\"\n",
        )?;
        self.write("delta_arrow_reader.pyi", "def read() -> int: ...\n")?;
        for (name, source, code) in [
            ("python", "lib.rs", "pub fn python() {}\n"),
            ("cli", "main.rs", "fn main() {}\n"),
        ] {
            self.write(
                &format!("crates/delta-arrow-reader-{name}/src/{source}"),
                code,
            )?;
            self.write(
                &format!("crates/delta-arrow-reader-{name}/Cargo.toml"),
                &format!(
                    r#"[package]
name = "delta-arrow-reader-{name}"
version.workspace = true
edition = "2024"
publish = false

[dependencies]
delta-arrow-reader = {{ version = "0.6.2", path = "../.." }}
"#
                ),
            )?;
        }
        Ok(())
    }

    fn write(&self, path: &str, contents: &str) -> Result<()> {
        let path = self.directory.join("project").join(path);
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, contents)?;
        Ok(())
    }

    fn run(&self, program: &str, args: &[&str]) -> Result<String> {
        let output = Command::new(program)
            .args(args)
            .current_dir(self.directory.join("project"))
            .env("CARGO_NET_OFFLINE", "true")
            .env("CARGO_TARGET_DIR", self.directory.join("target"))
            .env("GIT_CONFIG_GLOBAL", self.directory.join("empty-gitconfig"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()?;
        assert!(
            output.status.success(),
            "{program} {args:?}:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(String::from_utf8(output.stdout)?)
    }

    fn prepare_config(&self) -> Result<String> {
        self.run(
            env!("CARGO_BIN_EXE_xtask"),
            &["prepare-release-pr-config", "../release-plz.toml"],
        )?;
        assert_eq!(
            fs::read_to_string(self.directory.join("project/release-plz.toml"))?,
            RELEASE_CONFIG,
        );
        Ok(fs::read_to_string(self.directory.join("release-plz.toml"))?)
    }

    fn update(&self) -> Result<()> {
        self.prepare_config()?;
        // The local baseline avoids contacting a registry or publishing anything.
        self.run(
            "release-plz",
            &[
                "update",
                "--config",
                "../release-plz.toml",
                "--registry-manifest-path",
                "../published/Cargo.toml",
                "--repo-url",
                "https://github.com/mag1cfrog/delta-arrow-reader",
                "--release-date",
                "2026-01-01",
            ],
        )?;
        Ok(())
    }

    fn assert_version(&self, expected: &str) -> Result<()> {
        let metadata: Value = serde_json::from_str(
            &self.run("cargo", &["metadata", "--no-deps", "--format-version=1"])?,
        )?;
        let packages = metadata["packages"].as_array().unwrap();
        assert_eq!(packages.len(), 4);
        for package in packages {
            let expected_version = if package["name"] == "xtask" {
                "0.0.0"
            } else {
                expected
            };
            assert_eq!(package["version"], expected_version, "{}", package["name"]);
        }
        Ok(())
    }

    fn assert_shared_release_update(&self, changed_path: &str, contents: &str) -> Result<()> {
        self.write(changed_path, contents)?;
        self.run("git", &["add", changed_path])?;
        self.run(
            "git",
            &[
                "commit",
                "-m",
                "fix(python): update Python-only package input",
            ],
        )?;
        assert_eq!(
            self.run("git", &["diff", "--name-only", "v0.6.2", "HEAD"])?
                .trim(),
            changed_path
        );
        self.update()?;
        self.assert_version("0.6.3")?;
        let project = self.directory.join("project");
        let changelog = fs::read_to_string(project.join("CHANGELOG.md"))?;
        assert!(
            changelog.contains("update Python-only package input"),
            "{changelog}"
        );
        assert!(
            changelog.contains("/compare/v0.6.2...v0.6.3"),
            "{changelog}"
        );
        let files = self.run("git", &["ls-files", "--cached", "--others", "-z"])?;
        let changelogs: Vec<_> = files
            .split_terminator('\0')
            .filter(|path| path.rsplit('/').next() == Some("CHANGELOG.md"))
            .collect();
        assert_eq!(changelogs, ["CHANGELOG.md"]);
        Ok(())
    }
}

impl Drop for ReleasePlanningWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[test]
#[ignore = "requires release-plz 0.3.171 on PATH"]
fn python_only_change_updates_the_shared_release() -> Result<()> {
    ReleasePlanningWorkspace::new("source", true)?.assert_shared_release_update(
        "crates/delta-arrow-reader-python/src/lib.rs",
        "pub fn python() {}\npub fn python_only_feature() {}\n",
    )
}

#[test]
#[ignore = "requires release-plz 0.3.171 on PATH"]
fn python_metadata_only_change_updates_the_shared_release() -> Result<()> {
    ReleasePlanningWorkspace::new("metadata", true)?.assert_shared_release_update(
        "pyproject.toml",
        "[project]\nname = \"delta-arrow-reader\"\nrequires-python = \">=3.11\"\n",
    )
}

#[test]
#[ignore = "requires release-plz 0.3.171 on PATH"]
fn python_stubs_only_change_updates_the_shared_release() -> Result<()> {
    ReleasePlanningWorkspace::new("stubs", true)?.assert_shared_release_update(
        "delta_arrow_reader.pyi",
        "def read() -> int: ...\ndef version() -> str: ...\n",
    )
}

#[test]
fn bootstrap_overrides_only_python_until_the_highest_stable_tag_contains_it() -> Result<()> {
    let workspace = ReleasePlanningWorkspace::new("config-bootstrap", false)?;
    for tag in ["v99.0.0-rc.1", "v01.0.0", "v1.0", "other-v99.0.0"] {
        workspace.run("git", &["tag", tag])?;
    }
    let tags = workspace.run("git", &["tag"])?;
    let mut prepared: toml::Value = toml::from_str(&workspace.prepare_config()?)?;
    let python = prepared["package"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|package| package["name"].as_str() == Some("delta-arrow-reader-python"))
        .unwrap();
    assert_eq!(
        python.as_table_mut().unwrap().remove("git_tag_name"),
        Some("python-bootstrap-v{{ version }}".into()),
    );
    assert_eq!(prepared, toml::from_str::<toml::Value>(RELEASE_CONFIG)?);
    assert_eq!(workspace.run("git", &["tag"])?, tags);
    assert!(workspace.run("git", &["status", "--porcelain"])?.is_empty());

    workspace.run("git", &["tag", "-a", "v0.10.0", "-m", "Shared release"])?;
    workspace.run("git", &["tag", "v0.9.0", "v0.6.2"])?;
    assert_eq!(workspace.prepare_config()?, RELEASE_CONFIG);
    Ok(())
}

#[test]
fn no_stable_tags_already_means_an_initial_release() -> Result<()> {
    let workspace = ReleasePlanningWorkspace::new("config-no-tags", false)?;
    workspace.run("git", &["tag", "-d", "v0.6.2"])?;
    workspace.run("git", &["tag", "v0.7.0-rc.1"])?;
    assert_eq!(workspace.prepare_config()?, RELEASE_CONFIG);
    Ok(())
}

#[test]
fn invalid_baselines_stop_before_writing_a_config() -> Result<()> {
    for (name, tag) in [
        ("config-invalid-tag", "v9.0.0"),
        ("config-bootstrap-collision", "python-bootstrap-v0.6.2"),
    ] {
        let workspace = ReleasePlanningWorkspace::new(name, false)?;
        let object = workspace.run("git", &["rev-parse", "HEAD:Cargo.toml"])?;
        workspace.run("git", &["tag", tag, object.trim()])?;
        let destination = workspace.directory.join("release-plz.toml");
        fs::write(&destination, "existing configuration\n")?;
        let output = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .args(["prepare-release-pr-config", "../release-plz.toml"])
            .current_dir(workspace.directory.join("project"))
            .output()?;
        assert!(!output.status.success(), "{output:?}");
        assert_eq!(fs::read_to_string(destination)?, "existing configuration\n");
        assert!(workspace.run("git", &["status", "--porcelain"])?.is_empty());
    }
    Ok(())
}

#[test]
#[ignore = "requires release-plz 0.3.171 on PATH"]
fn first_python_release_and_subsequent_binding_changes_use_the_shared_version() -> Result<()> {
    let workspace = ReleasePlanningWorkspace::new("initial-python-release", false)?;
    workspace.write("src/lib.rs", "pub fn read() {}\npub fn core_fix() {}\n")?;
    workspace.run("git", &["add", "."])?;
    workspace.run("git", &["commit", "-m", "fix: improve the core reader"])?;
    workspace.update()?;
    workspace.assert_version("0.6.3")?;
    let changelog_path = workspace.directory.join("project/CHANGELOG.md");
    let changelog = fs::read_to_string(&changelog_path)?;
    assert!(
        changelog.contains("introduce Python bindings"),
        "{changelog}"
    );
    assert!(changelog.contains("improve the core reader"), "{changelog}");
    assert_eq!(workspace.run("git", &["tag"])?.trim(), "v0.6.2");

    workspace.run("git", &["switch", "-c", "release-candidate"])?;
    workspace.run("git", &["add", "."])?;
    workspace.run("git", &["commit", "-m", "chore: release v0.6.3"])?;
    workspace.update()?;
    assert!(workspace.run("git", &["status", "--porcelain"])?.is_empty());

    workspace.run("git", &["switch", "main"])?;
    workspace.write(
        "src/lib.rs",
        "pub fn read() {}\npub fn core_fix() {}\npub fn follow_up_fix() {}\n",
    )?;
    workspace.run("git", &["add", "."])?;
    workspace.run(
        "git",
        &["commit", "-m", "fix: follow up on the core reader"],
    )?;
    workspace.update()?;
    workspace.assert_version("0.6.3")?;
    let changelog = fs::read_to_string(&changelog_path)?;
    for change in [
        "introduce Python bindings",
        "improve the core reader",
        "follow up on the core reader",
    ] {
        assert_eq!(changelog.matches(change).count(), 1, "{changelog}");
    }

    workspace.run("git", &["add", "."])?;
    workspace.run("git", &["commit", "-m", "chore: release v0.6.3"])?;
    workspace.run("git", &["tag", "v0.6.3"])?;
    workspace.run(
        "git",
        &["-C", "../published", "fetch", "--quiet", "origin", "--tags"],
    )?;
    workspace.run("git", &["-C", "../published", "checkout", "v0.6.3"])?;
    assert_eq!(workspace.prepare_config()?, RELEASE_CONFIG);
    workspace.update()?;
    assert!(workspace.run("git", &["status", "--porcelain"])?.is_empty());

    workspace.write(
        "crates/delta-arrow-reader-python/src/lib.rs",
        "pub fn python() {}\npub fn binding_fix() {}\n",
    )?;
    workspace.run("git", &["add", "."])?;
    workspace.run("git", &["commit", "-m", "fix(python): repair the binding"])?;
    workspace.update()?;
    workspace.assert_version("0.6.4")?;
    let changelog = fs::read_to_string(changelog_path)?;
    assert!(changelog.contains("repair the binding"), "{changelog}");
    assert!(
        changelog.contains("/compare/v0.6.3...v0.6.4"),
        "{changelog}"
    );
    assert_eq!(
        workspace.run("git", &["tag"])?.lines().collect::<Vec<_>>(),
        ["v0.6.2", "v0.6.3"],
    );
    assert!(
        !workspace
            .directory
            .join("project/crates/delta-arrow-reader-python/CHANGELOG.md")
            .exists()
    );
    Ok(())
}

#[test]
fn release_pr_uses_a_prepared_config_outside_the_checkout() -> Result<()> {
    let workflow: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/release-plz.yml"))?;
    let steps = workflow["jobs"]["release-pr"]["steps"].as_array().unwrap();
    let prepare = steps
        .iter()
        .position(|step| step["name"] == "Prepare release PR configuration")
        .unwrap();
    let release_pr = steps
        .iter()
        .position(|step| step["with"]["command"] == "release-pr")
        .unwrap();
    assert!(prepare < release_pr);
    assert_eq!(
        steps[prepare]["run"],
        "cargo xtask prepare-release-pr-config \"$RUNNER_TEMP/release-plz.toml\"",
    );
    assert_eq!(
        steps[release_pr]["with"]["config"],
        "${{ runner.temp }}/release-plz.toml",
    );
    let checkout = steps
        .iter()
        .find(|step| step["name"] == "Checkout")
        .unwrap();
    assert_eq!(checkout["with"]["fetch-depth"], 0);
    let publish = workflow["jobs"]["release"]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["with"]["command"] == "release")
        .unwrap();
    assert!(publish["with"]["config"].is_null());
    Ok(())
}
