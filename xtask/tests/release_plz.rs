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
    fn new(name: &str) -> Result<Self> {
        let directory = env::temp_dir().join(format!("release-plz-{name}-{}", std::process::id()));
        fs::create_dir(&directory)?;
        let workspace = Self { directory };
        let root_manifest: toml::Value = toml::from_str(include_str!("../../Cargo.toml"))?;
        let exclude = serde_json::to_string(&root_manifest["package"]["exclude"])?;
        workspace.write(
            "Cargo.toml",
            &format!(
                r#"[workspace]
members = ["crates/python", "crates/cli", "xtask"]
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
        workspace.write(
            "pyproject.toml",
            "[project]\nname = \"delta-arrow-reader\"\nrequires-python = \">=3.10\"\n",
        )?;
        workspace.write("delta_arrow_reader.pyi", "def read() -> int: ...\n")?;
        for (name, source, code) in [
            ("python", "lib.rs", "pub fn python() {}\n"),
            ("cli", "main.rs", "fn main() {}\n"),
        ] {
            workspace.write(&format!("crates/{name}/src/{source}"), code)?;
            workspace.write(
                &format!("crates/{name}/Cargo.toml"),
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
        Ok(workspace)
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
        // The local baseline avoids contacting a registry or publishing anything.
        self.run(
            "release-plz",
            &[
                "update",
                "--registry-manifest-path",
                "../published/Cargo.toml",
                "--repo-url",
                "https://github.com/mag1cfrog/delta-arrow-reader",
                "--release-date",
                "2026-01-01",
            ],
        )?;
        let metadata: Value = serde_json::from_str(
            &self.run("cargo", &["metadata", "--no-deps", "--format-version=1"])?,
        )?;
        let packages = metadata["packages"].as_array().unwrap();
        assert_eq!(packages.len(), 4);
        for package in packages {
            let expected_version = if package["name"] == "xtask" {
                "0.0.0"
            } else {
                "0.6.3"
            };
            assert_eq!(package["version"], expected_version, "{}", package["name"]);
        }
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
    ReleasePlanningWorkspace::new("source")?.assert_shared_release_update(
        "crates/python/src/lib.rs",
        "pub fn python() {}\npub fn python_only_feature() {}\n",
    )
}

#[test]
#[ignore = "requires release-plz 0.3.171 on PATH"]
fn python_metadata_only_change_updates_the_shared_release() -> Result<()> {
    ReleasePlanningWorkspace::new("metadata")?.assert_shared_release_update(
        "pyproject.toml",
        "[project]\nname = \"delta-arrow-reader\"\nrequires-python = \">=3.11\"\n",
    )
}

#[test]
#[ignore = "requires release-plz 0.3.171 on PATH"]
fn python_stubs_only_change_updates_the_shared_release() -> Result<()> {
    ReleasePlanningWorkspace::new("stubs")?.assert_shared_release_update(
        "delta_arrow_reader.pyi",
        "def read() -> int: ...\ndef version() -> str: ...\n",
    )
}
