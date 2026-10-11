use std::{env, error::Error, fs, path::PathBuf, process::Command};

use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

struct ReleaseRequestRepo {
    directory: PathBuf,
    sha: String,
}

impl ReleaseRequestRepo {
    fn new(name: &str) -> Result<Self> {
        let directory =
            env::temp_dir().join(format!("python-release-{name}-{}", std::process::id()));
        fs::create_dir(&directory)?;
        let mut repo = Self {
            directory,
            sha: String::new(),
        };
        repo.git(&["init", "--initial-branch=main"])?;
        repo.git(&["config", "user.name", "Release request test"])?;
        repo.git(&["config", "user.email", "release-test@example.invalid"])?;
        repo.commit_manifest("[workspace.package]\nversion = '1.2.3'\n")?;
        repo.git(&["tag", "-a", "v1.2.3", "-m", "Release fixture"])?;
        Ok(repo)
    }

    fn git(&self, args: &[&str]) -> Result<String> {
        let output = Command::new("git")
            .args(args)
            .current_dir(&self.directory)
            .env("GIT_CONFIG_GLOBAL", self.directory.join("empty-gitconfig"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .output()?;
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    }

    fn commit_manifest(&mut self, manifest: &str) -> Result<()> {
        fs::write(self.directory.join("Cargo.toml"), manifest)?;
        self.git(&["add", "Cargo.toml"])?;
        self.git(&["commit", "-m", "test: set release manifest"])?;
        self.sha = self.git(&["rev-parse", "HEAD"])?;
        Ok(())
    }

    fn assert_request(
        &self,
        event_name: &str,
        event: Value,
        reference: &str,
        sha: &str,
        expected: std::result::Result<&str, &str>,
    ) -> Result<()> {
        let event_path = self.directory.join("event.json");
        let output_path = self.directory.join("outputs");
        fs::write(&event_path, serde_json::to_vec(&event)?)?;
        fs::write(&output_path, "existing=kept\n")?;
        let result = Command::new(env!("CARGO_BIN_EXE_xtask"))
            .arg("check-python-release-request")
            .current_dir(&self.directory)
            .env("GIT_CONFIG_GLOBAL", self.directory.join("empty-gitconfig"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GITHUB_EVENT_PATH", event_path)
            .env("GITHUB_OUTPUT", &output_path)
            .env("GITHUB_EVENT_NAME", event_name)
            .env("GITHUB_REF", reference)
            .env("GITHUB_SHA", sha)
            .output()?;
        let stderr = String::from_utf8_lossy(&result.stderr);
        let outputs = fs::read_to_string(output_path)?;
        match expected {
            Ok(mode) => {
                assert!(result.status.success(), "{event_name} {event}: {stderr}");
                assert_eq!(
                    outputs,
                    format!(
                        "existing=kept\nsource-ref={sha}\nexpected-version=1.2.3\npublish-to={mode}\n"
                    )
                );
            }
            Err(message) => {
                assert!(!result.status.success(), "accepted {event_name} {event}");
                assert!(stderr.contains(message), "expected {message:?}: {stderr}");
                assert_eq!(outputs, "existing=kept\n", "failed checks emitted outputs");
            }
        }
        Ok(())
    }
}

impl Drop for ReleaseRequestRepo {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn manual_modes_require_an_exact_version_and_pypi_requires_a_tag() -> Result<()> {
    let repo = ReleaseRequestRepo::new("manual")?;
    repo.assert_request(
        "workflow_dispatch",
        json!({"inputs": {"expected-version": "1.2.3"}}),
        "refs/heads/main",
        &repo.sha,
        Ok("none"),
    )?;
    for mode in ["none", "testpypi", "pypi"] {
        for reference in ["refs/heads/main", "refs/tags/v1.2.3"] {
            let expected = if mode == "pypi" && reference == "refs/heads/main" {
                Err("PyPI requires a version tag")
            } else {
                Ok(mode)
            };
            repo.assert_request(
                "workflow_dispatch",
                json!({"inputs": {"publish-to": mode, "expected-version": "1.2.3"}}),
                reference,
                &repo.sha,
                expected,
            )?;
        }
    }
    for inputs in [
        json!({}),
        json!(null),
        json!({"expected-version": "1.2.4"}),
        json!({"expected-version": "v1.2.3"}),
        json!({"expected-version": "1.2.3\npublish-to=pypi"}),
        json!({"expected-version": 123}),
    ] {
        repo.assert_request(
            "workflow_dispatch",
            json!({"inputs": inputs}),
            "refs/heads/main",
            &repo.sha,
            Err("expected-version must match Cargo version"),
        )?;
    }
    for mode in [json!("unknown"), json!(null), json!(false)] {
        repo.assert_request(
            "workflow_dispatch",
            json!({"inputs": {"publish-to": mode, "expected-version": "1.2.3"}}),
            "refs/heads/main",
            &repo.sha,
            Err("publish-to must be"),
        )?;
    }
    Ok(())
}

#[test]
fn automatic_pypi_publication_requires_a_stable_published_release() -> Result<()> {
    let repo = ReleaseRequestRepo::new("published")?;
    let event = json!({
        "action": "published",
        "release": {"tag_name": "v1.2.3", "draft": false, "prerelease": false},
    });
    repo.assert_request(
        "release",
        event.clone(),
        "refs/tags/v1.2.3",
        &repo.sha,
        Ok("pypi"),
    )?;
    for (field, value, error) in [
        ("draft", json!(true), "only stable published releases"),
        ("draft", json!(null), "only stable published releases"),
        ("draft", json!(0), "only stable published releases"),
        ("prerelease", json!(true), "only stable published releases"),
        ("prerelease", json!(null), "only stable published releases"),
        ("tag_name", json!("other-v1.2.3"), "release tag must match"),
        ("tag_name", json!("v1.2.4"), "release tag must match"),
        ("tag_name", json!("v1.2.3-rc.1"), "release tag must match"),
    ] {
        let mut invalid = event.clone();
        invalid["release"][field] = value;
        repo.assert_request(
            "release",
            invalid,
            "refs/tags/v1.2.3",
            &repo.sha,
            Err(error),
        )?;
    }
    for (name, action, error) in [
        ("release", "edited", "only stable published releases"),
        ("push", "published", "unsupported release event"),
        ("pull_request", "published", "unsupported release event"),
    ] {
        let mut invalid = event.clone();
        invalid["action"] = json!(action);
        repo.assert_request(name, invalid, "refs/tags/v1.2.3", &repo.sha, Err(error))?;
    }
    for (reference, error) in [
        ("refs/heads/main", "PyPI requires a version tag"),
        ("refs/tags/other-v1.2.3", "selected tag must match"),
        ("refs/tags/v1.2.4", "selected tag must match"),
    ] {
        repo.assert_request("release", event.clone(), reference, &repo.sha, Err(error))?;
    }
    Ok(())
}

#[test]
fn refs_and_commits_must_match_the_checkout_and_version_tag() -> Result<()> {
    let mut repo = ReleaseRequestRepo::new("refs")?;
    let event = json!({"inputs": {"expected-version": "1.2.3"}});
    for (reference, error) in [
        ("refs/tags/v1.2.4", "selected tag must match"),
        ("refs/tags/other-v1.2.3", "selected tag must match"),
        (
            "refs/pull/1/merge",
            "other modes accept branches or version tags",
        ),
    ] {
        repo.assert_request(
            "workflow_dispatch",
            event.clone(),
            reference,
            &repo.sha,
            Err(error),
        )?;
    }
    for (sha, error) in [
        ("0".repeat(40), "checkout does not match the event commit"),
        (repo.sha[..7].to_owned(), "source must be a full commit SHA"),
        ("A".repeat(40), "source must be a full commit SHA"),
        ("g".repeat(40), "source must be a full commit SHA"),
    ] {
        repo.assert_request(
            "workflow_dispatch",
            event.clone(),
            "refs/heads/main",
            &sha,
            Err(error),
        )?;
    }
    repo.git(&[
        "commit",
        "--allow-empty",
        "-m",
        "test: move past release tag",
    ])?;
    repo.sha = repo.git(&["rev-parse", "HEAD"])?;
    repo.assert_request(
        "workflow_dispatch",
        event.clone(),
        "refs/tags/v1.2.3",
        &repo.sha,
        Err("selected tag does not point to the event commit"),
    )?;
    repo.git(&["tag", "--delete", "v1.2.3"])?;
    repo.assert_request(
        "workflow_dispatch",
        event.clone(),
        "refs/tags/v1.2.3",
        &repo.sha,
        Err("git failed"),
    )?;
    repo.git(&["tag", "v1.2.3"])?;
    repo.assert_request(
        "workflow_dispatch",
        event,
        "refs/tags/v1.2.3",
        &repo.sha,
        Ok("none"),
    )
}

#[test]
fn version_comes_from_the_committed_workspace_manifest() -> Result<()> {
    let mut repo = ReleaseRequestRepo::new("version")?;
    let event = json!({"inputs": {"expected-version": "1.2.3"}});
    fs::write(
        repo.directory.join("Cargo.toml"),
        "[workspace.package]\nversion = '9.9.9'\n",
    )?;
    repo.assert_request(
        "workflow_dispatch",
        event.clone(),
        "refs/heads/main",
        &repo.sha,
        Ok("none"),
    )?;
    repo.commit_manifest("workspace.package.version = '1.2.3'\n[package]\nversion = '8.9.0'\n")?;
    repo.assert_request(
        "workflow_dispatch",
        event,
        "refs/heads/main",
        &repo.sha,
        Ok("none"),
    )?;
    for version in [
        "1.2.3-rc.1",
        "1.2.3+build",
        "01.2.3",
        "1.02.3",
        "1.2.03",
        "1.2",
        "1.2.3.4",
        "1..3",
        "",
    ] {
        repo.commit_manifest(&format!("[workspace.package]\nversion = '{version}'\n"))?;
        repo.assert_request(
            "workflow_dispatch",
            json!({"inputs": {"expected-version": version}}),
            "refs/heads/main",
            &repo.sha,
            Err("stable X.Y.Z Cargo version"),
        )?;
    }
    for manifest in [
        "[package]\nversion = '1.2.3'\n",
        "[workspace.package]\nversion = 123\n",
    ] {
        repo.commit_manifest(manifest)?;
        repo.assert_request(
            "workflow_dispatch",
            json!({"inputs": {"expected-version": "1.2.3"}}),
            "refs/heads/main",
            &repo.sha,
            Err("workspace.package.version must be a string"),
        )?;
    }
    Ok(())
}
