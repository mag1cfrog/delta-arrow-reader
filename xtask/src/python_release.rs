use std::{env, fs, io::Write};

use serde_json::Value;

use crate::{Result, run};

pub fn validate_release_request() -> Result<()> {
    let event: Value = serde_json::from_slice(&fs::read(env::var("GITHUB_EVENT_PATH")?)?)?;
    let event_name = env::var("GITHUB_EVENT_NAME")?;
    let reference = env::var("GITHUB_REF")?;
    let source_sha = env::var("GITHUB_SHA")?;

    if source_sha.len() != 40
        || !source_sha
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err("source must be a full commit SHA".into());
    }
    let head = String::from_utf8(run("git", &["rev-parse", "HEAD"])?)?;
    if head.trim() != source_sha {
        return Err("checkout does not match the event commit".into());
    }
    // Read committed bytes so local edits cannot change the release version.
    let manifest = String::from_utf8(run("git", &["show", "HEAD:Cargo.toml"])?)?;
    let manifest: toml::Table = manifest.parse()?;
    let version = manifest
        .get("workspace")
        .and_then(|value| value.get("package"))
        .and_then(|value| value.get("version"))
        .and_then(toml::Value::as_str)
        .ok_or("workspace.package.version must be a string")?;
    let parts: Vec<_> = version.split('.').collect();
    if parts.len() != 3
        || parts.iter().any(|part| {
            part.is_empty()
                || !part.bytes().all(|byte| byte.is_ascii_digit())
                || (part.len() > 1 && part.starts_with('0'))
        })
    {
        return Err("Python releases require a stable X.Y.Z Cargo version".into());
    }

    let tag = format!("v{version}");
    let publish_to = match event_name.as_str() {
        "workflow_dispatch" => {
            let inputs = &event["inputs"];
            let publish_to = match inputs.get("publish-to") {
                Some(value) => value.as_str().ok_or("publish-to must be a string")?,
                None => "none",
            };
            if !matches!(publish_to, "none" | "testpypi" | "pypi") {
                return Err("publish-to must be none, testpypi, or pypi".into());
            }
            if inputs["expected-version"].as_str() != Some(version) {
                return Err(format!("expected-version must match Cargo version {version}").into());
            }
            publish_to
        }
        "release" => {
            let release = &event["release"];
            if event["action"].as_str() != Some("published")
                || release["draft"].as_bool() != Some(false)
                || release["prerelease"].as_bool() != Some(false)
            {
                return Err("only stable published releases are supported".into());
            }
            if release["tag_name"].as_str() != Some(&tag) {
                return Err(format!("release tag must match {tag}").into());
            }
            "pypi"
        }
        _ => return Err(format!("unsupported release event: {event_name}").into()),
    };

    if reference.starts_with("refs/tags/") {
        if reference != format!("refs/tags/{tag}") {
            return Err(format!("selected tag must match {tag}").into());
        }
        let tag_sha = String::from_utf8(run(
            "git",
            &["rev-parse", "--verify", &format!("{reference}^{{commit}}")],
        )?)?;
        if tag_sha.trim() != source_sha {
            return Err("selected tag does not point to the event commit".into());
        }
    } else if publish_to == "pypi" || !reference.starts_with("refs/heads/") {
        return Err(
            "PyPI requires a version tag; other modes accept branches or version tags".into(),
        );
    }

    let mut output = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(env::var("GITHUB_OUTPUT")?)?;
    write!(
        output,
        "source-ref={source_sha}\nexpected-version={version}\npublish-to={publish_to}\n"
    )?;
    Ok(())
}
