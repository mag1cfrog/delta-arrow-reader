use std::{fs, path::Path};

use crate::{Result, run};

fn stable_version(tag: &str) -> Option<[u64; 3]> {
    let parts: Vec<_> = tag.strip_prefix('v')?.split('.').collect();
    let [major, minor, patch]: [&str; 3] = parts.try_into().ok()?;
    let version = [
        major.parse().ok()?,
        minor.parse().ok()?,
        patch.parse().ok()?,
    ];
    (format!("v{}.{}.{}", version[0], version[1], version[2]) == tag).then_some(version)
}

pub fn prepare_config(destination: &Path) -> Result<()> {
    let mut source = fs::read_to_string("release-plz.toml")?;
    let tags = String::from_utf8(run("git", &["tag", "--list", "v*"])?)?;
    // Match release-plz 0.3.171: select the highest stable version, not the newest commit.
    let latest = tags
        .lines()
        .filter_map(|tag| stable_version(tag).map(|version| (version, tag)))
        .max();
    if let Some((_, tag)) = latest {
        let files = run(
            "git",
            &[
                "ls-tree",
                "--name-only",
                &format!("refs/tags/{tag}^{{commit}}"),
                "--",
                "crates/delta-arrow-reader-python/Cargo.toml",
            ],
        )?;
        if files.is_empty() {
            let mut config: toml::Table = source.parse()?;
            let python = config
                .get_mut("package")
                .and_then(toml::Value::as_array_mut)
                .and_then(|packages| {
                    packages.iter_mut().find(|package| {
                        package.get("name").and_then(toml::Value::as_str)
                            == Some("delta-arrow-reader-python")
                    })
                })
                .and_then(toml::Value::as_table_mut)
                .ok_or("release-plz config must contain delta-arrow-reader-python")?;
            if !run("git", &["tag", "--list", "python-bootstrap-v*"])?.is_empty() {
                return Err("the Python bootstrap tag prefix must have no existing tags".into());
            }
            // shortcut: release-plz rejects missing baseline packages; remove when upstream handles initial releases.
            python.insert(
                "git_tag_name".into(),
                "python-bootstrap-v{{ version }}".into(),
            );
            source = toml::to_string(&config)?;
            println!("{tag} predates the Python crate; plan its initial release");
        }
    }
    fs::write(destination, source)?;
    Ok(())
}
