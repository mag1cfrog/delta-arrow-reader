use std::{
    env,
    error::Error,
    fs, io,
    path::Path,
    process::{Command, Stdio},
};

use serde::Deserialize;

mod python_release;
mod python_upload;
mod python_wheels;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

const CACHE_PREFIX: &str = "v0-rust-core-ci-targets-Windows_NT-x64-";
const CACHE_REF: &str = "refs/heads/main";

#[derive(Deserialize)]
struct SccacheStats {
    stats: WriteStats,
}

#[derive(Deserialize)]
struct WriteStats {
    cache_write_errors: usize,
}

#[derive(Deserialize)]
struct CachePage {
    actions_caches: Vec<Cache>,
}

#[derive(Deserialize)]
struct Cache {
    id: u64,
    key: String,
    r#ref: String,
    last_accessed_at: String,
}

fn run(program: &str, args: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new(program)
        .args(args)
        .stderr(Stdio::inherit())
        .output()?;
    if !output.status.success() {
        return Err(format!("{program} failed: {}", output.status).into());
    }
    Ok(output.stdout)
}

fn check_writes(stats: &WriteStats, log: &str) -> Result<()> {
    let count = stats.cache_write_errors;
    if count == 0 {
        return Ok(());
    }
    let errors: Vec<_> = log
        .lines()
        .rev()
        .filter(|line| line.contains("Error executing cache write:"))
        .take(count)
        .collect();
    // Concurrent builds can upload the same immutable entry before either sees it.
    if errors.len() != count || errors.iter().any(|line| !line.contains("AlreadyExists")) {
        let startup_errors = log
            .lines()
            .filter(|line| line.contains("storage write check failed:"))
            .collect::<Vec<_>>()
            .join("\n");
        return Err(format!(
            "sccache reported {count} cache write errors\n{startup_errors}\n{}",
            errors.join("\n")
        )
        .into());
    }
    println!("Ignoring {count} cache entries that already exist");
    Ok(())
}

fn check_cache_writes() -> Result<()> {
    let stats: SccacheStats = serde_json::from_slice(&run(
        "sccache",
        &["--show-stats", "--stats-format", "json"],
    )?)?;
    let log =
        match fs::read(env::var_os("SCCACHE_ERROR_LOG").ok_or("SCCACHE_ERROR_LOG is required")?) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error.into()),
        };
    check_writes(&stats.stats, &String::from_utf8_lossy(&log))
}

fn stale_caches(pages: Vec<CachePage>) -> Vec<Cache> {
    let mut caches: Vec<_> = pages
        .into_iter()
        .flat_map(|page| page.actions_caches)
        .filter(|cache| cache.r#ref == CACHE_REF && cache.key.starts_with(CACHE_PREFIX))
        .collect();
    caches.sort_by(|a, b| b.last_accessed_at.cmp(&a.last_accessed_at));
    caches.into_iter().skip(2).collect()
}

fn prune_ci_caches() -> Result<()> {
    let endpoint = format!("repos/{}/actions/caches", env::var("GH_REPO")?);
    let pages = serde_json::from_slice(&run(
        "gh",
        &[
            "api",
            "--paginate",
            "--slurp",
            &format!("{endpoint}?key={CACHE_PREFIX}&ref={CACHE_REF}&per_page=100"),
        ],
    )?)?;
    for cache in stale_caches(pages) {
        println!("Deleting superseded Windows target cache: {}", cache.key);
        run(
            "gh",
            &[
                "api",
                "--method",
                "DELETE",
                &format!("{endpoint}/{}", cache.id),
            ],
        )?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = env::args().skip(1).collect();
    let args: Vec<_> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["check-cache-writes"] => check_cache_writes(),
        ["prune-ci-caches"] => prune_ci_caches(),
        ["prepare-python-release"] => python_release::prepare_release(),
        [
            "check-python-wheel",
            path,
            "--expected-version",
            version,
            "--platform-tag",
            platform,
        ] => python_wheels::check_wheel(Path::new(path), version, platform),
        [
            "check-python-wheels",
            directory,
            "--expected-version",
            version,
        ] => python_wheels::check_wheel_set(Path::new(directory), version).map(|_| ()),
        ["prepare-python-upload", artifacts, "--expected-version", version, "--index", index,
         "--destination", destination] => python_upload::prepare_upload(
            Path::new(artifacts), version, index, Path::new(destination),
        ),
        _ => Err(concat!(
            "usage: cargo xtask <task>\n",
            "  check-cache-writes\n",
            "  prune-ci-caches\n",
            "  prepare-python-release\n",
            "  check-python-wheel <path> --expected-version <version> --platform-tag <tag>\n",
            "  check-python-wheels <directory> --expected-version <version>\n",
            "  prepare-python-upload <artifacts> --expected-version <version> --index <pypi|testpypi> --destination <directory>",
        )
        .into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(id: u64) -> Cache {
        Cache {
            id,
            key: format!("{CACHE_PREFIX}{id}"),
            r#ref: CACHE_REF.into(),
            last_accessed_at: format!("2026-10-{id:02}T00:00:00Z"),
        }
    }

    #[test]
    fn retains_two_recently_used_caches_across_pages_and_preserves_other_families_and_refs() {
        let mut other_family = cache(5);
        other_family.key = "sccache-other".into();
        let mut other_ref = cache(6);
        other_ref.r#ref = "refs/pull/1/merge".into();
        let pages = vec![
            CachePage {
                actions_caches: vec![cache(3), cache(1), other_family],
            },
            CachePage {
                actions_caches: vec![cache(4), cache(2), other_ref],
            },
        ];
        assert_eq!(
            stale_caches(pages)
                .iter()
                .map(|cache| cache.id)
                .collect::<Vec<_>>(),
            [2, 1]
        );
        for count in 0..=2 {
            assert!(
                stale_caches(vec![CachePage {
                    actions_caches: (1..=count).map(cache).collect()
                }])
                .is_empty()
            );
        }
    }

    #[test]
    fn retains_an_old_cache_reused_after_a_dependency_revert() {
        let mut reused = cache(1);
        reused.last_accessed_at = "2026-10-05T00:00:00Z".into();
        let pages = vec![CachePage {
            actions_caches: vec![reused, cache(2), cache(3), cache(4)],
        }];
        assert_eq!(
            stale_caches(pages)
                .iter()
                .map(|cache| cache.id)
                .collect::<Vec<_>>(),
            [3, 2]
        );
    }

    #[test]
    fn accepts_success_and_existing_entries() -> Result<()> {
        check_writes(
            &WriteStats {
                cache_write_errors: 0,
            },
            "",
        )?;
        check_writes(
            &WriteStats {
                cache_write_errors: 1,
            },
            "Error executing cache write: AlreadyExists: immutable entry",
        )
    }

    #[test]
    fn rejects_backend_errors_and_missing_diagnostics() {
        for log in [
            "",
            "Error executing cache write: RateLimited",
            "Error executing cache write: AlreadyExists\nError executing cache write: PermissionDenied",
        ] {
            assert!(
                check_writes(
                    &WriteStats {
                        cache_write_errors: 1
                    },
                    log
                )
                .is_err()
            );
        }
        for stats in [
            r#"{"stats": {}}"#,
            r#"{"stats": {"cache_write_errors": "0"}}"#,
        ] {
            assert!(serde_json::from_str::<SccacheStats>(stats).is_err());
        }
    }

    #[test]
    fn includes_the_reason_sccache_fell_back_to_read_only() {
        let error = check_writes(
            &WriteStats {
                cache_write_errors: 1,
            },
            "storage write check failed: RateLimited\nError executing cache write: Cannot write to read-only storage",
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("storage write check failed: RateLimited")
        );
    }
}
