#![cfg(unix)]

use std::{
    env,
    error::Error,
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use serde_json::{Value, json};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

fn root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap()
}

fn test_dir(name: &str) -> PathBuf {
    let path = root().join(format!("xtask/target/test-{name}-{}", std::process::id()));
    fs::create_dir_all(&path).unwrap();
    path
}

fn workflow_script(source: &str, step: &str) -> String {
    let marker = format!("      - name: {step}\n");
    assert_eq!(source.matches(&marker).count(), 1);
    let (_, step) = source.split_once(&marker).unwrap();
    let (_, script) = step.split_once("        run: |\n").unwrap();
    let script = script
        .lines()
        .take_while(|line| line.is_empty() || line.starts_with("          "))
        .map(|line| line.strip_prefix("          ").unwrap_or(""))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!script.is_empty());
    script
}

fn filter_matches(filters: &Value, name: &str, path: &str) -> Result<bool> {
    let patterns: Vec<_> = filters[name]
        .as_array()
        .unwrap()
        .iter()
        .map(|pattern| pattern.as_str().unwrap())
        .collect();
    let output = Command::new("bash")
        .args([
            "-c",
            "for pattern; do [[ \"$MATCH_PATH\" == $pattern ]] && exit 0; done; exit 1",
            "--",
        ])
        .args(patterns)
        .env("MATCH_PATH", path)
        .output()?;
    Ok(output.status.success())
}

#[test]
fn python_filter_excludes_unrelated_sources_and_tools() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    for (path, expected) in [
        ("crates/delta-arrow-reader-core/src/reader/scan.rs", true),
        ("Cargo.lock", true),
        ("Cargo.toml", true),
        ("crates/delta-arrow-reader-cli/Cargo.toml", true),
        ("crates/delta-arrow-reader-python/src/lib.rs", true),
        ("crates/delta-arrow-reader-python/tests/test_table.py", true),
        ("crates/delta-arrow-reader-core/tests/reader/https.py", true),
        (
            "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/table/data.parquet",
            true,
        ),
        ("crates/delta-arrow-reader-python/pyproject.toml", true),
        (
            "crates/delta-arrow-reader-python/delta_arrow_reader.pyi",
            true,
        ),
        ("README.md", true),
        ("LICENSE", true),
        (".cargo/config.toml", true),
        ("xtask/Cargo.toml", true),
        ("xtask/src/main.rs", true),
        ("xtask/Cargo.lock", false),
        ("xtask/src/python_wheel_build.rs", true),
        ("xtask/src/python_cli/tests.rs", true),
        ("xtask/src/notices.rs", true),
        ("crates/delta-arrow-reader-cli/release/about.toml", true),
        (".github/workflows/cache-warm.yml", true),
        (".github/workflows/pypi-build.yml", true),
        (".github/workflows/pypi.yml", true),
        (".github/workflows/release-plz.yml", true),
        ("xtask/src/python_release.rs", false),
        ("xtask/src/python_wheels.rs", false),
        ("xtask/src/python_upload.rs", false),
        ("xtask/tests/python_upload.rs", false),
        ("xtask/tests/python_release.rs", false),
        ("xtask/tests/python_wheels.rs", false),
        ("xtask/tests/python_workflow.rs", false),
        ("xtask/tests/release_plz.rs", false),
        ("release-plz.toml", true),
        ("crates/delta-arrow-reader-cli/src/main.rs", true),
        ("crates/delta-arrow-reader-cli/tests/scan.py", false),
        ("benches/selective_read/src/main.rs", false),
        (
            "crates/delta-arrow-reader-core/tests/reader/direct.rs",
            false,
        ),
        ("docs/content/contributing.md", false),
    ] {
        assert_eq!(
            filter_matches(&filters, "python", path)?,
            expected,
            "{path}"
        );
    }
    Ok(())
}

#[test]
fn python_sources_skip_core_checks_without_losing_formatting() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    for path in [
        "crates/delta-arrow-reader-python/src/lib.rs",
        "crates/delta-arrow-reader-python/src/reader.rs",
        "crates/delta-arrow-reader-python/tests/test_table.py",
    ] {
        assert!(filter_matches(&filters, "python", path)?, "{path}");
        assert!(!filter_matches(&filters, "code", path)?, "{path}");
    }
    for path in [
        "crates/delta-arrow-reader-python/Cargo.toml",
        "crates/delta-arrow-reader-python/pyproject.toml",
        "crates/delta-arrow-reader-python/delta_arrow_reader.pyi",
    ] {
        for name in ["code", "python"] {
            assert!(filter_matches(&filters, name, path)?, "{name}: {path}");
        }
    }
    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let steps = ci["jobs"]["python-package"]["steps"].as_array().unwrap();
    let toolchain = steps
        .iter()
        .find(|step| step["uses"] == "dtolnay/rust-toolchain@stable")
        .unwrap();
    assert!(
        toolchain["with"]["components"]
            .as_str()
            .unwrap()
            .split(',')
            .any(|component| component.trim() == "rustfmt")
    );
    assert!(steps.iter().any(|step| {
        step["run"] == "cargo fmt --package delta-arrow-reader-python -- --check"
            && step["if"].is_null()
    }));
    Ok(())
}

#[test]
fn cli_sources_run_separate_required_checks() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    for path in [
        "crates/delta-arrow-reader-cli/src/main.rs",
        "crates/delta-arrow-reader-cli/src/predicate/tests.rs",
        "crates/delta-arrow-reader-cli/tests/cli.rs",
        "crates/delta-arrow-reader-cli/tests/predicates/mod.rs",
    ] {
        assert!(filter_matches(&filters, "cli", path)?, "{path}");
        assert_eq!(
            filter_matches(&filters, "python", path)?,
            path.contains("/src/")
        );
        for name in ["code", "tooling", "selective_read"] {
            assert!(!filter_matches(&filters, name, path)?, "{name}: {path}");
        }
    }
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "xtask/Cargo.toml",
        "crates/delta-arrow-reader-cli/Cargo.toml",
        "crates/delta-arrow-reader-python/Cargo.toml",
        ".cargo/config.toml",
        ".github/path-filters.json",
        ".github/workflows/ci.yml",
        ".github/workflows/cache-warm.yml",
        "clippy.toml",
        "crates/delta-arrow-reader-core/src/reader/options.rs",
        "crates/delta-arrow-reader-core/tests/reader/fixtures/external_writer/corpus/table/data.parquet",
    ] {
        for name in ["code", "cli"] {
            assert!(filter_matches(&filters, name, path)?, "{name}: {path}");
        }
    }
    for path in [
        "crates/delta-arrow-reader-python/src/lib.rs",
        "crates/delta-arrow-reader-python/tests/test_table.py",
        "xtask/src/main.rs",
        "crates/delta-arrow-reader-core/benches/page_index.rs",
        "benches/selective_read/src/main.rs",
        "crates/delta-arrow-reader-core/tests/reader/direct.rs",
        ".github/workflows/cli-build.yml",
    ] {
        assert!(!filter_matches(&filters, "cli", path)?, "{path}");
    }

    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let jobs = &ci["jobs"];
    assert_eq!(
        jobs["changes"]["outputs"]["cli"],
        "${{ steps.filter.outputs.cli }}"
    );
    assert_eq!(jobs["cli-checks"]["needs"], "changes");
    assert_eq!(
        jobs["cli-checks"]["if"],
        "needs.changes.outputs.cli == 'true'"
    );
    assert!(
        jobs["ci"]["needs"]
            .as_array()
            .unwrap()
            .contains(&json!("cli-checks"))
    );
    let steps = jobs["cli-checks"]["steps"].as_array().unwrap();
    let toolchain = steps
        .iter()
        .find(|step| step["uses"] == "dtolnay/rust-toolchain@stable")
        .unwrap();
    for component in ["clippy", "rustfmt"] {
        assert!(
            toolchain["with"]["components"]
                .as_str()
                .unwrap()
                .split(',')
                .any(|installed| installed.trim() == component)
        );
    }
    for command in [
        "cargo fmt --package delta-arrow-reader-cli -- --check",
        "cargo clippy --locked -p delta-arrow-reader-cli --all-targets -- -D warnings",
        "cargo test --locked -p delta-arrow-reader-cli",
    ] {
        assert!(
            steps
                .iter()
                .any(|step| step["run"] == command && step["if"].is_null())
        );
    }
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    assert!(
        warmer["on"]["push"]["paths"]
            .as_array()
            .unwrap()
            .contains(&json!("crates/delta-arrow-reader-cli/**"))
    );
    Ok(())
}

#[test]
fn tooling_changes_run_separate_required_checks() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    for path in [
        "xtask/src/main.rs",
        "xtask/tests/ci.rs",
        "xtask/src/python_wheels.rs",
        ".github/workflows/cli-images.yml",
        "crates/delta-arrow-reader-cli/Dockerfile",
        "examples/go-cli/Dockerfile",
        "examples/go-cli/scan_test.go",
    ] {
        assert!(filter_matches(&filters, "tooling", path)?, "{path}");
        assert!(!filter_matches(&filters, "code", path)?, "{path}");
        assert_eq!(
            filter_matches(&filters, "python", path)?,
            path == "xtask/src/main.rs"
        );
    }
    for path in [
        "Cargo.toml",
        "Cargo.lock",
        "xtask/Cargo.toml",
        ".cargo/config.toml",
        "crates/delta-arrow-reader-python/Cargo.toml",
    ] {
        for name in ["code", "python", "tooling"] {
            assert!(filter_matches(&filters, name, path)?, "{name}: {path}");
        }
    }
    for (path, expected) in [
        (".github/path-filters.json", true),
        (".github/workflows/ci.yml", true),
        (".github/workflows/cache-warm.yml", true),
        (".github/workflows/cli-build.yml", true),
        (".github/workflows/pypi.yml", true),
        (".github/workflows/pypi-build.yml", true),
        (".github/workflows/release-plz.yml", true),
        ("release-plz.toml", true),
        ("clippy.toml", true),
        ("LICENSE", true),
        ("NOTICE", true),
        (
            "crates/delta-arrow-reader-core/src/reader/options.rs",
            false,
        ),
        ("crates/delta-arrow-reader-python/src/lib.rs", false),
        ("crates/delta-arrow-reader-cli/src/main.rs", false),
        ("benches/selective_read/src/fixtures.rs", false),
    ] {
        assert_eq!(
            filter_matches(&filters, "tooling", path)?,
            expected,
            "{path}"
        );
    }
    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let jobs = &ci["jobs"];
    assert_eq!(
        jobs["changes"]["outputs"]["tooling"],
        "${{ steps.filter.outputs.tooling }}"
    );
    assert_eq!(jobs["tooling-checks"]["needs"], "changes");
    assert_eq!(
        jobs["tooling-checks"]["if"],
        "needs.changes.outputs.tooling == 'true'"
    );
    assert!(
        jobs["ci"]["needs"]
            .as_array()
            .unwrap()
            .contains(&json!("tooling-checks"))
    );
    Ok(())
}

#[test]
fn benchmark_checks_exclude_unrelated_ci_changes() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    for (path, expected) in [
        (".github/workflows/ci.yml", false),
        (".github/workflows/cli-build.yml", false),
        (".github/workflows/pypi-build.yml", false),
        (".github/workflows/benchmark-fixtures.yml", true),
        (".github/path-filters.json", true),
        (".cargo/config.toml", true),
        ("clippy.toml", true),
        ("benches/selective_read/Cargo.toml", true),
        ("benches/selective_read/Cargo.lock", true),
        ("benches/selective_read/src/fixtures.rs", true),
        ("benches/selective_read/oracle.py", true),
        (
            "crates/delta-arrow-reader-core/src/reader/options.rs",
            false,
        ),
        ("xtask/src/main.rs", false),
    ] {
        assert_eq!(
            filter_matches(&filters, "selective_read", path)?,
            expected,
            "{path}"
        );
    }
    let workflow = ".github/workflows/benchmark-fixtures.yml";
    assert!(filter_matches(&filters, "tooling", workflow)?);
    for name in ["code", "python"] {
        assert!(!filter_matches(&filters, name, workflow)?, "{name}");
    }

    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let jobs = &ci["jobs"];
    assert_eq!(
        jobs["changes"]["outputs"]["selective_read"],
        "${{ steps.filter.outputs.selective_read }}"
    );
    let caller = &jobs["selective-read-fixtures"];
    assert_eq!(caller["needs"], "changes");
    assert_eq!(
        caller["if"],
        "needs.changes.outputs.selective_read == 'true'"
    );
    assert_eq!(caller["uses"], format!("./{workflow}"));
    assert!(
        jobs["ci"]["needs"]
            .as_array()
            .unwrap()
            .contains(&json!("selective-read-fixtures"))
    );
    let lint = jobs["tooling-checks"]["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["name"] == "Check CI workflows")
        .unwrap();
    assert!(
        lint["with"]["args"]
            .as_str()
            .unwrap()
            .split_whitespace()
            .any(|argument| argument == workflow)
    );
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    assert!(
        warmer["on"]["push"]["paths"]
            .as_array()
            .unwrap()
            .contains(&json!(workflow))
    );
    Ok(())
}

#[test]
fn core_filter_preserves_benchmark_targets_and_shared_inputs() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    let manifest: toml::Value = toml::from_str(include_str!(
        "../../crates/delta-arrow-reader-core/Cargo.toml"
    ))?;
    for target in manifest["bench"].as_array().unwrap() {
        let path = target["path"].as_str().unwrap();
        let path = format!("crates/delta-arrow-reader-core/{path}");
        assert!(filter_matches(&filters, "code", &path)?, "{path}");
    }
    for path in [
        "crates/delta-arrow-reader-core/benches/range_planning/controlled_http.rs",
        "crates/delta-arrow-reader-core/benches/range_planning/plan_diagnostics.rs",
        "benches/selective_read/src/control_rows.rs",
        "benches/scan_scheduling.py",
        "benches/test_scan_scheduling.py",
        "benches/run_order.py",
    ] {
        assert!(filter_matches(&filters, "code", path)?, "{path}");
    }
    for path in [
        "benches/selective_read/Cargo.toml",
        "benches/selective_read/Cargo.lock",
        "benches/selective_read/rust-toolchain.toml",
        "benches/selective_read/src/main.rs",
        "benches/selective_read/src/fixtures.rs",
        "benches/selective_read/src/production.rs",
        "benches/selective_read/src/large.rs",
        "benches/selective_read/src/tests.rs",
        "benches/selective_read/tests/large_cli.rs",
        "benches/selective_read/oracle.py",
        "benches/selective_read/runners/dar/src/main.rs",
    ] {
        assert!(!filter_matches(&filters, "code", path)?, "{path}");
        assert!(filter_matches(&filters, "selective_read", path)?, "{path}");
    }
    for path in [
        "benches/query_shapes.sql",
        "benches/render_comparison_chart.py",
        "benches/render_selective_read_chart.py",
        "benches/render_selective_s3_chart.py",
    ] {
        assert!(!filter_matches(&filters, "code", path)?, "{path}");
    }
    Ok(())
}

#[test]
fn standalone_docs_skip_rust_checks() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    for path in [
        "docs/content/contributing.md",
        "docs/content/benchmarks/intra-page-reads.md",
        "crates/delta-arrow-reader-core/docs/public/python.md",
        "crates/delta-arrow-reader-core/docs/public/cli.md",
        "crates/delta-arrow-reader-core/docs/public/benchmarks/selective-read-results.md",
    ] {
        for name in ["code", "python", "cli", "tooling", "selective_read"] {
            assert!(!filter_matches(&filters, name, path)?, "{name}: {path}");
        }
    }
    for path in [
        "docs/content/benchmarks/selective-read-protocol.md",
        "docs/content/benchmarks/selective-read-production-shapes.md",
        "docs/content/benchmarks/selective-read-large-workloads.md",
    ] {
        assert!(filter_matches(&filters, "selective_read", path)?, "{path}");
        assert!(!filter_matches(&filters, "code", path)?, "{path}");
    }
    Ok(())
}

#[test]
fn embedded_docs_trigger_their_rust_checks() -> Result<()> {
    let filters: Value = serde_json::from_str(include_str!("../../.github/path-filters.json"))?;
    let include = regex::Regex::new(r#"include_str!\("([^"]+\.md)"\)"#)?;
    for (source, filter) in [
        ("crates/delta-arrow-reader-core/src/guides.rs", "code"),
        ("benches/selective_read/src/main.rs", "selective_read"),
        ("benches/selective_read/src/production.rs", "selective_read"),
        ("benches/selective_read/src/large.rs", "selective_read"),
    ] {
        assert!(filter_matches(&filters, "tooling", source)?, "{source}");
        let source = root().join(source);
        let contents = fs::read_to_string(&source)?;
        let documents: Vec<_> = include.captures_iter(&contents).collect();
        assert!(!documents.is_empty(), "{}", source.display());
        for document in documents {
            let path = source.parent().unwrap().join(&document[1]).canonicalize()?;
            let path = path.strip_prefix(root())?.to_str().unwrap();
            assert!(filter_matches(&filters, filter, path)?, "{filter}: {path}");
        }
    }
    Ok(())
}

fn check_ci_results(values: &[(&str, &str)]) -> Result<Output> {
    let script = workflow_script(
        include_str!("../../.github/workflows/ci.yml"),
        "Check results",
    );
    let mut rest = script.as_str();
    let mut rendered = String::new();
    while let Some((before, after)) = rest.split_once("${{ needs.") {
        let (key, after) = after.split_once(" }}").unwrap();
        rendered.push_str(before);
        rendered.push_str(
            values
                .iter()
                .find(|(name, _)| *name == key)
                .map_or("success", |(_, value)| *value),
        );
        rest = after;
    }
    rendered.push_str(rest);
    Ok(Command::new("bash")
        .args(["-e", "-c", &rendered])
        .output()?)
}

#[test]
fn summary_requires_python_only_when_requested() -> Result<()> {
    for code in ["true", "false"] {
        for python in ["true", "false"] {
            for package in ["success", "failure", "skipped", "cancelled"] {
                let values = [
                    ("changes.outputs.code", code),
                    ("changes.outputs.python", python),
                    ("changes.outputs.selective_read", "false"),
                    ("changes.outputs.release_pr", "false"),
                    ("python-package.result", package),
                ];
                let output = check_ci_results(&values)?;
                let expected = python != "true" || package == "success";
                assert_eq!(
                    output.status.success(),
                    expected,
                    "code={code}, python={python}, package={package}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn summary_requires_cli_independently_of_core_checks() -> Result<()> {
    for code in ["true", "false"] {
        for cli in ["true", "false"] {
            for result in ["success", "failure", "skipped", "cancelled", ""] {
                let output = check_ci_results(&[
                    ("changes.outputs.code", code),
                    ("changes.outputs.python", "false"),
                    ("changes.outputs.cli", cli),
                    ("changes.outputs.release_pr", "false"),
                    ("cli-checks.result", result),
                ])?;
                assert_eq!(
                    output.status.success(),
                    cli != "true" || result == "success",
                    "code={code}, cli={cli}, result={result}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn summary_requires_benchmarks_independently_of_core_checks() -> Result<()> {
    for code in ["true", "false"] {
        for benchmark in ["true", "false"] {
            for result in ["success", "failure", "skipped", "cancelled", ""] {
                let output = check_ci_results(&[
                    ("changes.outputs.code", code),
                    ("changes.outputs.python", "false"),
                    ("changes.outputs.selective_read", benchmark),
                    ("changes.outputs.release_pr", "false"),
                    ("selective-read-fixtures.result", result),
                ])?;
                assert_eq!(
                    output.status.success(),
                    benchmark != "true" || result == "success",
                    "code={code}, benchmark={benchmark}, result={result}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn summary_requires_tooling_independently_of_core_checks() -> Result<()> {
    for code in ["true", "false"] {
        for tooling in ["true", "false"] {
            for result in ["success", "failure", "skipped", "cancelled", ""] {
                let output = check_ci_results(&[
                    ("changes.outputs.code", code),
                    ("changes.outputs.python", "false"),
                    ("changes.outputs.tooling", tooling),
                    ("changes.outputs.selective_read", "false"),
                    ("changes.outputs.release_pr", "false"),
                    ("tooling-checks.result", result),
                ])?;
                assert_eq!(
                    output.status.success(),
                    tooling != "true" || result == "success",
                    "code={code}, tooling={tooling}, result={result}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn release_pr_requires_approved_and_validated_wheels() -> Result<()> {
    for code in ["true", "false"] {
        for release_pr in ["true", "false"] {
            for approval in ["success", "failure", "skipped", "cancelled"] {
                for wheels in ["success", "failure", "skipped", "cancelled"] {
                    for validated in ["true", "false", ""] {
                        let output = check_ci_results(&[
                            ("changes.outputs.code", code),
                            ("changes.outputs.python", "true"),
                            ("changes.outputs.selective_read", "false"),
                            ("changes.outputs.release_pr", release_pr),
                            ("release-approval.result", approval),
                            ("release-wheels.result", wheels),
                            ("release-wheels.outputs.validated", validated),
                            ("release-cli.outputs.validated", "true"),
                            ("release-cli-images.outputs.validated", "true"),
                            ("release-cli-record.outputs.validated", "true"),
                        ])?;
                        let expected = release_pr != "true"
                            || approval == "success" && wheels == "success" && validated == "true";
                        assert_eq!(
                            output.status.success(),
                            expected,
                            "code={code}, release_pr={release_pr}, approval={approval}, wheels={wheels}, validated={validated}: {output:?}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn release_pr_requires_the_record_upload_and_matching_pr_identity() -> Result<()> {
    for code in ["true", "false"] {
        for release_pr in ["true", "false"] {
            for (key, value, passes) in [
                ("release-cli-record.result", "success", true),
                ("release-cli-record.result", "failure", false),
                ("release-cli-record.result", "cancelled", false),
                ("release-cli-record.result", "skipped", false),
                ("release-cli-record.result", "", false),
                ("release-cli-record.outputs.validated", "false", false),
                ("release-cli-record.outputs.validated", "", false),
                ("release-cli-record.outputs.artifact-id", "", false),
                ("release-cli-record.outputs.artifact-name", "", false),
                ("release-cli-record.outputs.artifact-url", "", false),
                ("release-cli-record.outputs.source-commit", "", false),
                (
                    "release-cli-record.outputs.source-commit",
                    "different",
                    false,
                ),
                ("release-cli-record.outputs.pr-head-sha", "", false),
                ("release-cli-record.outputs.pr-head-sha", "different", false),
                ("release-cli-record.outputs.version", "", false),
                ("release-cli-record.outputs.version", "different", false),
            ] {
                let output = check_ci_results(&[
                    (key, value),
                    ("changes.outputs.code", code),
                    ("changes.outputs.python", "false"),
                    ("changes.outputs.selective_read", "false"),
                    ("changes.outputs.release_pr", release_pr),
                    ("release-cli.outputs.validated", "true"),
                    ("release-cli-images.outputs.validated", "true"),
                    ("release-cli-record.outputs.validated", "true"),
                    ("release-wheels.outputs.validated", "true"),
                ])?;
                assert_eq!(
                    output.status.success(),
                    release_pr != "true" || passes,
                    "code={code}, release={release_pr}, {key}={value:?}: {output:?}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn release_wheel_builds_share_approval_with_cli() -> Result<()> {
    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let jobs = &ci["jobs"];
    let approvals: Vec<_> = jobs
        .as_object()
        .unwrap()
        .iter()
        .filter(|(_, job)| !job["environment"].is_null())
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(approvals, ["release-approval"]);
    assert_eq!(jobs["release-approval"]["environment"], "release-checks");
    assert_eq!(jobs["release-approval"]["needs"], "changes");
    for name in ["release-approval", "release-wheels", "release-cli"] {
        assert_eq!(
            jobs[name]["if"],
            "needs.changes.outputs.release_pr == 'true'"
        );
        assert!(
            jobs["ci"]["needs"]
                .as_array()
                .unwrap()
                .contains(&json!(name))
        );
    }
    let wheels = &jobs["release-wheels"];
    assert_eq!(wheels["uses"], "./.github/workflows/pypi-build.yml");
    for name in ["release-wheels", "release-cli"] {
        assert_eq!(jobs[name]["needs"], json!(["changes", "release-approval"]));
    }
    assert_eq!(
        wheels["with"],
        json!({
            "source-ref": "${{ github.sha }}",
            "expected-version": "${{ needs.changes.outputs.package_version }}",
        })
    );
    Ok(())
}

#[test]
fn release_helpers_match_the_main_dependency_cache() -> Result<()> {
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    let core = &warmer["jobs"]["core"];
    let cache = core["steps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| step["with"]["shared-key"] == "core-ci-targets")
        .unwrap();
    for source in [
        include_str!("../../.github/workflows/cli-build.yml"),
        include_str!("../../.github/workflows/cli-images.yml"),
        include_str!("../../.github/workflows/cli-release.yml"),
        include_str!("../../.github/workflows/pypi-build.yml"),
        include_str!("../../.github/workflows/pypi.yml"),
        include_str!("../../.github/workflows/release-plz.yml"),
    ] {
        let workflow: Value = serde_yaml_ng::from_str(source)?;
        let mut readers = 0;
        for job in workflow["jobs"].as_object().unwrap().values() {
            let Some(steps) = job["steps"].as_array() else {
                continue;
            };
            for (index, step) in steps.iter().enumerate() {
                if step["with"]["shared-key"] != "core-ci-targets" {
                    continue;
                }
                readers += 1;
                for input in ["env-vars", "cache-targets", "cache-bin"] {
                    assert_eq!(step["with"][input], cache["with"][input], "{input}");
                }
                for variable in [
                    "CARGO_PROFILE_DEV_DEBUG",
                    "AWS_LC_SYS_USE_SYSTEM",
                    "CI_TARGET_CACHE_VERSION",
                    "RUSTC_WRAPPER",
                ] {
                    let value = job["env"]
                        .get(variable)
                        .unwrap_or(&workflow["env"][variable]);
                    assert_eq!(value, &core["env"][variable], "{variable}");
                }
                assert!(step["with"]["workspaces"].is_null());
                assert!(
                    steps[..index].iter().all(|earlier| {
                        !earlier["run"]
                            .as_str()
                            .unwrap_or_default()
                            .contains("CARGO_TARGET_DIR=")
                    }),
                    "changing the target path before restoration prevents main cache hits"
                );
            }
        }
        assert!(
            readers > 0,
            "{} has no compiled cache readers",
            workflow["name"]
        );
    }
    Ok(())
}

#[test]
fn auxiliary_builds_keep_restored_dependencies_outside_checkout() -> Result<()> {
    for (workflow, job, step) in [
        (
            include_str!("../../.github/workflows/cli-release.yml"),
            "prepare",
            "Preserve compiled dependencies across checkout changes",
        ),
        (
            include_str!("../../.github/workflows/release-plz.yml"),
            "release-pr",
            "Configure Cargo target",
        ),
    ] {
        let workflow: Value = serde_yaml_ng::from_str(workflow)?;
        let script = workflow["jobs"][job]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["name"] == step)
            .unwrap()["run"]
            .as_str()
            .unwrap();
        for restored in [false, true] {
            let temporary = tempfile::tempdir()?;
            let workspace = temporary.path().join("source");
            let runner = temporary.path().join("runner");
            fs::create_dir(&workspace)?;
            fs::create_dir(&runner)?;
            if restored {
                fs::create_dir(workspace.join("target"))?;
                fs::write(workspace.join("target/dependency"), "cached dependency")?;
            }
            let output_file = temporary.path().join("environment");
            let output = Command::new("bash")
                .args(["-euo", "pipefail", "-c", script])
                .current_dir(&workspace)
                .env("RUNNER_TEMP", &runner)
                .env("GITHUB_ENV", &output_file)
                .output()?;
            assert!(output.status.success(), "{output:?}");
            let environment = fs::read_to_string(output_file)?;
            let target = Path::new(
                environment
                    .trim()
                    .strip_prefix("CARGO_TARGET_DIR=")
                    .unwrap(),
            );
            assert!(target.starts_with(&runner));
            assert!(target.is_dir());
            fs::remove_dir_all(&workspace)?;
            if restored {
                assert_eq!(
                    fs::read_to_string(target.join("dependency"))?,
                    "cached dependency"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn wheel_cache_warming_requires_manual_dispatch() -> Result<()> {
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    assert_eq!(
        warmer["jobs"]["wheels"]["if"],
        "github.event_name == 'workflow_dispatch'"
    );
    Ok(())
}

#[test]
fn benchmark_cache_warming_matches_pr_builds() -> Result<()> {
    let benchmark: Value = serde_yaml_ng::from_str(include_str!(
        "../../.github/workflows/benchmark-fixtures.yml"
    ))?;
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    let check = &benchmark["jobs"]["fixtures"];
    let warm = &warmer["jobs"]["selective-read-fixtures"];

    assert_eq!(warm["runs-on"], check["runs-on"]);
    assert_eq!(warm["defaults"], check["defaults"]);
    for name in ["CARGO_TARGET_DIR", "RUSTFLAGS"] {
        assert_eq!(warm["env"][name], check["env"][name], "{name}");
    }
    let check_steps = check["steps"].as_array().unwrap();
    let warm_steps = warm["steps"].as_array().unwrap();
    for (action, inputs) in [
        ("dtolnay/rust-toolchain@stable", &["toolchain"][..]),
        (
            "Swatinem/rust-cache@v2",
            &["workspaces", "shared-key", "cache-targets"][..],
        ),
    ] {
        let check_step = check_steps
            .iter()
            .find(|step| step["uses"] == action)
            .unwrap();
        let warm_step = warm_steps
            .iter()
            .find(|step| step["uses"] == action)
            .unwrap();
        for input in inputs {
            assert_eq!(
                warm_step["with"][input], check_step["with"][input],
                "{action}: {input}"
            );
        }
    }
    let warm_builds = warm_steps
        .iter()
        .find(|step| step["name"] == "Warm benchmark caches")
        .unwrap()["run"]
        .as_str()
        .unwrap();
    for command in check_steps.iter().filter_map(|step| step["run"].as_str()) {
        if command.starts_with("cargo clippy ") || command.starts_with("cargo test ") {
            let expected = if command.starts_with("cargo test ") {
                format!("{command} --no-run")
            } else {
                command.to_owned()
            };
            assert!(
                warm_builds.lines().any(|line| line == expected),
                "{expected}"
            );
        }
    }
    assert_eq!(warm["env"]["SCCACHE_GHA_RW_MODE"], "READ_WRITE");
    assert_eq!(benchmark["on"], json!({"workflow_call": null}));
    assert_eq!(benchmark["env"]["SCCACHE_GHA_ENABLED"], "true");
    assert_eq!(benchmark["env"]["SCCACHE_GHA_RW_MODE"], "READ_ONLY");
    assert_eq!(benchmark["env"]["RUSTC_WRAPPER"], "sccache");
    assert_eq!(warm["needs"], "core");
    assert!(
        warmer["jobs"]["wheels"]["needs"]
            .as_array()
            .unwrap()
            .contains(&json!("selective-read-fixtures"))
    );
    Ok(())
}

#[test]
fn cache_warming_requires_the_triggering_main_commit() -> Result<()> {
    let script = workflow_script(
        include_str!("../../.github/workflows/pypi-build.yml"),
        "Check build inputs and read Rust version",
    );
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root())
        .output()?;
    assert!(output.status.success());
    let sha = String::from_utf8(output.stdout)?;
    let version = include_str!("../../Cargo.toml")
        .lines()
        .find_map(|line| {
            line.strip_prefix("version = \"")
                .and_then(|line| line.strip_suffix('"'))
        })
        .unwrap();
    let directory = test_dir("main-guard");
    for git_ref in [
        "refs/heads/main",
        "refs/heads/fix/example",
        "refs/pull/1/merge",
    ] {
        for event in [
            "push",
            "workflow_dispatch",
            "pull_request",
            "pull_request_target",
        ] {
            for same_commit in [true, false] {
                let output = Command::new("bash")
                    .args(["-e", "-c", &script])
                    .current_dir(root())
                    .env("CACHE_ONLY", "true")
                    .env("SOURCE_REF", sha.trim())
                    .env(
                        "GITHUB_SHA",
                        if same_commit {
                            sha.trim()
                        } else {
                            "0000000000000000000000000000000000000000"
                        },
                    )
                    .env("GITHUB_REF", git_ref)
                    .env("GITHUB_EVENT_NAME", event)
                    .env("EXPECTED_VERSION", version)
                    .env("GITHUB_OUTPUT", directory.join("output"))
                    .output()?;
                let expected = git_ref == "refs/heads/main"
                    && matches!(event, "push" | "workflow_dispatch")
                    && same_commit;
                assert_eq!(
                    output.status.success(),
                    expected,
                    "{git_ref}, {event}, same_commit={same_commit}: {output:?}"
                );
            }
        }
    }
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn linux_cache_check_isolates_build_outputs_without_masking_build_failures() -> Result<()> {
    let directory = test_dir("linux-exit");
    let cargo = directory.join("cargo");
    fs::write(
        &cargo,
        "#!/bin/sh\ntest -z \"$RUSTC_WRAPPER\" || exit 10\nprintf '%s\\n' \"$@\"\nexit \"$CACHE_CHECK_EXIT\"\n",
    )?;
    fs::set_permissions(&cargo, fs::Permissions::from_mode(0o755))?;
    let trap = include_str!("../../.github/workflows/pypi-build.yml")
        .lines()
        .find(|line| line.trim_start().starts_with("trap '"))
        .unwrap();
    for build_status in [0, 7] {
        for check_status in [0, 9] {
            let output = Command::new("/bin/bash")
                .args(["-euc", &format!("{trap}\nexit {build_status}")])
                .env("PATH", &directory)
                .env("GITHUB_WORKSPACE", root())
                .env("RUSTC_WRAPPER", "sccache")
                .env("CACHE_CHECK_EXIT", check_status.to_string())
                .output()?;
            assert_eq!(
                output.status.success(),
                build_status == 0 && check_status == 0,
                "build={build_status}, check={check_status}: {output:?}"
            );
            let stdout = String::from_utf8(output.stdout)?;
            let args: Vec<_> = stdout.lines().collect();
            let target = args
                .windows(2)
                .find(|args| args[0] == "--target-dir")
                .expect("container cache check must use a separate target directory")[1];
            let target = Path::new(target);
            assert!(target.is_absolute(), "container target must be absolute");
            assert!(
                !target.starts_with(root()),
                "container target must stay outside the mounted workspace"
            );
        }
    }
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn cache_retry_cleans_the_correct_target_and_restarts_native_sccache() -> Result<()> {
    let directory = test_dir("cache-retry");
    let log = directory.join("commands");
    for program in ["sudo", "sccache", "cargo", "sleep"] {
        let path = directory.join(program);
        fs::write(
            &path,
            "#!/bin/sh\nprogram=${0##*/}\n[ -z \"$RUSTC_WRAPPER\" ] || exit 10\nprintf '%s %s\\n' \"$program\" \"$*\" >> \"$COMMAND_LOG\"\n[ \"$program\" != \"$FAIL_COMMAND\" ]\n",
        )?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    }
    let wheel_script = workflow_script(
        include_str!("../../.github/workflows/pypi-build.yml"),
        "Prepare cache write retry",
    );
    let benchmark_script = workflow_script(
        include_str!("../../.github/workflows/cache-warm.yml"),
        "Prepare benchmark cache write retry",
    );
    for (script, runner, expected) in [
        (
            &wheel_script,
            "Linux",
            "sudo rm -rf .python-sdist-source/target\nsleep 60\n",
        ),
        (
            &wheel_script,
            "Windows",
            "sccache --stop-server\ncargo clean --target-dir target\nsleep 60\n",
        ),
        (
            &wheel_script,
            "macOS",
            "sccache --stop-server\ncargo clean --target-dir target\nsleep 60\n",
        ),
        (
            &benchmark_script,
            "Linux",
            "sccache --stop-server\ncargo clean --target-dir /tmp/benchmark-fixtures\nsleep 60\n",
        ),
    ] {
        for failure in ["", "sudo", "sccache", "cargo"] {
            fs::write(&log, "")?;
            let output = Command::new("/bin/bash")
                .args(["-euc", script])
                .env("PATH", &directory)
                .env("RUNNER_OS", runner)
                .env("RUSTC_WRAPPER", "")
                .env("CARGO_TARGET_DIR", "/tmp/benchmark-fixtures")
                .env("COMMAND_LOG", &log)
                .env("FAIL_COMMAND", failure)
                .output()?;
            let should_fail = !failure.is_empty() && expected.contains(failure);
            assert_eq!(
                output.status.success(),
                !should_fail,
                "{runner}: {output:?}"
            );
            let actual = fs::read_to_string(&log)?;
            if should_fail {
                assert!(!actual.contains("sleep"));
            } else {
                assert_eq!(actual, expected);
            }
        }
    }
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn cache_read_report_counts_backend_failures_without_exposing_request_details() -> Result<()> {
    let directory = test_dir("cache-reads");
    let log = directory.join("sccache.log");
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command.arg("report-cache-reads");
    assert!(
        !command
            .env_remove("SCCACHE_ERROR_LOG")
            .output()?
            .status
            .success()
    );
    command.env("SCCACHE_ERROR_LOG", &log);
    assert!(!command.output()?.status.success());

    fs::write(&log, "")?;
    let output = command.output()?;
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "Remote cache read failures recorded: 0\n  RateLimited: 0\n  Other: 0\n"
    );

    fs::write(
        &log,
        b"[2026-10-11T20:00:00Z WARN  sccache::cache::cache] Got unexpected error: RateLimited (temporary) at read, context: { uri: https://example.invalid/cache?sig=secret, status: 429 }\r\n\
          [2026-10-11T20:00:01Z WARN  sccache::cache::cache] Got unexpected error: Unexpected (temporary) at read, context: { status: 500 } => RateLimited\n\
          [2026-10-11T20:00:02Z WARN  sccache::cache::cache] Got unexpected error: PermissionDenied (persistent) at read => \xff\n\
          [2026-10-11T20:00:03Z DEBUG sccache::server] Error executing cache write: RateLimited\n\
          [2026-10-11T20:00:04Z WARN  other] Got unexpected error: RateLimited\n",
    )?;
    let output = command.output()?;
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout)?,
        "Remote cache read failures recorded: 3\n  RateLimited: 1\n  Other: 2\n"
    );
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn core_cache_warming_matches_platform_checks_and_stops_on_failure() -> Result<()> {
    let directory = test_dir("core-warm");
    for tool in ["cargo", "python"] {
        let path = directory.join(tool);
        fs::write(
            &path,
            "#!/bin/sh\nprintf '%s\\n' \"$*\"\n[ \"$*\" != \"$FAIL_COMMAND\" ]\n",
        )?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))?;
    }
    let venv = directory.join("python-tests/bin");
    fs::create_dir_all(&venv)?;
    fs::write(venv.join("activate"), "")?;
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    let steps = warmer["jobs"]["core"]["steps"].as_array().unwrap();
    let script = steps
        .iter()
        .find(|step| step["name"] == "Warm core caches")
        .unwrap()["run"]
        .as_str()
        .unwrap();
    let retry = steps
        .iter()
        .find(|step| step["name"] == "Retry core cache writes")
        .unwrap();
    assert_eq!(retry["run"], script);
    assert_eq!(retry["if"], "steps.cache-writes.outcome == 'failure'");

    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    for variable in [
        "CARGO_PROFILE_DEV_DEBUG",
        "AWS_LC_SYS_USE_SYSTEM",
        "CI_TARGET_CACHE_VERSION",
    ] {
        assert!(ci["env"][variable].is_string(), "{variable}");
        assert_eq!(
            ci["env"][variable], warmer["jobs"]["core"]["env"][variable],
            "{variable} must match for compiled dependency reuse"
        );
    }
    let cache = steps
        .iter()
        .find(|step| step["name"] == "Cache compiled dependencies")
        .unwrap();
    assert_eq!(cache["with"]["env-vars"], "CI_TARGET_CACHE_VERSION");
    for job in ci["jobs"].as_object().unwrap().values() {
        for step in job["steps"].as_array().into_iter().flatten() {
            if step["with"]["shared-key"] == "core-ci-targets" {
                for input in ["shared-key", "env-vars", "cache-targets", "cache-bin"] {
                    assert_eq!(step["with"][input], cache["with"][input], "{input}");
                }
                assert_eq!(step["with"]["save-if"], false);
            }
        }
    }
    for job in ["checks", "tooling-checks", "python-package", "cli-checks"] {
        let commands = ci["jobs"][job]["steps"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|step| step["run"].as_str())
            .flat_map(str::lines);
        for command in commands {
            if command.starts_with("cargo clippy ") || command.starts_with("RUSTDOCFLAGS=") {
                assert!(
                    script.lines().any(|line| line.trim() == command),
                    "{command}"
                );
            }
        }
    }

    for runner in ["Linux", "Windows", "macOS"] {
        let mut commands = vec![
            "test --locked --all-features --no-run",
            "test --locked --no-run",
        ];
        if runner == "Linux" {
            commands.extend([
                "test --locked -p delta-arrow-reader-python --no-run",
                "test --locked -p delta-arrow-reader-cli --no-run",
                "test --locked --package xtask --bin xtask --no-run",
                "clippy --locked --all-targets --all-features -- -D warnings",
                "clippy --locked -p delta-arrow-reader-python --all-targets -- -D warnings",
                "clippy --locked -p delta-arrow-reader-cli --all-targets -- -D warnings",
                "clippy --locked --package xtask --all-targets -- -D warnings",
                "doc --locked --all-features --no-deps",
                "-m pip install --verbose --no-build-isolation --no-deps --editable crates/delta-arrow-reader-python",
            ]);
        }
        for fail_at in 0..=commands.len() {
            let output = Command::new("/bin/bash")
                .args(["-euc", script])
                .env("PATH", &directory)
                .env("RUNNER_OS", runner)
                .env("RUNNER_TEMP", &directory)
                .env("FAIL_COMMAND", commands.get(fail_at).unwrap_or(&""))
                .output()?;
            assert_eq!(
                output.status.success(),
                fail_at == commands.len(),
                "{runner}, fail_at={fail_at}: {output:?}"
            );
            let expected = commands[..(fail_at + 1).min(commands.len())].join("\n") + "\n";
            assert_eq!(String::from_utf8(output.stdout)?, expected, "{runner}");
        }
    }
    fs::remove_dir_all(directory)?;
    Ok(())
}

#[test]
fn python_cache_warming_matches_the_editable_build() -> Result<()> {
    let ci: Value = serde_yaml_ng::from_str(include_str!("../../.github/workflows/ci.yml"))?;
    let warmer: Value =
        serde_yaml_ng::from_str(include_str!("../../.github/workflows/cache-warm.yml"))?;
    let check_steps = ci["jobs"]["python-package"]["steps"].as_array().unwrap();
    let warm_steps = warmer["jobs"]["core"]["steps"].as_array().unwrap();
    for action in ["actions/setup-python@v6", "taiki-e/install-action@v2"] {
        let check = check_steps
            .iter()
            .find(|step| step["uses"] == action)
            .unwrap();
        let warm = warm_steps
            .iter()
            .find(|step| step["uses"] == action)
            .unwrap();
        assert!(check["with"].is_object());
        assert_eq!(warm["with"], check["with"], "{action}");
    }
    let prepare = warm_steps
        .iter()
        .find(|step| step["name"] == "Prepare Python build environment")
        .unwrap();
    assert_eq!(prepare["if"], "runner.os == 'Linux'");
    let commands: Vec<_> = warm_steps
        .iter()
        .filter(|step| {
            step["name"] == "Prepare Python build environment" || step["name"] == "Warm core caches"
        })
        .filter_map(|step| step["run"].as_str())
        .flat_map(str::lines)
        .map(str::trim)
        .collect();
    let build = check_steps
        .iter()
        .find(|step| step["name"] == "Build Python package")
        .unwrap()["run"]
        .as_str()
        .unwrap();
    for command in build.lines() {
        assert!(commands.contains(&command), "{command}");
    }
    Ok(())
}

#[test]
fn prune_deletes_by_id_and_stops_on_command_or_schema_errors() -> Result<()> {
    let directory = test_dir("prune");
    let gh = directory.join("gh");
    fs::write(
        &gh,
        r#"#!/bin/sh
case "$*" in
  'api --paginate --slurp repos/owner/repo/actions/caches?key=v0-rust-core-ci-targets-'*'&ref=refs/heads/main&per_page=100')
    [ "$FAIL_LIST" != true ] || exit 9
    printf '%s' "$CACHE_PAGES" ;;
  'api --method DELETE repos/owner/repo/actions/caches/'*)
    printf '%s\n' "$4" >> "$DELETE_LOG"
    [ "$FAIL_DELETE" != true ] || exit 7 ;;
  *) exit 8 ;;
esac
"#,
    )?;
    fs::set_permissions(&gh, fs::Permissions::from_mode(0o755))?;
    let cache = |id, platform| {
        json!({"id": id, "key": format!("v0-rust-core-ci-targets-{platform}-{id}"),
        "ref": "refs/heads/main", "last_accessed_at": format!("2026-10-{id:02}T00:00:00Z")})
    };
    let pages = json!([
        {"actions_caches": [
            cache(14, "Linux-x64"), cache(13, "Linux-x64"),
            cache(24, "Darwin-arm64"), cache(23, "Darwin-arm64"),
            cache(4, "Windows_NT-x64"), cache(3, "Windows_NT-x64"),
        ]},
        {"actions_caches": [
            cache(12, "Linux-x64"), cache(11, "Linux-x64"),
            cache(22, "Darwin-arm64"), cache(21, "Darwin-arm64"),
            cache(2, "Windows_NT-x64"), cache(1, "Windows_NT-x64"),
        ]},
    ]);
    let log = directory.join("deletions");
    let mut command = Command::new(env!("CARGO_BIN_EXE_xtask"));
    command
        .arg("prune-ci-caches")
        .env("PATH", &directory)
        .env("GH_REPO", "owner/repo")
        .env("CACHE_PAGES", pages.to_string())
        .env("DELETE_LOG", &log)
        .env("FAIL_DELETE", "false")
        .env("FAIL_LIST", "false");
    assert!(command.output()?.status.success());
    assert_eq!(
        fs::read_to_string(&log)?,
        "repos/owner/repo/actions/caches/12\nrepos/owner/repo/actions/caches/11\n\
         repos/owner/repo/actions/caches/22\nrepos/owner/repo/actions/caches/21\n\
         repos/owner/repo/actions/caches/2\nrepos/owner/repo/actions/caches/1\n"
    );
    fs::write(&log, "")?;
    assert!(
        !command
            .env("FAIL_DELETE", "true")
            .output()?
            .status
            .success()
    );
    assert_eq!(
        fs::read_to_string(&log)?,
        "repos/owner/repo/actions/caches/12\n"
    );
    fs::write(&log, "")?;
    assert!(!command.env("FAIL_LIST", "true").output()?.status.success());
    assert!(fs::read_to_string(&log)?.is_empty());
    assert!(
        !command
            .env("FAIL_LIST", "false")
            .env(
                "CACHE_PAGES",
                r#"[{"actions_caches": [{"id": "wrong type"}]}]"#
            )
            .output()?
            .status
            .success()
    );
    assert!(fs::read_to_string(&log)?.is_empty());
    fs::remove_dir_all(directory)?;
    Ok(())
}
