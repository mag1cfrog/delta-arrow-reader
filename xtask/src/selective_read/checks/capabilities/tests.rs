use super::*;
use std::os::unix::fs::PermissionsExt;

#[test]
fn capability_variants_preserve_the_original_corpus_and_change_only_owned_copies() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let original = temporary.path().join("original");
    fs::create_dir_all(original.join("_delta_log"))?;
    let log = original.join("_delta_log/00000000000000000000.json");
    let actions = "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\"]}}\n{\"metaData\":{\"configuration\":{\"delta.enableDeletionVectors\":\"true\",\"retained\":\"value\"}}}\n";
    fs::write(&log, actions)?;
    fs::write(original.join("_delta_log/00000000000000000001.json"), "{}")?;
    fs::write(original.join("deletion_vector_test.bin"), "sidecar")?;
    let (plain, unknown, missing) = stage(&original, temporary.path())?;
    assert_eq!(fs::read_to_string(&log)?, actions);
    assert!(!plain.join("_delta_log/00000000000000000001.json").exists());
    let plain = fs::read_to_string(plain.join("_delta_log/00000000000000000000.json"))?;
    assert!(plain.contains("\"minReaderVersion\":1"));
    assert!(plain.contains("\"retained\":\"value\""));
    assert!(!plain.contains("delta.enableDeletionVectors"));
    let unknown = fs::read_to_string(unknown.join("_delta_log/00000000000000000000.json"))?;
    assert_eq!(unknown.matches("unknownBenchmarkProbe").count(), 2);
    assert!(!missing.join("deletion_vector_test.bin").exists());
    assert!(original.join("deletion_vector_test.bin").exists());
    Ok(())
}

#[test]
fn raw_probe_checks_exit_status_and_preserves_startup_errors() -> Result<()> {
    let root = tempfile::tempdir()?;
    let binary = root.path().join("reader");
    fs::write(&binary, "#!/bin/sh\necho startup-failure >&2\nexit 1\n")?;
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o700))?;
    let failed = probe(&binary, &json!({}), &root.path().join("failed"), None, None)?;
    assert_eq!(failed["status"], "invalid_input");
    assert_eq!(failed["failure_reason"], "startup-failure\n");
    fs::write(
        &binary,
        "#!/bin/sh\nmkdir \"$2\"\necho '{\"status\":\"success\"}' > \"$2/record.json\"\nexit 1\n",
    )?;
    assert!(
        probe(
            &binary,
            &json!({}),
            &root.path().join("inconsistent"),
            None,
            None
        )
        .is_err()
    );
    fs::write(
        &binary,
        "#!/bin/sh\nmkdir \"$2\"\ncat /proc/self/limits > \"$2/limits\"\necho '{\"status\":\"success\"}' > \"$2/record.json\"\n",
    )?;
    let previous = getrlimit(Resource::Fsize).current;
    let bounded = root.path().join("bounded");
    probe(&binary, &json!({}), &bounded, None, Some(4096))?;
    let limits = fs::read_to_string(bounded.join("reader/limits"))?;
    let actual = limits
        .lines()
        .find_map(|line| line.strip_prefix("Max file size"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse::<u64>()?;
    assert_eq!(actual, previous.unwrap_or(u64::MAX).min(4096));
    assert_eq!(getrlimit(Resource::Fsize).current, previous);
    Ok(())
}
