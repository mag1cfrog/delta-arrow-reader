use super::*;

#[test]
fn matrix_matches_the_retired_python_runner() {
    let cases = cases();
    assert_eq!(cases.len(), 92);
    assert_eq!(
        cases.iter().filter(|case| case.role == "progress").count(),
        16
    );
    let matrix: String = cases
        .iter()
        .map(|case| format!("{}\t{}\n", case.name(), case.role))
        .collect();
    // Captured from the Python runner at fec8403, including the original case order.
    assert_eq!(
        format!("{:x}", Sha256::digest(matrix)),
        "5ea34129d12e8f0f2b43c9d5a8e17fd307ba3c76025b6bc3c6f028dd9aadbe0b"
    );
    let order = ordered_cases(&cases, 0);
    assert_eq!(order, ordered_cases(&cases, 0));
    assert_ne!(order, ordered_cases(&cases, 1));
    assert_eq!(
        order.iter().copied().collect::<BTreeSet<_>>(),
        (0..92).collect()
    );
}

#[test]
fn binary_orders_balance_positions_and_adjacencies() {
    assert_eq!(
        balanced_orders(3),
        vec![
            vec![0, 1, 2],
            vec![1, 2, 0],
            vec![2, 0, 1],
            vec![2, 1, 0],
            vec![0, 2, 1],
            vec![1, 0, 2],
        ]
    );
    for count in 1..=8 {
        let mut positions = BTreeMap::new();
        let mut pairs = BTreeMap::new();
        for order in balanced_orders(count) {
            for (position, binary) in order.iter().enumerate() {
                *positions.entry((binary.to_owned(), position)).or_insert(0) += 1;
            }
            for pair in order.windows(2) {
                *pairs.entry((pair[0], pair[1])).or_insert(0) += 1;
            }
        }
        assert_eq!(positions.len(), count * count);
        assert!(positions.values().all(|value| *value == 2));
        assert_eq!(pairs.len(), count * (count - 1));
        assert!(pairs.values().all(|value| *value == 2));
    }
}

#[test]
fn tree_hash_matches_python_file_identity() -> Result<()> {
    let root = tempfile::tempdir()?;
    fs::create_dir(root.path().join("a"))?;
    fs::write(root.path().join("z"), b"world")?;
    fs::write(root.path().join("a/one"), b"hello")?;
    assert_eq!(
        tree_hash(root.path())?,
        "e92898788e438c70f3849f9984ac75fc8802103ddbf93cbba690146a2b05a9fb"
    );
    Ok(())
}

#[test]
fn summaries_exclude_warmups_and_censor_mixed_results() -> Result<()> {
    let root = tempfile::tempdir()?;
    let output = root.path().join("summary.csv");
    let sample = |case, status, elapsed| {
        json!({
            "case": case, "binary": "new", "phase": "measured", "role": "progress",
            "status": status, "elapsed_us": elapsed, "cpu_seconds": 0.25,
        })
    };
    let mut warmup = sample("complete", "ok", 9999);
    warmup["phase"] = json!("warmup");
    summarize(
        &[
            warmup,
            sample("complete", "ok", 100),
            sample("complete", "ok", 200),
            sample("mixed", "ok", 100),
            sample("mixed", "timeout", 500),
            sample("failed", "process_timeout", 500),
        ],
        &output,
    )?;
    let rows: Vec<BTreeMap<String, String>> = csv::Reader::from_path(output)?
        .deserialize()
        .collect::<std::result::Result<_, _>>()?;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["runs"], "2");
    assert_eq!(rows[0]["median_elapsed_us"], "150");
    assert_eq!(rows[0]["min_elapsed_us"], "100");
    assert_eq!(rows[0]["max_elapsed_us"], "200");
    assert_eq!(rows[0]["median_first_batch_us"], "");
    assert_eq!(rows[1]["other_failures"], "1");
    assert_eq!(rows[2]["completed"], "1");
    assert_eq!(rows[2]["timeouts"], "1");
    for row in &rows[1..] {
        assert_eq!(row["median_elapsed_us"], "");
        assert_eq!(row["min_elapsed_us"], "");
        assert_eq!(row["max_elapsed_us"], "");
        assert_eq!(row["median_cpu_seconds"], "");
    }
    Ok(())
}

#[test]
fn measurement_records_child_failures_invalid_json_and_watchdog_timeout() -> Result<()> {
    let failed = measure(
        Command::new("sh").args(["-c", "printf failure >&2; exit 17"]),
        Duration::from_secs(5),
    )?;
    assert_eq!(failed["status"], "child_error");
    assert_eq!(failed["exit_code"], 17);
    assert_eq!(failed["stderr"], "failure");
    for stdout in [
        "not-json",
        "[]",
        "{}",
        r#"{"status":"ok","elapsed_us":"bad"}"#,
    ] {
        let invalid = measure(Command::new("printf").arg(stdout), Duration::from_secs(5))?;
        assert_eq!(invalid["status"], "invalid_output");
        assert_eq!(invalid["stdout"], stdout);
    }
    let timeout = measure(Command::new("sleep").arg("30"), Duration::from_millis(20))?;
    assert_eq!(timeout["status"], "process_timeout");
    assert!(timeout["cpu_seconds"].as_f64().unwrap() >= 0.0);
    assert!(timeout["load_average"].as_f64().unwrap() >= 0.0);
    Ok(())
}
