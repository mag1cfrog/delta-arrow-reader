use super::*;
use crate::repository::repository_root;

fn timings() -> Vec<u8> {
    fs::read(repository_root().join(format!(
        "{PUBLIC}/benchmarks/selective-read-current-timings.csv"
    )))
    .unwrap()
}

#[test]
fn published_charts_are_byte_identical() -> Result<()> {
    let values = load_medians(timings().as_slice())?;
    for (light, theme) in [(false, "dark"), (true, "light")] {
        assert_eq!(
            render(light, &values)?.as_bytes(),
            fs::read(
                repository_root()
                    .join(format!("{PUBLIC}/assets/selective-read-readme-{theme}.svg"))
            )?
        );
    }
    Ok(())
}

fn encode(headers: &csv::StringRecord, rows: &[Vec<String>]) -> Vec<u8> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(headers).unwrap();
    for row in rows {
        writer.write_record(row).unwrap();
    }
    writer.into_inner().unwrap()
}

#[test]
fn invalid_or_incomplete_samples_are_rejected() -> Result<()> {
    let input = timings();
    let mut reader = csv::Reader::from_reader(input.as_slice());
    let headers = reader.headers()?.clone();
    let rows: Vec<Vec<String>> = reader
        .records()
        .map(|row| Ok(row?.iter().map(str::to_owned).collect()))
        .collect::<Result<_>>()?;
    for (field, value) in [
        ("status", "failed"),
        ("execution_mode", "open"),
        ("reader_id", "unknown"),
        ("case_id", "unknown"),
        ("repetition", "1"),
        ("query_0_completion_ns", "0"),
        ("query_0_completion_ns", "-1"),
        ("query_0_completion_ns", "NaN"),
    ] {
        let mut changed = rows.clone();
        let index = headers.iter().position(|header| header == field).unwrap();
        changed[0][index] = value.to_owned();
        assert!(
            load_medians(encode(&headers, &changed).as_slice()).is_err(),
            "{field}={value}"
        );
    }
    assert!(load_medians(encode(&headers, &rows[1..]).as_slice()).is_err());
    let mut changed = rows.clone();
    changed[1] = changed[0].clone();
    assert!(load_medians(encode(&headers, &changed).as_slice()).is_err());
    let mut reversed = rows.clone();
    reversed.reverse();
    assert_eq!(
        load_medians(encode(&headers, &reversed).as_slice())?,
        load_medians(input.as_slice())?
    );
    let mut changed = rows;
    let reader = headers
        .iter()
        .position(|header| header == "reader_id")
        .unwrap();
    let duration = headers
        .iter()
        .position(|header| header == "query_0_completion_ns")
        .unwrap();
    for row in &mut changed {
        if row[reader] == READERS[0].0 {
            row[duration] = "1000000000000".into();
        }
    }
    let error = load_medians(encode(&headers, &changed).as_slice()).unwrap_err();
    assert!(error.to_string().contains("headline"), "{error}");
    Ok(())
}

#[test]
fn chart_rejects_values_outside_its_scale() -> Result<()> {
    let mut values = load_medians(timings().as_slice())?;
    for value in [0.0, 81.0, f64::NAN, f64::INFINITY] {
        values.get_mut("production.q2.localized.dv").unwrap()[1] = value;
        assert!(render(false, &values).is_err());
    }
    Ok(())
}
