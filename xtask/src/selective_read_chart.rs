use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::Read,
};

use serde::Deserialize;

use crate::{Result, repository::cargo_workspace_root};

const PUBLIC: &str = "crates/delta-arrow-reader-core/docs/public";
const READERS: [(&str, &str); 5] = [
    ("delta-arrow-reader", "Delta Arrow Reader"),
    ("duckdb", "DuckDB"),
    ("spark", "Spark (single machine)"),
    ("polars", "Polars"),
    ("delta-rs", "delta-rs"),
];
const QUERIES: [(&str, u32, u32); 2] = [("q2", 416, 80), ("q4", 90, 200)];
type Medians = BTreeMap<String, [f64; READERS.len()]>;

#[derive(Deserialize)]
struct Sample {
    run_id: String,
    case_id: String,
    reader_id: String,
    execution_mode: String,
    repetition: u32,
    status: String,
    query_0_completion_ns: u64,
}

fn load_medians(source: impl Read) -> Result<Medians> {
    let mut runs = BTreeSet::new();
    let mut samples = BTreeMap::<_, Vec<_>>::new();
    for row in csv::Reader::from_reader(source).deserialize::<Sample>() {
        let row = row?;
        if row.status != "success"
            || row.execution_mode != "reuse"
            || row.query_0_completion_ns == 0
            || !runs.insert(row.run_id.clone())
        {
            return Err(format!("invalid or duplicate timing sample: {}", row.run_id).into());
        }
        samples
            .entry((row.case_id, row.reader_id))
            .or_default()
            .push((row.repetition, row.query_0_completion_ns as f64 / 1e9));
    }
    if runs.len() != 200 || samples.len() != 40 {
        return Err("expected 200 unique runs covering eight cases and five readers".into());
    }
    let mut values = Medians::new();
    for (query, _, _) in QUERIES {
        for layout in ["localized", "scattered"] {
            for dv in ["", ".dv"] {
                let case = format!("production.{query}.{layout}{dv}");
                let mut medians = [0.0; READERS.len()];
                for (index, (reader, _)) in READERS.iter().enumerate() {
                    let mut runs = samples
                        .remove(&(case.clone(), (*reader).to_owned()))
                        .ok_or_else(|| format!("missing timing samples: {case}/{reader}"))?;
                    runs.sort_by_key(|run| run.0);
                    if runs.len() != 5 || runs.iter().map(|run| run.0).ne(0..5) {
                        return Err(format!("expected repetitions 0..4 for {case}/{reader}").into());
                    }
                    runs.sort_by(|a, b| a.1.total_cmp(&b.1));
                    medians[index] = runs[2].1;
                }
                if medians[1..].iter().any(|value| *value <= medians[0]) {
                    return Err(format!("README headline does not match {case}").into());
                }
                values.insert(case, medians);
            }
        }
    }
    Ok(values)
}

fn render(light: bool, values: &Medians) -> Result<String> {
    let (background, text, muted, accent, other_bar) = if light {
        ("#ffffff", "#1f2328", "#68707d", "#075985", "#d0d7de")
    } else {
        ("#0b0d10", "#f0f2f5", "#98a2b0", "#7dd3fc", "#53606e")
    };
    let mut parts = vec![format!(
        r##"<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="584" viewBox="0 0 1200 584" role="img" aria-labelledby="title description">
<title id="title">Faster Delta Lake reads</title>
<desc id="description">Five readers query 416-column and 90-column Delta tables with about 60 million rows derived from TPC-H lineitem at SF10. All four cases use deletion vectors to mark deleted rows. Matching rows are grouped together in the top panels and spread out within row groups in the bottom panels. Reader names on the left apply to both columns. Blue bars show Delta Arrow Reader; gray bars show the other readers. Bars show median seconds for the first complete query after table initialization over five independent runs. Linear scales start at zero; shorter bars are faster. The 416-column panels span 0 to 80 seconds, and the 90-column panels span 0 to 200 seconds, as labeled in the column headings. Compare bar lengths within the same column. Startup and table initialization are excluded. The full report also includes initialization costs and four cases without deletion vectors.</desc>
<style>text{{font-family:Inter,ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}}</style>
<rect width="1200" height="584" fill="{background}"/>
<text x="44" y="50" fill="{text}" font-size="30" font-weight="700">Faster Delta Lake reads</text>
<text x="44" y="80" fill="{muted}" font-size="16">Query time in seconds. Shorter is faster.</text>"##
    )];
    for (column, (query, columns, max_seconds)) in QUERIES.into_iter().enumerate() {
        let plot_left = 260 + column * 510;
        let plot_right = plot_left + 340;
        parts.push(format!(r#"<text x="{plot_left}" y="124" fill="{text}" font-size="22" font-weight="600">{columns} columns</text>
<text x="{plot_right}" y="124" text-anchor="end" fill="{muted}" font-size="13">Scale: 0-{max_seconds} s</text>"#));
        for (row, (arrangement, matches)) in [
            ("localized", "Matches grouped"),
            ("scattered", "Matches spread out"),
        ]
        .into_iter()
        .enumerate()
        {
            let case = format!("production.{query}.{arrangement}.dv");
            let medians = values
                .get(&case)
                .ok_or_else(|| format!("missing case: {case}"))?;
            let top = 190 + row * 214;
            let case_label = format!(
                "{columns}-column table, {}, with deletion vectors",
                matches.to_ascii_lowercase()
            );
            if column == 0 {
                let y = top - 36;
                parts.push(format!(r#"<text x="44" y="{y}" fill="{text}" font-size="18" font-weight="500">{matches}</text>"#));
            }
            for (index, (reader, label)) in READERS.iter().enumerate() {
                let value = medians[index];
                if !value.is_finite() || value <= 0.0 || value > f64::from(max_seconds) {
                    return Err(
                        format!("timing outside chart scale: {case}/{reader}: {value}").into(),
                    );
                }
                let width = 340.0 * value / f64::from(max_seconds);
                let y = top + index * 28;
                let color = if index == 0 { "#7dd3fc" } else { other_bar };
                let label_color = if index == 0 { accent } else { muted };
                let weight = if index == 0 { 600 } else { 400 };
                let label_y = y + 5;
                if column == 0 {
                    parts.push(format!(r#"<text x="44" y="{label_y}" fill="{label_color}" font-size="16" font-weight="{weight}">{label}</text>"#));
                }
                let bar_y = y - 7;
                let label_x = plot_left as f64 + width + 10.0;
                parts.push(format!(r#"<rect x="{plot_left}" y="{bar_y}" width="{width:.2}" height="14" rx="2" fill="{color}"><title>{case_label}: {label}, {value:.3} s</title></rect>
<text x="{label_x:.2}" y="{label_y}" fill="{label_color}" font-size="15" font-weight="{weight}">{value:.2} s</text>"#));
            }
        }
    }
    parts.push(format!(r#"<text x="44" y="560" fill="{muted}" font-size="13">TPC-H-derived data, about 60M rows per table, with deletion vectors. Median of 5 first queries; startup and table initialization excluded.</text>
</svg>"#));
    Ok(parts.join("\n") + "\n")
}

pub(crate) fn run(check: bool) -> Result<()> {
    let root = cargo_workspace_root(&env::current_dir()?)?;
    let values = load_medians(fs::File::open(root.join(format!(
        "{PUBLIC}/benchmarks/selective-read-current-timings.csv"
    )))?)?;
    for (light, theme) in [(false, "dark"), (true, "light")] {
        let relative = format!("{PUBLIC}/assets/selective-read-readme-{theme}.svg");
        let target = root.join(&relative);
        let rendered = render(light, &values)?;
        if check {
            if fs::read_to_string(&target)? != rendered {
                return Err(format!("generated chart differs: {relative}").into());
            }
        } else {
            fs::write(target, rendered)?;
        }
        println!("{relative}");
    }
    Ok(())
}

#[cfg(test)]
mod tests;
