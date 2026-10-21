use super::{Result, State, files, require, table};
use crate::selective_read::{array, text};
use serde_json::{Value, json};
use std::path::Path;

impl State {
    pub(super) fn experiment_reference(&mut self, request: &Value) -> Result<Value> {
        let driver = text(&request["driver"])?;
        require(
            matches!(driver, "numeric" | "length"),
            "unknown experiment driver",
        )?;
        let fixture = Path::new(text(&request["fixture"])?);
        let manifest = &request["manifest"];
        let expected = if driver == "numeric" && request["selection"] == "dense" {
            let paths = array(&manifest["files"])?
                .iter()
                .map(|f| files::inside(fixture, text(&f["path"])?))
                .collect::<Result<Vec<_>>>()?;
            let column_type = text(&request["column_type"])?;
            let columns = std::iter::once("row_id".to_owned())
                .chain((0..8).map(|i| format!("{column_type}_{i}")))
                .collect::<Vec<_>>();
            table::parquet(&mut self.decoder, &paths, &columns)?
        } else {
            let key = text(
                &request[if driver == "numeric" {
                    "column_type"
                } else {
                    "selection"
                }],
            )?;
            table::stream(
                &files::inside(fixture, text(&manifest["references"][key]["path"])?)?,
                None,
            )?
        };
        let expected = if driver == "numeric" {
            table::sort(&expected, "row_id")?
        } else {
            expected
        };
        let rows = expected.num_rows();
        self.experiments.insert(driver.to_owned(), expected);
        Ok(json!(rows))
    }

    pub(super) fn experiment_check(&self, request: &Value) -> Result<Value> {
        let driver = text(&request["driver"])?;
        let expected = self
            .experiments
            .get(driver)
            .ok_or("experiment reference not loaded")?;
        let actual = table::stream(Path::new(text(&request["path"])?), None)?;
        let actual = if driver == "numeric" {
            table::sort(&actual, "row_id")?
        } else {
            table::cast(&actual, expected.schema())?
        };
        table::equal(&actual, expected)?;
        Ok(Value::Null)
    }
}

#[cfg(test)]
mod tests;
