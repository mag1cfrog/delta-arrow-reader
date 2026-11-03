//! Compare MAP entry bags without changing captured values or ordered containers.

use serde_json::Value;

pub(crate) fn unordered_rows_match(actual: &Value, expected: &Value, schema: &Value) -> bool {
    let comparable_rows = |rows: &Value| -> Option<Vec<Value>> {
        rows.as_array()?.iter().map(|row| comparable_value(row, schema)).collect()
    };
    let (Some(mut actual), Some(mut expected)) =
        (comparable_rows(actual), comparable_rows(expected)) else { return false; };
    // These constructor captures already promise unordered rows; MAP iteration can change their sort key.
    actual.sort_by_key(Value::to_string);
    expected.sort_by_key(Value::to_string);
    actual == expected
}

fn comparable_value(value: &Value, data_type: &Value) -> Option<Value> {
    if value.is_null() {
        return Some(Value::Null);
    }
    Some(match data_type.get("type").and_then(Value::as_str) {
        Some("struct") => {
            let fields = data_type["fields"].as_array()?;
            let values = value.as_array()?;
            if fields.len() != values.len() { return None; }
            Value::Array(values.iter().zip(fields).map(|(value, field)|
                comparable_value(value, &field["type"])).collect::<Option<Vec<_>>>()?)
        }
        Some("array") => Value::Array(value.as_array()?.iter().map(|value|
            comparable_value(value, &data_type["elementType"])).collect::<Option<Vec<_>>>()?),
        Some("map") => {
            let mut entries = value.as_array()?.iter().map(|entry| {
                let pair = entry.as_array()?;
                if pair.len() != 2 || pair[0].is_null() { return None; }
                Some(Value::Array(vec![
                    comparable_value(&pair[0], &data_type["keyType"])?,
                    comparable_value(&pair[1], &data_type["valueType"])?,
                ]))
            }).collect::<Option<Vec<_>>>()?;
            // Pinned Spark's Python transport uses HashMap, so only MAP nodes are entry bags.
            entries.sort_by_key(Value::to_string);
            Value::Array(entries)
        }
        _ => value.clone(),
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn map_entry_iteration_preserves_associations_and_positional_values() {
        let data_type = json!({"type":"map", "keyType":"string", "valueType":{
            "type":"struct", "fields":[
                {"name":"item", "type":"integer"},
                {"name":"item", "type":"integer"},
                {"name":"values", "type":{"type":"array", "elementType":"integer"}},
                {"name":"nested", "type":{"type":"map", "keyType":"string", "valueType":"integer"}}
            ]}});
        let actual = json!([["none",null], ["a",["11","12",["1",null,"2"],[["x","3"],["y","4"]]]]]);
        let expected = json!([["a",["11","12",["1",null,"2"],[["y","4"],["x","3"]]]], ["none",null]]);
        let original = actual.clone();
        assert_ne!(actual, expected);
        assert_eq!(comparable_value(&actual, &data_type), comparable_value(&expected, &data_type));
        assert_eq!(actual, original);
        let mut changed = expected.clone();
        changed[0][1][0] = json!("99");
        assert_ne!(comparable_value(&actual, &data_type), comparable_value(&changed, &data_type));
        changed = expected.clone();
        changed[0][1][0] = json!("12");
        changed[0][1][1] = json!("11");
        assert_ne!(comparable_value(&actual, &data_type), comparable_value(&changed, &data_type));
        changed = expected.clone();
        changed[0][1][2] = json!(["2",null,"1"]);
        assert_ne!(comparable_value(&actual, &data_type), comparable_value(&changed, &data_type));
        for changed in [json!([["none",null]]), json!([["a",null],["none",expected[0][1]]]),
            json!([["none",null],["none",null],expected[0]]), json!([["none",[]],expected[0]])] {
            assert_ne!(comparable_value(&actual, &data_type), comparable_value(&changed, &data_type));
        }
        assert!(comparable_value(&json!([["a"]]), &data_type).is_none());
        assert!(comparable_value(&json!([[null,null]]), &data_type).is_none());
    }

    #[test]
    fn map_comparison_keeps_array_and_promised_row_order() {
        let schema = json!({"type":"struct", "fields":[
            {"name":"r", "type":{"type":"map", "keyType":"string", "valueType":"integer"}}
        ]});
        let row_type = json!({"type":"array", "elementType":schema});
        let first_row = json!([[["b","2"],["a","1"]]]);
        let same_first_row = json!([[["a","1"],["b","2"]]]);
        let second_row = json!([[["c","3"]]]);
        let actual = json!([first_row, second_row]);
        let permuted_maps = json!([same_first_row, second_row]);
        let permuted_rows = json!([second_row, same_first_row]);
        assert_eq!(comparable_value(&actual, &row_type), comparable_value(&permuted_maps, &row_type));
        assert_ne!(comparable_value(&actual, &row_type), comparable_value(&permuted_rows, &row_type));
        assert!(unordered_rows_match(&actual, &permuted_rows, &schema));
        assert!(!unordered_rows_match(&actual, &json!([same_first_row]), &schema));
    }
}
