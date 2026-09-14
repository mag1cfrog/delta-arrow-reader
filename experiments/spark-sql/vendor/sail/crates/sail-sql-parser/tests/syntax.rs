// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use sail_sql_parser::ast::statement::Statement;
use sail_sql_parser::tree::SyntaxGraph;
use serde_json::{Value, json};

#[test]
#[expect(clippy::unwrap_used)]
fn test_syntax() {
    let actual = json!({"tests": [{
        "input": null,
        "output": {"success": SyntaxGraph::build::<Statement>()}
    }]});
    let expected: Value = serde_json::from_str(include_str!("gold_data/syntax.json")).unwrap();
    assert_eq!(actual, expected);
}
