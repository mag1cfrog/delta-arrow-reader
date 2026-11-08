//! The retained native timestamp parser cause for the recorded STRING input 'bad'.

use std::error::Error;

use datafusion::common::DataFusionError;

const BAD_TIMESTAMP_PARSE_ERROR: &str = concat!(
    "error in SQL parser: found b at 0:1 expected '-', '+', '0', '1', '2', ",
    "'3', '4', '5', '6', '7', '8', or '9'",
);

pub(crate) fn matches_recorded_bad_input_error(error: &(dyn Error + 'static)) -> bool {
    error
        .downcast_ref::<DataFusionError>()
        .is_some_and(|error| {
            matches!(error.find_root(), DataFusionError::Execution(message)
            if message == BAD_TIMESTAMP_PARSE_ERROR)
        })
}

#[test]
fn recorded_bad_input_error_requires_the_exact_execution_root() {
    for error in [
        DataFusionError::Execution(BAD_TIMESTAMP_PARSE_ERROR.into()),
        DataFusionError::Execution(BAD_TIMESTAMP_PARSE_ERROR.into())
            .context("Optimizer rule 'simplify_expressions' failed"),
    ] {
        assert!(matches_recorded_bad_input_error(&error));
    }
    for error in [
        DataFusionError::Execution("unrelated failure".into()),
        DataFusionError::Plan(BAD_TIMESTAMP_PARSE_ERROR.into()),
        DataFusionError::Execution(BAD_TIMESTAMP_PARSE_ERROR.replace("found b", "found x")),
        DataFusionError::Execution(format!("{BAD_TIMESTAMP_PARSE_ERROR}: another failure")),
    ] {
        assert!(!matches_recorded_bad_input_error(&error));
    }
}
