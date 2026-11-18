//! Conditional expressions regression tests.

#![allow(
    clippy::duplicate_mod,
    reason = "Each regression module retains its independently tested support modules."
)]

mod boolean_evaluation;
mod conditional_error_identifiers;
mod conditional_evaluation;
mod conditional_nested;
mod conditional_semantics;
mod in_list_evaluation;
mod in_list_semantics;
mod null_logic;
mod nullif_float;
mod tuple_not_in_null;
