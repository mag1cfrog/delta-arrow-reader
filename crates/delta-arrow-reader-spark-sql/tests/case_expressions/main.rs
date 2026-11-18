//! Case expressions regression tests.

#![allow(
    clippy::duplicate_mod,
    reason = "Each regression module retains its independently tested support modules."
)]

mod case_explicit_else;
mod case_hidden_sort_declarations;
mod case_nested_declarations;
mod case_reachable_fields;
mod case_result_tails;
