//! Set operations regression tests.

#![allow(
    clippy::duplicate_mod,
    reason = "Each regression module retains its independently tested support modules."
)]

mod set_operation_coercion;
mod set_operation_duplicates;
mod set_operation_float_keys;
mod set_operation_zero_results;
mod union_struct_planning;
