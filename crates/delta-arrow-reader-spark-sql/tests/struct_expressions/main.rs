//! Struct expressions regression tests.

#![allow(
    clippy::duplicate_mod,
    reason = "Each regression module retains its independently tested support modules."
)]

mod constant_struct_fields;
mod named_struct_declarations;
mod null_base_extraction;
mod null_struct_parent;
mod struct_dictionary_fields;
mod struct_expression_names;
mod struct_field_resolution;
