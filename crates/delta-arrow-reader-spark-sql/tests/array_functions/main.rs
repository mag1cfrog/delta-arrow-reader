//! Array functions regression tests.

#![allow(
    clippy::duplicate_mod,
    reason = "Each regression module retains its independently tested support modules."
)]

mod array_constructor_storage;
mod array_contains;
mod array_map_explode;
mod array_result_fields;
mod array_sort_join;
mod array_struct_declarations;
mod arrays_overlap_result_fields;
mod compact_null_inspection;
mod compact_result_fields;
mod flatten_inner_views;
mod flatten_result_fields;
mod list_constructor_logical_nulls;
mod literal_list_fields;
