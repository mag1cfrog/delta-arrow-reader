//! JSON and hash function result coverage.

#[allow(dead_code)]
#[path = "../support/spark_inputs.rs"]
mod spark_inputs;
#[allow(dead_code)]
#[path = "../support/spark_results.rs"]
mod spark_results;

mod from_json_permissive;
mod get_json_object_extraction;
mod hash_decimal_encoding;
mod hash_nested_values;
mod hash_null_arguments;
mod json_array_length_roots;
mod sha2_literal_lengths;
mod xxhash64_decimal_encoding;
mod xxhash64_dictionary_seeds;
mod xxhash64_null_structs;
