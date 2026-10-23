//! A small row sample for choosing a projection or checking a predicate.
//!
//! For example, `dar preview ./table --column id --limit 3` returns up to three
//! typed rows. Reaching a limit never claims that the snapshot was exhausted.

use std::num::NonZeroUsize;

use clap::Args;

use crate::{RowReadArgs, parse_unsigned_decimal};

#[derive(Args)]
pub(crate) struct PreviewArgs {
    #[command(flatten)]
    pub(crate) read: RowReadArgs,
    /// Maximum sample rows; zero returns only the selected schema.
    #[arg(long, default_value = "20", value_name = "N", value_parser = parse_unsigned_decimal::<usize>)]
    pub(crate) limit: usize,
    /// Whole JSON response budget, including schema and newline. Narrow the projection first.
    #[arg(long, default_value = "8192", value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) max_output_bytes: NonZeroUsize,
    /// Whole-command deadline in milliseconds, plus bounded cleanup and failure reporting.
    #[arg(long, default_value = "30000", value_name = "N", value_parser = parse_unsigned_decimal::<NonZeroUsize>)]
    pub(crate) deadline_ms: NonZeroUsize,
}
