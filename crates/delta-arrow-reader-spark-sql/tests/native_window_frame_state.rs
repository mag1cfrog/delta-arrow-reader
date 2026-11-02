//! Run the vendored frame-state unit tests through ordinary workspace discovery.

extern crate self as datafusion_common;
pub use datafusion::common::*;
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};

#[expect(
    dead_code,
    reason = "the included upstream module also exposes non-test helpers"
)]
#[rustfmt::skip] // Preserve the vendored module's upstream formatting.
#[path = "../vendor/dependencies/datafusion-expr/src/window_state.rs"]
mod window_state;
