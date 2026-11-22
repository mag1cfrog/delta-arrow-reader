//! Disposable benchmark entry point for the two RANGE workloads.

use std::error::Error;
use std::io::Write;
use std::mem::{align_of, offset_of, size_of};
use std::path::Path;

use datafusion::logical_expr::window_state::{WindowAggState, WindowFrameStateRange};
use datafusion::physical_expr::window::WindowState;

#[path = "native_controls.rs"]
mod native_controls;
#[path = "../pending_range_1099/driver.rs"]
mod pending;

fn record_sample(id: &str, iteration: usize, elapsed_ns: u64) -> std::io::Result<()> {
    let mut output = std::io::stdout().lock();
    writeln!(
        output,
        "{}",
        serde_json::json!({"kind": "retained_sample", "id": id,
                           "iteration": iteration, "elapsed_ns": elapsed_ns}),
    )?;
    output.flush()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("trace") => pending::run().await?,
        Some("measure") => {
            pending::run().await?;
            let block = args.get(3).ok_or("missing block")?.parse()?;
            native_controls::run(
                Path::new(args.get(4).ok_or("missing controls")?),
                true,
                block,
            )
            .await?;
        }
        Some("ordinary-trace") => {
            native_controls::run(Path::new(args.get(1).ok_or("missing controls")?), false, 0)
                .await?;
        }
        Some("layout") => {
            println!(
                "{}",
                serde_json::json!({
                    "range_state": [size_of::<WindowFrameStateRange>(), align_of::<WindowFrameStateRange>()],
                    "aggregate_state": [size_of::<WindowAggState>(), align_of::<WindowAggState>()],
                    "window_state": [size_of::<WindowState>(), align_of::<WindowState>()],
                    "aggregate_state_field_offsets": [
                        offset_of!(WindowAggState, window_frame_range),
                        offset_of!(WindowAggState, window_frame_ctx),
                        offset_of!(WindowAggState, last_calculated_index),
                        offset_of!(WindowAggState, offset_pruned_rows),
                        offset_of!(WindowAggState, out_col),
                        offset_of!(WindowAggState, n_row_result_missing),
                        offset_of!(WindowAggState, is_end),
                    ],
                    "window_state_field_offsets": [
                        offset_of!(WindowState, state), offset_of!(WindowState, window_fn),
                    ],
                })
            );
        }
        _ => return Err("unknown pending RANGE workload".into()),
    }
    Ok(())
}
