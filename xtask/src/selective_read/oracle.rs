//! Independent value rules and comparisons; never invoke a candidate reader.

use std::{io::Read, path::PathBuf};

use super::require;
use crate::Result;

mod io;
mod rows;
mod sort;

#[derive(clap::Args)]
pub(crate) struct Compare {
    #[arg(long)]
    python: PathBuf,
    #[arg(long)]
    harness: PathBuf,
    #[arg(long)]
    reference: PathBuf,
    #[arg(long, value_delimiter = ',')]
    projection: Vec<String>,
    #[arg(long)]
    expected: u64,
    #[arg(long)]
    max_bytes: Option<u64>,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    memory_bytes: usize,
    #[arg(long)]
    subset: bool,
}

pub(crate) fn compare_cli(args: Compare) -> Result<()> {
    let layout = rows::Layout::new(&args.projection)?;
    let input = std::io::stdin().lock();
    let mut input = arrow_ipc::reader::StreamReader::try_new(input, None)?;
    layout.check(input.schema().as_ref())?;
    let mut sorted = sort::Sorted::new(
        &layout,
        input.by_ref().map(|batch| Ok(batch?)),
        args.reference.parent().ok_or("reference has no parent")?,
        args.max_bytes,
        args.memory_bytes,
    )?;
    require(input.get_mut().read(&mut [0])? == 0, "trailing Arrow bytes")?;
    let mut expected = io::Parquet::open(&args.python, &args.harness, &args.reference, &[])?;
    let mut wanted = rows::Rows::new(expected.by_ref(), &layout);
    let count = sort::compare(&mut sorted, &mut wanted, args.expected, args.subset)?;
    expected.finish()?;
    println!("{count}");
    Ok(())
}
