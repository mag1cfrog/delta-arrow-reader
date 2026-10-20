mod supervise;

use clap::Subcommand;
use serde_json::Value;

use crate::Result;

#[derive(Subcommand)]
pub(crate) enum Task {
    /// Supervise a reader process. Read the launch request as JSON from stdin.
    Supervise,
}

impl Task {
    pub(crate) fn run(self) -> Result<()> {
        let input: Value = serde_json::from_reader(std::io::stdin().lock())?;
        let result = match self {
            Self::Supervise => supervise::launch(serde_json::from_value(input)?)?,
        };
        println!("{}", serde_json::to_string(&result)?);
        Ok(())
    }
}
