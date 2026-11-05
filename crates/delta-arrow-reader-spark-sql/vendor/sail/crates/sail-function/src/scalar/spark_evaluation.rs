// Local Spark evaluation marker. See the host crate's UPSTREAM.md.
use datafusion_common::config::{ConfigExtension, ConfigOptions};

datafusion_common::extensions_options! {
    /// Marks expressions evaluated the way Spark's optimizer evaluates them: interpreted,
    /// while folding constants or converting local rows. Spark evaluates the remaining rows
    /// with generated code at run time, and a few errors differ between the two paths. Only
    /// the planner's own evaluation of such expressions carries this marker.
    pub struct SparkInterpretedEvaluation {}
}

impl ConfigExtension for SparkInterpretedEvaluation {
    const PREFIX: &'static str = "dar_spark_interpreted_evaluation";
}

impl SparkInterpretedEvaluation {
    /// Whether `options` belong to an evaluation that Spark would interpret.
    pub fn applies_to(options: &ConfigOptions) -> bool {
        options.extensions.get::<Self>().is_some()
    }
}
