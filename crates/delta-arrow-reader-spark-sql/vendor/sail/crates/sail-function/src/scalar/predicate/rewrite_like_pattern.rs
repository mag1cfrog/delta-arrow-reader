// Local Spark LIKE escape policy. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use datafusion_common::{Result, exec_err};

/// Validate Spark escapes and translate them for Arrow's backslash-based LIKE.
/// The caller supplies the original escape even when ILIKE lowercases a pattern.
/// Reuse the buffer for dynamic patterns; matching stays with Arrow.
pub(crate) fn rewrite_like_into_string(
    buffer: &mut String,
    pattern: &str,
    escape: char,
) -> Result<()> {
    buffer.clear();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == escape {
            match chars.next() {
                Some(next) if next == '%' || next == '_' || next == escape => {
                    buffer.push('\\');
                    buffer.push(next);
                }
                Some(other) => {
                    return exec_err!(
                        "[INVALID_FORMAT.ESC_IN_THE_MIDDLE] The format is invalid: '{pattern}'. The escape character is not allowed to precede '{other}'."
                    );
                }
                None => {
                    return exec_err!(
                        "[INVALID_FORMAT.ESC_AT_THE_END] The format is invalid: '{pattern}'. The escape character is not allowed to end with."
                    );
                }
            }
        } else if c == '\\' {
            buffer.push_str("\\\\");
        } else {
            buffer.push(c);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_patterns_preserve_literals_and_wildcards() -> Result<()> {
        let mut buffer = String::new();
        for (pattern, escape, expected) in [
            ("a_b%c", '\\', "a_b%c"),
            (r"\_", '\\', r"\_"),
            ("!%", '!', r"\%"),
            ("!_", '!', r"\_"),
            ("!!", '!', r"\!"),
            (r"a\b", '!', r"a\\b"),
            (r"a!_\b", '!', r"a\_\\b"),
            ("!!%", '!', r"\!%"),
            ("!!!%", '!', r"\!\%"),
            ("ä%ä_ää", 'ä', r"\%\_\ä"),
            ("%%", '%', r"\%"),
            ("__", '_', r"\_"),
        ] {
            rewrite_like_into_string(&mut buffer, pattern, escape)?;
            assert_eq!(buffer, expected, "pattern={pattern:?}, escape={escape:?}");
        }
        Ok(())
    }

    #[test]
    fn invalid_patterns_report_the_escape_cause() -> Result<()> {
        let mut buffer = String::new();
        for (pattern, escape, condition) in [
            ("a!b", '!', "ESC_IN_THE_MIDDLE"),
            (r"a\q", '\\', "ESC_IN_THE_MIDDLE"),
            (r"!\%", '!', "ESC_IN_THE_MIDDLE"),
            ("abc!", '!', "ESC_AT_THE_END"),
            ("abc\\", '\\', "ESC_AT_THE_END"),
            ("ä%ä_äää", 'ä', "ESC_AT_THE_END"),
        ] {
            let error = rewrite_like_into_string(&mut buffer, pattern, escape)
                .err()
                .ok_or_else(|| {
                    datafusion_common::internal_datafusion_err!("expected invalid LIKE pattern")
                })?;
            assert!(
                error
                    .to_string()
                    .contains(&format!("[INVALID_FORMAT.{condition}]"))
            );
            assert!(error.to_string().contains(pattern));
        }
        Ok(())
    }
}
