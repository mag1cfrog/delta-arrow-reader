use std::borrow::Cow;
use std::collections::BTreeMap;

use regex::{Regex, RegexBuilder};

use crate::error::{SqlError, SqlResult};

const REDACTED: &str = "*********(redacted)";

/// Expand explicit SQL configuration references before lexing the command.
///
/// Unprefixed names and `spark`, `sparkconf`, `hivevar`, and `hiveconf` share
/// `config_values`; missing names become empty text. Other namespaces remain
/// literal, including `env` and `system`. No ambient properties are read.
/// Redaction checks both names and values before recursively expanding values.
/// Cycles and redaction patterns unsupported by the native ASCII regex matcher
/// return errors. Disabled substitution performs neither lookup nor validation.
pub fn substitute_sql_variables<'a>(
    sql: &'a str,
    enabled: bool,
    config_values: &BTreeMap<String, String>,
) -> SqlResult<Cow<'a, str>> {
    if !enabled || !sql.contains("${") {
        return Ok(Cow::Borrowed(sql));
    }
    // Java's default \w and \S use ASCII classes, unlike Rust's Unicode defaults.
    let references = Regex::new(r"\$\{(?:([A-Za-z0-9_]+?):)?([^ \t\n\x0B\f\r]+?)\}")
        .map_err(|error| SqlError::invalid(error.to_string()))?;
    let mut redaction_patterns = None;
    expand(
        sql,
        config_values,
        &references,
        &mut redaction_patterns,
        &mut Vec::new(),
    )
    .map(Cow::Owned)
}

fn expand(
    input: &str,
    config_values: &BTreeMap<String, String>,
    references: &Regex,
    redaction_patterns: &mut Option<[Regex; 2]>,
    used_references: &mut Vec<String>,
) -> SqlResult<String> {
    let mut output = String::new();
    let mut offset = 0;
    for captures in references.captures_iter(input) {
        let Some(matched) = captures.get(0) else {
            continue;
        };
        output.push_str(&input[offset..matched.start()]);
        offset = matched.end();
        let prefix = captures.get(1).map(|value| value.as_str());
        if !matches!(
            prefix,
            None | Some("spark" | "sparkconf" | "hivevar" | "hiveconf")
        ) {
            output.push_str(matched.as_str());
            continue;
        }
        let name = captures
            .get(2)
            .ok_or_else(|| SqlError::invalid("missing SQL configuration reference name"))?
            .as_str();
        let reference =
            prefix.map_or_else(|| name.to_string(), |prefix| format!("{prefix}:{name}"));
        if used_references.contains(&reference) {
            return Err(SqlError::invalid(format!(
                "java.lang.IllegalArgumentException: requirement failed: Circular reference in {input}: {reference}"
            )));
        }
        if redaction_patterns.is_none() {
            let compile = |key, default| {
                let pattern = config_values.get(key).map_or(default, String::as_str);
                RegexBuilder::new(pattern)
                    .unicode(false)
                    .build()
                    .map_err(|error| SqlError::unsupported(format!(
                        "SQL variable redaction {key} requires a supported native ASCII regex: {error}"
                    )))
            };
            *redaction_patterns = Some([
                compile("spark.sql.redaction.options.regex", "(?i)url")?,
                compile(
                    "spark.redaction.regex",
                    "(?i)secret|password|token|access[.]?key",
                )?,
            ]);
        }
        let mut value = config_values.get(name).map_or("", String::as_str);
        if let Some(patterns) = redaction_patterns {
            for pattern in patterns {
                if pattern.is_match(name) || pattern.is_match(value) {
                    value = REDACTED;
                }
            }
        }
        used_references.push(reference);
        let replacement = expand(
            value,
            config_values,
            references,
            redaction_patterns,
            used_references,
        )?;
        used_references.pop();
        output.push_str(&replacement);
    }
    output.push_str(&input[offset..]);
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_names_follow_java_reference_classes_and_namespaces() -> SqlResult<()> {
        let values = BTreeMap::from([
            ("x".into(), "X".into()),
            (
                "m\u{e9}ta:key".into(),
                "unicode prefix is part of the name".into(),
            ),
            (
                "a\u{a0}b".into(),
                "non-ASCII whitespace is part of the name".into(),
            ),
        ]);
        for (input, expected) in [
            ("${x}", "X"),
            ("${spark:x}", "X"),
            ("${sparkconf:x}", "X"),
            ("${hivevar:x}", "X"),
            ("${hiveconf:x}", "X"),
            ("${missing}", ""),
            ("${other:x}", "${other:x}"),
            ("${env:x}", "${env:x}"),
            ("${system:x}", "${system:x}"),
            ("${}", "${}"),
            ("${x }", "${x }"),
            ("${", "${"),
            ("${m\u{e9}ta:key}", "unicode prefix is part of the name"),
            ("${a\u{a0}b}", "non-ASCII whitespace is part of the name"),
        ] {
            assert_eq!(
                substitute_sql_variables(input, true, &values)?,
                expected,
                "{input}"
            );
        }
        Ok(())
    }

    #[test]
    fn recursive_values_keep_literal_replacement_bytes_and_allow_siblings() -> SqlResult<()> {
        let values = BTreeMap::from([
            ("outer".into(), "${inner}".into()),
            ("inner".into(), "$1\\Q".into()),
        ]);
        assert_eq!(
            substitute_sql_variables("'${outer}/${outer}'", true, &values)?,
            "'$1\\Q/$1\\Q'"
        );
        Ok(())
    }

    #[test]
    fn cycles_fail_only_when_substitution_is_enabled() -> SqlResult<()> {
        let values = BTreeMap::from([("x".into(), "${y}".into()), ("y".into(), "${x}".into())]);
        let error = substitute_sql_variables("${x}", true, &values)
            .err()
            .ok_or_else(|| SqlError::invalid("cycle did not fail"))?;
        assert!(error.to_string().contains("Circular reference in ${x}: x"));
        assert_eq!(substitute_sql_variables("${x}", false, &values)?, "${x}");
        Ok(())
    }

    #[test]
    fn redaction_checks_keys_and_values_before_expansion() -> SqlResult<()> {
        let values = BTreeMap::from([
            ("service.url".into(), "public".into()),
            ("plain".into(), "password in a value".into()),
            ("outer".into(), "${service.url}".into()),
        ]);
        for input in ["${service.url}", "${plain}", "${outer}"] {
            assert_eq!(substitute_sql_variables(input, true, &values)?, REDACTED);
        }
        let before_recursion = BTreeMap::from([
            ("spark.sql.redaction.options.regex".into(), r"\$\{".into()),
            ("outer".into(), "${inner}".into()),
            ("inner".into(), "safe".into()),
        ]);
        assert_eq!(
            substitute_sql_variables("${outer}", true, &before_recursion)?,
            REDACTED
        );
        let unsupported = BTreeMap::from([
            ("spark.redaction.regex".into(), "(?<=private)value".into()),
            ("x".into(), "private value".into()),
        ]);
        assert!(substitute_sql_variables("${x}", true, &unsupported).is_err());
        assert_eq!(
            substitute_sql_variables("${x}", false, &unsupported)?,
            "${x}"
        );
        Ok(())
    }
}
