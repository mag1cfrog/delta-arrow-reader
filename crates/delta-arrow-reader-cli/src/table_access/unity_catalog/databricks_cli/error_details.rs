//! The part of a Databricks failure a caller can use to correct a request.
//!
//! For example, a 403 keeps the service's explanation and identifies an access
//! problem. An unfamiliar error keeps its explanation without guessing a cause.

use std::{fmt, sync::LazyLock};

use regex::Regex;
use url::Url;

// The renderer can shorten this further when snapshot bindings fill the terminal record.
const MAX_MESSAGE_BYTES: usize = 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DatabricksErrorDetails {
    pub(crate) message: Option<String>,
    pub(crate) message_truncated: bool,
    pub(crate) source_kind: Option<&'static str>,
}

impl DatabricksErrorDetails {
    pub(super) fn from_stderr(stderr: &[u8], profile: &str) -> Self {
        let Ok(stderr) = std::str::from_utf8(stderr) else {
            return Self::default();
        };
        static ANSI_ESCAPE: LazyLock<Result<Regex, regex::Error>> =
            LazyLock::new(|| Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]"));
        let Ok(ansi_escape) = ANSI_ESCAPE.as_ref() else {
            return Self::default();
        };
        let stderr: String = ansi_escape
            .replace_all(stderr, "")
            .chars()
            .filter(|ch| !ch.is_control() || matches!(ch, '\n' | '\t'))
            .collect();
        let Some(mut message) = stderr
            .strip_prefix("Error: ")
            .or_else(|| stderr.split_once("\nError: ").map(|(_, error)| error))
        else {
            return Self::default();
        };

        let mut details = Self::default();
        if let Some((body, footer)) = message.rsplit_once("\n\nProfile:") {
            details.source_kind = auth_footer_source_kind(footer, profile);
            message = body;
        }
        // SDK decoding failures append credential-bearing HTTP dumps even without debug logging.
        for marker in [
            ". This is likely a bug in the Databricks SDK for Go",
            ". Config: ",
            "\nRequest log:",
        ] {
            message = message.split_once(marker).map_or(message, |(body, _)| body);
        }
        let Some(mut message) = redact_credentials(message.trim()) else {
            return details;
        };
        if message.len() > MAX_MESSAGE_BYTES {
            message.truncate(message.floor_char_boundary(MAX_MESSAGE_BYTES));
            details.message_truncated = true;
        }
        details.message = (!message.is_empty()).then_some(message);
        details
    }

    pub(crate) fn hint(&self) -> Option<&'static str> {
        match self.source_kind {
            Some("unauthenticated") => {
                Some("Check the login for the same Databricks CLI profile, then retry.")
            }
            Some("permission_denied") => Some(
                "Ask an administrator to check table access and any privilege named in the upstream error.",
            ),
            _ => None,
        }
    }
}

impl fmt::Display for DatabricksErrorDetails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(message) = &self.message {
            write!(formatter, ": {message}")?;
            if self.message_truncated {
                formatter.write_str(" [truncated]")?;
            }
        } else {
            formatter.write_str(".")?;
        }
        Ok(())
    }
}

fn auth_footer_source_kind(footer: &str, profile: &str) -> Option<&'static str> {
    // CLI v1.15.0 libs/auth/error.go emits this footer only for HTTP 401/403.
    // shortcut: new footer layouts stay unknown; add a layout only after checking upstream.
    let lines: Vec<_> = footer.trim_end().lines().collect();
    if lines.len() != 7
        || lines[0] != format!("   {profile}")
        || !lines[1].starts_with("Host:      ")
        || !lines[2].starts_with("Auth type: ")
        || !lines[3].is_empty()
        || lines[4] != "Next steps:"
        || lines[6]
            != format!("  - Check your identity: databricks auth describe --profile {profile}")
    {
        return None;
    }
    if lines[5] == "  - Verify you have the required permissions for this operation" {
        return Some("permission_denied");
    }
    let authentication_steps = [
        format!("  - Re-authenticate: databricks auth login --profile {profile}"),
        format!(
            "  - Regenerate your access token or run: databricks auth login --profile {profile}"
        ),
        format!(
            "  - Check your username/password or run: databricks auth login --profile {profile}"
        ),
        "  - Re-authenticate with Azure: az login".into(),
        "  - Check your service principal client ID and secret".into(),
        "  - Check your authentication credentials".into(),
    ];
    authentication_steps
        .iter()
        .any(|step| lines[5] == step)
        .then_some("unauthenticated")
}

fn redact_credentials(message: &str) -> Option<String> {
    // shortcut: unlabelled opaque secrets cannot be identified; extend known formats with evidence.
    static URL: LazyLock<Result<Regex, regex::Error>> = LazyLock::new(|| {
        Regex::new(concat!(
            r#"(?i)"[a-z][a-z0-9+.-]*:(?://|\\/\\/)(?:[^"\\]|\\.)*"|"#,
            r"\b[a-z][a-z0-9+.-]*:(?://|\\/\\/)[^\s<>]+",
        ))
    });
    static PRIVATE_KEY: LazyLock<Result<Regex, regex::Error>> = LazyLock::new(|| {
        Regex::new(
            r"(?s)-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----.*?(?:-----END [A-Z0-9 ]*PRIVATE KEY-----|\z)",
        )
    });
    static BEARER: LazyLock<Result<Regex, regex::Error>> =
        LazyLock::new(|| Regex::new(r"(?i)\bBearer\s+[a-z0-9._~+/=-]+"));
    // If a filter cannot compile, omit the excerpt instead of returning unfiltered text.
    let urls = URL.as_ref().ok()?;
    let private_keys = PRIVATE_KEY.as_ref().ok()?;
    let bearer = BEARER.as_ref().ok()?;
    // Redact labels before URL normalization can percent-encode them.
    let message = redact_credential_assignments(message)?;
    let message = private_keys.replace_all(&message, "[redacted]");
    let message = bearer.replace_all(&message, "Bearer [redacted]");
    Some(
        urls.replace_all(&message, |captures: &regex::Captures<'_>| {
            if captures[0].starts_with('"') {
                return serde_json::from_str::<String>(&captures[0])
                    .map(|url| serde_json::json!(redact_url(&url)).to_string())
                    .unwrap_or_else(|_| "[URL omitted]".into());
            }
            redact_url(&captures[0])
        })
        .into_owned(),
    )
}

fn redact_credential_assignments(message: &str) -> Option<String> {
    static CREDENTIAL_LABEL: LazyLock<Result<Regex, regex::Error>> = LazyLock::new(|| {
        Regex::new(concat!(
            // Accept environment prefixes and the SDKs' snake_case/camelCase spellings.
            r#"(?i)\b(?:[a-z][a-z0-9]*[_-])*(?:token|access[_-]?token|refresh[_-]?token|id[_-]?token|token[_-]?value|password|"#,
            r#"client[_-]?secret|azure[_-]?client[_-]?secret|google[_-]?credentials|private[_-]?key|authorization|"#,
            r#"access[_-]?key[_-]?id|secret[_-]?access[_-]?key|session[_-]?token|sas[_-]?token|aad[_-]?token|oauth[_-]?token|sig)"#,
            r#"\b["']?\s*[:=]\s*"#,
        ))
    });
    static SINGLE_QUOTED_VALUE: LazyLock<Result<Regex, regex::Error>> =
        LazyLock::new(|| Regex::new(r"^'(?:[^'\\]|\\.)*'"));
    let labels = CREDENTIAL_LABEL.as_ref().ok()?;
    let single_quoted = SINGLE_QUOTED_VALUE.as_ref().ok()?;
    let mut output = String::new();
    let mut consumed_bytes = 0;
    for label in labels.find_iter(message) {
        if label.start() < consumed_bytes {
            continue;
        }
        output.push_str(&message[consumed_bytes..label.end()]);
        output.push_str("[redacted]");
        let value = &message[label.end()..];
        let value_bytes = if value.starts_with(['"', '{', '[']) {
            let mut values =
                serde_json::Deserializer::from_str(value).into_iter::<serde_json::Value>();
            values
                .next()
                .and_then(Result::ok)
                .map(|_| values.byte_offset())
        } else {
            single_quoted.find(value).map(|quoted| quoted.end())
        };
        let value_bytes = value_bytes.filter(|end| {
            let suffix = &value[*end..];
            suffix.chars().next().is_none_or(|next| {
                next.is_whitespace() || matches!(next, ',' | ';' | '}' | ']' | ')')
            }) || suffix.strip_prefix('.').is_some_and(|after_period| {
                after_period.chars().next().is_none_or(char::is_whitespace)
            })
        });
        // An ambiguous/unquoted value can include separators or nested data; omit the remainder.
        consumed_bytes = label.end() + value_bytes.unwrap_or(value.len());
    }
    output.push_str(&message[consumed_bytes..]);
    Some(output)
}

fn redact_url(text: &str) -> String {
    let normalized = text.replace(r"\/", "/");
    let Ok(mut url) = Url::parse(&normalized) else {
        return "[URL omitted]".to_owned();
    };
    if url.query().is_none()
        && url.fragment().is_none()
        && url.username().is_empty()
        && url.password().is_none()
    {
        return text.to_owned();
    }
    if url.set_username("").is_err() || url.set_password(None).is_err() {
        return "[URL omitted]".to_owned();
    }
    url.set_query(None);
    url.set_fragment(None);
    format!("{url}[URL details removed]")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(stderr: &str) -> DatabricksErrorDetails {
        DatabricksErrorDetails::from_stderr(stderr.as_bytes(), "work")
    }

    #[test]
    fn recognized_auth_footer_classifies_without_guessing_the_privilege() {
        for (step, kind) in [
            (
                "Verify you have the required permissions for this operation",
                "permission_denied",
            ),
            (
                "Re-authenticate: databricks auth login --profile work",
                "unauthenticated",
            ),
            (
                "Regenerate your access token or run: databricks auth login --profile work",
                "unauthenticated",
            ),
            (
                "Check your username/password or run: databricks auth login --profile work",
                "unauthenticated",
            ),
            ("Re-authenticate with Azure: az login", "unauthenticated"),
            (
                "Check your service principal client ID and secret",
                "unauthenticated",
            ),
            ("Check your authentication credentials", "unauthenticated"),
        ] {
            let stderr = format!(
                "Error: Access refused.\n\nProfile:   work\nHost:      https://workspace.invalid\nAuth type: fixture\n\nNext steps:\n  - {step}\n  - Check your identity: databricks auth describe --profile work\n"
            );
            let details = parse(&stderr);
            assert_eq!(details.source_kind, Some(kind));
            assert_eq!(details.message.as_deref(), Some("Access refused."));
            assert!(details.hint().is_some());
            // A different profile or a changed layout cannot establish a cause.
            for altered in [
                stderr.replace("Profile:   work", "Profile:   other"),
                stderr.replace("Next steps:", "Suggested steps:"),
            ] {
                assert_eq!(parse(&altered).source_kind, None);
            }
        }
        for explanation in [
            "Unsupported fixture option: sample_mode.",
            "Invalid column EXTERNAL_USE_SCHEMA.",
            "EXTERNAL_USE_SCHEMA is already granted; another permission is missing.",
            "The option is invalid.\nAllowed values are A and B.",
        ] {
            let details = parse(&format!("Error: {explanation}\n"));
            assert_eq!(details.message.as_deref(), Some(explanation));
            assert_eq!(details.source_kind, None);
            assert_eq!(details.hint(), None);
        }
        assert_eq!(
            parse("unrecognized stderr secret-sentinel"),
            DatabricksErrorDetails::default()
        );
        assert_eq!(
            DatabricksErrorDetails::from_stderr(b"Error: \xff", "work"),
            DatabricksErrorDetails::default()
        );
    }

    #[test]
    fn sdk_logs_and_config_are_removed_before_filtering_the_explanation() {
        // SDK v0.175.0 response.go appends this dump on a successful HTTP response
        // whose body cannot be decoded; its logger does not redact all cloud keys.
        for trailer in [
            ". This is likely a bug in the Databricks SDK for Go or the underlying REST API. Request log:\n```\n< secret_access_key: sdk-secret\n```",
            ". Config: host=https://workspace.invalid, token=***. Env: DATABRICKS_TOKEN",
            "\nRequest log:\n< oauth_token: sdk-secret",
        ] {
            let details = parse(&format!(
                "debug log sdk-secret\nError: Failed to decode response{trailer}"
            ));
            assert_eq!(
                details.message.as_deref(),
                Some("Failed to decode response")
            );
        }
        for field in [
            "token",
            "access_token",
            "refresh_token",
            "id_token",
            "token_value",
            "password",
            "client_secret",
            "azure_client_secret",
            "google_credentials",
            "private_key",
            "authorization",
            "access_key_id",
            "secret_access_key",
            "session_token",
            "sas_token",
            "aad_token",
            "oauth_token",
            "sig",
        ] {
            for value in [
                "sensitive value",
                "\"sensitive \\\"value\\\"\"",
                "'sensitive value'",
                "\"sensitive unterminated value",
            ] {
                let details = parse(&format!("Error: Rejected {field}={value}"));
                let message = details.message.unwrap();
                assert!(message.contains("[redacted]"), "{message}");
                assert!(!message.contains("sensitive"), "{message}");
            }
        }
        let details = parse(
            "Error: Refused Bearer sensitive-bearer at https://name:sensitive-password@example.invalid/table?sig=sensitive-signature#sensitive-fragment",
        );
        assert_eq!(
            details.message.as_deref(),
            Some("Refused Bearer [redacted] at https://example.invalid/table[URL details removed]")
        );
        assert!(
            !parse("Error: Cannot reach https://[broken?sig=sensitive-signature")
                .message
                .unwrap()
                .contains("sensitive")
        );
    }

    #[test]
    fn redaction_precedes_utf8_bounding_and_removes_terminal_controls() {
        let details = parse(&format!(
            "Error: token={}",
            "secret".repeat(MAX_MESSAGE_BYTES)
        ));
        assert_eq!(details.message.as_deref(), Some("token=[redacted]"));
        assert!(!details.message_truncated);
        let details = parse(&format!("Error: {}", "界".repeat(MAX_MESSAGE_BYTES)));
        assert!(details.message_truncated);
        assert!(details.message.unwrap().len() <= MAX_MESSAGE_BYTES);
        assert_eq!(
            parse("\x1b[31mError: tok\0en=secret\x1b[0m")
                .message
                .as_deref(),
            Some("token=[redacted]")
        );
    }

    #[test]
    fn credential_aliases_private_keys_and_quoted_urls_are_filtered() {
        for stderr in [
            "Error: DATABRICKS_TOKEN=fixture-secret-env",
            "Error: AWS_SECRET_ACCESS_KEY=fixture-secret-env",
            r#"Error: {"SecretAccessKey":"fixture-secret-camel"}"#,
            "Error: private_key=-----BEGIN PRIVATE KEY-----\nfixture-secret-pem\n-----END PRIVATE KEY-----",
            "Error: -----BEGIN RSA PRIVATE KEY-----\nfixture-secret-pem",
            r#"Error: {"url":"https:\/\/user:fixture-secret-url@example.invalid/a?x=fixture-secret-query"}"#,
            "Error: https://user:fixture-secret-'apostrophe@example.invalid/a?x=fixture-secret-query",
            "Error: https://user:fixture-secret-\"quote@example.invalid/a?x=fixture-secret-query",
        ] {
            let details = parse(stderr);
            let message = details.message.as_ref().unwrap();
            assert!(!message.contains("fixture-secret"), "{message}");
            assert!(
                message.contains("[redacted]") || message.contains("[URL details removed]"),
                "{message}"
            );
            assert!(!format!("{details:?} {details}").contains("fixture-secret"));
        }
    }

    #[test]
    fn complete_credential_values_are_redacted_before_url_conversion() {
        for stderr in [
            r#"Error: {"url":"https://user:fixture-first@example.invalid/a","access_token":"fixture-second"}"#,
            r#"Error: {"url":"https://user:fixture-first@example.invalid/a","authorization":"Bearer fixture-second"}"#,
            r#"Error: {"url":"https:\/\/user:fixture-first@example.invalid/a","access_token":"fixture-second"}"#,
            "Error: password=[redacted]fixture-second",
            "Error: password=\"[redacted]\"fixture-second",
            "Error: password='[redacted]'fixture-second",
            "Error: password=\"[redacted]\".fixture-second",
            r#"Error: google_credentials={"refresh_token":"fixture-first,fixture-second"}"#,
            r#"Error: google_credentials={"client_secret":"fixture-first;fixture-second"}"#,
            "Error: google_credentials={\n\"opaque\":\"fixture-first\",\n\"nested\":[{\"opaque\":\"fixture-second\"}]}\nNext action: check login.",
            "Error: google_credentials={malformed \n opaque=fixture-first",
        ] {
            let message = parse(stderr).message.unwrap();
            assert!(!message.contains("fixture-"), "{message}");
        }
        let message = parse(r#"Error: {"url":"https://user:fixture-first@example.invalid/a","access_token":"fixture-second","reason":"Check login."}"#).message.unwrap();
        assert!(message.contains(r#""reason":"Check login.""#), "{message}");
        assert!(
            message.contains(r#""access_token":[redacted]"#),
            "{message}"
        );
        for value in [r#""fixture-first""#, "'fixture-first'"] {
            let explanation = ". EXTERNAL_USE_SCHEMA is required on catalog.schema.";
            let message = parse(&format!("Error: Rejected token={value}{explanation}"))
                .message
                .unwrap();
            assert_eq!(message, format!("Rejected token=[redacted]{explanation}"));
        }
    }
}
