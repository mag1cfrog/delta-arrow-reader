/// Options for the SQL parser.
#[derive(Debug, Clone, Default)]
pub struct ParserOptions {
    /// Whether to escape quotes by doubling them in non-raw double-quoted
    /// strings and prefixed non-raw single-quoted strings.
    /// Unprefixed single-quoted strings and backtick identifiers always
    /// recognize doubled quotes.
    pub allow_dual_quote_escape: bool,
    /// Whether a string can be delimited by triple quote characters.
    pub allow_triple_quote_string: bool,
    /// Whether to treat double-quoted strings as identifiers.
    pub allow_double_quote_identifier: bool,
}
