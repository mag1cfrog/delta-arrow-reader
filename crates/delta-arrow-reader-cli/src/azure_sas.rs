//! SAS decoding shared by direct Azure access and Unity Catalog credentials.

/// Decodes a SAS token into Azure query parameters, rejecting malformed credentials.
pub(crate) fn parse_query_pairs(token: &str) -> Option<Vec<(String, String)>> {
    let sas = token.strip_prefix('?').unwrap_or(token);
    // SAS signatures use literal '+', which form decoding would otherwise turn into a space.
    let query_pairs: Vec<_> = url::form_urlencoded::parse(sas.replace('+', "%2B").as_bytes())
        .into_owned()
        .collect();
    // Form decoding is lossy for invalid UTF-8, so reject replacement characters too.
    if sas.contains('#')
        || sas.chars().any(char::is_control)
        || sas
            .split('&')
            .any(|part| !part.trim().is_empty() && !part.contains('='))
        || query_pairs
            .iter()
            .any(|(key, value)| key.contains('\u{fffd}') || value.contains('\u{fffd}'))
        || !query_pairs
            .iter()
            .any(|(key, value)| key == "sig" && !value.is_empty())
    {
        return None;
    }
    Some(query_pairs)
}
