// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Map Volume HTTP public URLs to the co-located gRPC listener (HTTP port + 10000).

/// Convert `http://host:8080` → `http://host:18080` (tonic endpoint).
/// Returns `None` if the URL cannot be parsed or the port would overflow.
pub fn volume_grpc_endpoint(http_url: &str) -> Option<String> {
    let raw = http_url.trim().trim_end_matches('/');
    if raw.is_empty() {
        return None;
    }
    let without_scheme = raw
        .strip_prefix("https://")
        .or_else(|| raw.strip_prefix("http://"))
        .unwrap_or(raw);
    let authority = without_scheme.split('/').next().unwrap_or("");
    if authority.is_empty() {
        return None;
    }
    let (host, port): (String, u16) = if authority.starts_with('[') {
        let end = authority.find(']')?;
        let host = &authority[..=end];
        let port = authority
            .get(end + 1..)
            .and_then(|s| s.strip_prefix(':'))
            .and_then(|p| p.parse().ok())
            .unwrap_or(80);
        (host.to_string(), port)
    } else if let Some((h, p)) = authority.rsplit_once(':') {
        if p.chars().all(|c| c.is_ascii_digit()) {
            (h.to_string(), p.parse().ok()?)
        } else {
            (authority.to_string(), 80)
        }
    } else {
        (authority.to_string(), 80)
    };
    let grpc_port = port.checked_add(10_000)?;
    Some(format!("http://{host}:{grpc_port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_http_port() {
        assert_eq!(
            volume_grpc_endpoint("http://10.0.0.1:8080").as_deref(),
            Some("http://10.0.0.1:18080")
        );
        assert_eq!(
            volume_grpc_endpoint("http://vol-0.hl:8080/").as_deref(),
            Some("http://vol-0.hl:18080")
        );
        assert_eq!(
            volume_grpc_endpoint("https://vol:443").as_deref(),
            Some("http://vol:10443")
        );
    }
}
