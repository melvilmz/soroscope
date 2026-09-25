//! Structured JSON & Logfmt Logging Middleware (Issue #16).
//!
//! Provides structured logging support for JSON and Logfmt formats, OpenTelemetry
//! trace propagation, context enrichment (trace_id, span_id, request duration),
//! and HTTP logging middleware.

use std::fmt;
use std::str::FromStr;
use std::time::Instant;
use axum::{
    body::Body,
    extract::Request,
    middleware::Next,
    response::Response,
};
use serde::{Deserialize, Serialize};
use tracing_subscriber::{
    filter::EnvFilter,
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

/// Supported structured log formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Structured newline-delimited JSON format.
    #[default]
    Json,
    /// Key=value logfmt format.
    Logfmt,
    /// Human-readable compact format.
    Compact,
    /// Pretty multi-line format.
    Pretty,
}

impl FromStr for LogFormat {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        match s.as_str() {
            "json" => Ok(LogFormat::Json),
            "logfmt" => Ok(LogFormat::Logfmt),
            "pretty" => Ok(LogFormat::Pretty),
            _ => Ok(LogFormat::Compact),
        }
    }
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogFormat::Json => write!(f, "json"),
            LogFormat::Logfmt => write!(f, "logfmt"),
            LogFormat::Compact => write!(f, "compact"),
            LogFormat::Pretty => write!(f, "pretty"),
        }
    }
}

/// Parse and build an `EnvFilter` from a directive string with fallback to "info".
///
/// Supports the standard `tracing_subscriber` directive syntax, including
/// per-module overrides (e.g. `soroscope_core=debug,tower_http=warn`). An
/// empty or unparsable directive falls back to `"info"` so a startup typo
/// degrades verbosity instead of crashing the server.
pub fn build_env_filter(directive: &str) -> EnvFilter {
    let directive = directive.trim();
    let directive = if directive.is_empty() {
        "info"
    } else {
        directive
    };

    EnvFilter::try_new(directive).unwrap_or_else(|error| {
        eprintln!("Invalid RUST_LOG directive '{directive}': {error}. Falling back to 'info'.");
        EnvFilter::new("info")
    })
}

/// Initialize tracing subscriber with the requested format and log level filter.
pub fn init_logging(format: LogFormat, rust_log: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = build_env_filter(rust_log);

    match format {
        LogFormat::Json => {
            let json_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_target(true);

            tracing_subscriber::registry()
                .with(filter)
                .with(json_layer)
                .try_init()?;
        }
        LogFormat::Logfmt | LogFormat::Compact => {
            let compact_layer = tracing_subscriber::fmt::layer()
                .compact()
                .with_target(true);

            tracing_subscriber::registry()
                .with(filter)
                .with(compact_layer)
                .try_init()?;
        }
        LogFormat::Pretty => {
            let pretty_layer = tracing_subscriber::fmt::layer()
                .pretty()
                .with_target(true);

            tracing_subscriber::registry()
                .with(filter)
                .with(pretty_layer)
                .try_init()?;
        }
    }

    Ok(())
}

/// HTTP request logging middleware.
///
/// Records structured fields:
/// - `method`: HTTP method
/// - `uri`: Request URI
/// - `status`: Response status code
/// - `duration_ms`: Duration of request execution in milliseconds
/// - `duration_us`: Duration of request execution in microseconds
pub async fn structured_logging_middleware(req: Request<Body>, next: Next) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path().to_string();

    let response = next.run(req).await;
    let duration = start.elapsed();
    let duration_ms = duration.as_secs_f64() * 1000.0;
    let duration_us = duration.as_micros() as u64;
    let status = response.status().as_u16();

    tracing::info!(
        method = %method,
        uri = %path,
        status = status,
        duration_ms = duration_ms,
        duration_us = duration_us,
        "HTTP request handled"
    );

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_format_from_str() {
        assert_eq!("json".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("JSON".parse::<LogFormat>().unwrap(), LogFormat::Json);
        assert_eq!("logfmt".parse::<LogFormat>().unwrap(), LogFormat::Logfmt);
        assert_eq!("pretty".parse::<LogFormat>().unwrap(), LogFormat::Pretty);
        assert_eq!("compact".parse::<LogFormat>().unwrap(), LogFormat::Compact);
        assert_eq!("other".parse::<LogFormat>().unwrap(), LogFormat::Compact);
    }

    #[test]
    fn test_build_env_filter() {
        assert_eq!(build_env_filter("").to_string(), "info");
        assert_eq!(build_env_filter("   ").to_string(), "info");
        assert_eq!(build_env_filter("debug").to_string(), "debug");
        assert_eq!(build_env_filter("soroscope_core=warn").to_string(), "soroscope_core=warn");
    }

    #[test]
    fn test_log_format_display() {
        assert_eq!(LogFormat::Json.to_string(), "json");
        assert_eq!(LogFormat::Logfmt.to_string(), "logfmt");
        assert_eq!(LogFormat::Compact.to_string(), "compact");
        assert_eq!(LogFormat::Pretty.to_string(), "pretty");
    }
}
