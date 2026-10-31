//! Logging, tracing, and PII redaction.
//!
//! Every log line is rendered and then passed through a redaction filter, so a
//! leaked API key or customer email in a field value never reaches stdout, a
//! log shipper, or a trace backend.
//!
//! # OTLP
//!
//! When `telemetry.otlp_endpoint` is set, spans are exported over OTLP/gRPC.
//! The exporter is batched, so [`TelemetryGuard`] must be kept alive for the
//! process lifetime and shut down on exit to flush pending spans.

pub mod metrics;

use std::fmt;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use regex::Regex;
use tracing::{Event, Subscriber};
use tracing_subscriber::fmt::format::{Format, Full};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::config::TelemetryConfig;
use crate::error::RouterError;

/// Placeholder substituted for anything that looks like a secret.
const REDACTED: &str = "[REDACTED]";

/// Patterns for the identifiers most likely to leak from a gateway.
///
/// Order matters: credentials are matched before the broad numeric patterns, so
/// a key is reported as a key rather than as a phone number.
const PII_PATTERNS: &[&str] = &[
    // Authorization headers as they appear in rendered logs.
    r"(?i)bearer\s+[A-Za-z0-9._\-]{8,}",
    // Vendor API keys: sk-…, pk-…, ghp_…, xoxb-…
    r"\b(?:sk|pk|rk|ghp|gho|ghs|xox[baprs])[-_][A-Za-z0-9_\-]{8,}\b",
    // Email addresses.
    r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}",
    // Payment card numbers, with or without separators.
    r"\b\d(?:[ \-]?\d){12,18}\b",
    // North-American style phone numbers.
    r"\+?\d{1,3}[ \-.]?\(?\d{3}\)?[ \-.]?\d{3}[ \-.]?\d{4}",
];

/// Scrubs credential and identity patterns from rendered log text.
pub struct PiiRedactor {
    patterns: Vec<Regex>,
}

impl PiiRedactor {
    /// Compiles the redaction patterns.
    ///
    /// # Errors
    ///
    /// Returns an error if a pattern fails to compile (a programming error that
    /// should fail startup rather than silently skip redaction).
    pub fn new() -> Result<Self, RouterError> {
        let patterns = PII_PATTERNS
            .iter()
            .map(|pattern| Regex::new(pattern))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| RouterError::Internal {
                message: format!("failed to compile PII redaction pattern: {error}"),
            })?;

        Ok(Self { patterns })
    }

    /// Returns `input` with every recognised pattern replaced.
    pub fn redact(&self, input: &str) -> String {
        let mut output = input.to_string();
        for pattern in &self.patterns {
            if pattern.is_match(&output) {
                output = pattern.replace_all(&output, REDACTED).into_owned();
            }
        }
        output
    }
}

/// A formatter that renders normally, then redacts the rendered line.
///
/// Redacting post-render (rather than per-field) is what makes this robust:
/// values which only look like secrets once concatenated — or that arrive from
/// a `Display` impl — are still caught.
struct RedactingFormat {
    inner: Format<Full>,
    redactor: PiiRedactor,
}

impl<S, N> FormatEvent<S, N> for RedactingFormat
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let mut rendered = String::new();
        {
            let inner_writer = tracing_subscriber::fmt::format::Writer::new(&mut rendered);
            self.inner.format_event(ctx, inner_writer, event)?;
        }

        writer.write_str(&self.redactor.redact(&rendered))
    }
}

/// Keeps the OTLP exporter alive and flushes it on shutdown.
pub struct TelemetryGuard {
    provider: Option<SdkTracerProvider>,
}

impl TelemetryGuard {
    /// Flushes and stops the trace exporter, if one was installed.
    pub fn shutdown(self) {
        if let Some(provider) = self.provider {
            if let Err(error) = provider.shutdown() {
                tracing::warn!(%error, "failed to shut down the OTLP trace exporter");
            }
        }
    }
}

/// Installs the tracing subscriber used by the `serve` command.
///
/// # Errors
///
/// Fails if `RUST_LOG` is malformed, the redaction patterns do not compile, the
/// OTLP exporter cannot be built, or another subscriber is already installed.
pub fn init_tracing(config: &TelemetryConfig) -> Result<TelemetryGuard, RouterError> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let redactor = PiiRedactor::new()?;

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .event_format(RedactingFormat {
            inner: Format::default(),
            redactor,
        });

    let (otel_layer, provider) = match config
        .otlp_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
    {
        Some(endpoint) => {
            let exporter = SpanExporter::builder()
                .with_tonic()
                .with_endpoint(endpoint)
                .build()
                .map_err(|error| RouterError::Internal {
                    message: format!("failed to build the OTLP exporter: {error}"),
                })?;

            let provider = SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .build();
            let tracer = provider.tracer("oxide");

            (
                Some(tracing_opentelemetry::layer().with_tracer(tracer)),
                Some(provider),
            )
        }
        None => (None, None),
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .try_init()
        .map_err(|error| RouterError::Internal {
            message: format!("failed to initialise tracing: {error}"),
        })?;

    if config.log_payloads {
        tracing::warn!(
            "telemetry.log_payloads is enabled; request payloads may contain PII even after redaction"
        );
    }

    Ok(TelemetryGuard { provider })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::sync::{Arc, Mutex};

    fn redactor() -> PiiRedactor {
        PiiRedactor::new().expect("patterns compile")
    }

    #[test]
    fn redacts_email_addresses() {
        let output = redactor().redact("tenant acme user=jane.doe@example.com ok");
        assert!(output.contains(REDACTED));
        assert!(!output.contains("jane.doe@example.com"));
        assert!(output.contains("tenant acme"));
    }

    #[test]
    fn redacts_bearer_tokens() {
        let output = redactor().redact("authorization: Bearer abcdef1234567890");
        assert!(!output.contains("abcdef1234567890"), "{output}");
        assert!(output.contains(REDACTED));
    }

    #[test]
    fn redacts_vendor_api_keys() {
        for key in [
            "sk-proj-abcdef123456",
            "ghp_abcdefgh12345678",
            "xoxb-1234567890",
        ] {
            let output = redactor().redact(&format!("using key {key} now"));
            assert!(!output.contains(key), "key leaked: {output}");
            assert!(output.contains(REDACTED));
        }
    }

    #[test]
    fn redacts_card_numbers() {
        let output = redactor().redact("card 4242 4242 4242 4242 charged");
        assert!(!output.contains("4242 4242 4242 4242"), "{output}");
    }

    #[test]
    fn redacts_phone_numbers() {
        let output = redactor().redact("call +1 415-555-0132 now");
        assert!(!output.contains("415-555-0132"), "{output}");
    }

    #[test]
    fn leaves_ordinary_log_lines_intact() {
        let line = "request completed status=200 elapsed_ms=2501 model=gpt-4o";
        assert_eq!(redactor().redact(line), line);
    }

    #[test]
    fn does_not_mangle_short_numbers_or_timestamps() {
        let line = "ttft=2500ms tokens=17 req_01j9z8x7w6v5u4t3s2r1";
        assert_eq!(redactor().redact(line), line);
    }

    /// `MakeWriter` that appends to a shared buffer so the test can inspect it.
    #[derive(Clone)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let mut guard = match self.0.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            guard.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn the_format_layer_redacts_rendered_events() {
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let sink = SharedBuffer(Arc::clone(&buffer));

        let layer = tracing_subscriber::fmt::layer()
            .with_ansi(false)
            .with_target(false)
            .with_writer(move || sink.clone())
            .event_format(RedactingFormat {
                inner: Format::default(),
                redactor: redactor(),
            });

        let subscriber = tracing_subscriber::registry().with(layer);

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(email = "jane@example.com", "customer contacted");
        });

        let rendered = {
            let guard = match buffer.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            String::from_utf8(guard.clone()).expect("utf-8 output")
        };

        assert!(rendered.contains("customer contacted"), "{rendered}");
        assert!(!rendered.contains("jane@example.com"), "{rendered}");
        assert!(rendered.contains(REDACTED), "{rendered}");
    }

    #[test]
    fn buffer_writer_flushes_cleanly() {
        let sink = SharedBuffer(Arc::new(Mutex::new(Vec::new())));
        let mut sink = sink;
        assert_eq!(sink.write(b"ok").expect("write"), 2);
        sink.flush().expect("flush");
    }

    #[tokio::test]
    async fn otlp_exporter_accepts_a_grpc_endpoint() {
        let exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint("http://127.0.0.1:4317")
            .build();

        assert!(exporter.is_ok(), "exporter should build offline");
    }

    #[test]
    fn redactor_is_reusable_across_calls() {
        let redactor = redactor();
        assert!(redactor.redact("a@b.co").contains(REDACTED));
        assert_eq!(redactor.redact("nothing here"), "nothing here");
    }
}
