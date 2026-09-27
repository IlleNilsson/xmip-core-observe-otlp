#![forbid(unsafe_code)]

//! OTLP: a node's figures as OpenTelemetry metrics, sent to an operator's
//! collector over OTLP/HTTP in protobuf.
//!
//! A technology of `xmip-core-observe`. What is exported is observe's —
//! the snapshot, and the figures `observe::FIGURES` names in it; this crate
//! writes them as an `ExportMetricsServiceRequest` ([`metrics`]) with the
//! estate's protobuf writer, and posts it from a thread of its own
//! ([`Exporter`]) over `xmip-core-transport-http`: HTTP/2 where ALPN agrees
//! it, HTTP/1.1 otherwise, TLS through `xmip-core-library-tls`.
//!
//! Metrics only. OTLP's traces want a trace and span identity per Journey
//! and Message and their timing; observe's activity holds an item's
//! identity and when it was seen, and no correlation, so there is no trace
//! here to send (observability-model.md sections 4 and 6).

pub mod exporter;
pub mod metrics;
pub mod resource;
mod response;

#[cfg(test)]
mod collector;

pub use exporter::{DEFAULT_PORT, Exporter, METRICS_PATH, Otlp, Tally};
pub use resource::Resource;
