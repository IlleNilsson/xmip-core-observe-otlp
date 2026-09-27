//! A snapshot as an `ExportMetricsServiceRequest`: what OTLP/HTTP posts to
//! `/v1/metrics`, in protobuf, written with the estate's own protobuf
//! writer (`message::protobuf`).
//!
//! One resource — the node — and one instrumentation scope — this crate —
//! holding one metric per figure observe names (`observe::FIGURES`) that
//! has points. A gauge figure is an OTLP `Gauge`; a window figure a `Sum`,
//! delta and monotonic, its point's start the window's opening. Every point
//! carries the scope it is of, and a mood's word where it has one.
//!
//! The field numbers are opentelemetry-proto's, `opentelemetry/proto/`
//! `collector/metrics/v1/metrics_service.proto`, `metrics/v1/metrics.proto`,
//! `common/v1/common.proto` and `resource/v1/resource.proto`, each named
//! here by the message and field it is.

use message::protobuf::{write_delimited, write_i64, write_message, write_varint};
use observe::{FIGURES, Figure, Health, Kind, Point, Reading, Snapshot};

use crate::resource::Resource;

/// `ExportMetricsServiceRequest.resource_metrics`.
const REQUEST_RESOURCE_METRICS: u32 = 1;
/// `ResourceMetrics.resource`.
const RESOURCE_METRICS_RESOURCE: u32 = 1;
/// `ResourceMetrics.scope_metrics`.
const RESOURCE_METRICS_SCOPE_METRICS: u32 = 2;
/// `Resource.attributes`.
const RESOURCE_ATTRIBUTES: u32 = 1;
/// `ScopeMetrics.scope`.
const SCOPE_METRICS_SCOPE: u32 = 1;
/// `ScopeMetrics.metrics`.
const SCOPE_METRICS_METRICS: u32 = 2;
/// `InstrumentationScope.name`.
const SCOPE_NAME: u32 = 1;
/// `InstrumentationScope.version`.
const SCOPE_VERSION: u32 = 2;
/// `Metric.name`.
const METRIC_NAME: u32 = 1;
/// `Metric.description`.
const METRIC_DESCRIPTION: u32 = 2;
/// `Metric.unit`.
const METRIC_UNIT: u32 = 3;
/// `Metric.gauge`.
const METRIC_GAUGE: u32 = 5;
/// `Metric.sum`.
const METRIC_SUM: u32 = 7;
/// `Gauge.data_points` and `Sum.data_points`.
const DATA_POINTS: u32 = 1;
/// `Sum.aggregation_temporality`.
const SUM_TEMPORALITY: u32 = 2;
/// `Sum.is_monotonic`.
const SUM_MONOTONIC: u32 = 3;
/// `AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA`.
const DELTA: u64 = 1;
/// `NumberDataPoint.start_time_unix_nano`, a `fixed64`.
const POINT_START: u32 = 2;
/// `NumberDataPoint.time_unix_nano`, a `fixed64`.
const POINT_TIME: u32 = 3;
/// `NumberDataPoint.as_int`, an `sfixed64`.
const POINT_AS_INT: u32 = 6;
/// `NumberDataPoint.attributes`.
const POINT_ATTRIBUTES: u32 = 7;
/// `KeyValue.key`.
const KEY: u32 = 1;
/// `KeyValue.value`.
const VALUE: u32 = 2;
/// `AnyValue.string_value`.
const STRING_VALUE: u32 = 1;

/// The instrumentation scope every metric is written under: this crate.
const INSTRUMENTATION: &str = env!("CARGO_PKG_NAME");
const INSTRUMENTATION_VERSION: &str = env!("CARGO_PKG_VERSION");

/// `snapshot` as an `ExportMetricsServiceRequest` from `resource`,
/// appended to `out`.
pub fn write_request(out: &mut Vec<u8>, snapshot: &Snapshot, resource: &Resource) {
    write_message(out, REQUEST_RESOURCE_METRICS, |metrics| {
        write_message(metrics, RESOURCE_METRICS_RESOURCE, |held| {
            for (key, value) in resource.attributes() {
                write_attribute(held, RESOURCE_ATTRIBUTES, key, value);
            }
        });
        write_message(metrics, RESOURCE_METRICS_SCOPE_METRICS, |scoped| {
            write_message(scoped, SCOPE_METRICS_SCOPE, |scope| {
                write_delimited(scope, SCOPE_NAME, INSTRUMENTATION.as_bytes());
                write_delimited(scope, SCOPE_VERSION, INSTRUMENTATION_VERSION.as_bytes());
            });
            let reading = Reading::of(snapshot);
            let attributes = Attributes::of(&reading);
            for figure in &FIGURES {
                write_metric(scoped, figure, &reading, &attributes);
            }
        });
    });
}

/// One figure as a `Metric`, or nothing where the snapshot holds no point
/// of it: a metric with no points is one a collector refuses.
fn write_metric(
    out: &mut Vec<u8>,
    figure: &Figure,
    reading: &Reading<'_>,
    attributes: &Attributes,
) {
    let points = reading.points(figure);
    if points.len() == 0 {
        return;
    }
    write_message(out, SCOPE_METRICS_METRICS, |metric| {
        write_delimited(metric, METRIC_NAME, figure.name('.').as_bytes());
        write_delimited(metric, METRIC_DESCRIPTION, figure.description.as_bytes());
        write_delimited(metric, METRIC_UNIT, figure.unit.as_bytes());
        match figure.kind {
            Kind::Gauge => write_message(metric, METRIC_GAUGE, |gauge| {
                for point in points {
                    write_point(gauge, &point, false, attributes);
                }
            }),
            Kind::Window => write_message(metric, METRIC_SUM, |sum| {
                for point in points {
                    write_point(sum, &point, true, attributes);
                }
                write_varint(sum, SUM_TEMPORALITY, DELTA);
                write_varint(sum, SUM_MONOTONIC, 1);
            }),
        }
    });
}

/// One point as a `NumberDataPoint`: its start where it has a window, its
/// time, its value as an integer, and what it is of.
fn write_point(out: &mut Vec<u8>, point: &Point<'_>, window: bool, attributes: &Attributes) {
    write_message(out, DATA_POINTS, |data| {
        if window {
            write_i64(data, POINT_START, point.start_unix_nanos.cast_unsigned());
        }
        write_i64(data, POINT_TIME, point.time_unix_nanos.cast_unsigned());
        let value = i64::try_from(point.value).unwrap_or(i64::MAX);
        write_i64(data, POINT_AS_INT, value.cast_unsigned());
        data.extend_from_slice(attributes.scope(point.at));
        if let Some(mood) = point.mood {
            data.extend_from_slice(attributes.mood(mood));
        }
    });
}

/// Each scope's attribute and each mood's, written once for an export
/// rather than once for every point that carries it: a thousand scopes
/// with a mood and every count are eight thousand points.
struct Attributes {
    /// Where each scope's written attribute ends in `written`, in the
    /// order of [`Reading::scopes`].
    ends: Vec<usize>,
    written: Vec<u8>,
    /// Each mood's written attribute, by rank.
    moods: Vec<Vec<u8>>,
}

impl Attributes {
    fn of(reading: &Reading<'_>) -> Self {
        let scopes = reading.scopes();
        let mut written = Vec::with_capacity(scopes.len() * 64);
        let mut ends = Vec::with_capacity(scopes.len());
        for scope in scopes {
            write_attribute(&mut written, POINT_ATTRIBUTES, Point::SCOPE, scope);
            ends.push(written.len());
        }
        let moods = Health::ALL
            .iter()
            .map(|mood| {
                let mut one = Vec::new();
                write_attribute(&mut one, POINT_ATTRIBUTES, Point::MOOD, mood.word());
                one
            })
            .collect();
        Self {
            ends,
            written,
            moods,
        }
    }

    /// The written attribute of the scope at `at` in [`Reading::scopes`].
    fn scope(&self, at: usize) -> &[u8] {
        let start = if at == 0 { 0 } else { self.ends[at - 1] };
        &self.written[start..self.ends[at]]
    }

    fn mood(&self, mood: Health) -> &[u8] {
        &self.moods[usize::from(mood.rank())]
    }
}

/// A `KeyValue` whose value is a string, as field `number`.
fn write_attribute(out: &mut Vec<u8>, number: u32, key: &str, value: &str) {
    write_message(out, number, |pair| {
        write_delimited(pair, KEY, key.as_bytes());
        write_message(pair, VALUE, |any| {
            write_delimited(any, STRING_VALUE, value.as_bytes());
        });
    });
}
