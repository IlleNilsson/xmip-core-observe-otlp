//! A collector stand-in on loopback: it takes what the exporter posts, walks
//! it by the OTLP specification's field numbers with the estate's protobuf
//! walker, and the tests hold what it finds to what the snapshot held.

// Test code, all of it: lib.rs declares this module under `#[cfg(test)]`,
// and the style gates find where production code ends by the line below.
#[cfg(test)]
use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use message::protobuf::{Field, WireType, fields};
use net::http::{Request, Response};
use observe::{Count, Counted, Health, HealthRecord, Snapshot};

use crate::metrics::write_request;
use crate::{Exporter, Otlp, Resource};

/// One field's value, found by number, as the bytes it holds.
fn field<'b>(bytes: &'b [u8], found: &[Field], number: u32, wire: WireType) -> Vec<&'b [u8]> {
    found
        .iter()
        .filter(|field| field.number == number)
        .inspect(|field| assert_eq!(field.wire, wire, "field {number}'s wire type"))
        .map(|field| &bytes[field.value.clone()])
        .collect()
}

fn walk(bytes: &[u8]) -> Vec<Field> {
    fields(bytes, 0..bytes.len()).expect("walks as protobuf")
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8(bytes.to_vec()).expect("utf-8")
}

fn fixed(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("eight bytes"))
}

/// A string `KeyValue`'s key and value.
fn attribute(bytes: &[u8]) -> (String, String) {
    let pair = walk(bytes);
    let key = text(field(bytes, &pair, 1, WireType::Len)[0]);
    let any = field(bytes, &pair, 2, WireType::Len)[0];
    let value = text(field(any, &walk(any), 1, WireType::Len)[0]);
    (key, value)
}

/// One `NumberDataPoint` as the collector reads it.
#[derive(Debug, PartialEq, Eq)]
struct Received {
    start: Option<u64>,
    time: u64,
    value: u64,
    attributes: Vec<(String, String)>,
}

/// One `Metric`: its name, unit, whether a sum, and its points.
#[derive(Debug)]
struct Metric {
    name: String,
    unit: String,
    sum: bool,
    points: Vec<Received>,
}

/// What a collector reads out of an `ExportMetricsServiceRequest`: the
/// resource's attributes, the instrumentation scope's name, the metrics.
fn read(bytes: &[u8]) -> (Vec<(String, String)>, String, Vec<Metric>) {
    let request = walk(bytes);
    let resource_metrics = field(bytes, &request, 1, WireType::Len);
    assert_eq!(resource_metrics.len(), 1, "one resource: the node");
    let held = resource_metrics[0];
    let fields_held = walk(held);
    let resource = field(held, &fields_held, 1, WireType::Len)[0];
    let attributes = field(resource, &walk(resource), 1, WireType::Len)
        .into_iter()
        .map(attribute)
        .collect();
    let scoped = field(held, &fields_held, 2, WireType::Len)[0];
    let scoped_fields = walk(scoped);
    let scope = field(scoped, &scoped_fields, 1, WireType::Len)[0];
    let scope_name = text(field(scope, &walk(scope), 1, WireType::Len)[0]);
    let metrics = field(scoped, &scoped_fields, 2, WireType::Len)
        .into_iter()
        .map(metric)
        .collect();
    (attributes, scope_name, metrics)
}

fn metric(bytes: &[u8]) -> Metric {
    let found = walk(bytes);
    let name = text(field(bytes, &found, 1, WireType::Len)[0]);
    let unit = text(field(bytes, &found, 3, WireType::Len)[0]);
    let gauge = field(bytes, &found, 5, WireType::Len);
    let sum = field(bytes, &found, 7, WireType::Len);
    let data = gauge.first().or(sum.first()).expect("a gauge or a sum");
    let data_fields = walk(data);
    if !sum.is_empty() {
        // AGGREGATION_TEMPORALITY_DELTA is 1; is_monotonic true is 1.
        assert_eq!(field(data, &data_fields, 2, WireType::Varint), [&[1u8][..]]);
        assert_eq!(field(data, &data_fields, 3, WireType::Varint), [&[1u8][..]]);
    }
    let points = field(data, &data_fields, 1, WireType::Len)
        .into_iter()
        .map(|point| {
            let found = walk(point);
            Received {
                start: field(point, &found, 2, WireType::I64)
                    .first()
                    .map(|b| fixed(b)),
                time: fixed(field(point, &found, 3, WireType::I64)[0]),
                value: fixed(field(point, &found, 6, WireType::I64)[0]),
                attributes: field(point, &found, 7, WireType::Len)
                    .into_iter()
                    .map(attribute)
                    .collect(),
            }
        })
        .collect();
    Metric {
        name,
        unit,
        sum: !sum.is_empty(),
        points,
    }
}

/// The `index`th Receive Location, beneath the test cluster's first node,
/// read from its `xmip.toml` once.
fn scope(index: usize) -> String {
    static NODE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    let node = NODE.get_or_init(|| configure::fixture::test_cluster().node_scope(0));
    format!("{node}/receive/location-{index:04}")
}

/// `scopes` Receive Locations, each with a mood and a count of every kind.
fn snapshot(scopes: usize) -> Snapshot {
    let mut snapshot = Snapshot::new();
    for index in 0..scopes {
        snapshot.record_health(HealthRecord {
            scope: scope(index),
            health: if index % 7 == 0 {
                Health::Stressed
            } else {
                Health::Fine
            },
            severity: u8::try_from(index % 100).expect("under 100"),
            evidence: String::new(),
            observed_unix_nanos: 1_000,
        });
        for counted in Counted::ALL {
            snapshot.record_count(Count {
                scope: scope(index),
                counted,
                value: index as u64,
                window_start_unix_nanos: 500,
                window_end_unix_nanos: 1_000,
                observed_unix_nanos: 1_000,
            });
        }
    }
    snapshot
}

#[test]
fn every_figure_reaches_the_collector_by_the_specification_s_field_numbers() {
    let mut bytes = Vec::new();
    write_request(&mut bytes, &snapshot(3), &Resource::node("n1"));
    let (resource, instrumentation, metrics) = read(&bytes);

    assert!(resource.contains(&("service.name".into(), "xmip".into())));
    assert!(resource.contains(&("service.instance.id".into(), "n1".into())));
    assert_eq!(instrumentation, "xmip-core-observe-otlp");
    let names: Vec<&str> = metrics.iter().map(|metric| metric.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "xmip.health",
            "xmip.health.severity",
            "xmip.streams",
            "xmip.messages",
            "xmip.journeys",
            "xmip.bytes",
            "xmip.retrying",
            "xmip.failed"
        ]
    );

    let health = &metrics[0];
    assert!(!health.sum, "a mood is a gauge");
    assert_eq!(health.points.len(), 3);
    assert_eq!(health.points[0].value, 3, "stressed ranks 3");
    assert_eq!(health.points[0].start, None, "a gauge has no window");
    assert!(
        health.points[0]
            .attributes
            .contains(&("scope".into(), scope(0)))
    );
    assert!(
        health.points[0]
            .attributes
            .contains(&("mood".into(), "stressed".into()))
    );

    let bytes_metric = &metrics[5];
    assert_eq!(bytes_metric.unit, "By");
    assert!(bytes_metric.sum, "a window is a delta sum");
    assert_eq!(bytes_metric.points[2].value, 2);
    assert_eq!(
        (bytes_metric.points[2].start, bytes_metric.points[2].time),
        (Some(500), 1_000)
    );
    assert!(!metrics[6].sum, "what awaits another try is a level");
}

#[test]
fn an_empty_snapshot_sends_a_resource_and_no_metric() {
    let mut bytes = Vec::new();
    write_request(&mut bytes, &Snapshot::new(), &Resource::node("n1"));
    let (_, _, metrics) = read(&bytes);
    assert!(metrics.is_empty(), "a metric with no points is refused");
}

/// Serve one request on `listener` as a collector would, answering with
/// `answer`, and hand back what was posted.
fn collect(listener: TcpListener, answer: Response) -> std::thread::JoinHandle<Request> {
    std::thread::spawn(move || {
        transport_http::server::serve_one(&listener, Some(Duration::from_secs(5)), |request| {
            (request.clone(), answer)
        })
        .expect("a request")
    })
}

fn otlp(url: String, h2c: bool) -> Otlp {
    Otlp {
        endpoint: url,
        h2c,
        timeout: Duration::from_secs(5),
        resource: Resource::node("n1"),
    }
}

fn loopback() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("address").to_string();
    (listener, address)
}

#[test]
fn an_export_is_posted_in_http_1_1_to_v1_metrics_as_protobuf() {
    let (listener, address) = loopback();
    let collector = collect(listener, Response::new(200));
    let exporter = Exporter::start(otlp(format!("http://{address}"), false)).expect("start");
    exporter.offer(Arc::new(snapshot(2)));
    let posted = collector.join().expect("collector");
    let tally = exporter.close();

    assert_eq!(
        (posted.method.as_str(), posted.path.as_str()),
        ("POST", "/v1/metrics")
    );
    assert_eq!(
        posted.header_value("content-type"),
        Some("application/x-protobuf")
    );
    assert_eq!(read(&posted.body).2.len(), 8);
    assert_eq!((tally.offered, tally.sent, tally.failed), (1, 1, 0));
}

#[test]
fn a_collector_speaking_http_2_is_sent_in_http_2() {
    let (listener, address) = loopback();
    let collector = collect(listener, Response::new(200));
    let url = format!("http://{address}/otlp/v1/metrics");
    let exporter = Exporter::start(otlp(url, true)).expect("start");
    exporter.offer(Arc::new(snapshot(2)));
    let posted = collector.join().expect("collector");
    let tally = exporter.close();

    assert_eq!(posted.path, "/otlp/v1/metrics", "a path given is kept");
    assert_eq!(read(&posted.body).2[0].points.len(), 2);
    assert_eq!(tally.sent, 1);
}

#[test]
fn a_partial_success_and_an_absent_collector_are_counted_not_raised() {
    let (listener, address) = loopback();
    let mut answer = Vec::new();
    message::protobuf::write_message(&mut answer, 1, |partial| {
        message::protobuf::write_varint(partial, 1, 4);
        message::protobuf::write_delimited(partial, 2, b"too old");
    });
    let collector = collect(listener, Response::new(200).body(&answer));
    let exporter = Exporter::start(otlp(format!("http://{address}"), false)).expect("start");
    exporter.offer(Arc::new(snapshot(1)));
    collector.join().expect("collector");
    let tally = exporter.close();
    assert_eq!((tally.sent, tally.rejected_points), (1, 4));
    assert!(tally.last_failure.expect("why").contains("too old"));

    let (gone, address) = loopback();
    drop(gone);
    let exporter = Exporter::start(otlp(format!("http://{address}"), false)).expect("start");
    exporter.offer(Arc::new(snapshot(1)));
    let tally = exporter.close();
    assert_eq!((tally.sent, tally.failed), (0, 1));
    assert!(tally.last_failure.is_some());
}

#[test]
fn an_offer_never_waits_for_a_collector_that_does_not_answer() {
    // Nothing accepts: the connection sits in the backlog, and the sender
    // waits out its timeout. Every offer returns at once regardless, and an
    // offer made while one waits replaces it.
    let (listener, address) = loopback();
    let exporter = Exporter::start(otlp(format!("http://{address}"), false)).expect("start");
    let snapshot = Arc::new(snapshot(10));
    let began = Instant::now();
    for _ in 0..5 {
        exporter.offer(Arc::clone(&snapshot));
    }
    assert!(
        began.elapsed() < Duration::from_secs(1),
        "offers never wait"
    );
    let tally = exporter.tally();
    assert_eq!(tally.offered, 5);
    assert!(tally.superseded >= 3, "one sending, one waiting: {tally:?}");
    drop(listener);
}

/// The costs of an export, run on purpose in release:
/// `cargo test --release -- --ignored --nocapture cost`. What the node's
/// thread pays per change (an offer), what the sender pays to write a
/// snapshot of a thousand scopes, and how long a change takes to reach a
/// collector on loopback.
#[test]
#[ignore = "a measurement, run on purpose in release"]
fn cost_of_one_export_of_a_thousand_scopes() {
    let snapshot = Arc::new(snapshot(1_000));
    let resource = Resource::node("n1");
    let mut buffer = Vec::new();
    let mut written = Vec::new();
    for _ in 0..200 {
        buffer.clear();
        let began = Instant::now();
        write_request(&mut buffer, &snapshot, &resource);
        written.push(began.elapsed());
    }
    written.sort();
    let points: usize = read(&buffer).2.iter().map(|m| m.points.len()).sum();
    println!(
        "otlp write: {points} points, {} bytes, fastest {:?}, median {:?}, 95th {:?}",
        buffer.len(),
        written[0],
        written[100],
        written[190]
    );

    let (listener, address) = loopback();
    let exporter = Exporter::start(otlp(format!("http://{address}"), false)).expect("start");
    let mut offered = Vec::new();
    let mut reached = Vec::new();
    for _ in 0..50 {
        let began = Instant::now();
        exporter.offer(Arc::clone(&snapshot));
        offered.push(began.elapsed());
        // A blocking accept, not `serve_one`'s: its bounded accept polls
        // every two milliseconds, which would be measured here as the
        // exporter's.
        let (stream, _) = listener.accept().expect("accepted");
        transport_http::server::answer_on(stream, |_| {
            reached.push(began.elapsed());
            ((), Response::new(200))
        })
        .expect("served");
    }
    offered.sort();
    reached.sort();
    println!(
        "otlp offer: fastest {:?}, median {:?}; change to collector: fastest {:?}, median {:?}",
        offered[0], offered[25], reached[0], reached[25]
    );
    assert_eq!(exporter.close().sent, 50);
}
