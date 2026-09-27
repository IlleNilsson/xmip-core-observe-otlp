# xmip-core-observe-otlp

OTLP exporter: a node's moods, severities and counted figures as
OpenTelemetry metrics, sent to an operator's collector over OTLP/HTTP in
protobuf. A technology of
[xmip-core-observe](https://github.com/IlleNilsson/xmip-core-observe).

## What is sent

What is exported is observe's: the snapshot, and the figures
`observe::FIGURES` names in it, read once per export with
`observe::Reading`. This crate names no figure of its own; a figure added
to observe reaches the collector and the Prometheus endpoint alike.

`metrics::write_request` writes one `ExportMetricsServiceRequest`: one
resource — the node, `service.name` `xmip` and `service.instance.id` its
name (`Resource::node`, more attributes by `with`) — and one
instrumentation scope, this crate, holding a metric per figure with
points. A level is a `Gauge`; a count over its window a `Sum`, delta and
monotonic, each point's start the window's opening. Every point carries
the attribute `scope`, the Xmip URI it is of, and a mood its word as
`mood`. A mood's value is its rank, `fine` 0 to `holding` 6. The field
numbers are opentelemetry-proto's, each named where it is written. It is
written with the estate's own protobuf writer, `message::protobuf`, not
prost; each scope's attribute is written once per export and copied to
every point that carries it.

Metrics only. OTLP's traces want a trace and span identity per Journey
and Message and their timing; observe's activity holds an item's identity
and when it was seen, and no correlation, so there is nothing here to send
as a trace.

## How it is sent

`Exporter::start(Otlp)` starts a sender on a thread of its own. The node
hands it each snapshot it publishes, `Exporter::offer(Arc<Snapshot>)` —
a lock and a handle, nothing written on the node's thread. The sender
wakes at once, writes the request and posts it to the collector:
`transport-http`'s `endpoint::exchange`, so HTTP/2 where ALPN agrees it,
HTTP/1.1 otherwise, HTTP/2 over cleartext where `h2c` says the collector
speaks it, and TLS for `https://` through `xmip-core-library-tls`. The
endpoint is the collector's URL, port 4318 where it names none and
`/v1/metrics` where it names no path; the body is
`application/x-protobuf`.

An export goes on every change, with no interval: OTLP asks for none, and
a collector batches what it receives itself. One snapshot waits at a
time; one offered while another waits replaces it, the newer being the
truer, and the replaced one is counted. `Exporter::tally` says what
became of them — sent, superseded, failed, and the points a collector
rejected in a partial success, with the last reason — and `close` stops
the sender once what it holds is done. A collector that is slow or gone
costs the node nothing but the exports it never receives.

## Cost

Measured in release, `cargo test --release -- --ignored --nocapture cost`,
on Windows 11 and on AlmaLinux 10 under WSL, the fastest of 200 runs while
the machine was otherwise quiet (under the estate's own builds every figure
doubled or trebled): a snapshot of a thousand Receive Locations, each with
a mood and every count, is 8,000 points and 663 kB.

| What | Where it runs | Linux | Windows |
| --- | --- | --- | --- |
| `offer` | the node's thread | 1.5 µs | 0.3 µs |
| Writing the request | the sender | 0.52 ms | 0.65 ms |
| A change reaching a collector on loopback | end to end | 1.2 ms | 1.5 ms |

The request is written in one buffer, reused; an embedded message's length
takes the one byte held for it or moves the message once. Each export opens
a connection of its own: over TLS that is a handshake per export, and
keeping one HTTP/2 connection to the collector is the next saving.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`. The tests stand a collector up on loopback, walk
what it received by the specification's field numbers with the estate's
protobuf walker, and send over HTTP/1.1 and HTTP/2.
