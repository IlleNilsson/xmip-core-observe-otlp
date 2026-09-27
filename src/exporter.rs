//! Sending exports to a collector from a thread of its own, so the node
//! never waits on one.
//!
//! The node hands each snapshot it publishes to [`Exporter::offer`] — the
//! shared handle it publishes, `Arc<Snapshot>`, never a copy — and that is
//! all the node's thread does: a lock and a handle. The sender wakes on it
//! at once, writes the request and posts it to the collector over
//! `transport-http`'s `endpoint::Connections` — HTTP/2 where ALPN agrees
//! it, HTTP/1.1 otherwise, TLS for `https://` — within the timeout it was
//! given, on the connection the export before it opened. So an export goes on every change, with no interval: OTLP asks
//! for none, and a collector batches what it receives itself. One snapshot
//! waits at a time: one offered while one waits replaces it, the newer
//! being the truer, and the replaced one is counted. A collector that is
//! slow or gone costs the node nothing but the exports it never receives.

use std::mem;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::JoinHandle;
use std::time::Duration;

use net::Endpoint;
use net::http::Request;
use observe::Snapshot;
use transport_http::endpoint::{Connections, Offer};

use crate::metrics::write_request;
use crate::resource::Resource;
use crate::response::{Rejected, rejected};

/// The port an OTLP/HTTP collector listens on by convention.
pub const DEFAULT_PORT: u16 = 4318;

/// The path metrics are posted to where the URL names none.
pub const METRICS_PATH: &str = "/v1/metrics";

/// The media type of a binary protobuf body, as OTLP/HTTP names it.
const PROTOBUF: &str = "application/x-protobuf";

/// Where exports go and how.
#[derive(Clone, Debug)]
pub struct Otlp {
    /// The collector: `http://` or `https://`, port 4318 where it names
    /// none, `/v1/metrics` where it names no path.
    pub endpoint: String,
    /// Speak HTTP/2 over cleartext by prior knowledge. Over TLS the version
    /// is what ALPN agrees, whatever this says.
    pub h2c: bool,
    /// How long one export may take to connect, send and be answered.
    pub timeout: Duration,
    pub resource: Resource,
}

/// What became of the exports offered so far.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    /// Handed to the sender.
    pub offered: u64,
    /// Answered with success by the collector.
    pub sent: u64,
    /// Replaced by a newer one before the sender took it.
    pub superseded: u64,
    /// Not delivered: the collector could not be reached, refused, or
    /// answered with a failure.
    pub failed: u64,
    /// Points a collector said it rejected in an answer that was otherwise
    /// a success (OTLP's partial success).
    pub rejected_points: u64,
    /// Why the last export that failed or was partly rejected did.
    pub last_failure: Option<String>,
}

/// What the node and the sender share: the snapshot waiting to be sent,
/// and the tally.
#[derive(Default)]
struct Shared {
    waiting: Option<Arc<Snapshot>>,
    closed: bool,
    tally: Tally,
}

/// The shared state and the signal that an export is waiting.
#[derive(Default)]
struct Outbox {
    shared: Mutex<Shared>,
    ready: Condvar,
}

impl Outbox {
    fn lock(&self) -> MutexGuard<'_, Shared> {
        // A sender that panicked leaves nothing half-written worth refusing.
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// An exporter sending to one collector.
pub struct Exporter {
    outbox: Arc<Outbox>,
    sender: Option<JoinHandle<()>>,
}

impl Exporter {
    /// Start the sender for `otlp`.
    ///
    /// # Errors
    /// Where the endpoint is not an `http://` or `https://` URL, or the
    /// thread could not be started.
    pub fn start(otlp: Otlp) -> Result<Self, String> {
        let mut endpoint = Endpoint::parse(&otlp.endpoint)
            .map_err(|failure| failure.to_string())?
            .or_port(DEFAULT_PORT);
        if endpoint.path() == "/" {
            endpoint = Endpoint::parse(&format!(
                "{}://{}{METRICS_PATH}",
                if endpoint.secure() { "https" } else { "http" },
                endpoint.address()
            ))
            .map_err(|failure| failure.to_string())?;
        }
        let outbox = Arc::new(Outbox::default());
        let target = Target {
            endpoint,
            h2c: otlp.h2c,
            timeout: otlp.timeout,
            resource: otlp.resource,
            connections: Connections::new(),
        };
        let shared = Arc::clone(&outbox);
        let sender = std::thread::Builder::new()
            .name("xmip-observe-otlp".to_string())
            .spawn(move || send_until_closed(&shared, &target))
            .map_err(|failure| format!("starting the OTLP sender: {failure}"))?;
        Ok(Self {
            outbox,
            sender: Some(sender),
        })
    }

    /// Leave `snapshot` for the sender, replacing one still waiting, and
    /// wake it. Never waits on the collector, and writes nothing.
    pub fn offer(&self, snapshot: Arc<Snapshot>) {
        let mut shared = self.outbox.lock();
        shared.tally.offered += 1;
        if shared.waiting.replace(snapshot).is_some() {
            shared.tally.superseded += 1;
        }
        drop(shared);
        self.outbox.ready.notify_one();
    }

    /// What became of the exports offered so far.
    #[must_use]
    pub fn tally(&self) -> Tally {
        self.outbox.lock().tally.clone()
    }

    /// Stop the sender once the export it is sending, and one waiting, are
    /// done, and say what became of them all. Waits at most two timeouts.
    #[must_use]
    pub fn close(mut self) -> Tally {
        self.stop();
        self.tally()
    }

    fn stop(&mut self) {
        self.outbox.lock().closed = true;
        self.outbox.ready.notify_one();
        if let Some(sender) = self.sender.take() {
            // The sender's own panic is not the closer's to raise again.
            let _ = sender.join();
        }
    }
}

impl Drop for Exporter {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The collector, as the sender reaches it.
struct Target {
    endpoint: Endpoint,
    h2c: bool,
    timeout: Duration,
    resource: Resource,
    /// The connection kept to the collector between exports.
    connections: Connections,
}

/// Take each export as it is offered and send it, until closed and none
/// waits.
fn send_until_closed(outbox: &Outbox, target: &Target) {
    let mut body = Vec::new();
    loop {
        let mut shared = outbox.lock();
        while shared.waiting.is_none() && !shared.closed {
            shared = outbox
                .ready
                .wait(shared)
                .unwrap_or_else(PoisonError::into_inner);
        }
        let Some(snapshot) = shared.waiting.take() else {
            return;
        };
        drop(shared);

        body.clear();
        write_request(&mut body, &snapshot, &target.resource);
        drop(snapshot);
        let outcome;
        (outcome, body) = send(target, body);
        let mut shared = outbox.lock();
        match outcome {
            Ok(None) => shared.tally.sent += 1,
            Ok(Some(points)) => {
                shared.tally.sent += 1;
                shared.tally.rejected_points += points.count;
                shared.tally.last_failure = Some(points.why);
            }
            Err(why) => {
                shared.tally.failed += 1;
                shared.tally.last_failure = Some(why);
            }
        }
    }
}

/// Post one export, and hand its buffer back for the next.
fn send(target: &Target, body: Vec<u8>) -> (Result<Option<Rejected>, String>, Vec<u8>) {
    let mut request = Request::new("POST", target.endpoint.path())
        .header("Host", &target.endpoint.authority())
        .header("Content-Type", PROTOBUF);
    request.body = body;
    let answer = target.connections.exchange(
        &target.endpoint,
        Some(target.timeout),
        Offer::agreed(target.h2c),
        &request,
    );
    let body = mem::take(&mut request.body);
    let outcome = match answer {
        Err(failure) => Err(format!("sending to the collector: {}", failure.message)),
        Ok(answer) if (200..300).contains(&answer.status) => Ok(rejected(&answer.body)),
        Ok(answer) => Err(format!(
            "the collector answered {}: {}",
            answer.status,
            answer.text().map_or_else(
                |refused| refused.to_string(),
                |text| text.chars().take(200).collect::<String>()
            )
        )),
    };
    (outcome, body)
}
