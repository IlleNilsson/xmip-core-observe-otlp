//! What a collector's answer says about the points it did not take: an
//! `ExportMetricsServiceResponse`, whose `partial_success` counts the
//! points it rejected and says why. An empty answer took everything.

use codec::varint;
use message::protobuf::{WireType, fields};

/// `ExportMetricsServiceResponse.partial_success`.
const RESPONSE_PARTIAL_SUCCESS: u32 = 1;
/// `ExportMetricsPartialSuccess.rejected_data_points`, an `int64`.
const REJECTED_DATA_POINTS: u32 = 1;
/// `ExportMetricsPartialSuccess.error_message`.
const ERROR_MESSAGE: u32 = 2;

/// Points a collector rejected from an export it otherwise took.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Rejected {
    pub count: u64,
    pub why: String,
}

/// What `body` says was rejected, or `None` where it was all taken — an
/// empty answer, one with no partial success, or one that could not be
/// read, which says nothing either way.
pub(crate) fn rejected(body: &[u8]) -> Option<Rejected> {
    let outer = fields(body, 0..body.len()).ok()?;
    let partial = outer
        .iter()
        .find(|field| field.number == RESPONSE_PARTIAL_SUCCESS && field.wire == WireType::Len)?;
    let mut count = 0;
    let mut why = String::new();
    for field in fields(body, partial.value.clone()).ok()? {
        let value = &body[field.value.clone()];
        match (field.number, field.wire) {
            (REJECTED_DATA_POINTS, WireType::Varint) => {
                count = varint::decode(value).map_or(0, |(count, _)| count);
            }
            (ERROR_MESSAGE, WireType::Len) => why = String::from_utf8_lossy(value).into_owned(),
            _ => {}
        }
    }
    if count == 0 && why.is_empty() {
        return None;
    }
    Some(Rejected {
        count,
        why: format!("the collector rejected {count} points: {why}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use message::protobuf::{write_delimited, write_message, write_varint};

    #[test]
    fn a_partial_success_says_how_many_and_why() {
        let mut body = Vec::new();
        write_message(&mut body, RESPONSE_PARTIAL_SUCCESS, |partial| {
            write_varint(partial, REJECTED_DATA_POINTS, 2);
            write_delimited(partial, ERROR_MESSAGE, b"a stale point");
        });
        let found = rejected(&body).expect("rejected");
        assert_eq!(found.count, 2);
        assert!(found.why.ends_with("a stale point"));
    }

    #[test]
    fn an_empty_answer_or_an_empty_partial_success_took_everything() {
        assert_eq!(rejected(&[]), None);
        let mut body = Vec::new();
        write_message(&mut body, RESPONSE_PARTIAL_SUCCESS, |_| {});
        assert_eq!(rejected(&body), None);
        assert_eq!(rejected(b"\xff\xff"), None, "unreadable says nothing");
    }
}
