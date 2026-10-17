//! Packet Summary: one `Record` per packet, for record sinks like the CSV
//! writer.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use parkourflow_core::{Node, NodeContext, NodeRunner, Packet, Record, StreamItem};

use crate::headers::summarize;

pub const SUMMARY_TYPE: &str = "net.packet_summary";

/// The record columns, in order.
pub const COLUMNS: [&str; 5] = ["time", "length", "protocol", "source", "destination"];

/// Turns each [`Packet`] into a [`Record`]: capture `time` (UTC, ISO 8601
/// with microseconds), `length` on the wire, `protocol`, `source` and
/// `destination` (see [`Summary`](crate::headers::Summary)).
///
/// Pausable: it waits for downstream credit. Behind a live source, any loss
/// happens (and is counted) on the source's connection.
pub struct PacketSummary;

impl PacketSummary {
    pub fn build(_node: &Node) -> Result<Box<dyn NodeRunner>, String> {
        Ok(Box::new(Self))
    }
}

impl NodeRunner for PacketSummary {
    fn run(self: Box<Self>, mut ctx: NodeContext) -> Result<(), String> {
        let input = ctx.take_input("packets")?;
        let output = ctx.take_output("records")?;
        let columns: Arc<[String]> = COLUMNS.map(String::from).into();

        while let Some(item) = input.recv() {
            let Some(packet) = item.payload::<Packet>() else {
                return Err("Packet Summary received an item that isn't a packet.".into());
            };
            let summary = summarize(packet.link_type, &packet.data);
            let record = Record::new(
                columns.clone(),
                vec![
                    utc_timestamp(packet.timestamp),
                    packet.original_len.to_string(),
                    summary.protocol,
                    summary.source,
                    summary.destination,
                ],
            );
            // Keep the packet's sequence and stream time.
            let mut out = StreamItem::new(item.sequence, record);
            out.timestamp = item.timestamp;
            if output.send(out).is_err() {
                break;
            }
        }
        Ok(())
    }
}

/// Formats a time as `YYYY-MM-DDTHH:MM:SS.ffffffZ` (UTC).
pub fn utc_timestamp(time: SystemTime) -> String {
    let since = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = since.as_secs();
    let (days, rem) = (secs / 86_400, secs % 86_400);
    let (year, month, day) = civil_from_days(days as i64);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}Z",
        rem / 3600,
        rem / 60 % 60,
        rem % 60,
        since.subsec_micros()
    )
}

/// Days since 1970-01-01 to a (year, month, day) date, from Howard
/// Hinnant's `civil_from_days`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn formats_utc_timestamps() {
        let at = |secs: u64, micros: u64| {
            UNIX_EPOCH + Duration::from_secs(secs) + Duration::from_micros(micros)
        };
        assert_eq!(utc_timestamp(at(0, 0)), "1970-01-01T00:00:00.000000Z");
        assert_eq!(
            utc_timestamp(at(1_700_000_000, 123_456)),
            "2023-11-14T22:13:20.123456Z"
        );
        // Leap day, and the day after.
        assert_eq!(
            utc_timestamp(at(1_709_164_800, 0)),
            "2024-02-29T00:00:00.000000Z"
        );
        assert_eq!(
            utc_timestamp(at(1_709_251_199, 999_999)),
            "2024-02-29T23:59:59.999999Z"
        );
        assert_eq!(
            utc_timestamp(at(1_709_251_200, 0)),
            "2024-03-01T00:00:00.000000Z"
        );
    }
}
