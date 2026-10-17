//! Previews: a sampled view of what a node is seeing, for the UI.
//!
//! A node keeps the latest item it saw in its [`PreviewSlot`]. That costs a
//! lock and a reference-count bump per item, with no copying. The shell
//! samples slots on a timer and sends a [`PreviewSnapshot`] to the UI only
//! when one changed. Payloads stay in Rust; only a cut-down copy of the
//! latest item crosses into the UI.

use std::fmt;
use std::sync::{Arc, Mutex};

use serde::Serialize;

use crate::buffer::StreamItem;
use crate::record::Record;

/// Longest value a preview carries, in characters; longer ones are cut and
/// end in `…`.
pub const MAX_PREVIEW_VALUE: usize = 200;
/// Most columns a record preview carries.
pub const MAX_PREVIEW_COLUMNS: usize = 64;

/// A display-ready copy of one stream item.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Preview {
    /// A [`Record`]. `columns` is empty when the stream has no column names.
    Record {
        columns: Vec<String>,
        values: Vec<String>,
        /// Columns left out after [`MAX_PREVIEW_COLUMNS`].
        omitted: usize,
    },
    /// An item of a type that has no preview yet.
    Unsupported,
}

impl Preview {
    pub fn of(item: &StreamItem) -> Self {
        if let Some(record) = item.payload::<Record>() {
            let width = record.values.len().max(record.columns.len());
            let take = |v: &[String]| {
                v.iter()
                    .take(MAX_PREVIEW_COLUMNS)
                    .map(|s| shorten(s))
                    .collect()
            };
            return Preview::Record {
                columns: take(&record.columns),
                values: take(&record.values),
                omitted: width.saturating_sub(MAX_PREVIEW_COLUMNS),
            };
        }
        Preview::Unsupported
    }
}

fn shorten(s: &str) -> String {
    match s.char_indices().nth(MAX_PREVIEW_VALUE) {
        Some((cut, _)) => format!("{}…", &s[..cut]),
        None => s.to_owned(),
    }
}

/// What the UI is sent for a node's preview.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PreviewSnapshot {
    /// Items seen since the node last started.
    pub received: u64,
    /// The latest item's sequence number in its stream.
    pub sequence: u64,
    pub latest: Preview,
}

#[derive(Default)]
struct Slot {
    received: u64,
    latest: Option<StreamItem>,
}

/// The latest item a node saw, shared between the running node and the
/// sampler. Cloning shares the slot.
#[derive(Clone, Default)]
pub struct PreviewSlot(Arc<Mutex<Slot>>);

impl PreviewSlot {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Slot> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Keeps `item` as the latest. Cheap: the payload is shared, not copied.
    pub fn record(&self, item: &StreamItem) {
        let mut slot = self.lock();
        slot.received += 1;
        slot.latest = Some(item.clone());
    }

    /// Forgets everything, e.g. when the node starts a new run.
    pub fn clear(&self) {
        *self.lock() = Slot::default();
    }

    /// Items seen since the last [`clear`](Self::clear).
    pub fn received(&self) -> u64 {
        self.lock().received
    }

    /// A display-ready copy of the latest item, or `None` before the first.
    pub fn snapshot(&self) -> Option<PreviewSnapshot> {
        let slot = self.lock();
        let item = slot.latest.as_ref()?;
        Some(PreviewSnapshot {
            received: slot.received,
            sequence: item.sequence,
            latest: Preview::of(item),
        })
    }
}

impl fmt::Debug for PreviewSlot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreviewSlot")
            .field("received", &self.received())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(columns: &[&str], values: &[&str]) -> StreamItem {
        let columns: Arc<[String]> = columns.iter().map(|s| s.to_string()).collect();
        StreamItem::new(
            7,
            Record::new(columns, values.iter().map(|s| s.to_string()).collect()),
        )
    }

    #[test]
    fn keeps_the_latest_item() {
        let slot = PreviewSlot::new();
        assert_eq!(slot.snapshot(), None);
        slot.record(&record(&["a"], &["1"]));
        slot.record(&record(&["a"], &["2"]));
        assert_eq!(
            slot.snapshot(),
            Some(PreviewSnapshot {
                received: 2,
                sequence: 7,
                latest: Preview::Record {
                    columns: vec!["a".into()],
                    values: vec!["2".into()],
                    omitted: 0,
                },
            })
        );
        slot.clear();
        assert_eq!((slot.received(), slot.snapshot()), (0, None));
    }

    #[test]
    fn previews_are_cut_down() {
        let long = "é".repeat(MAX_PREVIEW_VALUE + 5);
        let many: Vec<String> = (0..MAX_PREVIEW_COLUMNS + 3)
            .map(|i| i.to_string())
            .collect();
        let mut values: Vec<&str> = many.iter().map(String::as_str).collect();
        values[0] = &long;
        let Preview::Record {
            columns,
            values,
            omitted,
        } = Preview::of(&record(&[], &values))
        else {
            panic!("expected a record preview");
        };
        assert!(columns.is_empty());
        assert_eq!(values.len(), MAX_PREVIEW_COLUMNS);
        assert_eq!(omitted, 3);
        assert_eq!(values[0].chars().count(), MAX_PREVIEW_VALUE + 1);
        assert!(values[0].ends_with('…'));
    }

    #[test]
    fn other_payloads_are_unsupported() {
        assert_eq!(
            Preview::of(&StreamItem::new(0, 42u32)),
            Preview::Unsupported
        );
    }
}
