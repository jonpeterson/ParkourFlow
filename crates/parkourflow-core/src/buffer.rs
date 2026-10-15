//! Connection buffers: the queue of stream items waiting on an edge.

use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::workflow::OverflowPolicy;

/// A stream item's data. Shared by reference so passing an item along (or to
/// several consumers) does not copy the payload.
pub type Payload = Arc<dyn Any + Send + Sync>;

/// One value in a stream, plus the metadata that travels with it.
#[derive(Clone)]
pub struct StreamItem {
    /// Position of this item in its stream, starting at 0.
    pub sequence: u64,
    /// Capture or presentation time, relative to the start of the stream.
    pub timestamp: Option<Duration>,
    pub payload: Payload,
}

impl StreamItem {
    pub fn new(sequence: u64, payload: impl Any + Send + Sync) -> Self {
        Self {
            sequence,
            timestamp: None,
            payload: Arc::new(payload),
        }
    }

    pub fn with_timestamp(mut self, timestamp: Duration) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Returns the payload as `T`, or `None` if it is a different type.
    pub fn payload<T: Any>(&self) -> Option<&T> {
        self.payload.downcast_ref()
    }
}

impl fmt::Debug for StreamItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamItem")
            .field("sequence", &self.sequence)
            .field("timestamp", &self.timestamp)
            .finish_non_exhaustive()
    }
}

/// What [`ConnectionBuffer::offer`] did with an item.
#[derive(Debug)]
pub enum Offer {
    /// The item was queued without dropping anything.
    Queued,
    /// The item was queued and the oldest queued item was dropped.
    Evicted,
    /// The buffer was full and the item was dropped.
    Discarded,
    /// The buffer is full and the policy is [`OverflowPolicy::Block`]: the
    /// item is handed back so the producer can wait for space.
    Full(StreamItem),
}

/// A bounded FIFO of stream items for one connection.
///
/// The buffer never grows past its capacity: [`push`](Self::push) hands the
/// item back when the buffer is full, and [`offer`](Self::offer) applies an
/// overflow policy. Every item the buffer drops is counted in
/// [`dropped`](Self::dropped).
#[derive(Debug)]
pub struct ConnectionBuffer {
    capacity: usize,
    items: VecDeque<StreamItem>,
    dropped: u64,
}

impl ConnectionBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity),
            dropped: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn is_full(&self) -> bool {
        self.items.len() >= self.capacity
    }

    /// Free slots: how many more items fit before the buffer is full.
    pub fn free(&self) -> usize {
        self.capacity.saturating_sub(self.items.len())
    }

    /// Total items this buffer has dropped, by overflow or by shrinking.
    pub fn dropped(&self) -> u64 {
        self.dropped
    }

    /// Counts an item dropped before it reached the buffer, such as a live
    /// source's item on a full [`OverflowPolicy::Block`] connection.
    pub fn record_drop(&mut self) {
        self.dropped += 1;
    }

    /// Appends an item, or returns it if the buffer is full.
    pub fn push(&mut self, item: StreamItem) -> Result<(), StreamItem> {
        if self.is_full() {
            return Err(item);
        }
        self.items.push_back(item);
        Ok(())
    }

    /// Appends an item, applying `policy` if the buffer is full.
    ///
    /// [`OverflowPolicy::KeepLatest`] treats the buffer as full when it
    /// holds an item, whatever its capacity.
    pub fn offer(&mut self, item: StreamItem, policy: OverflowPolicy) -> Offer {
        let limit = match policy {
            OverflowPolicy::KeepLatest => 1,
            _ => self.capacity,
        };
        if self.items.len() < limit {
            self.items.push_back(item);
            return Offer::Queued;
        }
        match policy {
            OverflowPolicy::Block => Offer::Full(item),
            // With zero capacity there is nothing to evict, so DropOldest
            // discards too.
            _ if policy == OverflowPolicy::DropNewest || limit == 0 => {
                self.dropped += 1;
                Offer::Discarded
            }
            _ => {
                // Make room for the new item: normally one eviction, more if
                // the buffer holds more than `limit`.
                let excess = self.items.len() + 1 - limit;
                self.items.drain(..excess);
                self.dropped += excess as u64;
                self.items.push_back(item);
                Offer::Evicted
            }
        }
    }

    /// Removes and returns the oldest item.
    pub fn pop(&mut self) -> Option<StreamItem> {
        self.items.pop_front()
    }

    /// Changes the capacity. When shrinking below the current length, the
    /// oldest items are dropped; returns how many were dropped.
    pub fn set_capacity(&mut self, capacity: usize) -> usize {
        self.capacity = capacity;
        let excess = self.items.len().saturating_sub(capacity);
        self.items.drain(..excess);
        self.dropped += excess as u64;
        excess
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_rejects_when_full() {
        let mut buffer = ConnectionBuffer::with_capacity(2);
        buffer.push(StreamItem::new(0, "a")).unwrap();
        buffer.push(StreamItem::new(1, "b")).unwrap();
        assert!(buffer.is_full());

        let rejected = buffer.push(StreamItem::new(2, "c")).unwrap_err();
        assert_eq!(rejected.sequence, 2);
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn pop_is_fifo() {
        let mut buffer = ConnectionBuffer::with_capacity(4);
        buffer.push(StreamItem::new(0, 10u32)).unwrap();
        buffer.push(StreamItem::new(1, 20u32)).unwrap();

        let item = buffer.pop().unwrap();
        assert_eq!(item.sequence, 0);
        assert_eq!(item.payload::<u32>(), Some(&10));
        assert_eq!(item.payload::<String>(), None);
        assert_eq!(buffer.pop().unwrap().sequence, 1);
        assert!(buffer.pop().is_none());
    }

    #[test]
    fn shrinking_drops_oldest() {
        let mut buffer = ConnectionBuffer::with_capacity(4);
        for i in 0..4 {
            buffer.push(StreamItem::new(i, ())).unwrap();
        }
        assert_eq!(buffer.set_capacity(2), 2);
        assert_eq!(buffer.pop().unwrap().sequence, 2);
        assert_eq!(buffer.set_capacity(8), 0);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.dropped(), 2);
    }

    fn offer_five(policy: OverflowPolicy, capacity: usize) -> (Vec<u64>, u64) {
        let mut buffer = ConnectionBuffer::with_capacity(capacity);
        for i in 0..5 {
            match buffer.offer(StreamItem::new(i, ()), policy) {
                Offer::Full(item) => assert_eq!(item.sequence, i),
                Offer::Queued | Offer::Evicted | Offer::Discarded => {}
            }
        }
        let dropped = buffer.dropped();
        let items = std::iter::from_fn(|| buffer.pop())
            .map(|i| i.sequence)
            .collect();
        (items, dropped)
    }

    #[test]
    fn offer_applies_policy() {
        assert_eq!(offer_five(OverflowPolicy::Block, 3), (vec![0, 1, 2], 0));
        assert_eq!(
            offer_five(OverflowPolicy::DropOldest, 3),
            (vec![2, 3, 4], 2)
        );
        assert_eq!(
            offer_five(OverflowPolicy::DropNewest, 3),
            (vec![0, 1, 2], 2)
        );
        assert_eq!(offer_five(OverflowPolicy::KeepLatest, 3), (vec![4], 4));
        assert_eq!(offer_five(OverflowPolicy::DropOldest, 0), (vec![], 5));
    }
}
