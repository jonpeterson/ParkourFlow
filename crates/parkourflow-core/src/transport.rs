//! Edge transport: the bounded, credit-based channel behind each connection.
//!
//! An [`Edge`] wraps a [`ConnectionBuffer`] and the connection's
//! [`OverflowPolicy`]. The consumer's free slots are the producer's
//! *credit*: a [`Sender`] sends at most that many items before the policy
//! decides what happens. Under [`OverflowPolicy::Block`] the producer waits,
//! and waiting producers are woken in batches, once at least half the buffer
//! is free, so demand costs no per-item round trip.
//!
//! The channel uses only `std` synchronization, so it does not tie nodes to
//! one scheduler. Every operation has an async form for tasks (on any
//! executor, including Tokio), a blocking form for dedicated threads, and a
//! non-blocking form for live sources and custom schedulers:
//!
//! | | waits for space | never waits |
//! | --- | --- | --- |
//! | async task | [`Sender::send`] | [`Sender::try_send`], [`Sender::send_live`] |
//! | thread | [`Sender::send_blocking`] | [`Sender::try_send`], [`Sender::send_live`] |
//!
//! Only [`OverflowPolicy::Block`] ever waits. A live source uses
//! [`Sender::send_live`], which drops the item (and counts it) when a
//! `Block` edge is full instead of stalling the capture loop.

use std::fmt;
use std::future::poll_fn;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Poll, Waker};

use serde::Serialize;

use crate::buffer::{ConnectionBuffer, Offer, StreamItem};
use crate::workflow::{Connection, OverflowPolicy};

/// What happened to an item handed to a [`Sender`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sent {
    /// Queued without losing anything.
    Queued,
    /// Queued, and the oldest queued item was dropped to make room.
    Evicted,
    /// The item itself was dropped.
    Dropped,
}

/// The item could not be sent because the edge is closed. Holds the item.
#[derive(Debug)]
pub struct SendError(pub StreamItem);

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("edge is closed")
    }
}

impl std::error::Error for SendError {}

/// Why [`Sender::try_send`] did not send an item. Holds the item.
#[derive(Debug)]
pub enum TrySendError {
    /// A [`OverflowPolicy::Block`] edge has no credit.
    Full(StreamItem),
    Closed(StreamItem),
}

/// Why [`Receiver::try_recv`] returned no item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    /// The edge is closed and every queued item has been received.
    Closed,
}

/// Telemetry for one edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct EdgeStats {
    /// Items waiting for the consumer.
    pub queue_depth: usize,
    /// Effective capacity (1 for [`OverflowPolicy::KeepLatest`]).
    pub capacity: usize,
    /// Items dropped on this edge so far, for any reason.
    pub dropped: u64,
}

/// The runtime half of a connection: its buffer, policy, and waiters.
///
/// Cloning an `Edge` shares it. Producers and consumers get handles from
/// [`sender`](Self::sender) and [`receiver`](Self::receiver).
#[derive(Clone)]
pub struct Edge {
    shared: Arc<Shared>,
}

/// Producer handle. Cloneable; the edge closes when the last one drops.
pub struct Sender {
    shared: Arc<Shared>,
}

/// Consumer handle. The edge closes when it drops.
pub struct Receiver {
    shared: Arc<Shared>,
}

struct Shared {
    inner: Mutex<Inner>,
    /// Signalled when credit is granted, the policy changes, or the edge
    /// closes.
    space: Condvar,
    /// Signalled when an item arrives or the edge closes.
    items: Condvar,
}

struct Inner {
    buffer: ConnectionBuffer,
    policy: OverflowPolicy,
    closed: bool,
    senders: usize,
    receivers: usize,
    send_wakers: Vec<Waker>,
    recv_wakers: Vec<Waker>,
}

/// How [`Inner::offer`] treats a full `Block` edge.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Wait,
    Live,
}

impl Inner {
    /// Credit is granted once this many slots are free.
    fn grant_batch(&self) -> usize {
        self.buffer.capacity().div_ceil(2).max(1)
    }

    fn offer(&mut self, item: StreamItem, mode: Mode) -> Result<Sent, TrySendError> {
        if self.closed {
            return Err(TrySendError::Closed(item));
        }
        let sent = match self.buffer.offer(item, self.policy) {
            Offer::Queued => Sent::Queued,
            Offer::Evicted => Sent::Evicted,
            Offer::Discarded => return Ok(Sent::Dropped),
            Offer::Full(item) if mode == Mode::Live => {
                drop(item);
                self.buffer.record_drop();
                return Ok(Sent::Dropped);
            }
            Offer::Full(item) => return Err(TrySendError::Full(item)),
        };
        wake_all(&mut self.recv_wakers);
        Ok(sent)
    }

    fn take(&mut self) -> Result<StreamItem, TryRecvError> {
        match self.buffer.pop() {
            Some(item) => {
                if self.buffer.free() >= self.grant_batch() {
                    wake_all(&mut self.send_wakers);
                }
                Ok(item)
            }
            None if self.closed => Err(TryRecvError::Closed),
            None => Err(TryRecvError::Empty),
        }
    }

    fn close(&mut self) {
        self.closed = true;
        wake_all(&mut self.send_wakers);
        wake_all(&mut self.recv_wakers);
    }
}

fn wake_all(wakers: &mut Vec<Waker>) {
    for waker in wakers.drain(..) {
        waker.wake();
    }
}

fn register(wakers: &mut Vec<Waker>, waker: &Waker) {
    if !wakers.iter().any(|w| w.will_wake(waker)) {
        wakers.push(waker.clone());
    }
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        // A panic while holding the lock leaves the buffer consistent (every
        // mutation is a single push or pop), so keep going.
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn wait<'a>(&self, condvar: &Condvar, guard: MutexGuard<'a, Inner>) -> MutexGuard<'a, Inner> {
        condvar.wait(guard).unwrap_or_else(|e| e.into_inner())
    }

    /// Wakes blocked threads after `inner` woke tasks. Called after every
    /// state change that can unblock a waiter.
    fn notify(&self) {
        self.space.notify_all();
        self.items.notify_all();
    }
}

impl Edge {
    /// Creates an open, empty edge. `capacity` is the configured capacity;
    /// [`OverflowPolicy::KeepLatest`] uses 1 regardless.
    pub fn new(capacity: usize, policy: OverflowPolicy) -> Self {
        Self {
            shared: Arc::new(Shared {
                inner: Mutex::new(Inner {
                    buffer: ConnectionBuffer::with_capacity(policy.effective_capacity(capacity)),
                    policy,
                    closed: false,
                    senders: 0,
                    receivers: 0,
                    send_wakers: Vec::new(),
                    recv_wakers: Vec::new(),
                }),
                space: Condvar::new(),
                items: Condvar::new(),
            }),
        }
    }

    pub fn for_connection(connection: &Connection) -> Self {
        Self::new(connection.buffer_capacity, connection.overflow_policy)
    }

    pub fn sender(&self) -> Sender {
        self.shared.lock().senders += 1;
        Sender {
            shared: self.shared.clone(),
        }
    }

    pub fn receiver(&self) -> Receiver {
        self.shared.lock().receivers += 1;
        Receiver {
            shared: self.shared.clone(),
        }
    }

    /// Producer handle for a node that is starting a run. If the edge is
    /// closed and no node holds a handle to it, it is reopened first, so the
    /// restarted node begins a new stream. Items left from the previous
    /// stream stay queued.
    pub fn attach_sender(&self) -> Sender {
        let mut inner = self.shared.lock();
        if inner.closed && inner.senders == 0 && inner.receivers == 0 {
            inner.closed = false;
        }
        inner.senders += 1;
        Sender {
            shared: self.shared.clone(),
        }
    }

    /// Consumer handle for a node that is starting a run. A closed edge is
    /// reopened only if no node holds a handle and nothing is queued;
    /// otherwise the consumer gets what is left of the ended stream, then
    /// its end.
    pub fn attach_receiver(&self) -> Receiver {
        let mut inner = self.shared.lock();
        if inner.closed && inner.senders == 0 && inner.receivers == 0 && inner.buffer.is_empty() {
            inner.closed = false;
        }
        inner.receivers += 1;
        Receiver {
            shared: self.shared.clone(),
        }
    }

    /// Wakes every producer and consumer waiting on this edge without
    /// changing it, so they re-check their `stop` condition (see
    /// [`Receiver::recv_blocking_until`]).
    pub fn wake(&self) {
        let mut inner = self.shared.lock();
        wake_all(&mut inner.send_wakers);
        wake_all(&mut inner.recv_wakers);
        // Notify after taking the lock: a waiter is then either already
        // waiting or will see the stop condition before it waits.
        drop(inner);
        self.shared.notify();
    }

    pub fn policy(&self) -> OverflowPolicy {
        self.shared.lock().policy
    }

    pub fn stats(&self) -> EdgeStats {
        let inner = self.shared.lock();
        EdgeStats {
            queue_depth: inner.buffer.len(),
            capacity: inner.buffer.capacity(),
            dropped: inner.buffer.dropped(),
        }
    }

    /// Applies a new configured capacity and policy. Shrinking drops the
    /// oldest items (and counts them); blocked producers re-check, so
    /// switching away from `Block` releases them.
    pub fn configure(&self, capacity: usize, policy: OverflowPolicy) {
        let mut inner = self.shared.lock();
        let capacity = policy.effective_capacity(capacity);
        if inner.buffer.capacity() != capacity {
            inner.buffer.set_capacity(capacity);
        }
        inner.policy = policy;
        wake_all(&mut inner.send_wakers);
        drop(inner);
        self.shared.notify();
    }

    /// Closes the edge: sends fail, and the receiver gets the remaining
    /// items and then end of stream.
    pub fn close(&self) {
        self.shared.lock().close();
        self.shared.notify();
    }

    pub fn is_closed(&self) -> bool {
        self.shared.lock().closed
    }
}

impl fmt::Debug for Edge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Edge")
            .field("policy", &self.policy())
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

impl Sender {
    /// Free slots the consumer has granted.
    pub fn credit(&self) -> usize {
        self.shared.lock().buffer.free()
    }

    /// Sends without waiting. A full `Block` edge hands the item back.
    pub fn try_send(&self, item: StreamItem) -> Result<Sent, TrySendError> {
        let result = self.shared.lock().offer(item, Mode::Wait);
        if result.is_ok() {
            self.shared.items.notify_all();
        }
        result
    }

    /// Sends from a live source: never waits. A full `Block` edge drops the
    /// item and counts it.
    pub fn send_live(&self, item: StreamItem) -> Result<Sent, SendError> {
        let result = self.shared.lock().offer(item, Mode::Live);
        self.shared.items.notify_all();
        result.map_err(|e| match e {
            TrySendError::Closed(item) | TrySendError::Full(item) => SendError(item),
        })
    }

    /// Sends from an async task, waiting for credit on a `Block` edge.
    pub async fn send(&self, item: StreamItem) -> Result<Sent, SendError> {
        let mut item = Some(item);
        poll_fn(|cx| {
            let mut inner = self.shared.lock();
            let pending = item.take().expect("send polled after completion");
            match inner.offer(pending, Mode::Wait) {
                Ok(sent) => {
                    drop(inner);
                    self.shared.items.notify_all();
                    Poll::Ready(Ok(sent))
                }
                Err(TrySendError::Full(pending)) => {
                    item = Some(pending);
                    register(&mut inner.send_wakers, cx.waker());
                    Poll::Pending
                }
                Err(TrySendError::Closed(pending)) => Poll::Ready(Err(SendError(pending))),
            }
        })
        .await
    }

    /// Sends from a thread, blocking it until there is credit on a `Block`
    /// edge. Don't call this from an async task; use [`send`](Self::send).
    pub fn send_blocking(&self, item: StreamItem) -> Result<Sent, SendError> {
        self.send_blocking_until(item, || false)
    }

    /// Like [`send_blocking`](Self::send_blocking), but gives up and hands
    /// the item back once `stop` returns true. `stop` is checked whenever the
    /// thread wakes; call [`Edge::wake`] after making it true.
    pub fn send_blocking_until(
        &self,
        mut item: StreamItem,
        stop: impl Fn() -> bool,
    ) -> Result<Sent, SendError> {
        let mut inner = self.shared.lock();
        loop {
            if stop() {
                return Err(SendError(item));
            }
            match inner.offer(item, Mode::Wait) {
                Ok(sent) => {
                    drop(inner);
                    self.shared.items.notify_all();
                    return Ok(sent);
                }
                Err(TrySendError::Full(pending)) => {
                    item = pending;
                    // Wait for a batch of credit, a policy change, or close.
                    while !inner.closed
                        && !stop()
                        && inner.policy == OverflowPolicy::Block
                        && inner.buffer.free() < inner.grant_batch()
                    {
                        inner = self.shared.wait(&self.shared.space, inner);
                    }
                }
                Err(TrySendError::Closed(pending)) => return Err(SendError(pending)),
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.shared.lock().closed
    }
}

impl Clone for Sender {
    fn clone(&self) -> Self {
        self.shared.lock().senders += 1;
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let mut inner = self.shared.lock();
        inner.senders -= 1;
        if inner.senders == 0 {
            inner.close();
            drop(inner);
            self.shared.notify();
        }
    }
}

impl Receiver {
    /// Takes the oldest item without waiting.
    pub fn try_recv(&self) -> Result<StreamItem, TryRecvError> {
        let result = self.shared.lock().take();
        if result.is_ok() {
            self.shared.space.notify_all();
        }
        result
    }

    /// Waits for the next item in an async task. Returns `None` once the edge
    /// is closed and drained.
    pub async fn recv(&self) -> Option<StreamItem> {
        poll_fn(|cx| {
            let mut inner = self.shared.lock();
            match inner.take() {
                Ok(item) => {
                    drop(inner);
                    self.shared.space.notify_all();
                    Poll::Ready(Some(item))
                }
                Err(TryRecvError::Closed) => Poll::Ready(None),
                Err(TryRecvError::Empty) => {
                    register(&mut inner.recv_wakers, cx.waker());
                    Poll::Pending
                }
            }
        })
        .await
    }

    /// Blocks the thread until the next item. Returns `None` once the edge
    /// is closed and drained.
    pub fn recv_blocking(&self) -> Option<StreamItem> {
        self.recv_blocking_until(|| false)
    }

    /// Like [`recv_blocking`](Self::recv_blocking), but returns `None` once
    /// `stop` returns true. `stop` is checked whenever the thread wakes; call
    /// [`Edge::wake`] after making it true.
    pub fn recv_blocking_until(&self, stop: impl Fn() -> bool) -> Option<StreamItem> {
        let mut inner = self.shared.lock();
        loop {
            if stop() {
                return None;
            }
            match inner.take() {
                Ok(item) => {
                    drop(inner);
                    self.shared.space.notify_all();
                    return Some(item);
                }
                Err(TryRecvError::Closed) => return None,
                Err(TryRecvError::Empty) => inner = self.shared.wait(&self.shared.items, inner),
            }
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let mut inner = self.shared.lock();
        inner.receivers -= 1;
        if inner.receivers == 0 {
            inner.close();
            drop(inner);
            self.shared.notify();
        }
    }
}

/// One output port feeding several edges.
///
/// Each edge applies its own policy. Lossy edges get the item first; the
/// item then waits on each full `Block` edge in turn, so the slowest
/// lossless consumer gates the producer while lossy branches never do.
pub struct FanOut {
    senders: Vec<Sender>,
}

impl FanOut {
    pub fn new(senders: Vec<Sender>) -> Self {
        Self { senders }
    }

    /// Sends to every open edge from an async task. Fails only when every
    /// edge is closed.
    pub async fn send(&self, item: StreamItem) -> Result<(), SendError> {
        let blocked = self.offer_all(&item, Mode::Wait)?;
        for sender in blocked {
            // A closed edge just stops receiving; the others carry on.
            let _ = sender.send(item.clone()).await;
        }
        Ok(())
    }

    /// Sends to every open edge from a thread.
    pub fn send_blocking(&self, item: StreamItem) -> Result<(), SendError> {
        self.send_blocking_until(item, || false)
    }

    /// Like [`send_blocking`](Self::send_blocking), but stops waiting on full
    /// `Block` edges once `stop` returns true. Edges still waiting then miss
    /// the item; it is not counted as a drop, because it never reached them.
    pub fn send_blocking_until(
        &self,
        item: StreamItem,
        stop: impl Fn() -> bool,
    ) -> Result<(), SendError> {
        let blocked = self.offer_all(&item, Mode::Wait)?;
        for sender in blocked {
            if sender.send_blocking_until(item.clone(), &stop).is_err() && stop() {
                return Err(SendError(item));
            }
        }
        Ok(())
    }

    /// Number of edges this output feeds.
    pub fn len(&self) -> usize {
        self.senders.len()
    }

    pub fn is_empty(&self) -> bool {
        self.senders.is_empty()
    }

    /// Sends to every open edge from a live source, never waiting.
    pub fn send_live(&self, item: StreamItem) -> Result<(), SendError> {
        self.offer_all(&item, Mode::Live).map(drop)
    }

    /// Offers `item` to every edge without waiting and returns the `Block`
    /// edges that were full.
    fn offer_all(&self, item: &StreamItem, mode: Mode) -> Result<Vec<&Sender>, SendError> {
        let mut open = false;
        let mut blocked = Vec::new();
        for sender in &self.senders {
            let result = sender.shared.lock().offer(item.clone(), mode);
            match result {
                Ok(_) => {
                    open = true;
                    sender.shared.items.notify_all();
                }
                Err(TrySendError::Full(_)) => {
                    open = true;
                    blocked.push(sender);
                }
                Err(TrySendError::Closed(_)) => {}
            }
        }
        if open {
            Ok(blocked)
        } else {
            Err(SendError(item.clone()))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake};
    use std::thread;
    use std::time::Duration;

    use super::*;

    fn item(sequence: u64) -> StreamItem {
        StreamItem::new(sequence, sequence)
    }

    fn drain(rx: &Receiver) -> Vec<u64> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|i| i.sequence)
            .collect()
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_time()
            .build()
            .unwrap()
    }

    /// Sends 0..n with `send_live` and returns the outcomes.
    fn send_live_n(tx: &Sender, n: u64) -> Vec<Sent> {
        (0..n).map(|i| tx.send_live(item(i)).unwrap()).collect()
    }

    #[test]
    fn block_hands_back_items_and_drops_nothing() {
        let edge = Edge::new(2, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        assert_eq!(tx.try_send(item(0)).unwrap(), Sent::Queued);
        assert_eq!(tx.try_send(item(1)).unwrap(), Sent::Queued);
        let Err(TrySendError::Full(back)) = tx.try_send(item(2)) else {
            panic!("expected Full");
        };
        assert_eq!(back.sequence, 2);
        assert_eq!(edge.stats().dropped, 0);
        assert_eq!(drain(&rx), [0, 1]);
    }

    #[test]
    fn drop_oldest_evicts_the_head() {
        let edge = Edge::new(3, OverflowPolicy::DropOldest);
        let (tx, rx) = (edge.sender(), edge.receiver());
        let outcomes = send_live_n(&tx, 5);
        assert_eq!(outcomes[..3], [Sent::Queued; 3]);
        assert_eq!(outcomes[3..], [Sent::Evicted; 2]);
        assert_eq!(
            edge.stats(),
            EdgeStats {
                queue_depth: 3,
                capacity: 3,
                dropped: 2
            }
        );
        assert_eq!(drain(&rx), [2, 3, 4]);
    }

    #[test]
    fn drop_newest_discards_the_incoming_item() {
        let edge = Edge::new(3, OverflowPolicy::DropNewest);
        let (tx, rx) = (edge.sender(), edge.receiver());
        // `send` and `send_blocking` never wait on a lossy edge either.
        for i in 0..5 {
            tx.send_blocking(item(i)).unwrap();
        }
        assert_eq!(edge.stats().dropped, 2);
        assert_eq!(drain(&rx), [0, 1, 2]);
    }

    #[test]
    fn keep_latest_holds_one_item() {
        // The configured capacity is ignored: KeepLatest holds one item.
        let edge = Edge::new(8, OverflowPolicy::KeepLatest);
        let (tx, rx) = (edge.sender(), edge.receiver());
        let outcomes = runtime().block_on(async {
            let mut outcomes = Vec::new();
            for i in 0..4 {
                outcomes.push(tx.send(item(i)).await.unwrap());
            }
            outcomes
        });
        assert_eq!(
            outcomes,
            [Sent::Queued, Sent::Evicted, Sent::Evicted, Sent::Evicted]
        );
        assert_eq!(
            edge.stats(),
            EdgeStats {
                queue_depth: 1,
                capacity: 1,
                dropped: 3
            }
        );
        assert_eq!(drain(&rx), [3]);
    }

    #[test]
    fn credit_tracks_free_slots() {
        let edge = Edge::new(4, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        assert_eq!(tx.credit(), 4);
        for i in 0..4 {
            tx.try_send(item(i)).unwrap();
            assert_eq!(tx.credit(), 3 - i as usize);
        }
        rx.try_recv().unwrap();
        assert_eq!(tx.credit(), 1);
        drain(&rx);
        assert_eq!(tx.credit(), 4);
    }

    #[test]
    fn blocking_producer_never_exceeds_credit() {
        const N: u64 = 500;
        let edge = Edge::new(4, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        let observer = edge.clone();
        let producer = thread::spawn(move || {
            let mut max_depth = 0;
            for i in 0..N {
                assert_eq!(tx.send_blocking(item(i)).unwrap(), Sent::Queued);
                max_depth = max_depth.max(observer.stats().queue_depth);
            }
            max_depth
        });

        let mut received = Vec::new();
        while let Some(item) = rx.recv_blocking() {
            if item.sequence % 50 == 0 {
                thread::sleep(Duration::from_millis(1));
            }
            received.push(item.sequence);
        }
        // The edge closed when the producer dropped its sender.
        assert!(producer.join().unwrap() <= 4);
        assert_eq!(received, (0..N).collect::<Vec<_>>());
        assert_eq!(edge.stats().dropped, 0);
    }

    /// A waker that counts how often it is woken.
    #[derive(Default)]
    struct CountingWaker(AtomicUsize);

    impl Wake for CountingWaker {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn block_waits_for_a_batch_of_credit() {
        let edge = Edge::new(4, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        for i in 0..4 {
            tx.try_send(item(i)).unwrap();
        }
        let counter = Arc::new(CountingWaker::default());
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        let mut send = pin!(tx.send(item(4)));
        assert!(send.as_mut().poll(&mut cx).is_pending());

        // One free slot is less than the grant batch (half of 4), so the
        // producer is not woken yet.
        rx.try_recv().unwrap();
        assert_eq!(counter.0.load(Ordering::SeqCst), 0);
        rx.try_recv().unwrap();
        assert_eq!(counter.0.load(Ordering::SeqCst), 1);

        assert!(matches!(
            send.as_mut().poll(&mut cx),
            Poll::Ready(Ok(Sent::Queued))
        ));
        assert_eq!(drain(&rx), [2, 3, 4]);
    }

    #[test]
    fn async_producer_feeds_thread_consumer() {
        const N: u64 = 200;
        let edge = Edge::new(2, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        let rt = runtime();
        let producer = rt.spawn(async move {
            for i in 0..N {
                tx.send(item(i)).await.unwrap();
            }
        });
        let consumer = thread::spawn(move || {
            std::iter::from_fn(|| rx.recv_blocking())
                .map(|i| i.sequence)
                .collect::<Vec<_>>()
        });
        rt.block_on(producer).unwrap();
        assert_eq!(consumer.join().unwrap(), (0..N).collect::<Vec<_>>());
    }

    #[test]
    fn thread_producer_feeds_async_consumer() {
        const N: u64 = 200;
        let edge = Edge::new(2, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        let producer = thread::spawn(move || {
            for i in 0..N {
                tx.send_blocking(item(i)).unwrap();
            }
        });
        let received = runtime().block_on(async move {
            let mut received = Vec::new();
            while let Some(item) = rx.recv().await {
                received.push(item.sequence);
            }
            received
        });
        producer.join().unwrap();
        assert_eq!(received, (0..N).collect::<Vec<_>>());
    }

    #[test]
    fn live_source_drops_on_full_block_edge() {
        let edge = Edge::new(2, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        // No consumer is reading. A live source must not wait.
        let outcomes = send_live_n(&tx, 5);
        assert_eq!(
            outcomes,
            [
                Sent::Queued,
                Sent::Queued,
                Sent::Dropped,
                Sent::Dropped,
                Sent::Dropped
            ]
        );
        assert_eq!(
            edge.stats(),
            EdgeStats {
                queue_depth: 2,
                capacity: 2,
                dropped: 3
            }
        );
        // Block keeps what was queued; the live source drops the newcomers.
        assert_eq!(drain(&rx), [0, 1]);
        assert_eq!(tx.send_live(item(5)).unwrap(), Sent::Queued);
    }

    #[test]
    fn policy_change_releases_blocked_producer() {
        let edge = Edge::new(1, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        tx.try_send(item(0)).unwrap();
        let producer = thread::spawn(move || tx.send_blocking(item(1)).unwrap());
        thread::sleep(Duration::from_millis(20));
        assert!(!producer.is_finished());

        edge.configure(1, OverflowPolicy::DropOldest);
        assert_eq!(producer.join().unwrap(), Sent::Evicted);
        assert_eq!(edge.stats().dropped, 1);
        assert_eq!(drain(&rx), [1]);
    }

    #[test]
    fn closing_ends_the_stream() {
        // Dropping the last sender: the receiver drains, then sees the end.
        let edge = Edge::new(4, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        let tx2 = tx.clone();
        tx.try_send(item(0)).unwrap();
        drop(tx);
        assert!(!edge.is_closed());
        drop(tx2);
        assert_eq!(rx.recv_blocking().unwrap().sequence, 0);
        assert_eq!(rx.try_recv().unwrap_err(), TryRecvError::Closed);
        assert!(rx.recv_blocking().is_none());

        // Dropping the receiver fails sends, including a blocked one.
        let edge = Edge::new(1, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        tx.try_send(item(0)).unwrap();
        let producer = thread::spawn(move || tx.send_blocking(item(1)));
        thread::sleep(Duration::from_millis(20));
        drop(rx);
        let SendError(back) = producer.join().unwrap().unwrap_err();
        assert_eq!(back.sequence, 1);
    }

    #[test]
    fn fan_out_applies_each_edges_policy() {
        const N: u64 = 100;
        let main = Edge::new(2, OverflowPolicy::Block);
        let preview = Edge::new(8, OverflowPolicy::KeepLatest);
        let recent = Edge::new(3, OverflowPolicy::DropOldest);
        let first = Edge::new(3, OverflowPolicy::DropNewest);
        let edges = [&main, &preview, &recent, &first];
        let receivers: Vec<_> = edges.iter().map(|e| e.receiver()).collect();
        let out = FanOut::new(edges.iter().map(|e| e.sender()).collect());

        // Only the main branch is read. The lossless edge gates the
        // producer; the lossy ones never do.
        let rt = runtime();
        let producer = rt.spawn(async move {
            for i in 0..N {
                out.send(item(i)).await.unwrap();
            }
        });
        let main_rx = &receivers[0];
        let received = rt.block_on(async {
            let mut received = Vec::new();
            while let Some(item) = main_rx.recv().await {
                received.push(item.sequence);
            }
            received
        });
        rt.block_on(producer).unwrap();

        assert_eq!(received, (0..N).collect::<Vec<_>>());
        assert_eq!(main.stats().dropped, 0);
        assert_eq!(drain(&receivers[1]), [N - 1]);
        assert_eq!(preview.stats().dropped, N - 1);
        assert_eq!(drain(&receivers[2]), [N - 3, N - 2, N - 1]);
        assert_eq!(recent.stats().dropped, N - 3);
        assert_eq!(drain(&receivers[3]), [0, 1, 2]);
        assert_eq!(first.stats().dropped, N - 3);
    }

    #[test]
    fn live_fan_out_never_waits() {
        let main = Edge::new(2, OverflowPolicy::Block);
        let preview = Edge::new(1, OverflowPolicy::KeepLatest);
        let (main_rx, preview_rx) = (main.receiver(), preview.receiver());
        let out = FanOut::new(vec![main.sender(), preview.sender()]);
        for i in 0..10 {
            out.send_live(item(i)).unwrap();
        }
        assert_eq!(drain(&main_rx), [0, 1]);
        assert_eq!(main.stats().dropped, 8);
        assert_eq!(drain(&preview_rx), [9]);

        // A closed branch is skipped; the send fails only when all are.
        drop(main_rx);
        out.send_blocking(item(10)).unwrap();
        assert_eq!(drain(&preview_rx), [10]);
        drop(preview_rx);
        assert!(out.send_live(item(11)).is_err());
    }

    #[test]
    fn attach_reopens_idle_edges() {
        let edge = Edge::new(4, OverflowPolicy::Block);
        let tx = edge.attach_sender();
        tx.try_send(item(0)).unwrap();
        drop(tx);
        assert!(edge.is_closed());

        // A consumer of an ended stream gets what is left, then the end.
        let rx = edge.attach_receiver();
        assert!(edge.is_closed());
        assert_eq!(rx.recv_blocking().unwrap().sequence, 0);
        assert!(rx.recv_blocking().is_none());
        drop(rx);

        // With nothing queued and nobody attached, either side starts a new
        // stream.
        let rx = edge.attach_receiver();
        assert!(!edge.is_closed());
        let tx = edge.attach_sender();
        tx.try_send(item(1)).unwrap();
        drop(rx);
        assert!(edge.is_closed());

        // A producer can't reopen an edge whose consumer is still attached.
        drop(tx);
        let rx = edge.attach_receiver();
        drop(edge.attach_sender());
        let tx = edge.attach_sender();
        assert!(tx.is_closed());
        drop(rx);
    }

    #[test]
    fn waiting_stops_when_asked() {
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stopped = {
            let stop = stop.clone();
            move || stop.load(Ordering::SeqCst)
        };

        // A full edge: the producer waits until stopped.
        let edge = Edge::new(1, OverflowPolicy::Block);
        let (tx, rx) = (edge.sender(), edge.receiver());
        tx.try_send(item(0)).unwrap();
        let wait = stopped.clone();
        let producer = thread::spawn(move || tx.send_blocking_until(item(1), wait));
        // An empty edge: the consumer waits until stopped.
        let empty = Edge::new(1, OverflowPolicy::Block);
        let (_tx2, rx2) = (empty.sender(), empty.receiver());
        let consumer = thread::spawn(move || rx2.recv_blocking_until(stopped));
        thread::sleep(Duration::from_millis(20));
        assert!(!producer.is_finished());
        assert!(!consumer.is_finished());

        stop.store(true, Ordering::SeqCst);
        edge.wake();
        empty.wake();
        let SendError(back) = producer.join().unwrap().unwrap_err();
        assert_eq!(back.sequence, 1);
        assert!(consumer.join().unwrap().is_none());
        assert_eq!(drain(&rx), [0]);
    }
}
