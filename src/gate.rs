//! The gate every worker's reader passes to start a read transaction, which
//! the writer closes to empty the WAL.
//!
//! The WAL only restarts from its beginning once no reader uses it, and our
//! readers chain read transactions without pause: left alone, the WAL grows
//! without end. So once it passes a size, the writer closes the gate, waits
//! for the read transactions in progress to end, empties the WAL, and opens
//! the gate again. A reader waiting at the gate yields: its worker goes on
//! with its other tasks.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::time::{Duration, Instant};

pub(crate) struct ReadGate {
    /// The writer wants the WAL to itself: no new read transaction.
    closed: AtomicBool,
    /// Read transactions in progress, on every worker.
    active: AtomicUsize,
}

impl ReadGate {
    pub(crate) fn new() -> Self {
        ReadGate {
            closed: AtomicBool::new(false),
            active: AtomicUsize::new(0),
        }
    }

    /// For a reader, before a read transaction: waits while the gate is
    /// closed. Pair with [`leave`](Self::leave).
    pub(crate) async fn enter(&self) {
        loop {
            // Counted before looking: a writer that closes the gate after the
            // look sees this reader and waits for it.
            self.active.fetch_add(1, SeqCst);
            if !self.closed.load(SeqCst) {
                return;
            }
            self.active.fetch_sub(1, SeqCst);
            while self.closed.load(SeqCst) {
                slopio::yield_now().await;
            }
        }
    }

    /// For a reader, once its read transaction has ended.
    pub(crate) fn leave(&self) {
        self.active.fetch_sub(1, SeqCst);
    }

    /// For the writer: closes the gate and waits for the read transactions in
    /// progress to end; `false` if they did not within `patience`, and the
    /// gate is then open again.
    pub(crate) fn close(&self, patience: Duration) -> bool {
        self.closed.store(true, SeqCst);
        let deadline = Instant::now() + patience;
        while self.active.load(SeqCst) != 0 {
            if Instant::now() >= deadline {
                self.open();
                return false;
            }
            std::hint::spin_loop();
        }
        true
    }

    pub(crate) fn open(&self) {
        self.closed.store(false, SeqCst);
    }
}
