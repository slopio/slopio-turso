//! The envelope a request travels in while it is shared between the caller and
//! the DB thread.
//!
//! # The handoff protocol
//!
//! Sharing a `Request` is sound only under a discipline the compiler cannot
//! check: the wire erases the type, so `send_shared_to_peer` hands its contract
//! to us. Every `unsafe` in the crate rests on these five rules.
//!
//! 1. The caller allocates the message, keeps one clone, and sends the other.
//! 2. From the send until the wakeup lands, **only the DB thread** touches the
//!    request, through [`request_mut`](Pending::request_mut).
//! 3. In that window the caller touches **nothing but** `cancelled` — never the
//!    request.
//! 4. The DB thread drops its clone **before** signalling, and touches nothing
//!    afterwards. Dropping first is what leaves the caller's `Arc` unique, so it
//!    can take the request back out whole.
//! 5. The `MSG_RING` hop provides the happens-before in both directions.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::shared::request::Request;

/// A request plus the one bit its caller can still set after handing it over.
///
/// The two are separate fields, not one behind the other, so that the caller's
/// access to the flag never forms a reference covering the request — which the
/// DB thread holds `&mut` to.
pub(crate) struct Pending {
    /// Monotonic: only ever `false → true`, so "set" is final.
    cancelled: AtomicBool,
    request: UnsafeCell<Request>,
}

impl Pending {
    pub(crate) fn new(request: Request) -> Self {
        Pending {
            cancelled: AtomicBool::new(false),
            request: UnsafeCell::new(request),
        }
    }

    /// `Relaxed` suffices: no data is published alongside the flag, and the
    /// happens-before that matters is the `MSG_RING` hop's.
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// A *false* answer is never wrong to act on, only wasteful — the flag may
    /// be set an instant later. A *true* answer is final.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// For the DB thread to watch while the request runs.
    pub(crate) fn cancel_flag(&self) -> &AtomicBool {
        &self.cancelled
    }

    /// # Safety
    /// No `&mut` from [`request_mut`](Self::request_mut) may be live on any
    /// thread: the DB thread before it has signalled, or the caller after its
    /// wakeup has landed — never in between.
    pub(crate) unsafe fn request(&self) -> &Request {
        unsafe { &*self.request.get() }
    }

    /// # Safety
    /// Caller must be the DB thread, between receiving the request and
    /// signalling its answer — the window rule 2 grants it sole access. No other
    /// reference to the request may exist on any thread meanwhile.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn request_mut(&self) -> &mut Request {
        unsafe { &mut *self.request.get() }
    }

    /// Taking `self` by value proves the `Arc` was unique, so no other thread
    /// holds the envelope — the state rule 4 leaves behind.
    pub(crate) fn into_request(self) -> Request {
        self.request.into_inner()
    }
}
