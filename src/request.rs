//! What a worker sends the DB thread over MSG_RING, and how it gets its
//! answer: a `Connect`, and the doorbell of the shared inbox, which writes go
//! through as [`job`](crate::job)s.
//!
//! The request is shared: the worker keeps a clone of the `Arc` and touches
//! only `cancelled`; the DB thread replaces the body with its answer, drops its
//! clone, then wakes the worker. The wakeup is the worker's proof that nobody
//! else holds the request anymore, so it takes the answer without a lock.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use slopio::io_driver::{Endpoint, Header, Message};
use slopio::runtime::{
    ReplySignal, send_msg_to_peer, send_shared_to_peer, signal_reply_to_peer, thread_id,
};

use crate::Error;
use crate::db::Shared;

/// The question going out, then the answer coming back.
pub(crate) enum Body {
    /// Asks for the database.
    Connect,
    /// The database, and the inbox to push writes into.
    Opened(Result<Arc<Shared>, Error>),
    /// Writes were pushed into the inbox; not answered.
    Doorbell,
}

/// A request as the DB thread holds it.
pub(crate) type Msg = Arc<Message<Request>>;

pub(crate) struct Request {
    cancelled: AtomicBool,
    body: UnsafeCell<Body>,
}

impl Request {
    fn new(body: Body) -> Self {
        Request {
            cancelled: AtomicBool::new(false),
            body: UnsafeCell::new(body),
        }
    }

    /// The worker no longer waits for the answer.
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// For the DB thread: whether the worker still waits for the answer.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// For the DB thread, until it answers: the worker does not touch the body
    /// before the wakeup.
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn body(&self) -> &mut Body {
        // SAFETY: only the DB thread reaches the body while the request is in
        // flight, and it holds one `&mut` at a time.
        unsafe { &mut *self.body.get() }
    }
}

/// For the DB thread: answers `msg` with `body`, unless the worker gave up on
/// it, and wakes the worker.
pub(crate) fn reply(msg: Msg, body: Body) {
    let header = msg.header;
    if !msg.payload.is_cancelled() {
        *msg.payload.body() = body;
    }
    // Dropped before the wakeup: the worker must find its clone the last one.
    drop(msg);
    // Sent even when cancelled: the worker's reply slot stays reserved until a
    // wakeup lands in it.
    signal_reply_to_peer(&header, 0);
}

/// Sends `body` to the DB thread `db_thread` over MSG_RING and waits for its
/// answer.
///
/// # Safety
///
/// `db_thread` must run [`serve`](crate::db::serve): only it receives a
/// `Request`.
pub(crate) async unsafe fn send(db_thread: usize, body: Body) -> Body {
    let (signal, msg) = request(db_thread, body);
    // SAFETY: we keep a clone, but touch nothing but the cancellation flag
    // until the wakeup, by which time the DB thread has dropped its own; and
    // the caller vouches that `db_thread` expects a `Request`.
    unsafe { send_shared_to_peer(db_thread, Arc::clone(&msg)) };
    answer(signal, msg).await
}

/// Rings the DB thread's doorbell: writes are in the inbox.
pub(crate) fn ring(db_thread: usize) {
    send_msg_to_peer(db_thread, Request::new(Body::Doorbell));
}

/// A request for `db_thread`, and the slot its answer will wake.
fn request(db_thread: usize, body: Body) -> (ReplySignal, Msg) {
    let signal = ReplySignal::reserve();
    let header = header(db_thread, &signal);
    (signal, Message::with_header(header, Request::new(body)))
}

/// From this thread's `signal` to `db_thread`: where a request goes, and where
/// its answer's wakeup comes back.
pub(crate) fn header(db_thread: usize, signal: &ReplySignal) -> Header {
    Header::new(
        Endpoint::with_slab(thread_id() as u16, signal.slot()),
        // Truncated, it would name another thread.
        Endpoint::new(u16::try_from(db_thread).expect("no such thread")),
    )
}

async fn answer(signal: ReplySignal, msg: Msg) -> Body {
    let pending = Pending(Some(msg));
    signal.await;
    pending.take()
}

/// A request in flight: dropped before the answer, it flags the request
/// cancelled so the DB thread does not bother running it.
struct Pending(Option<Msg>);

impl Pending {
    fn take(mut self) -> Body {
        let msg = self.0.take().unwrap();
        Arc::into_inner(msg)
            .expect("the DB thread dropped its clone before the wakeup")
            .payload
            .body
            .into_inner()
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(msg) = &self.0 {
            msg.payload.cancel();
        }
    }
}
