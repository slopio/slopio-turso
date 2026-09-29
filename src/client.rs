//! The calling-thread side: hand a [`Request`] to the DB thread and await its
//! filled result.
//!
//! The request is shared, not handed over: this side keeps a clone of the
//! `Arc<Message<Pending>>` in the awaiting future, so the DB thread's answer
//! needs to carry nothing but a wakeup. The discipline that makes the sharing
//! sound lives in [`crate::pending`]; this module is one of its two halves.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use slopio::io_driver::{Endpoint, Header, Message};
use slopio::runtime::{ReplySignal, send_shared_to_peer, thread_id};

use crate::pending::Pending;
use crate::request::Request;

/// A handle from a caller thread to the DB thread.
#[derive(Debug, Clone, Copy)]
pub struct Client {
    db_thread: usize,
}

impl Client {
    pub fn new(db_thread: usize) -> Self {
        Client { db_thread }
    }

    /// Send `request` to the DB thread and await its filled result.
    ///
    /// Nothing happens until the returned future is polled — an eagerly-sent
    /// request would mean a future built and then discarded unpolled had already
    /// run its query.
    pub async fn query(&self, request: Request) -> Request {
        self.start(request).await
    }

    /// Put `request` in flight and hand back the future awaiting its wakeup.
    ///
    /// Not public: calling it without awaiting the result is the footgun
    /// [`query`](Self::query) exists to keep shut.
    fn start(&self, request: Request) -> PendingQuery {
        let signal = ReplySignal::reserve();
        let header = Header::new(
            Endpoint::with_slab(thread_id() as u16, signal.slot()),
            Endpoint::new(self.db_thread as u16),
        );
        let msg = Message::with_header(header, Pending::new(request));
        // SAFETY: a clone survives here, so the payload really is shared; the
        // handoff protocol in `crate::pending` is what makes that sound — we
        // touch nothing but the cancellation flag until the wakeup lands.
        unsafe { send_shared_to_peer(self.db_thread, Arc::clone(&msg)) };
        PendingQuery {
            signal,
            msg: Some(msg),
        }
    }
}

/// A request already in flight on the DB thread, resolving to itself once
/// filled.
///
/// Taking `msg` on resolution is also how `Drop` tells "finished" from
/// "abandoned", so there is no separate flag to arm and no disarm to forget.
struct PendingQuery {
    signal: ReplySignal,
    msg: Option<Arc<Message<Pending>>>,
}

impl Future for PendingQuery {
    type Output = Request;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Request> {
        let this = self.get_mut();
        std::task::ready!(Pin::new(&mut this.signal).poll(cx));
        let msg = this
            .msg
            .take()
            .expect("PendingQuery polled after completion");
        // The DB thread drops its clone before signalling (rule 4), so the
        // wakeup is proof ours is the last reference and nobody is still writing.
        Arc::into_inner(msg)
            .map(|msg| Poll::Ready(msg.payload.into_request()))
            .expect("the DB thread released its clone before signalling")
    }
}

impl Drop for PendingQuery {
    /// Flag the request abandoned; `ReplySignal`'s own `Drop` then keeps the slot
    /// reserved until the late wakeup lands. Nothing is sent.
    fn drop(&mut self) {
        if let Some(msg) = &self.msg {
            msg.payload.cancel();
        }
    }
}
