//! The calling-thread side: hand a [`Request`] to the DB thread and await its
//! filled result.
//!
//! The request is shared, not handed over: this side keeps a clone of the
//! `Arc<Message<Pending>>` in the awaiting future, so the DB thread's answer
//! needs to carry nothing but a wakeup. The discipline that makes the sharing
//! sound lives in [`crate::shared::pending`]; this module is one of its two halves.

pub mod query;
mod transaction;

pub use transaction::TransactionToken;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use slopio::io_driver::{Endpoint, Header, Message};
use slopio::runtime::{ReplySignal, send_shared_to_peer, thread_id};

use turso_core::ValueRef;

use crate::client::query::{Query, Rows};
use crate::shared::pending::Pending;
use crate::shared::request::{Error, Request, ResultSet};

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

    /// Run the query `Q` with `params` and hand back its typed rows, or the
    /// error it failed with. Rows are decoded here, on the caller's thread, as
    /// they are iterated.
    pub async fn query_typed<Q: Query>(&self, params: &[ValueRef<'_>]) -> Result<Rows<Q>, Error> {
        Rows::new(self.query(Request::prepared(Q::STATEMENT, params)).await)
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
        // handoff protocol in `crate::shared::pending` is what makes that sound — we
        // touch nothing but the cancellation flag until the wakeup lands.
        unsafe { send_shared_to_peer(self.db_thread, Arc::clone(&msg)) };
        PendingQuery {
            signal,
            msg: Some(msg),
        }
    }

    /// Open a write transaction on the writer. Every other write waits until it
    /// ends, so keep it short.
    pub async fn begin_write(&self) -> Result<TransactionToken, Request> {
        self.begin(Request::begin_write()).await
    }

    /// Open a read transaction on a reader: every read in it sees the same
    /// snapshot, and writes are refused.
    pub async fn begin_read(&self) -> Result<TransactionToken, Request> {
        self.begin(Request::begin_read()).await
    }

    /// The DB thread stamps an id onto the reply only once the `BEGIN` actually
    /// left the connection in a transaction, so a stamped id is proof it is
    /// open. No id ⟹ nothing was opened, so no token exists and the filled
    /// `Request` comes back in the `Err` for inspection.
    async fn begin(&self, request: Request) -> Result<TransactionToken, Request> {
        let done = self.query(request).await;
        match done.transaction() {
            Some(id) => Ok(TransactionToken::new(id, self.db_thread)),
            None => Err(done),
        }
    }

    /// Run `request` inside the transaction that `token` owns.
    ///
    /// The token is moved in and handed back with the result, so a second
    /// in-transaction request cannot overlap the first — the type system
    /// serializes access to the transaction. It comes back even when the
    /// request failed: in turso a failed statement leaves the transaction open,
    /// so the caller decides whether to go on or roll back.
    pub async fn query_in(
        &self,
        token: TransactionToken,
        request: Request,
    ) -> (TransactionToken, Request) {
        let done = self.query(request.in_transaction(token.id())).await;
        (token, done)
    }

    /// Commit the transaction that `token` owns, consuming it.
    ///
    /// On error the transaction may still be open, so the token is dropped,
    /// which posts a `ROLLBACK`; the failed `Request` comes back in the `Err`.
    pub async fn commit(&self, token: TransactionToken) -> Result<Request, Request> {
        self.end(token, Request::commit()).await
    }

    /// Roll back the transaction that `token` owns, consuming it.
    pub async fn rollback(&self, token: TransactionToken) -> Result<Request, Request> {
        self.end(token, Request::rollback()).await
    }

    /// The token stays armed until the terminator succeeded: if this future is
    /// dropped mid-flight, or the terminator fails, its `Drop` posts the
    /// `ROLLBACK` — refused as ended if the transaction is already over.
    async fn end(&self, token: TransactionToken, request: Request) -> Result<Request, Request> {
        let done = self.query(request.in_transaction(token.id())).await;
        if matches!(done.result(), ResultSet::Error(_)) {
            Err(done)
        } else {
            // Disarm the token so its `Drop` doesn't post a spurious `ROLLBACK`.
            std::mem::forget(token);
            Ok(done)
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
