//! The caller's handle on an interactive transaction.

use slopio::runtime::send_msg_to_peer;

use crate::shared::pending::Pending;
use crate::shared::request::Request;
use crate::shared::transaction::TransactionId;

/// The caller's single-use-at-a-time capability to run requests inside one
/// interactive transaction.
///
/// Deliberately **not** `Copy` or `Clone`: each in-transaction request *moves*
/// the token in and hands it back only once the request has completed, so while
/// one is in flight the caller has no token and a second overlapping request on
/// the same transaction is a type error.
#[derive(Debug)]
pub struct TransactionToken {
    id: TransactionId,
    /// Where to post the `ROLLBACK` if the token is dropped.
    db_thread: usize,
}

impl TransactionToken {
    #[inline]
    pub(crate) fn new(id: TransactionId, db_thread: usize) -> Self {
        TransactionToken { id, db_thread }
    }

    #[inline]
    pub fn id(&self) -> TransactionId {
        self.id
    }
}

impl Drop for TransactionToken {
    /// The token died without an explicit `commit` or `rollback` — and this
    /// fires even when **no request is in flight**: callers hold the token idle
    /// between requests and may simply drop it there, leaving the connection
    /// pinned with the DB thread told nothing.
    ///
    /// So post a fire-and-forget `ROLLBACK`. Explicit terminators disarm this
    /// with `mem::forget` so it fires at most once. If the transaction is
    /// already over, the DB thread refuses it as ended, which is harmless.
    fn drop(&mut self) {
        let rollback = Request::rollback().in_transaction(self.id);
        // Move-only: nobody keeps a clone and no slot is reserved, so the DB
        // thread finds nothing to wake and drops it when done.
        send_msg_to_peer(self.db_thread, Pending::new(rollback));
    }
}
