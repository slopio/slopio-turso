//! Interactive-transaction identity.
//!
//! A [`TransactionId`] tags every request belonging to one interactive
//! transaction, so the DB thread can route it to the connection the
//! transaction lives on. A [`TransactionToken`] is the caller's handle on it.

use slopio::runtime::send_msg_to_peer;

use crate::pending::Pending;
use crate::request::Request;

/// The identity of one interactive transaction, packed into a `u64`.
///
/// ```text
///  63           48 47           32 31                             0
/// ┌───────────────┬───────────────┬───────────────────────────────┐
/// │   unused (16) │ connection(16)│          counter (32)         │
/// └───────────────┴───────────────┴───────────────────────────────┘
/// ```
///
/// Self-describing, so the DB thread finds the connection without a lookup
/// table. The `counter` need only be distinct within one connection: it is what
/// tells a live transaction from a finished one that ran on the same connection.
/// The top bits are free for a thread id, should there be several DB threads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(u64);

const CONNECTION_SHIFT: u32 = 32;
const COUNTER_MASK: u64 = 0xFFFF_FFFF;
const FIELD16_MASK: u64 = 0xFFFF;

impl TransactionId {
    #[inline]
    pub const fn new(raw: u64) -> Self {
        TransactionId(raw)
    }

    #[inline]
    pub const fn compose(connection: u16, counter: u32) -> Self {
        TransactionId(((connection as u64) << CONNECTION_SHIFT) | (counter as u64))
    }

    #[inline]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Index of the connection the transaction lives on.
    #[inline]
    pub const fn connection(self) -> u16 {
        ((self.0 >> CONNECTION_SHIFT) & FIELD16_MASK) as u16
    }

    #[inline]
    pub const fn counter(self) -> u32 {
        (self.0 & COUNTER_MASK) as u32
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fields_round_trip() {
        let id = TransactionId::compose(7, 0xDEAD_BEEF);
        assert_eq!(id.connection(), 7);
        assert_eq!(id.counter(), 0xDEAD_BEEF);
        assert_eq!(TransactionId::new(id.get()), id);
    }

    #[test]
    fn fields_do_not_overlap() {
        let id = TransactionId::compose(u16::MAX, 0);
        assert_eq!(id.counter(), 0);
        let id = TransactionId::compose(0, u32::MAX);
        assert_eq!(id.connection(), 0);
    }
}
