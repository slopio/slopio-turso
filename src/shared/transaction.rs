//! Interactive-transaction identity.
//!
//! A [`TransactionId`] tags every request belonging to one interactive
//! transaction, so the DB thread can route it to the connection the
//! transaction lives on. The caller holds it through a
//! [`TransactionToken`](crate::client::TransactionToken).

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
///
/// Only the DB thread mints ids: outside this crate there is no way to build
/// one, so a caller can only name a transaction whose token it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(u64);

const CONNECTION_SHIFT: u32 = 32;
const COUNTER_MASK: u64 = 0xFFFF_FFFF;
const FIELD16_MASK: u64 = 0xFFFF;

impl TransactionId {
    #[cfg(test)]
    const fn new(raw: u64) -> Self {
        TransactionId(raw)
    }

    #[inline]
    pub(crate) const fn compose(connection: u16, counter: u32) -> Self {
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
