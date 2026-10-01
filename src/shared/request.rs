//! A request, shared between its caller and the DB thread while in flight.
//!
//! Everything but the body is fixed before sending: its statement, its
//! transaction, and its cancel flag, which only the caller writes. The body
//! goes from the arguments to the result: the DB thread reads the arguments
//! once, to bind them, and overwrites them with the result — reusing their
//! buffer for rows. Both are encoded as in [`crate::shared::codec`].
//!
//! # The handoff protocol
//!
//! Sharing a `Request` is sound only under a discipline the compiler cannot
//! check: the wire erases the type, so `send_shared_to_peer` hands its contract
//! to us. Every `unsafe` in the crate rests on these five rules.
//!
//! 1. The caller allocates the message, keeps one clone, and sends the other.
//! 2. From the send until the wakeup lands, **only the DB thread** touches the
//!    body, through [`body_mut`](Request::body_mut).
//! 3. In that window the caller touches **nothing but** the cancel flag. The
//!    rest of the request is read-only on both sides.
//! 4. The DB thread drops its clone **before** signalling, and touches nothing
//!    afterwards. Dropping first is what leaves the caller's `Arc` unique, so it
//!    can take the request back out whole.
//! 5. The hops provide the happens-before: the mailbox's mutex or `MSG_RING`
//!    on the way in, `MSG_RING` on the way back.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

use turso_core::{LimboError, Value, ValueRef};

use crate::shared::codec;
use crate::shared::response::{Row, RowBuf};
use crate::shared::statement::Statement;
use crate::shared::transaction::TransactionId;

/// Bytes a request's buffer starts with: its arguments, then its rows.
const INITIAL_CAPACITY: usize = 64;

#[derive(Debug, Clone)]
pub enum Error {
    Turso(LimboError),
    Cancelled,
    /// The result grew past the row limit; the rows read so far are dropped.
    TooManyRows,
    /// The request names a transaction that is no longer open.
    TransactionEnded,
}

#[derive(Debug)]
pub struct Request {
    /// The request's identity: its address keys the prepared-statement cache.
    statement: &'static Statement,
    /// `None` outside an interactive transaction.
    transaction: Option<TransactionId>,
    meta: Meta,
    /// The DB thread's alone from the send until it signals its reply.
    body: UnsafeCell<Body>,
}

/// Who awaits the reply, and whether they still do.
#[derive(Debug)]
pub struct Meta {
    /// The client thread the reply goes back to.
    thread: usize,
    /// The awaiting future's slot in that thread's slab. `None` when nobody
    /// awaits the reply: the DB thread's own requests, a dropped token's
    /// `ROLLBACK`.
    slab_id: Option<u32>,
    /// Monotonic: only ever `false → true`, so "set" is final. Once set, the
    /// slot may already hold another request: the reply must not touch it.
    cancelled: AtomicBool,
}

impl Meta {
    pub(crate) fn thread(&self) -> usize {
        self.thread
    }

    pub(crate) fn slab_id(&self) -> Option<u32> {
        self.slab_id
    }

    /// `Relaxed` suffices: no data is published alongside the flag, and the
    /// happens-before that matters is the hops' (rule 5).
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
}

/// What a request carries: its arguments on the way in, its result on the way
/// back. Exactly one holds, so illegal combinations (rows *and* error) cannot
/// be represented.
#[derive(Debug)]
pub enum Body {
    /// Caller → DB thread: the arguments, encoded.
    Args(Vec<u8>),
    /// A statement that returns columns — possibly no rows.
    Rows(RowBuf),
    /// A statement that returns no columns (INSERT, UPDATE, CREATE…).
    Command {
        changes: i64,
        last_insert_rowid: i64,
    },
    /// The request opened an interactive transaction: its id, proof it is open.
    Transaction(TransactionId),
    Error(Error),
}

impl Request {
    /// Encodes `args` for `statement`'s `?1..` placeholders.
    pub fn new(statement: &'static Statement, args: &[ValueRef<'_>]) -> Self {
        // One allocation for the arguments and, once they are bound, a small
        // result: growing from empty would reallocate at 8, 16, 32 bytes.
        let mut bytes = Vec::with_capacity(INITIAL_CAPACITY);
        for arg in args {
            codec::encode(&mut bytes, *arg);
        }
        Request {
            statement,
            transaction: None,
            meta: Meta {
                thread: 0,
                slab_id: None,
                cancelled: AtomicBool::new(false),
            },
            body: UnsafeCell::new(Body::Args(bytes)),
        }
    }

    /// Opens a write transaction: turso classes it as a write, so it lands on
    /// the writer.
    pub fn begin_write() -> Self {
        static BEGIN_WRITE: Statement = Statement::new("BEGIN IMMEDIATE");
        Request::new(&BEGIN_WRITE, &[])
    }

    /// Opens a read transaction: turso classes it as read-only, so it lands on
    /// a reader.
    pub fn begin_read() -> Self {
        static BEGIN_READ: Statement = Statement::new("BEGIN");
        Request::new(&BEGIN_READ, &[])
    }

    pub fn commit() -> Self {
        static COMMIT: Statement = Statement::new("COMMIT");
        Request::new(&COMMIT, &[])
    }

    pub fn rollback() -> Self {
        static ROLLBACK: Statement = Statement::new("ROLLBACK");
        Request::new(&ROLLBACK, &[])
    }

    /// Consumes and returns `self` so it composes in a builder chain.
    pub fn in_transaction(mut self, id: TransactionId) -> Self {
        self.transaction = Some(id);
        self
    }

    pub fn statement(&self) -> &'static Statement {
        self.statement
    }

    pub fn transaction(&self) -> Option<TransactionId> {
        self.transaction
    }

    pub fn meta(&self) -> &Meta {
        &self.meta
    }

    /// Where the reply goes: set by the client just before sending.
    pub(crate) fn reply_to(&mut self, thread: usize, slab_id: u32) {
        self.meta.thread = thread;
        self.meta.slab_id = Some(slab_id);
    }

    /// The arguments before the request is sent, its result once it is back.
    ///
    /// Safe: outside this crate, a `Request` is only ever reachable while no
    /// other thread holds it — one in flight lives only in the crate's own
    /// `PendingQuery` and DB thread, which never call this on it.
    pub fn body(&self) -> &Body {
        // SAFETY: see above — no `&mut` from `body_mut` is live.
        unsafe { &*self.body.get() }
    }

    /// # Safety
    /// Caller must be the DB thread, between receiving the request and
    /// signalling its reply — the window rule 2 grants it sole access. No other
    /// reference to the body may exist on any thread meanwhile.
    #[allow(clippy::mut_from_ref)]
    pub(crate) unsafe fn body_mut(&self) -> &mut Body {
        unsafe { &mut *self.body.get() }
    }
}

impl Body {
    /// The arguments to bind; none once the body holds a result.
    pub fn args(&self) -> impl Iterator<Item = ValueRef<'_>> {
        let bytes: &[u8] = match self {
            Body::Args(bytes) => bytes,
            _ => &[],
        };
        codec::values(bytes)
    }

    /// The rows; none unless the body holds rows.
    pub fn rows(&self) -> impl Iterator<Item = Row<'_>> {
        match self {
            Body::Rows(rows) => Some(rows.iter()),
            _ => None,
        }
        .into_iter()
        .flatten()
    }

    pub fn row_count(&self) -> Option<usize> {
        match self {
            Body::Rows(rows) => Some(rows.len()),
            _ => None,
        }
    }

    // --- transitions driven by the DB thread ---

    /// The statement returns columns: so an empty result is still `Rows`, not
    /// `Command`. The arguments are bound by now; their buffer takes the rows.
    pub(crate) fn begin_rows(&mut self) {
        if let Body::Args(bytes) = self {
            *self = Body::Rows(RowBuf::from_vec(std::mem::take(bytes)));
        }
    }

    /// Appends one row (`stmt.row().get_values()`).
    pub(crate) fn push_row<'v>(&mut self, values: impl IntoIterator<Item = &'v Value>) {
        self.begin_rows();
        if let Body::Rows(rows) = self {
            rows.push_row(values);
        }
    }

    /// Unless this was row-returning, where the rows are the result.
    pub(crate) fn finish_command(&mut self, changes: i64, last_insert_rowid: i64) {
        if !matches!(self, Body::Rows(_)) {
            *self = Body::Command {
                changes,
                last_insert_rowid,
            };
        }
    }

    /// Replaces whatever the body held: the caller must learn the id, or the
    /// connection stays pinned until the transaction times out.
    pub(crate) fn set_transaction(&mut self, id: TransactionId) {
        *self = Body::Transaction(id);
    }

    /// Replaces whatever the body held: rows read so far are dropped.
    pub(crate) fn set_error(&mut self, err: Error) {
        *self = Body::Error(err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::Numeric;

    static USERS: Statement = Statement::new("SELECT id, name FROM users WHERE id = ?1");
    static INSERT: Statement = Statement::new("INSERT INTO users (name, age) VALUES (?1, ?2)");

    #[test]
    fn args_are_encoded_and_decoded() {
        let name = String::from("ada");
        let request = Request::new(
            &INSERT,
            &[
                Value::from_text(name.clone()).as_ref(),
                ValueRef::from_i64(36),
            ],
        );
        drop(name);
        let args: Vec<_> = request.body().args().collect();
        assert_eq!(args[0].to_text(), Some("ada"));
        assert!(matches!(args[1], ValueRef::Numeric(Numeric::Integer(36))));
    }

    #[test]
    fn rows_replace_args() {
        let request = Request::new(&USERS, &[ValueRef::from_i64(1)]);
        // SAFETY: the test is the only holder, here and below.
        unsafe { request.body_mut() }.push_row(&[Value::from_i64(1), Value::from_text("ada")]);

        assert_eq!(request.body().row_count(), Some(1));
        let row = request.body().rows().next().unwrap();
        assert_eq!(row.get(1).unwrap().to_text(), Some("ada"));
        assert_eq!(request.body().args().count(), 0);
    }

    #[test]
    fn empty_select_is_rows_not_command() {
        let request = Request::new(&USERS, &[ValueRef::from_i64(1)]);
        let body = unsafe { request.body_mut() };
        body.begin_rows();
        body.finish_command(0, 0);
        assert_eq!(request.body().row_count(), Some(0));
    }

    #[test]
    fn write_is_a_command() {
        let request = Request::new(&INSERT, &[Value::from_text("bob").as_ref(), ValueRef::Null]);
        unsafe { request.body_mut() }.finish_command(1, 7);
        assert!(matches!(
            request.body(),
            Body::Command {
                changes: 1,
                last_insert_rowid: 7
            }
        ));
    }

    #[test]
    fn error_drops_rows() {
        let request = Request::new(&USERS, &[]);
        let body = unsafe { request.body_mut() };
        body.push_row(&[Value::from_i64(1), Value::Null]);
        body.set_error(Error::TooManyRows);
        assert!(matches!(request.body(), Body::Error(Error::TooManyRows)));
    }

    #[test]
    fn cancel_is_final_and_transaction_rides_along() {
        let id = TransactionId::compose(1, 42);
        let request = Request::new(&USERS, &[]).in_transaction(id);
        assert_eq!(request.transaction(), Some(id));
        assert!(!request.meta().is_cancelled());
        request.meta().cancel();
        assert!(request.meta().is_cancelled());
    }
}
