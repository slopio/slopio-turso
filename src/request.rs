//! A request and its result, backed by a single bump arena.
//!
//! One [`Request`] owns a [`Bump`] holding its runtime data: the bound params,
//! and — as turso produces them — the rows, copied out of the statement's
//! registers (which the next step overwrites). The struct is self-referential
//! (params/rows point into the `Bump` it owns); `self_cell` wraps that so the
//! outside sees a plain owned, movable value.

use bumpalo::Bump;
use bumpalo::collections::Vec as BumpVec;
use self_cell::self_cell;
use turso_core::types::TextRef;
use turso_core::{LimboError, Value, ValueRef};

use crate::transaction::TransactionId;

/// One row: its columns, each borrowing the arena. The spine lives in the
/// arena too — no heap allocation per row.
type Row<'a> = BumpVec<'a, ValueRef<'a>>;

#[derive(Debug)]
pub enum Error {
    Turso(LimboError),
    Cancelled,
    /// The result grew past the row limit; the rows read so far are dropped.
    TooManyRows,
    /// The request names a transaction that is no longer open.
    TransactionEnded,
}

/// The outcome of one request. Exactly one case holds at the end, so illegal
/// combinations (rows *and* error) cannot be represented.
#[derive(Debug)]
pub enum ResultSet<'a> {
    /// Nothing decisive produced yet — still in flight.
    Building,
    Rows(BumpVec<'a, Row<'a>>),
    /// A statement that returns no columns (INSERT, UPDATE, CREATE…).
    Command {
        changes: i64,
        last_insert_rowid: i64,
    },
    Error(Error),
}

/// `'a` is the lifetime of the owning [`Bump`]; `name`/`sql` are `'static` and
/// never touch the arena.
struct Body<'a> {
    /// Key for the connection's statement cache.
    name: &'static str,
    /// Prepared on first use per connection.
    sql: &'static str,
    params: BumpVec<'a, ValueRef<'a>>,
    /// `None` outside an interactive transaction.
    transaction: Option<TransactionId>,
    result: ResultSet<'a>,
}

self_cell!(
    /// A request plus its accumulating result, all in one arena.
    pub struct Request {
        owner: Bump,
        #[covariant]
        dependent: Body,
    }
);

// SAFETY: `Bump` is `!Send` only by `NonNull`'s conservative auto-trait; it has
// no thread-affine state and its chunks come from the global allocator, so
// allocating on one thread and freeing on another is fine. A `Request` is only
// ever accessed by one thread at a time — see the handoff protocol in
// `crate::pending` — and the `MSG_RING` hop provides the happens-before.
//
// We deliberately do NOT implement `Sync`, and it would be **unsound** to:
// `ResultSet` is public and `BumpVec::bump()` hands out `&Bump`, whose `alloc`
// takes `&self`, so two threads holding `&Request` could race in the arena.
unsafe impl Send for Request {}

impl Request {
    /// Params are copied into a fresh arena; the result starts
    /// [`Building`](ResultSet::Building) and is filled by the transitions below.
    pub fn prepared(name: &'static str, sql: &'static str, params: &[ValueRef<'_>]) -> Self {
        Request::new(Bump::new(), |bump| {
            let mut bound = BumpVec::with_capacity_in(params.len(), bump);
            for param in params {
                bound.push(copy_into(bump, *param));
            }
            Body {
                name,
                sql,
                params: bound,
                transaction: None,
                result: ResultSet::Building,
            }
        })
    }

    pub fn name(&self) -> &'static str {
        self.borrow_dependent().name
    }

    pub fn sql(&self) -> &'static str {
        self.borrow_dependent().sql
    }

    pub fn params(&self) -> &[ValueRef<'_>] {
        &self.borrow_dependent().params[..]
    }

    pub fn transaction(&self) -> Option<TransactionId> {
        self.borrow_dependent().transaction
    }

    /// Consumes and returns `self` so it composes in a builder chain.
    pub fn in_transaction(mut self, id: TransactionId) -> Self {
        self.set_transaction(id);
        self
    }

    /// Used by the DB thread to stamp a freshly-minted id onto the request that
    /// opened a transaction, so it rides back to the caller in the reply.
    pub fn set_transaction(&mut self, id: TransactionId) {
        self.with_dependent_mut(|_bump, b| b.transaction = Some(id));
    }

    pub fn result(&self) -> &ResultSet<'_> {
        &self.borrow_dependent().result
    }

    pub fn row_count(&self) -> Option<usize> {
        match &self.borrow_dependent().result {
            ResultSet::Rows(rows) => Some(rows.len()),
            _ => None,
        }
    }

    /// Every value borrows the arena, so map each row into your own struct while
    /// this `Request` is alive.
    pub fn rows_iter(&self) -> impl Iterator<Item = &[ValueRef<'_>]> {
        let rows: &[_] = match &self.borrow_dependent().result {
            ResultSet::Rows(rows) => rows,
            _ => &[],
        };
        rows.iter().map(|row| &row[..])
    }

    // --- transitions driven by the DB thread ---

    /// The statement returns columns: so an empty result is still `Rows`, not
    /// `Command`.
    pub fn begin_rows(&mut self) {
        self.with_dependent_mut(|bump, b| {
            if matches!(b.result, ResultSet::Building) {
                b.result = ResultSet::Rows(BumpVec::new_in(bump));
            }
        });
    }

    /// Copies one row (`stmt.row().get_values()`) into the arena.
    pub fn push_row<'v>(&mut self, values: impl Iterator<Item = &'v Value>) {
        self.with_dependent_mut(|bump, b| {
            let mut row = BumpVec::with_capacity_in(values.size_hint().0, bump);
            for value in values {
                row.push(copy_into(bump, value.as_ref()));
            }
            match &mut b.result {
                ResultSet::Rows(rows) => rows.push(row),
                other => {
                    let mut rows = BumpVec::new_in(bump);
                    rows.push(row);
                    *other = ResultSet::Rows(rows);
                }
            }
        });
    }

    /// Unless this was row-returning, where the rows are the result.
    pub fn finish_command(&mut self, changes: i64, last_insert_rowid: i64) {
        self.with_dependent_mut(|_bump, b| {
            if !matches!(b.result, ResultSet::Rows(_)) {
                b.result = ResultSet::Command {
                    changes,
                    last_insert_rowid,
                };
            }
        });
    }

    pub fn set_error(&mut self, err: Error) {
        self.with_dependent_mut(|_bump, b| b.result = ResultSet::Error(err));
    }
}

/// Copies text and blob bytes into `bump`; numbers and NULL are plain values.
fn copy_into<'a>(bump: &'a Bump, value: ValueRef<'_>) -> ValueRef<'a> {
    match value {
        ValueRef::Null => ValueRef::Null,
        ValueRef::Numeric(n) => ValueRef::Numeric(n),
        ValueRef::Text(t) => ValueRef::Text(TextRef::new(bump.alloc_str(t.value), t.subtype)),
        ValueRef::Blob(b) => ValueRef::Blob(bump.alloc_slice_copy(b)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::Numeric;

    #[test]
    fn params_are_copied_into_the_arena() {
        let name = String::from("ada");
        let request = Request::prepared(
            "insert_user",
            "INSERT INTO users (name, age) VALUES (?1, ?2)",
            &[
                Value::from_text(name.clone()).as_ref(),
                ValueRef::from_i64(36),
            ],
        );
        drop(name);
        assert_eq!(request.params()[0].to_text(), Some("ada"));
        assert!(matches!(
            request.params()[1],
            ValueRef::Numeric(Numeric::Integer(36))
        ));
    }

    #[test]
    fn rows_accumulate() {
        let mut request = Request::prepared("users", "SELECT id, name FROM users", &[]);
        request.begin_rows();
        request.push_row([Value::Numeric(Numeric::Integer(1)), Value::from_text("ada")].iter());
        request.push_row([Value::Numeric(Numeric::Integer(2)), Value::Null].iter());

        assert_eq!(request.row_count(), Some(2));
        let names: Vec<_> = request.rows_iter().map(|row| row[1].to_text()).collect();
        assert_eq!(names, [Some("ada"), None]);
    }

    #[test]
    fn empty_select_is_rows_not_command() {
        let mut request = Request::prepared("none", "SELECT 1 WHERE 0", &[]);
        request.begin_rows();
        request.finish_command(0, 0);
        assert_eq!(request.row_count(), Some(0));
    }

    #[test]
    fn transaction_rides_along() {
        let request = Request::prepared("one", "SELECT 1", &[]);
        assert_eq!(request.transaction(), None);
        let id = TransactionId::compose(1, 42);
        assert_eq!(request.in_transaction(id).transaction(), Some(id));
    }

    #[test]
    fn write_is_a_command() {
        let mut request = Request::prepared("del", "DELETE FROM users", &[]);
        request.finish_command(3, 0);
        assert!(matches!(
            request.result(),
            ResultSet::Command { changes: 3, .. }
        ));
    }
}
