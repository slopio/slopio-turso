//! One turso connection running [`Request`]s, one at a time.
//!
//! Statements are prepared on first use and cached under their identity. A request
//! runs step by step and never blocks: [`Conn::step`] says what to wait for
//! before stepping again.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::num::NonZero;
use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::task::Waker;
use std::time::Duration;

use turso_core::{Connection, LimboError, Statement as Prepared, StepResult};

use crate::request::{Error, Request};
use crate::statement::Statement;

/// How many VM instructions run between two looks at the cancel flag.
const CANCEL_CHECK_OPS: u64 = 1000;

/// What to do before stepping again.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Step {
    /// Step again right away.
    Ready,
    /// Waiting on disk: drive the IO, then step again.
    Io,
    /// The database is locked: step again after this long.
    Sleep(Duration),
    /// The request is filled: rows, command, or error.
    Done,
}

pub(crate) struct Conn {
    conn: Arc<Connection>,
    /// Keyed by [`Statement::key`]: requests' statements and the DB thread's
    /// own alike, which never share a key.
    statements: HashMap<usize, Prepared>,
    /// Key of the statement in flight, if any.
    running: Option<usize>,
    /// The running request's cancel flag, set only for the duration of a step:
    /// turso's progress handler reads it from inside the step.
    cancel_flag: Arc<AtomicPtr<AtomicBool>>,
    max_rows: usize,
}

impl Conn {
    pub(crate) fn new(conn: Arc<Connection>, max_rows: usize) -> Self {
        // A single step can run for a long time (an aggregate, a sort) before it
        // returns, so the flag is checked from inside it, not between steps.
        let cancel_flag = Arc::new(AtomicPtr::new(ptr::null_mut()));
        let flag = Arc::clone(&cancel_flag);
        let cancelled = move || {
            let flag: *const AtomicBool = flag.load(Ordering::Relaxed);
            // SAFETY: non-null only while `step` runs, which borrows the flag.
            !flag.is_null() && unsafe { &*flag }.load(Ordering::Relaxed)
        };
        conn.set_progress_handler(CANCEL_CHECK_OPS, Some(Box::new(cancelled)));
        Conn {
            conn,
            statements: HashMap::new(),
            running: None,
            cancel_flag,
            max_rows,
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        self.running.is_none()
    }

    /// Whether a `BEGIN` left the connection inside a transaction.
    pub(crate) fn in_transaction(&self) -> bool {
        !self.conn.get_auto_commit()
    }

    /// Prepares the request's statement on first use and binds its params.
    pub(crate) fn start(&mut self, request: &mut Request) -> Step {
        debug_assert!(self.is_idle(), "one request at a time");
        match self.bind(request) {
            Ok(()) => {
                self.running = Some(request.statement().key());
                Step::Ready
            }
            Err(err) => {
                request.set_error(Error::Turso(err));
                Step::Done
            }
        }
    }

    /// Whether the request only reads, preparing its statement on first use.
    ///
    /// Follows `sqlite3_stmt_readonly`, which counts `BEGIN` and `COMMIT` as
    /// read-only.
    pub(crate) fn is_readonly(&mut self, request: &Request) -> Result<bool, LimboError> {
        Ok(self
            .prepared(request.statement())?
            .get_program()
            .is_readonly())
    }

    fn prepared(&mut self, statement: &'static Statement) -> Result<&mut Prepared, LimboError> {
        Ok(match self.statements.entry(statement.key()) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(self.conn.prepare(statement.sql())?),
        })
    }

    fn bind(&mut self, request: &mut Request) -> Result<(), LimboError> {
        let stmt = self.prepared(request.statement())?;
        // Parameters left over from a previous run would otherwise leak in.
        stmt.clear_bindings();
        for (index, param) in (1..).zip(request.params()) {
            stmt.bind_at(NonZero::new(index).unwrap(), param.to_owned()?)?;
        }
        if stmt.num_columns() > 0 {
            request.begin_rows();
        }
        Ok(())
    }

    /// Runs one of the DB thread's own statements (`BEGIN`, `SAVEPOINT`…) to
    /// completion, prepared on first use. Blocks on its IO: all of them run in
    /// memory but `COMMIT`, whose fsync would block the thread inside
    /// `IO::step` anyway.
    pub(crate) fn run_internal(&mut self, statement: &'static Statement) -> Result<(), LimboError> {
        debug_assert!(self.is_idle(), "no request may be running");
        let stmt = self.prepared(statement)?;
        let ran = stmt.run_ignore_rows();
        let reset = stmt.reset();
        ran.and(reset)
    }

    /// Ends the running request with `err`, e.g. when the IO backend failed.
    pub(crate) fn abort(&mut self, request: &mut Request, err: LimboError) {
        if let Some(key) = self.running.take() {
            // The statement may be mid-IO, so this can block; the IO is broken anyway.
            let _ = self.statements.get_mut(&key).map(Prepared::reset);
        }
        request.set_error(Error::Turso(err));
    }

    /// Advances the running request once. `cancelled` is its `Pending`'s flag;
    /// once set, the step ends as `Cancelled` within a few instructions. On
    /// `Io`, turso wakes `waker` once the operation it waits on has finished —
    /// at once, if it already has.
    pub(crate) fn step(
        &mut self,
        request: &mut Request,
        cancelled: &AtomicBool,
        waker: &Waker,
    ) -> Step {
        let key = self.running.expect("a request is running");
        let stmt = self
            .statements
            .get_mut(&key)
            .expect("running statement is cached");
        self.cancel_flag
            .store(ptr::from_ref(cancelled).cast_mut(), Ordering::Relaxed);
        let stepped = stmt.step_with_waker(waker);
        self.cancel_flag.store(ptr::null_mut(), Ordering::Relaxed);
        let outcome = match stepped {
            Ok(StepResult::Row) if request.row_count() >= Some(self.max_rows) => {
                Err(Error::TooManyRows)
            }
            Ok(StepResult::Row) => {
                let row = stmt.row().expect("row after StepResult::Row");
                request.push_row(row.get_values());
                return Step::Ready;
            }
            Ok(StepResult::IO) => return Step::Io,
            Ok(StepResult::Yield) => return Step::Ready,
            Ok(StepResult::Sleep { duration }) => return Step::Sleep(duration),
            Ok(StepResult::Done) => Ok((self.conn.changes(), self.conn.last_insert_rowid())),
            Ok(StepResult::Interrupt) => Err(Error::Cancelled),
            Ok(StepResult::Busy) => Err(Error::Turso(LimboError::Busy)),
            Err(err) => Err(Error::Turso(err)),
        };
        // No IO is in flight at this point, so reset() does not block.
        let reset = stmt.reset();
        self.running = None;
        match outcome.and_then(|done| reset.map(|()| done).map_err(Error::Turso)) {
            Ok((changes, last_insert_rowid)) => request.finish_command(changes, last_insert_rowid),
            Err(err) => request.set_error(err),
        }
        Step::Done
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::ResultSet;
    use turso_core::{Database, MemoryIO, Numeric, OpenOptions, SqliteDialect, Value, ValueRef};

    /// An in-memory database; `name` keeps tests apart in turso's registry.
    fn open(name: &str, max_rows: usize) -> Conn {
        let io = Arc::new(MemoryIO::new());
        let options = OpenOptions::new(Arc::new(SqliteDialect));
        let db = Database::open(io, &format!("{name}.db"), options).unwrap();
        Conn::new(db.connect().unwrap(), max_rows)
    }

    /// Runs `request` to completion, blocking on IO.
    fn run(conn: &mut Conn, mut request: Request) -> Request {
        let mut step = conn.start(&mut request);
        while step != Step::Done {
            if step == Step::Io {
                conn.conn.get_pager().io.step().unwrap();
            }
            step = conn.step(&mut request, &AtomicBool::new(false), Waker::noop());
        }
        request
    }

    static CREATE_USERS: Statement =
        Statement::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT UNIQUE)");
    static INSERT_USER: Statement = Statement::new("INSERT INTO users (name) VALUES (?1)");
    static USERS_BY_ID: Statement = Statement::new("SELECT id, name FROM users ORDER BY id");
    static USER_NAMES: Statement = Statement::new("SELECT name FROM users");
    static COUNT_USERS: Statement = Statement::new("SELECT count(*) FROM users");
    static MISSING: Statement = Statement::new("SELECT * FROM missing");
    static ONE: Statement = Statement::new("SELECT 1");

    fn exec(conn: &mut Conn, statement: &'static Statement, params: &[ValueRef<'_>]) -> Request {
        run(conn, Request::prepared(statement, params))
    }

    fn int(value: &ValueRef<'_>) -> i64 {
        match value {
            ValueRef::Numeric(Numeric::Integer(i)) => *i,
            other => panic!("not an integer: {other:?}"),
        }
    }

    fn users(conn: &mut Conn) {
        exec(conn, &CREATE_USERS, &[]);
        for name in ["ada", "bob", "cy"] {
            let text = Value::from_text(name);
            exec(conn, &INSERT_USER, &[text.as_ref()]);
        }
    }

    #[test]
    fn insert_then_select() {
        let mut conn = open("insert_then_select", 100);
        users(&mut conn);

        let done = exec(&mut conn, &USERS_BY_ID, &[]);
        let rows: Vec<_> = done
            .rows_iter()
            .map(|row| (int(&row[0]), row[1].to_text().unwrap().to_owned()))
            .collect();
        assert_eq!(
            rows,
            [(1, "ada".into()), (2, "bob".into()), (3, "cy".into())]
        );
    }

    #[test]
    fn write_reports_changes_and_rowid() {
        let mut conn = open("write_reports_changes_and_rowid", 100);
        users(&mut conn);
        let done = exec(&mut conn, &INSERT_USER, &[Value::from_text("dee").as_ref()]);
        assert!(matches!(
            done.result(),
            ResultSet::Command {
                changes: 1,
                last_insert_rowid: 4
            }
        ));
        // Prepared once, reused for every insert.
        assert_eq!(conn.statements.len(), 2);
    }

    #[test]
    fn errors_leave_the_connection_usable() {
        let mut conn = open("errors_leave_the_connection_usable", 100);
        users(&mut conn);

        let done = exec(&mut conn, &MISSING, &[]);
        assert!(matches!(done.result(), ResultSet::Error(Error::Turso(_))));

        let dup = Value::from_text("ada");
        let done = exec(&mut conn, &INSERT_USER, &[dup.as_ref()]);
        assert!(matches!(done.result(), ResultSet::Error(Error::Turso(_))));

        let done = exec(&mut conn, &COUNT_USERS, &[]);
        assert_eq!(int(&done.rows_iter().next().unwrap()[0]), 3);
    }

    #[test]
    fn row_limit() {
        let mut conn = open("row_limit", 2);
        users(&mut conn);
        let done = exec(&mut conn, &USER_NAMES, &[]);
        assert!(matches!(
            done.result(),
            ResultSet::Error(Error::TooManyRows)
        ));
    }

    #[test]
    fn cancel_interrupts_a_long_step() {
        let mut conn = open("cancel_interrupts_a_long_step", 100);
        // One row, at the very end: the whole count runs inside a single step.
        static COUNT_FOREVER: Statement = Statement::new(
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000000000) SELECT count(*) FROM c",
        );
        let mut request = Request::prepared(&COUNT_FOREVER, &[]);
        conn.start(&mut request);
        let cancelled = AtomicBool::new(true);
        assert_eq!(
            conn.step(&mut request, &cancelled, Waker::noop()),
            Step::Done
        );
        assert!(matches!(
            request.result(),
            ResultSet::Error(Error::Cancelled)
        ));

        // The interrupt does not leak into the next request.
        let done = exec(&mut conn, &ONE, &[]);
        assert_eq!(int(&done.rows_iter().next().unwrap()[0]), 1);
    }
}
