//! One turso connection, driven to completion on the thread that owns it: the
//! worker's reader, or the DB thread's writer.

use std::collections::hash_map::Entry;
use std::num::NonZero;
use std::sync::Arc;

use rustc_hash::FxHashMap;
use turso_core::{
    CheckpointMode, Connection, Database, IO, LimboError, Statement, StepResult, SyncMode,
};

use crate::row::{FromRow, Row};
use crate::{Error, Params};

/// Steps a bound statement to its end, decoding its rows into an `R`.
pub(crate) type Decode<R> = fn(&mut Statement, &Arc<dyn IO>) -> Result<R, Error>;

pub(crate) struct Conn {
    conn: Arc<Connection>,
    io: Arc<dyn IO>,
    begin: Statement,
    commit: Statement,
    rollback: Statement,
    /// The clients' statements, by the address of their `static`: a fast
    /// hash, as the keys come from our own statics.
    statements: FxHashMap<usize, Statement>,
}

impl Conn {
    /// A new connection to `db`, whose transactions open with `begin`.
    pub(crate) fn new(db: &Arc<Database>, begin: &str) -> Result<Self, Error> {
        let conn = db.connect()?;
        Ok(Conn {
            begin: conn.prepare(begin)?,
            commit: conn.prepare("COMMIT")?,
            rollback: conn.prepare("ROLLBACK")?,
            io: db.io.clone(),
            statements: FxHashMap::default(),
            conn,
        })
    }

    pub(crate) fn set_sync_mode(&self, mode: SyncMode) {
        self.conn.set_sync_mode(mode);
    }

    /// Refuses, as it prepares them, the statements that would write.
    pub(crate) fn set_query_only(&self) {
        self.conn.set_query_only(true);
    }

    /// The WAL's size, in pages.
    pub(crate) fn wal_pages(&self) -> Result<u64, Error> {
        Ok(self.conn.wal_state()?.max_frame)
    }

    /// Copies what it can of the WAL into the database file, up to the
    /// oldest read transaction in progress, without stopping anyone.
    pub(crate) fn checkpoint_wal(&self) -> Result<(), Error> {
        let mode = CheckpointMode::Passive {
            upper_bound_inclusive: None,
        };
        self.conn.checkpoint(mode)?;
        Ok(())
    }

    /// Copies every page of the WAL into the database file and restarts it
    /// from its beginning: the file keeps its size, writes go back to its
    /// start. Only succeeds while no read transaction uses the WAL.
    pub(crate) fn restart_wal(&self) -> Result<(), Error> {
        self.conn.checkpoint(CheckpointMode::Restart)?;
        Ok(())
    }

    pub(crate) fn begin(&mut self) -> Result<(), Error> {
        execute(&mut self.begin, &self.io)
    }

    /// Commits, or rolls back if the commit failed: never leaves a transaction
    /// open.
    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        let committed = execute(&mut self.commit, &self.io);
        if committed.is_err() {
            self.rollback();
        }
        committed
    }

    /// Undoes the open transaction, if any.
    pub(crate) fn rollback(&mut self) {
        if self.in_transaction() {
            let _ = execute(&mut self.rollback, &self.io);
        }
    }

    /// Prepares `sql`, one of the statements the crate runs itself.
    pub(crate) fn prepare(&self, sql: &str) -> Result<Statement, Error> {
        Ok(self.conn.prepare(sql)?)
    }

    /// Runs `stmt`, prepared by [`prepare`](Self::prepare): no parameters,
    /// no rows.
    pub(crate) fn execute(&self, stmt: &mut Statement) -> Result<(), Error> {
        execute(stmt, &self.io)
    }

    /// False once turso ended the transaction itself, as it does when a
    /// statement fails while running.
    pub(crate) fn in_transaction(&self) -> bool {
        !self.conn.get_auto_commit()
    }

    /// Runs the statement `sql`, prepared once and cached under `key`, with
    /// `params`; `decode` turns its rows into the answer.
    pub(crate) fn run<R>(
        &mut self,
        key: usize,
        sql: &'static str,
        params: impl Params,
        decode: Decode<R>,
    ) -> Result<R, Error> {
        let stmt = match self.statements.entry(key) {
            Entry::Occupied(prepared) => prepared.into_mut(),
            Entry::Vacant(entry) => entry.insert(self.conn.prepare(sql)?),
        };
        let answer = bind(stmt, params).and_then(|()| decode(stmt, &self.io));
        stmt.reset()?;
        answer
    }
}

/// Every row, as a `Vec<T>`.
pub(crate) fn all<T: FromRow>(stmt: &mut Statement, io: &Arc<dyn IO>) -> Result<Vec<T>, Error> {
    let mut rows = Vec::<T>::new();
    for_each_row(stmt, io, |row| {
        rows.push(T::from_row(&row)?);
        Ok(())
    })?;
    Ok(rows)
}

/// The row if there is one, as an `Option<T>`; more than one is an error.
pub(crate) fn one<T: FromRow>(stmt: &mut Statement, io: &Arc<dyn IO>) -> Result<Option<T>, Error> {
    let mut first = None::<T>;
    for_each_row(stmt, io, |row| {
        if first.is_some() {
            return Err(Error::TooManyRows);
        }
        first = Some(T::from_row(&row)?);
        Ok(())
    })?;
    Ok(first)
}

/// No rows wanted: `()`.
pub(crate) fn none(stmt: &mut Statement, io: &Arc<dyn IO>) -> Result<(), Error> {
    for_each_row(stmt, io, |_| Ok(()))
}

/// Binds every parameter: `reset` keeps the previous values, so one left out
/// would silently be the last run's.
fn bind(stmt: &mut Statement, params: impl Params) -> Result<(), Error> {
    let params = params.into_iter();
    let expected = stmt.parameters_count();
    if params.len() != expected {
        return Err(Error::Parameters {
            expected,
            given: params.len(),
        });
    }
    for (i, param) in params.enumerate() {
        stmt.bind_at(NonZero::new(i + 1).unwrap(), param)?;
    }
    Ok(())
}

/// Runs a statement without parameters nor rows, such as `BEGIN`.
fn execute(stmt: &mut Statement, io: &Arc<dyn IO>) -> Result<(), Error> {
    let done = for_each_row(stmt, io, |_| Ok(()));
    stmt.reset()?;
    done
}

/// Steps `stmt` to its end, handing every row to `f`.
fn for_each_row(
    stmt: &mut Statement,
    io: &Arc<dyn IO>,
    mut f: impl FnMut(Row<'_>) -> Result<(), Error>,
) -> Result<(), Error> {
    loop {
        match stmt.step()? {
            StepResult::Row => f(Row::new(stmt.row().unwrap()))?,
            StepResult::Done => return Ok(()),
            StepResult::Busy => return Err(LimboError::Busy.into()),
            StepResult::Interrupt => return Err(LimboError::Interrupt.into()),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => io.step()?,
        }
    }
}
