//! One turso connection, driven to completion on the thread that owns it: the
//! worker's reader, or the DB thread's writer.

use std::any::Any;
use std::collections::hash_map::{Entry, HashMap};
use std::num::NonZero;
use std::sync::Arc;

use turso_core::{Connection, Database, IO, LimboError, Statement, StepResult, SyncMode, Value};

use crate::Error;
use crate::row::{FromRow, Row};

/// A statement's decoded rows, their type erased.
pub(crate) type Answer = Result<Box<dyn Any>, Error>;

/// Steps a bound statement to its end, decoding its rows into an [`Answer`].
pub(crate) type Decode = fn(&mut Statement, &Arc<dyn IO>) -> Answer;

pub(crate) struct Conn {
    conn: Arc<Connection>,
    io: Arc<dyn IO>,
    begin: Statement,
    commit: Statement,
    rollback: Statement,
    /// The clients' statements, by the address of their `static`.
    statements: HashMap<usize, Statement>,
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
            statements: HashMap::new(),
            conn,
        })
    }

    pub(crate) fn set_sync_mode(&self, mode: SyncMode) {
        self.conn.set_sync_mode(mode);
    }

    pub(crate) fn begin(&mut self) -> Result<(), Error> {
        execute(&mut self.begin, &self.io)
    }

    /// Commits, or rolls back if the commit failed: never leaves a transaction
    /// open.
    pub(crate) fn commit(&mut self) -> Result<(), Error> {
        let committed = execute(&mut self.commit, &self.io);
        if committed.is_err() && self.in_transaction() {
            let _ = execute(&mut self.rollback, &self.io);
        }
        committed
    }

    /// False once turso ended the transaction itself, as it does when a
    /// statement fails while running.
    pub(crate) fn in_transaction(&self) -> bool {
        !self.conn.get_auto_commit()
    }

    /// Runs the statement `sql`, prepared once and cached under `key`, with
    /// `params`; `decode` turns its rows into the answer.
    pub(crate) fn run(
        &mut self,
        key: usize,
        sql: &'static str,
        params: Vec<Value>,
        decode: Decode,
    ) -> Answer {
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
pub(crate) fn all<T: FromRow>(stmt: &mut Statement, io: &Arc<dyn IO>) -> Answer {
    let mut rows = Vec::<T>::new();
    for_each_row(stmt, io, |row| {
        rows.push(T::from_row(&row)?);
        Ok(())
    })?;
    Ok(Box::new(rows))
}

/// The row if there is one, as an `Option<T>`; more than one is an error.
pub(crate) fn one<T: FromRow>(stmt: &mut Statement, io: &Arc<dyn IO>) -> Answer {
    let mut first = None::<T>;
    for_each_row(stmt, io, |row| {
        if first.is_some() {
            return Err(Error::TooManyRows);
        }
        first = Some(T::from_row(&row)?);
        Ok(())
    })?;
    Ok(Box::new(first))
}

/// No rows wanted: `()`.
pub(crate) fn none(stmt: &mut Statement, io: &Arc<dyn IO>) -> Answer {
    for_each_row(stmt, io, |_| Ok(()))?;
    Ok(Box::new(()))
}

/// Binds every parameter: `reset` keeps the previous values, so one left out
/// would silently be the last run's.
fn bind(stmt: &mut Statement, params: Vec<Value>) -> Result<(), Error> {
    let expected = stmt.parameters_count();
    if params.len() != expected {
        return Err(Error::Parameters {
            expected,
            given: params.len(),
        });
    }
    for (i, param) in params.into_iter().enumerate() {
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
