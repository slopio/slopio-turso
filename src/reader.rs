//! The worker's reader: one task that owns the worker's read connection.
//!
//! A client's read goes into the worker's [`Queue`] and its task sleeps. The
//! reader takes what is queued, a batch at a time, runs the batch in one read
//! transaction, decodes each read's rows into the caller's type right in its
//! slot, and wakes each task as its read is done.
//!
//! Everything here lives on one thread: no atomics, no locks.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::collections::hash_map::{Entry, HashMap};
use std::future::{Future, poll_fn};
use std::num::NonZero;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use turso_core::{Connection, Database, IO, LimboError, Statement, StepResult, Value};

use crate::row::{FromRow, Row};
use crate::{Error, Read};

/// The most reads one read transaction serves.
const MAX_BATCH: usize = 64;

/// A read's decoded rows, their type erased for the queue.
type Answer = Result<Box<dyn Any>, Error>;

/// Steps a bound statement to its end, decoding its rows into an [`Answer`].
pub(crate) type Decode = fn(&mut Statement, &Arc<dyn IO>) -> Answer;

/// A read waiting for the reader, shared by it and the task that asked.
struct Slot {
    statement: &'static Read,
    params: Cell<Vec<Value>>,
    decode: Decode,
    result: Cell<Option<Answer>>,
    /// The task that asked, while it sleeps.
    waker: Cell<Option<Waker>>,
}

pub(crate) struct Queue {
    reads: RefCell<VecDeque<Rc<Slot>>>,
    /// The reader, while it sleeps on an empty queue.
    reader: Cell<Option<Waker>>,
}

impl Queue {
    pub(crate) fn new() -> Self {
        Queue {
            reads: RefCell::new(VecDeque::new()),
            reader: Cell::new(None),
        }
    }

    /// Queues a read and waits for its rows, which `decode` turns into an `R`.
    pub(crate) async fn read<R: 'static>(
        &self,
        statement: &'static Read,
        params: Vec<Value>,
        decode: Decode,
    ) -> Result<R, Error> {
        let slot = Rc::new(Slot {
            statement,
            params: Cell::new(params),
            decode,
            result: Cell::new(None),
            waker: Cell::new(None),
        });
        self.reads.borrow_mut().push_back(slot.clone());
        self.wake_reader();
        let answer = Done(slot).await?;
        Ok(*answer.downcast().expect("`decode` answers with an `R`"))
    }

    pub(crate) fn wake_reader(&self) {
        if let Some(reader) = self.reader.take() {
            reader.wake();
        }
    }

    /// Moves up to [`MAX_BATCH`] reads into `batch`, waiting for one if the
    /// queue is empty. False once no client is left to queue any.
    async fn next_batch(self: &Rc<Self>, batch: &mut Vec<Rc<Slot>>) -> bool {
        poll_fn(|cx| {
            let mut reads = self.reads.borrow_mut();
            if reads.is_empty() {
                // Only the reader holds the queue: every client is gone.
                if Rc::strong_count(self) == 1 {
                    return Poll::Ready(false);
                }
                self.reader.set(Some(cx.waker().clone()));
                return Poll::Pending;
            }
            let n = reads.len().min(MAX_BATCH);
            batch.extend(reads.drain(..n));
            Poll::Ready(true)
        })
        .await
    }
}

/// Resolves once the reader has put the answer in the slot.
struct Done(Rc<Slot>);

impl Future for Done {
    type Output = Answer;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Answer> {
        match self.0.result.take() {
            Some(answer) => Poll::Ready(answer),
            None => {
                self.0.waker.set(Some(cx.waker().clone()));
                Poll::Pending
            }
        }
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

/// What the reader task owns: the worker's read connection and its prepared
/// statements.
pub(crate) struct Reader {
    conn: Arc<Connection>,
    io: Arc<dyn IO>,
    begin: Statement,
    commit: Statement,
    rollback: Statement,
    /// The clients' statements, by [`Read::key`].
    statements: HashMap<usize, Statement>,
}

impl Reader {
    pub(crate) fn new(db: &Arc<Database>) -> Result<Self, Error> {
        let conn = db.connect()?;
        Ok(Reader {
            begin: conn.prepare("BEGIN")?,
            commit: conn.prepare("COMMIT")?,
            rollback: conn.prepare("ROLLBACK")?,
            io: db.io.clone(),
            statements: HashMap::new(),
            conn,
        })
    }

    /// The reader task: serves `queue` until every client is gone.
    pub(crate) async fn run(mut self, queue: Rc<Queue>) {
        let mut batch = Vec::with_capacity(MAX_BATCH);
        while queue.next_batch(&mut batch).await {
            // A lone read needs no transaction of its own making.
            let shared = batch.len() > 1 && execute(&mut self.begin, &self.io).is_ok();
            for slot in batch.drain(..) {
                // Nobody waits for this read anymore.
                if Rc::strong_count(&slot) == 1 {
                    continue;
                }
                let answer = self.read(slot.statement, slot.params.take(), slot.decode);
                slot.result.set(Some(answer));
                if let Some(task) = slot.waker.take() {
                    task.wake();
                }
            }
            if shared && execute(&mut self.commit, &self.io).is_err() {
                // Never keep an old snapshot open.
                let _ = execute(&mut self.rollback, &self.io);
            }
        }
    }

    fn read(&mut self, statement: &'static Read, params: Vec<Value>, decode: Decode) -> Answer {
        let stmt = match self.statements.entry(statement.key()) {
            Entry::Occupied(prepared) => prepared.into_mut(),
            Entry::Vacant(entry) => entry.insert(self.conn.prepare(statement.sql())?),
        };
        let answer = bind(stmt, params).and_then(|()| decode(stmt, &self.io));
        stmt.reset()?;
        answer
    }
}

/// Binds every parameter: `reset` keeps the previous values, so one left out
/// would silently be the last read's.
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
