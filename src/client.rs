//! A worker thread's way into the database.

use std::rc::Rc;
use std::sync::Arc;

use turso_core::{Database, Value};

use crate::conn::Conn;
use crate::db::Shared;
use crate::job;
use crate::reader::{self, Queue};
use crate::request::{self, Body};
use crate::row::{FromRow, Returned};
use crate::writer::{Single, Transaction};
use crate::{Error, Read, Write};

/// A statement's parameters, in order: a `Vec<Value>`, or an array
/// `[Value; N]`, which a read keeps without allocating.
pub trait Params: IntoIterator<Item = Value, IntoIter: ExactSizeIterator> + 'static {}

impl<P> Params for P where P: IntoIterator<Item = Value, IntoIter: ExactSizeIterator> + 'static {}

/// A worker's handle on the database. Clone it for every task of the worker
/// that uses it; it cannot leave the worker.
///
/// Reads run on the worker's own read connection; writes cross to the DB
/// thread's writer.
#[derive(Clone)]
pub struct Client {
    /// The database, and where writes go.
    shared: Arc<Shared>,
    db_thread: usize,
    reads: Rc<Queue>,
}

impl Client {
    /// Asks the DB thread `db_thread` for the database, waiting for it to be
    /// open if it is not yet, and starts this worker's reader task.
    ///
    /// # Safety
    ///
    /// `db_thread` must be a thread whose root future is [`db::serve`]: the
    /// request crosses with its type erased, and any other receiver would
    /// take it for whatever type it expects.
    ///
    /// [`db::serve`]: crate::db::serve
    pub async unsafe fn connect(db_thread: usize) -> Result<Client, Error> {
        // SAFETY: the caller vouches that `db_thread` runs `serve`.
        let connected = unsafe { request::send(db_thread, Body::Connect) };
        let Body::Opened(db) = connected.await else {
            unreachable!("a `Connect` is answered with `Opened`");
        };
        let shared = db?;
        let conn = Conn::new(&shared.db, "BEGIN")?;
        // A `WITH … INSERT` passes for a `Read`: only turso can tell it writes.
        conn.set_query_only();
        let reads = Rc::new(Queue::new());
        slopio::spawn(reader::run(conn, reads.clone(), shared.clone()));
        Ok(Client {
            shared,
            db_thread,
            reads,
        })
    }

    /// Runs `statement` with `params` on this worker's read connection, every
    /// row decoded as a `T`.
    pub async fn read<T: FromRow>(
        &self,
        statement: &'static Read,
        params: impl Params,
    ) -> Result<Vec<T>, Error> {
        self.reads
            .read::<Vec<T>>(statement.key(), statement.sql(), params)
            .await
    }

    /// Like [`read`](Self::read), for a statement that gives at most one row:
    /// more is an error.
    pub async fn read_one<T: FromRow>(
        &self,
        statement: &'static Read,
        params: impl Params,
    ) -> Result<Option<T>, Error> {
        self.reads
            .read::<Option<T>>(statement.key(), statement.sql(), params)
            .await
    }

    /// Sends `statement` with `params` to the DB thread's writer, and answers
    /// once its batch has committed, with what its rows decode into.
    pub async fn write<R: Returned + Send>(
        &self,
        statement: &'static Write<R>,
        params: Vec<Value>,
    ) -> Result<R, Error> {
        let single = Single { statement, params };
        job::push(self.db_thread, &self.shared.writes, single).await
    }

    /// Sends `writes` to the DB thread's writer, to run in order and commit
    /// together or not at all, and answers once their batch has committed.
    /// The first write that fails undoes the others and is the answer.
    pub async fn transaction(
        &self,
        writes: Vec<(&'static Write, Vec<Value>)>,
    ) -> Result<(), Error> {
        if writes.is_empty() {
            return Ok(());
        }
        let transaction = Transaction { writes };
        job::push(self.db_thread, &self.shared.writes, transaction).await
    }

    pub fn database(&self) -> &Arc<Database> {
        &self.shared.db
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        // The last client is going: the reader must see it to stop.
        if Rc::strong_count(&self.reads) == 2 {
            self.reads.wake_reader();
        }
    }
}
