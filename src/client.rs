//! A worker thread's way into the database.

use std::rc::Rc;
use std::sync::Arc;

use turso_core::{Database, Value};

use crate::conn::{self, Conn};
use crate::reader::{self, Queue};
use crate::request::{self, Body};
use crate::row::FromRow;
use crate::{Error, Read, Write};

/// A worker's handle on the database. Clone it for every task of the worker
/// that uses it; it cannot leave the worker.
///
/// Reads run on the worker's own read connection; writes cross to the DB
/// thread's writer.
#[derive(Clone)]
pub struct Client {
    db: Arc<Database>,
    db_thread: usize,
    reads: Rc<Queue>,
}

impl Client {
    /// Asks the DB thread `db_thread` for the database, waiting for it to be
    /// open if it is not yet, and starts this worker's reader task.
    pub async fn connect(db_thread: usize) -> Result<Client, Error> {
        let Body::Opened(db) = request::send(db_thread, Body::Connect).await else {
            unreachable!("a `Connect` is answered with `Opened`");
        };
        let db = db?;
        let conn = Conn::new(&db, "BEGIN")?;
        let reads = Rc::new(Queue::new());
        slopio::spawn(reader::run(conn, reads.clone()));
        Ok(Client {
            db,
            db_thread,
            reads,
        })
    }

    /// Runs `statement` with `params` on this worker's read connection, every
    /// row decoded as a `T`.
    pub async fn read<T: FromRow>(
        &self,
        statement: &'static Read,
        params: Vec<Value>,
    ) -> Result<Vec<T>, Error> {
        self.reads
            .read(statement.key(), statement.sql(), params, conn::all::<T>)
            .await
    }

    /// Like [`read`](Self::read), for a statement that gives at most one row:
    /// more is an error.
    pub async fn read_one<T: FromRow>(
        &self,
        statement: &'static Read,
        params: Vec<Value>,
    ) -> Result<Option<T>, Error> {
        self.reads
            .read(statement.key(), statement.sql(), params, conn::one::<T>)
            .await
    }

    /// Sends `statement` with `params` to the DB thread's writer, and answers
    /// once its batch has committed.
    pub async fn write(&self, statement: &'static Write, params: Vec<Value>) -> Result<(), Error> {
        let body = Body::Write { statement, params };
        let Body::Written(written) = request::send(self.db_thread, body).await else {
            unreachable!("a `Write` is answered with `Written`");
        };
        written
    }

    pub fn database(&self) -> &Arc<Database> {
        &self.db
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
