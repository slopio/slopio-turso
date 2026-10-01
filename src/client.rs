//! A worker thread's way into the database.

use std::rc::Rc;
use std::sync::Arc;

use slopio::io_driver::{Endpoint, Header, Message};
use slopio::runtime::{ReplySignal, send_shared_to_peer, thread_id};
use turso_core::{Database, Value};

use crate::db::Connect;
use crate::reader::{self, Queue, Reader};
use crate::row::FromRow;
use crate::{Error, Read};

/// A worker's handle on the database. Clone it for every task of the worker
/// that reads; it cannot leave the worker.
#[derive(Clone)]
pub struct Client {
    db: Arc<Database>,
    reads: Rc<Queue>,
}

impl Client {
    /// Asks the DB thread `db_thread` for the database, waiting for it to be
    /// open if it is not yet, and starts this worker's reader task.
    pub async fn connect(db_thread: usize) -> Result<Client, Error> {
        let db = request_database(db_thread).await?;
        let reads = Rc::new(Queue::new());
        slopio::spawn(Reader::new(&db)?.run(reads.clone()));
        Ok(Client { db, reads })
    }

    /// Runs `statement` with `params` on this worker's read connection, every
    /// row decoded as a `T`.
    pub async fn read<T: FromRow>(
        &self,
        statement: &'static Read,
        params: Vec<Value>,
    ) -> Result<Vec<T>, Error> {
        self.reads.read(statement, params, reader::all::<T>).await
    }

    /// Like [`read`](Self::read), for a statement that gives at most one row:
    /// more is an error.
    pub async fn read_one<T: FromRow>(
        &self,
        statement: &'static Read,
        params: Vec<Value>,
    ) -> Result<Option<T>, Error> {
        self.reads.read(statement, params, reader::one::<T>).await
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

async fn request_database(db_thread: usize) -> Result<Arc<Database>, Error> {
    let signal = ReplySignal::reserve();
    let header = Header::new(
        Endpoint::with_slab(thread_id() as u16, signal.slot()),
        Endpoint::new(db_thread as u16),
    );
    let msg = Message::with_header(header, Connect::new());
    // SAFETY: we keep a clone, but touch nothing but the cancellation flag
    // until the wakeup, by which time the DB thread has dropped its own.
    unsafe { send_shared_to_peer(db_thread, Arc::clone(&msg)) };

    let pending = Pending(Some(msg));
    signal.await;
    let answer = pending.take().into_answer();
    let db = answer.expect("the DB thread answers every request it did not see cancelled")?;
    Ok(db)
}

/// A [`Connect`] in flight: dropped before the answer, it flags the request
/// cancelled so the DB thread does not bother filling it.
struct Pending(Option<Arc<Message<Connect>>>);

impl Pending {
    fn take(mut self) -> Connect {
        let msg = self.0.take().unwrap();
        Arc::into_inner(msg)
            .expect("the DB thread dropped its clone before the wakeup")
            .payload
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(msg) = &self.0 {
            msg.payload.cancel();
        }
    }
}
