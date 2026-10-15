//! The worker's reader: one task that owns the worker's read connection.
//!
//! A client's read goes into the worker's [`Queue`] and its task sleeps. The
//! reader takes what is queued, a batch at a time, runs the batch in one read
//! transaction, decodes each read's rows into the caller's type right in its
//! slot, and wakes each task as its read is done.
//!
//! A batch's snapshot is taken when the reader takes it, so a read queued after
//! a write was answered sees that write.
//!
//! Everything here lives on one thread: no atomics, no locks.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use crate::conn::{Conn, Decode};
use crate::db::Shared;
use crate::{Error, Params};

/// The most reads one read transaction serves.
const MAX_BATCH: usize = 64;

/// A read waiting for the reader, shared by it and the task that asked; its
/// rows are decoded into an `R`.
struct Slot<R, P> {
    /// The statement's key in the reader's cache, and its SQL.
    key: usize,
    sql: &'static str,
    /// Taken by the reader.
    params: Cell<Option<P>>,
    decode: Decode<R>,
    result: Cell<Option<Result<R, Error>>>,
    /// The task that asked, while it sleeps.
    waker: Cell<Option<Waker>>,
}

/// A queued read, whatever its rows decode into.
trait Job {
    /// Runs the read on `conn`, leaves its answer in the slot and wakes the
    /// task that asked.
    fn run(&self, conn: &mut Conn);
}

impl<R, P: Params> Job for Slot<R, P> {
    fn run(&self, conn: &mut Conn) {
        let params = self.params.take().expect("a read runs once");
        let answer = conn.run(self.key, self.sql, params, self.decode);
        self.result.set(Some(answer));
        if let Some(task) = self.waker.take() {
            task.wake();
        }
    }
}

pub(crate) struct Queue {
    slots: RefCell<VecDeque<Rc<dyn Job>>>,
    /// The reader, while it sleeps on an empty queue.
    reader: Cell<Option<Waker>>,
}

impl Queue {
    pub(crate) fn new() -> Self {
        Queue {
            slots: RefCell::new(VecDeque::new()),
            reader: Cell::new(None),
        }
    }

    /// Queues a read and waits for its rows, which `decode` turns into an `R`.
    pub(crate) async fn read<R: 'static>(
        &self,
        key: usize,
        sql: &'static str,
        params: impl Params,
        decode: Decode<R>,
    ) -> Result<R, Error> {
        let slot = Rc::new(Slot {
            key,
            sql,
            params: Cell::new(Some(params)),
            decode,
            result: Cell::new(None),
            waker: Cell::new(None),
        });
        self.slots.borrow_mut().push_back(slot.clone());
        self.wake_reader();
        Done(slot).await
    }

    pub(crate) fn wake_reader(&self) {
        if let Some(reader) = self.reader.take() {
            reader.wake();
        }
    }

    /// Moves up to [`MAX_BATCH`] reads into `batch`, waiting for one if the
    /// queue is empty. False once no client is left to queue any.
    async fn next_batch(self: &Rc<Self>, batch: &mut Vec<Rc<dyn Job>>) -> bool {
        poll_fn(|cx| {
            let mut slots = self.slots.borrow_mut();
            if slots.is_empty() {
                // Only the reader holds the queue: every client is gone.
                if Rc::strong_count(self) == 1 {
                    return Poll::Ready(false);
                }
                self.reader.set(Some(cx.waker().clone()));
                return Poll::Pending;
            }
            let n = slots.len().min(MAX_BATCH);
            batch.extend(slots.drain(..n));
            Poll::Ready(true)
        })
        .await
    }
}

/// Resolves once the reader has put the answer in the slot.
struct Done<R, P>(Rc<Slot<R, P>>);

impl<R, P> Future for Done<R, P> {
    type Output = Result<R, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.0.result.take() {
            Some(answer) => Poll::Ready(answer),
            None => {
                self.0.waker.set(Some(cx.waker().clone()));
                Poll::Pending
            }
        }
    }
}

/// The reader task: serves `queue` on `conn` until every client is gone.
pub(crate) async fn run(mut conn: Conn, queue: Rc<Queue>, shared: Arc<Shared>) {
    let mut batch = Vec::with_capacity(MAX_BATCH);
    while queue.next_batch(&mut batch).await {
        // Even a lone read holds a snapshot of the WAL while it runs.
        shared.gate.enter().await;
        // A lone read needs no transaction of its own making.
        let in_tx = batch.len() > 1 && conn.begin().is_ok();
        for slot in batch.drain(..) {
            // Nobody waits for this read anymore.
            if Rc::strong_count(&slot) == 1 {
                continue;
            }
            slot.run(&mut conn);
        }
        // Never keep an old snapshot open.
        if in_tx && conn.in_transaction() {
            let _ = conn.commit();
        }
        shared.gate.leave();
    }
}
