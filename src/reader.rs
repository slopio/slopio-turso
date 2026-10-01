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
use std::task::{Context, Poll, Waker};

use turso_core::Value;

use crate::Error;
use crate::conn::{Answer, Conn, Decode};

/// The most reads one read transaction serves.
const MAX_BATCH: usize = 64;

/// A read waiting for the reader, shared by it and the task that asked.
struct Slot {
    /// The statement's key in the reader's cache, and its SQL.
    key: usize,
    sql: &'static str,
    params: Cell<Vec<Value>>,
    decode: Decode,
    result: Cell<Option<Answer>>,
    /// The task that asked, while it sleeps.
    waker: Cell<Option<Waker>>,
}

pub(crate) struct Queue {
    slots: RefCell<VecDeque<Rc<Slot>>>,
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
        params: Vec<Value>,
        decode: Decode,
    ) -> Result<R, Error> {
        let slot = Rc::new(Slot {
            key,
            sql,
            params: Cell::new(params),
            decode,
            result: Cell::new(None),
            waker: Cell::new(None),
        });
        self.slots.borrow_mut().push_back(slot.clone());
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

/// The reader task: serves `queue` on `conn` until every client is gone.
pub(crate) async fn run(mut conn: Conn, queue: Rc<Queue>) {
    let mut batch = Vec::with_capacity(MAX_BATCH);
    while queue.next_batch(&mut batch).await {
        // A lone read needs no transaction of its own making.
        let shared = batch.len() > 1 && conn.begin().is_ok();
        for slot in batch.drain(..) {
            // Nobody waits for this read anymore.
            if Rc::strong_count(&slot) == 1 {
                continue;
            }
            let answer = conn.run(slot.key, slot.sql, slot.params.take(), slot.decode);
            slot.result.set(Some(answer));
            if let Some(task) = slot.waker.take() {
                task.wake();
            }
        }
        // Never keep an old snapshot open.
        if shared && conn.in_transaction() {
            let _ = conn.commit();
        }
    }
}
