//! The DB thread: receives requests from other threads over MSG_RING and runs
//! them on turso connections — one writer and a few readers — taking turns a
//! few steps at a time.
//!
//! A `BEGIN` pins its connection to an interactive transaction until turso says
//! it is over: `BEGIN` (read-only to turso) opens a read transaction on a
//! reader, `BEGIN IMMEDIATE` a write transaction on the writer. Readers are
//! query-only, so a write inside a read transaction fails.
//!
//! This is the other half of the handoff protocol in [`crate::pending`].

use std::collections::VecDeque;
use std::future::poll_fn;
use std::iter;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Instant;

use slopio::io_driver::Message;
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_msg_from_peer, signal_reply_to_peer, yield_now};
use turso_core::{Database, IO, LimboError};

use crate::conn::{Conn, Step};
use crate::pending::Pending;
use crate::request::Error;
use crate::transaction::TransactionId;

type Msg = Arc<Message<Pending>>;

/// Steps a connection takes in a row before the others get their turn.
const STEPS_PER_TURN: usize = 64;

pub struct Server {
    /// Writes run one at a time: turso takes one writer, and a second one
    /// would fail as busy.
    writer: Slot,
    readers: Vec<Slot>,
    writes: VecDeque<Msg>,
    reads: VecDeque<Msg>,
}

impl Server {
    pub fn new(db: &Arc<Database>, readers: usize, max_rows: usize) -> Result<Self, LimboError> {
        let slot = |index: u16, query_only: bool| {
            let conn = db.connect()?;
            conn.set_query_only(query_only);
            Ok::<_, LimboError>(Slot::new(index, Conn::new(conn, max_rows)))
        };
        let readers = u16::try_from(readers).expect("fewer than 65535 readers");
        Ok(Server {
            writer: slot(0, false)?,
            readers: (1..=readers)
                .map(|index| slot(index, true))
                .collect::<Result<_, _>>()?,
            writes: VecDeque::new(),
            reads: VecDeque::new(),
        })
    }

    /// Serves requests forever. Run it as the root future of the DB thread.
    ///
    /// `io` must be the backend the database was opened with. Waiting on disk
    /// still blocks the thread (turso's backends wait inside `IO::step`).
    pub async fn serve(mut self, io: Arc<dyn IO>) {
        loop {
            if self.is_idle() {
                // Nothing to do: sleep until a request arrives.
                let msg = recv().await;
                self.accept(msg);
            }
            // Let the runtime process its completions, so requests sent since
            // the last turn reach the inbox, then take them all.
            yield_now().await;
            while let Some(msg) = try_recv().await {
                self.accept(msg);
            }
            self.dispatch();
            let now = Instant::now();
            for slot in self.slots_mut() {
                slot.take_turn(now);
            }
            self.wait(io.as_ref()).await;
        }
    }

    /// Queues a request for its transaction's connection, or else for the
    /// writer or the readers.
    fn accept(&mut self, msg: Msg) {
        // SAFETY: rule 2 — ours until we reply.
        let request = unsafe { msg.payload.request() };
        if let Some(id) = request.transaction() {
            match self.slot_mut(id.connection()) {
                Some(slot) if slot.transaction == Some(id) => slot.transaction_queue.push_back(msg),
                _ => fail(msg, Error::TransactionEnded),
            }
            return;
        }
        // SQL that fails to prepare counts as a write: the writer reports the error.
        if self.writer.conn.is_readonly(request).unwrap_or(false) {
            self.reads.push_back(msg);
        } else {
            self.writes.push_back(msg);
        }
    }

    /// Hands queued requests to idle connections.
    fn dispatch(&mut self) {
        self.writer.fill(&mut self.writes);
        for reader in &mut self.readers {
            reader.fill(&mut self.reads);
        }
    }

    /// When no request can step right now, waits for what unblocks one.
    async fn wait(&mut self, io: &dyn IO) {
        if self.slots_mut().any(|slot| slot.wait == Wait::Io) {
            let result = io.step();
            for slot in self.slots_mut() {
                slot.io_done(result.as_ref().err());
            }
            return;
        }
        let now = Instant::now();
        if self.slots_mut().any(|slot| slot.is_runnable(now)) {
            return;
        }
        let deadline = self.slots_mut().filter_map(|slot| slot.deadline()).min();
        if let Some(deadline) = deadline {
            // A new request may arrive before the busy database frees up.
            let sleep = Sleep::during(deadline.saturating_duration_since(now));
            if let SelectResult::First(msg) = select(recv(), sleep).await {
                self.accept(msg);
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.writes.is_empty()
            && self.reads.is_empty()
            && iter::once(&self.writer)
                .chain(&self.readers)
                .all(|slot| slot.running.is_none() && slot.transaction_queue.is_empty())
    }

    fn slot_mut(&mut self, index: u16) -> Option<&mut Slot> {
        match index {
            0 => Some(&mut self.writer),
            i => self.readers.get_mut(usize::from(i) - 1),
        }
    }

    fn slots_mut(&mut self) -> impl Iterator<Item = &mut Slot> {
        iter::once(&mut self.writer).chain(&mut self.readers)
    }
}

/// Why the running request is not stepping.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Wait {
    Ready,
    Io,
    Until(Instant),
}

/// A connection and the request it is running, if any.
struct Slot {
    /// 0 for the writer, then the readers; the connection field of the
    /// transaction ids minted here.
    index: u16,
    conn: Conn,
    running: Option<Msg>,
    wait: Wait,
    /// The interactive transaction this connection is pinned to, if any.
    transaction: Option<TransactionId>,
    /// Requests of that transaction waiting for the connection.
    transaction_queue: VecDeque<Msg>,
    /// Counter field of the next transaction id minted here.
    next_transaction: u32,
}

impl Slot {
    fn new(index: u16, conn: Conn) -> Self {
        Slot {
            index,
            conn,
            running: None,
            wait: Wait::Ready,
            transaction: None,
            transaction_queue: VecDeque::new(),
            next_transaction: 0,
        }
    }

    /// Starts queued requests until one is running. A connection pinned to a
    /// transaction only takes that transaction's requests.
    fn fill(&mut self, queue: &mut VecDeque<Msg>) {
        while self.running.is_none() {
            let next = match self.transaction {
                Some(_) => self.transaction_queue.pop_front(),
                None => queue.pop_front(),
            };
            let Some(msg) = next else {
                return;
            };
            if msg.payload.is_cancelled() {
                reply(msg);
                continue;
            }
            // SAFETY: rule 2 — ours until we reply.
            let request = unsafe { msg.payload.request_mut() };
            match self.conn.start(request) {
                Step::Done => self.finish(msg),
                _ => {
                    self.running = Some(msg);
                    self.wait = Wait::Ready;
                }
            }
        }
    }

    fn is_runnable(&self, now: Instant) -> bool {
        self.running.is_some()
            && match self.wait {
                Wait::Ready => true,
                Wait::Io => false,
                Wait::Until(deadline) => now >= deadline,
            }
    }

    fn deadline(&self) -> Option<Instant> {
        match (&self.running, self.wait) {
            (Some(_), Wait::Until(deadline)) => Some(deadline),
            _ => None,
        }
    }

    /// Steps the running request until it waits, finishes, or its turn ends.
    fn take_turn(&mut self, now: Instant) {
        for _ in 0..STEPS_PER_TURN {
            if !self.is_runnable(now) {
                return;
            }
            let msg = self.running.as_ref().unwrap();
            // SAFETY: rule 2 — ours until we reply.
            let request = unsafe { msg.payload.request_mut() };
            match self.conn.step(request, msg.payload.cancel_flag()) {
                Step::Ready => self.wait = Wait::Ready,
                Step::Io => self.wait = Wait::Io,
                Step::Sleep(duration) => self.wait = Wait::Until(Instant::now() + duration),
                Step::Done => {
                    let msg = self.running.take().unwrap();
                    self.finish(msg);
                }
            }
        }
    }

    /// Replies, after pinning the connection if the request opened a
    /// transaction, or unpinning it if the request ended one.
    fn finish(&mut self, msg: Msg) {
        match (self.transaction, self.conn.in_transaction()) {
            (None, true) => {
                let id = TransactionId::compose(self.index, self.next_transaction);
                self.next_transaction = self.next_transaction.wrapping_add(1);
                self.transaction = Some(id);
                // SAFETY: rule 2 — not signalled yet, so still ours to write to.
                unsafe { msg.payload.request_mut() }.set_transaction(id);
            }
            (Some(_), false) => {
                self.transaction = None;
                for queued in self.transaction_queue.drain(..) {
                    fail(queued, Error::TransactionEnded);
                }
            }
            _ => {}
        }
        reply(msg);
    }

    /// The IO backend made progress, or failed.
    fn io_done(&mut self, err: Option<&LimboError>) {
        if self.wait != Wait::Io {
            return;
        }
        self.wait = Wait::Ready;
        if let (Some(err), Some(msg)) = (err, self.running.take()) {
            // SAFETY: rule 2 — ours until we reply.
            self.conn
                .abort(unsafe { msg.payload.request_mut() }, err.clone());
            self.finish(msg);
        }
    }
}

async fn recv() -> Msg {
    // SAFETY: the DB thread only ever receives `Message<Pending>` — the
    // requests `Client`s send it.
    unsafe { recv_msg_from_peer::<Pending>() }.await
}

/// The next request if one is already in the inbox, without waiting.
async fn try_recv() -> Option<Msg> {
    let mut recv = pin!(recv());
    poll_fn(|cx| {
        Poll::Ready(match recv.as_mut().poll(cx) {
            Poll::Ready(msg) => Some(msg),
            Poll::Pending => None,
        })
    })
    .await
}

/// Answers `msg` with `err` without running it.
fn fail(msg: Msg, err: Error) {
    // SAFETY: rule 2 — not signalled yet, so still ours to write to.
    unsafe { msg.payload.request_mut() }.set_error(err);
    reply(msg);
}

/// Wakes the caller. We release our clone *before* signalling, so the caller
/// wakes as the last holder and can take its request back out whole (rule 4).
/// A cancelled request is signalled too: that is what frees the caller's slot.
fn reply(msg: Msg) {
    let header = msg.header;
    drop(msg);
    signal_reply_to_peer(&header, 0);
}
