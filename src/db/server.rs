//! The DB thread: receives requests from other threads over MSG_RING and runs
//! them on turso connections — one writer and a few readers — taking turns a
//! few steps at a time.
//!
//! A `BEGIN` pins its connection to an interactive transaction until turso says
//! it is over: `BEGIN` (read-only to turso) opens a read transaction on a
//! reader, `BEGIN IMMEDIATE` a write transaction on the writer. Readers are
//! query-only, so a write inside a read transaction fails. A transaction left
//! idle past [`Config::transaction_timeout`] is rolled back by the DB thread.
//!
//! The writer batches: writes queued together run in one transaction, each
//! under its own savepoint so a failing one leaves the others alone, and share
//! a single `COMMIT` — one fsync. That `COMMIT` runs like any request, so
//! readers keep going while its fsync is in flight. A write is only answered
//! once the commit succeeded; a failed one is answered at once.
//!
//! This is the other half of the handoff protocol in [`crate::shared::pending`].

use std::collections::VecDeque;
use std::future::{self, poll_fn};
use std::iter;
use std::os::fd::RawFd;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, Wake, Waker};
use std::time::{Duration, Instant};

use io_uring::opcode;
use io_uring::types::Fd;
use slopio::io_driver::operations::OneshotToken;
use slopio::io_driver::{IO_DRIVER, Message};
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_msg_from_peer, signal_reply_to_peer, yield_now};
use turso_core::{Database, LimboError};

use crate::db::conn::{Conn, Step};
use crate::db::io::RingIo;
use crate::shared::pending::Pending;
use crate::shared::request::{Error, Request, ResultSet};
use crate::shared::statement::Statement;
use crate::shared::transaction::TransactionId;

type Msg = Arc<Message<Pending>>;

/// Steps a connection takes in a row before the others get their turn.
const STEPS_PER_TURN: usize = 64;

/// Writes one batch holds at most before it commits.
const MAX_BATCH: usize = 256;

static BEGIN_BATCH: Statement = Statement::new("BEGIN IMMEDIATE");
static SAVEPOINT: Statement = Statement::new("SAVEPOINT batch");
static RELEASE: Statement = Statement::new("RELEASE batch");
static ROLLBACK_TO: Statement = Statement::new("ROLLBACK TO batch");
static ROLLBACK: Statement = Statement::new("ROLLBACK");

#[derive(Debug, Clone)]
pub struct Config {
    /// Reader connections, on top of the single writer.
    pub readers: u16,
    /// A result past this many rows fails with `TooManyRows`.
    pub max_rows: usize,
    /// An interactive transaction with no request for this long is rolled back,
    /// freeing its connection; its next request fails with `TransactionEnded`.
    /// Callers are threads of this process, so a transaction idle this long is
    /// a bug (a forgotten token, a stuck task), not a slow user.
    pub transaction_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            readers: 2,
            max_rows: 10_000,
            transaction_timeout: Duration::from_secs(5),
        }
    }
}

pub struct Server {
    /// Writes run one at a time: turso takes one writer, and a second one
    /// would fail as busy.
    writer: Slot,
    readers: Vec<Slot>,
    writes: VecDeque<Msg>,
    reads: VecDeque<Msg>,
    transaction_timeout: Duration,
}

impl Server {
    pub fn new(db: &Arc<Database>, config: &Config) -> Result<Self, LimboError> {
        let slot = |index: u16, query_only: bool| {
            let conn = db.connect()?;
            conn.set_query_only(query_only);
            let conn = Conn::new(conn, config.max_rows);
            Ok::<_, LimboError>(Slot::new(index, conn, !query_only))
        };
        Ok(Server {
            writer: slot(0, false)?,
            readers: (1..=config.readers)
                .map(|index| slot(index, true))
                .collect::<Result<_, _>>()?,
            writes: VecDeque::new(),
            reads: VecDeque::new(),
            transaction_timeout: config.transaction_timeout,
        })
    }

    /// Serves requests forever. Run it as the root future of the DB thread.
    ///
    /// `io` must be the backend the database was opened with. A request
    /// waiting on disk holds up only its own connection: the thread sleeps
    /// only once nothing can run, until a request arrives, the disk answers,
    /// or a deadline passes.
    pub async fn serve(mut self, io: Arc<RingIo>) {
        let mut disk = DiskWatch::new(io.ring_fd());
        loop {
            // Let the runtime process its completions, so requests sent since
            // the last turn reach the inbox, then take them all.
            yield_now().await;
            while let Some(msg) = try_recv().await {
                self.accept(msg);
            }
            self.expire_transactions(Instant::now());
            self.dispatch();
            self.poll_io(&io);
            let now = Instant::now();
            for slot in self.slots_mut() {
                slot.take_turn(now);
            }
            self.wait(&io, &mut disk).await;
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

    /// Rolls back transactions idle past the timeout, freeing their connection.
    fn expire_transactions(&mut self, now: Instant) {
        let timeout = self.transaction_timeout;
        for slot in self.slots_mut() {
            slot.expire(now, timeout);
        }
    }

    /// Submits the operations the last turn queued and completes those the
    /// disk finished: turso wakes each request whose operation it was.
    fn poll_io(&mut self, io: &RingIo) {
        if let Err(err) = io.poll() {
            for slot in self.slots_mut() {
                slot.io_failed(&err);
            }
        }
    }

    /// When no request can step right now, sleeps until what unblocks one: a
    /// new request, the disk, or a deadline.
    async fn wait(&mut self, io: &RingIo, disk: &mut DiskWatch) {
        // Connections freed this turn take their next request first, and the
        // kernel gets what this turn queued before anything sleeps on it.
        self.dispatch();
        self.poll_io(io);
        let now = Instant::now();
        if self.slots_mut().any(|slot| slot.is_runnable(now)) {
            return;
        }
        let timeout = self.transaction_timeout;
        let deadline = self
            .slots_mut()
            .filter_map(|slot| slot.deadline(timeout))
            .min();
        let on_disk = self.slots_mut().any(|slot| slot.waits_on_disk());
        if let Some(msg) = recv_until(deadline, disk.readable(on_disk)).await {
            self.accept(msg);
        }
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

/// Writes sharing one transaction on the writer.
enum Batch {
    /// Writes that succeeded in it; more can join.
    Open(Vec<Msg>),
    /// Its `COMMIT` is running; these writes are answered once it ends.
    Committing(Vec<Msg>),
}

/// Set by turso once the operation a request waits on has finished.
struct IoDone(AtomicBool);

impl Wake for IoDone {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

/// A connection and the request it is running, if any.
struct Slot {
    /// 0 for the writer, then the readers; the connection field of the
    /// transaction ids minted here.
    index: u16,
    conn: Conn,
    running: Option<Msg>,
    wait: Wait,
    /// What turso wakes when the running request's operation has finished.
    io_done: Arc<IoDone>,
    waker: Waker,
    /// The interactive transaction this connection is pinned to, if any.
    transaction: Option<TransactionId>,
    /// Requests of that transaction waiting for the connection.
    transaction_queue: VecDeque<Msg>,
    /// Counter field of the next transaction id minted here.
    next_transaction: u32,
    /// Since when the transaction has had no request running or queued.
    idle_since: Option<Instant>,
    /// Whether writes queued together share a transaction: the writer only.
    batches: bool,
    /// The writes of the current batch, if one is open or committing.
    batch: Option<Batch>,
}

impl Slot {
    fn new(index: u16, conn: Conn, batches: bool) -> Self {
        let io_done = Arc::new(IoDone(AtomicBool::new(false)));
        Slot {
            index,
            conn,
            running: None,
            wait: Wait::Ready,
            waker: Waker::from(Arc::clone(&io_done)),
            io_done,
            transaction: None,
            transaction_queue: VecDeque::new(),
            next_transaction: 0,
            idle_since: None,
            batches,
            batch: None,
        }
    }

    /// Starts queued requests until one is running. A connection pinned to a
    /// transaction only takes that transaction's requests.
    fn fill(&mut self, queue: &mut VecDeque<Msg>) {
        while self.running.is_none() {
            if matches!(&self.batch, Some(Batch::Open(held)) if held.len() >= MAX_BATCH) {
                self.start_commit();
                continue;
            }
            let next = match self.transaction {
                Some(_) => self.transaction_queue.pop_front(),
                None => queue.pop_front(),
            };
            let Some(msg) = next else {
                // Nothing more to add right now: make the batch durable. Writes
                // arriving meanwhile queue up and form the next one.
                self.start_commit();
                return;
            };
            if msg.payload.is_cancelled() {
                reply(msg);
                continue;
            }
            self.idle_since = None;
            if self.batches && self.transaction.is_none() {
                // SAFETY: rule 2 — ours until we reply.
                let statement = unsafe { msg.payload.request() }.statement();
                if statement.controls_transaction() {
                    // It would end or nest inside the batch: commit the batch
                    // first, then run it on its own.
                    if matches!(self.batch, Some(Batch::Open(_))) {
                        queue.push_front(msg);
                        self.start_commit();
                        continue;
                    }
                } else if let Err(err) = self.enter_batch() {
                    fail(msg, Error::Turso(err));
                    continue;
                }
            }
            self.start(msg);
        }
    }

    /// Makes `msg` the running request, or finishes it if it could not start.
    fn start(&mut self, msg: Msg) {
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

    fn is_runnable(&self, now: Instant) -> bool {
        self.running.is_some()
            && match self.wait {
                Wait::Ready => true,
                Wait::Io => self.io_done.0.load(Ordering::Acquire),
                Wait::Until(deadline) => now >= deadline,
            }
    }

    fn waits_on_disk(&self) -> bool {
        self.running.is_some() && self.wait == Wait::Io && !self.io_done.0.load(Ordering::Acquire)
    }

    /// When this connection needs attention without a new request: a busy
    /// database to retry, or an idle transaction to roll back.
    fn deadline(&self, transaction_timeout: Duration) -> Option<Instant> {
        match (&self.running, self.wait) {
            (Some(_), Wait::Until(deadline)) => Some(deadline),
            (Some(_), _) => None,
            (None, _) => self.idle_since.map(|since| since + transaction_timeout),
        }
    }

    /// Queues a `ROLLBACK` for a transaction idle past `timeout`. It runs like
    /// any request of the transaction, so ending it unpins the connection and
    /// refuses whatever the caller sends meanwhile.
    fn expire(&mut self, now: Instant, timeout: Duration) {
        let Some(since) = self.idle_since else {
            return;
        };
        if !self.transaction_queue.is_empty() || now < since + timeout {
            return;
        }
        self.idle_since = None;
        // Nobody awaits this one: its header reserves no slot, so `reply` wakes
        // no one.
        let rollback = Message::new(Pending::new(Request::rollback()));
        self.transaction_queue.push_back(rollback);
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
            self.io_done.0.store(false, Ordering::Release);
            match self
                .conn
                .step(request, msg.payload.cancel_flag(), &self.waker)
            {
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

    /// Opens the batch if none is, then a savepoint for the next write in it.
    fn enter_batch(&mut self) -> Result<(), LimboError> {
        if self.batch.is_none() {
            self.conn.run_internal(&BEGIN_BATCH)?;
            self.batch = Some(Batch::Open(Vec::new()));
        }
        self.conn.run_internal(&SAVEPOINT)
    }

    /// Settles a write that ran in the batch: a failed one is undone and
    /// answered at once; a successful one waits for the commit.
    fn leave_batch(&mut self, msg: Msg) {
        // SAFETY: rule 2 — not signalled yet, so still ours.
        let failed = matches!(
            unsafe { msg.payload.request() }.result(),
            ResultSet::Error(_)
        );
        let undone = if failed {
            self.conn.run_internal(&ROLLBACK_TO)
        } else {
            Ok(())
        };
        let released = undone.and_then(|()| self.conn.run_internal(&RELEASE));
        let Some(Batch::Open(held)) = &mut self.batch else {
            unreachable!("a write ran in an open batch");
        };
        match (released, failed) {
            (Ok(()), true) => reply(msg),
            (Ok(()), false) => held.push(msg),
            // The batch's state is unknown: give it up, this write with it.
            (Err(err), _) => {
                held.push(msg);
                self.abort_batch(Error::Turso(err));
            }
        }
    }

    /// Starts the batch's `COMMIT` as the running request, like any other: the
    /// thread keeps serving the readers while its fsync is in flight.
    fn start_commit(&mut self) {
        let Some(Batch::Open(held)) = self.batch.take_if(|b| matches!(b, Batch::Open(_))) else {
            return;
        };
        self.batch = Some(Batch::Committing(held));
        // Our own: its header reserves no slot, so `reply` wakes no one.
        self.start(Message::new(Pending::new(Request::commit())));
    }

    /// The batch's `COMMIT` ended: answer every write in it, or fail them all
    /// if it failed, since none of them is durable then.
    fn end_commit(&mut self, commit: Msg, held: Vec<Msg>) {
        // SAFETY: our own message, shared with no one.
        match unsafe { commit.payload.request() }.result() {
            ResultSet::Error(err) => {
                self.batch = Some(Batch::Committing(held));
                self.abort_batch(err.clone());
            }
            _ => held.into_iter().for_each(reply),
        }
    }

    /// Rolls the batch back and fails every write in it with `err`.
    fn abort_batch(&mut self, err: Error) {
        let _ = self.conn.run_internal(&ROLLBACK);
        let held = match self.batch.take() {
            Some(Batch::Open(held) | Batch::Committing(held)) => held,
            None => Vec::new(),
        };
        for msg in held {
            fail(msg, err.clone());
        }
    }

    /// Replies, after pinning the connection if the request opened a
    /// transaction, or unpinning it if the request ended one.
    fn finish(&mut self, msg: Msg) {
        // Our own `BEGIN` holds the connection in a transaction: not the
        // caller's to pin.
        match self.batch.take() {
            Some(Batch::Committing(held)) => return self.end_commit(msg, held),
            Some(open) => {
                self.batch = Some(open);
                return self.leave_batch(msg);
            }
            None => {}
        }
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
        self.idle_since = self.transaction.map(|_| Instant::now());
        reply(msg);
    }

    /// The IO backend failed: a request waiting on it ends with `err`.
    fn io_failed(&mut self, err: &LimboError) {
        if !self.waits_on_disk() {
            return;
        }
        self.wait = Wait::Ready;
        let msg = self.running.take().expect("a request waits on IO");
        // SAFETY: rule 2 — ours until we reply.
        self.conn
            .abort(unsafe { msg.payload.request_mut() }, err.clone());
        self.finish(msg);
    }
}

async fn recv() -> Msg {
    // SAFETY: the DB thread only ever receives `Message<Pending>` — the
    // requests `Client`s send it.
    unsafe { recv_msg_from_peer::<Pending>() }.await
}

/// The next request, or `None` once `disk` fires or `deadline` passes.
async fn recv_until(deadline: Option<Instant>, disk: impl Future<Output = ()>) -> Option<Msg> {
    let deadline = async {
        match deadline {
            Some(deadline) => {
                Sleep::during(deadline.saturating_duration_since(Instant::now())).await
            }
            None => future::pending().await,
        }
    };
    match select(recv(), select(disk, deadline)).await {
        SelectResult::First(msg) => Some(msg),
        SelectResult::Second(_) => None,
    }
}

/// Watches the turso ring from slopio's: its fd turns readable once the disk
/// has finished something. The watch stays armed across the sleeps it did not
/// end, and is re-armed only once it fired.
struct DiskWatch {
    fd: RawFd,
    armed: Option<OneshotToken>,
}

impl DiskWatch {
    fn new(fd: RawFd) -> Self {
        DiskWatch { fd, armed: None }
    }

    /// Fires once the ring has completions; never, if `watch` is false.
    async fn readable(&mut self, watch: bool) {
        if !watch {
            return future::pending().await;
        }
        let fd = self.fd;
        let token = *self.armed.get_or_insert_with(|| {
            let poll = opcode::PollAdd::new(Fd(fd), libc::POLLIN as u32).build();
            IO_DRIVER.with(|d| d.push_op(poll))
        });
        poll_fn(|cx| IO_DRIVER.with(|d| d.poll_op(token, cx.waker()))).await;
        self.armed = None;
    }
}

impl Drop for DiskWatch {
    fn drop(&mut self) {
        if let Some(token) = self.armed.take() {
            IO_DRIVER.try_with(|d| {
                if let Some(d) = d {
                    d.cancel_single_op(token, None);
                }
            });
        }
    }
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
