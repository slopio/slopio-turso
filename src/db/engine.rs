//! The DB logic, with no messaging: requests go in with [`Engine::submit`],
//! come out of [`Engine::turn`] once answered. How they travel between threads,
//! and how the thread sleeps, is the host's business — see
//! [`Server`](crate::Server) for slopio.
//!
//! Requests run on turso connections — one writer and a few readers — taking
//! turns a few steps at a time. A `BEGIN` pins its connection to an interactive
//! transaction until turso says it is over: `BEGIN` (read-only to turso) opens a
//! read transaction on a reader, `BEGIN IMMEDIATE` a write transaction on the
//! writer. Readers are query-only, so a write inside a read transaction fails.
//! A transaction left idle past [`Config::transaction_timeout`] is rolled back.
//!
//! The writer batches: writes queued together run in one transaction, each
//! under its own savepoint so a failing one leaves the others alone, and share
//! a single `COMMIT` — one fsync. That `COMMIT` runs like any request, so
//! readers keep going while its fsync is in flight. A write is only answered
//! once the commit succeeded; a failed one is answered at once.
//!
//! Readers batch too: when several reads are queued, a reader runs them in one
//! read transaction, which turso opens and closes once instead of once per
//! read. A read must see every write answered before it arrived, so a reader
//! whose transaction predates the writer's last commit starts a new one first.

use std::collections::VecDeque;
use std::iter;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Wake, Waker};
use std::time::{Duration, Instant};

use turso_core::{Database, LimboError, SyncMode};

use crate::db::conn::{Conn, Step};
use crate::db::io::RingIo;
use crate::shared::request::{Body, Error, Request};
use crate::shared::statement::Statement;
use crate::shared::transaction::TransactionId;

/// Steps a connection takes in a row before the others get their turn.
const STEPS_PER_TURN: usize = 64;

/// Requests a connection may finish in one turn before the others get theirs.
const REQUESTS_PER_TURN: usize = 64;

/// Writes one batch holds at most before it commits.
const MAX_BATCH: usize = 256;

/// Reads that must be queued behind the one a reader takes for it to open a
/// shared read transaction: below that, `BEGIN` and `COMMIT` cost more than
/// they save.
const MIN_READ_BATCH: usize = 8;

static BEGIN_BATCH: Statement = Statement::new("BEGIN IMMEDIATE");
static SAVEPOINT: Statement = Statement::new("SAVEPOINT batch");
static RELEASE: Statement = Statement::new("RELEASE batch");
static ROLLBACK_TO: Statement = Statement::new("ROLLBACK TO batch");
static ROLLBACK: Statement = Statement::new("ROLLBACK");
static BEGIN_READ: Statement = Statement::new("BEGIN");
static COMMIT: Statement = Statement::new("COMMIT");

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
    /// What a commit waits for before its writes are answered.
    pub synchronous: Synchronous,
    /// How long a batch of writes stays open after its first write, gathering
    /// those that arrive meanwhile, before it commits. Zero commits as soon as
    /// no write is queued. Longer means fewer commits — each of which empties
    /// every reader's page cache — for that much more write latency.
    pub commit_interval: Duration,
}

/// How durable a commit is once its writes are answered. turso's `OFF`, which
/// may corrupt the database on a power loss, is deliberately not offered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Synchronous {
    /// Every commit fsyncs the WAL: an answered write survives a power loss.
    #[default]
    Full,
    /// The WAL is fsynced at checkpoints only: an answered write survives the
    /// process crashing, but a power loss or kernel crash may drop the last
    /// commits — never corrupting the database.
    Normal,
}

impl From<Synchronous> for SyncMode {
    fn from(synchronous: Synchronous) -> Self {
        match synchronous {
            Synchronous::Full => SyncMode::Full,
            Synchronous::Normal => SyncMode::Normal,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            readers: 2,
            max_rows: 10_000,
            transaction_timeout: Duration::from_secs(5),
            synchronous: Synchronous::Full,
            commit_interval: Duration::ZERO,
        }
    }
}

/// How the host carries a request: anything that leads to its [`Request`].
/// The engine fills the request's body in place and hands the handle back once
/// done.
pub trait Handle {
    fn request(&self) -> &Request;
}

/// A request kept on the engine's own thread, never shared.
impl Handle for Box<Request> {
    fn request(&self) -> &Request {
        self
    }
}

/// What the host must wait for before the next [`Engine::turn`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Next {
    /// A connection can step already.
    Now,
    /// Nothing can step before a new request, the disk if `disk` (see
    /// [`RingIo::ring_fd`]), or `deadline`, if any.
    Sleep {
        deadline: Option<Instant>,
        disk: bool,
    },
}

pub struct Engine<H> {
    /// Writes run one at a time: turso takes one writer, and a second one
    /// would fail as busy.
    writer: Slot<H>,
    readers: Vec<Slot<H>>,
    writes: VecDeque<Job<H>>,
    reads: VecDeque<Job<H>>,
    /// Requests answered without reaching a connection.
    refused: Vec<H>,
    transaction_timeout: Duration,
}

impl<H: Handle> Engine<H> {
    pub fn new(db: &Arc<Database>, config: &Config) -> Result<Self, LimboError> {
        let slot = |index: u16, query_only: bool| {
            let conn = db.connect()?;
            conn.set_query_only(query_only);
            conn.set_sync_mode(config.synchronous.into());
            let conn = Conn::new(conn, config.max_rows);
            Ok::<_, LimboError>(Slot::new(index, conn, !query_only))
        };
        let mut writer = slot(0, false)?;
        writer.commit_interval = config.commit_interval;
        Ok(Engine {
            writer,
            readers: (1..=config.readers)
                .map(|index| slot(index, true))
                .collect::<Result<_, _>>()?,
            writes: VecDeque::new(),
            reads: VecDeque::new(),
            refused: Vec::new(),
            transaction_timeout: config.transaction_timeout,
        })
    }

    /// Queues a request for its transaction's connection, or else for the
    /// writer or the readers.
    pub fn submit(&mut self, handle: H) {
        let request = handle.request();
        if let Some(id) = request.transaction() {
            match self.slot_mut(id.connection()) {
                Some(slot) if slot.transaction == Some(id) => {
                    slot.transaction_queue.push_back(Job::Client(handle))
                }
                _ => {
                    // SAFETY: rule 2 — ours until we hand it back.
                    unsafe { handle.request().body_mut() }.set_error(Error::TransactionEnded);
                    self.refused.push(handle);
                }
            }
            return;
        }
        // SQL that fails to prepare counts as a write: the writer reports the error.
        if self
            .writer
            .conn
            .is_readonly(request.statement())
            .unwrap_or(false)
        {
            self.reads.push_back(Job::Client(handle));
        } else {
            self.writes.push_back(Job::Client(handle));
        }
    }

    /// Runs every connection that can step, and appends the requests answered
    /// to `done`. `io` must be the backend the database was opened with.
    pub fn turn(&mut self, io: &RingIo, now: Instant, done: &mut Vec<H>) {
        let timeout = self.transaction_timeout;
        for slot in self.slots_mut() {
            slot.expire(now, timeout);
        }
        self.dispatch(now);
        self.poll_io(io);
        // A connection that finishes a request takes the next queued one in
        // the same turn: a turn costs far more than a point read.
        self.writer.take_turns(now, &mut self.writes, 0);
        let commits = self.writer.commits;
        for reader in &mut self.readers {
            reader.take_turns(now, &mut self.reads, commits);
        }
        // Connections freed this turn take their next request, and the kernel
        // gets what this turn queued before the host sleeps on it.
        self.dispatch(now);
        self.poll_io(io);
        done.append(&mut self.refused);
        for slot in self.slots_mut() {
            done.append(&mut slot.finished);
        }
    }

    /// What to wait for before the next turn.
    pub fn next_wake(&mut self, now: Instant) -> Next {
        if self.slots_mut().any(|slot| slot.is_runnable(now)) {
            return Next::Now;
        }
        let timeout = self.transaction_timeout;
        Next::Sleep {
            deadline: self
                .slots_mut()
                .filter_map(|slot| slot.deadline(timeout))
                .min(),
            disk: self.slots_mut().any(|slot| slot.waits_on_disk()),
        }
    }

    /// Hands queued requests to idle connections.
    fn dispatch(&mut self, now: Instant) {
        self.writer.fill(&mut self.writes, 0, now);
        let commits = self.writer.commits;
        for reader in &mut self.readers {
            reader.fill(&mut self.reads, commits, now);
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

    fn slot_mut(&mut self, index: u16) -> Option<&mut Slot<H>> {
        match index {
            0 => Some(&mut self.writer),
            i => self.readers.get_mut(usize::from(i) - 1),
        }
    }

    fn slots_mut(&mut self) -> impl Iterator<Item = &mut Slot<H>> {
        iter::once(&mut self.writer).chain(&mut self.readers)
    }
}

/// A request the engine runs: the host's, handed back once answered, or the
/// engine's own (a batch's `COMMIT`, an expired transaction's `ROLLBACK`),
/// dropped once done.
enum Job<H> {
    Client(H),
    Own(Request),
}

impl<H: Handle> Job<H> {
    fn request(&self) -> &Request {
        match self {
            Job::Client(handle) => handle.request(),
            Job::Own(request) => request,
        }
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
enum Batch<H> {
    /// Writes that succeeded in it; more can join.
    Open(Vec<Job<H>>),
    /// Its `COMMIT` is running; these writes are answered once it ends.
    Committing(Vec<Job<H>>),
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
struct Slot<H> {
    /// 0 for the writer, then the readers; the connection field of the
    /// transaction ids minted here.
    index: u16,
    conn: Conn,
    running: Option<Job<H>>,
    wait: Wait,
    /// What turso wakes when the running request's operation has finished.
    io_done: Arc<IoDone>,
    waker: Waker,
    /// The interactive transaction this connection is pinned to, if any.
    transaction: Option<TransactionId>,
    /// Requests of that transaction waiting for the connection.
    transaction_queue: VecDeque<Job<H>>,
    /// Counter field of the next transaction id minted here.
    next_transaction: u32,
    /// Since when the transaction has had no request running or queued.
    idle_since: Option<Instant>,
    /// Whether writes queued together share a transaction: the writer only.
    batches: bool,
    /// The writes of the current batch, if one is open or committing.
    batch: Option<Batch<H>>,
    /// When the open batch took its first write.
    batch_opened: Option<Instant>,
    /// The writer's [`Config::commit_interval`].
    commit_interval: Duration,
    /// The writer's commits so far, counted before their writes are answered.
    commits: u64,
    /// A reader's shared read transaction, if open: the writer's commit count
    /// when it opened.
    read_batch: Option<u64>,
    /// Requests answered, until the turn hands them to the host.
    finished: Vec<H>,
}

impl<H: Handle> Slot<H> {
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
            batch_opened: None,
            commit_interval: Duration::ZERO,
            commits: 0,
            read_batch: None,
            finished: Vec::new(),
        }
    }

    /// Starts queued requests until one is running. A connection pinned to a
    /// transaction only takes that transaction's requests. `commits` is the
    /// writer's commit count, for a reader to tell whether its shared read
    /// transaction is still fresh.
    fn fill(&mut self, queue: &mut VecDeque<Job<H>>, commits: u64, now: Instant) {
        while self.running.is_none() {
            if matches!(&self.batch, Some(Batch::Open(held)) if held.len() >= MAX_BATCH) {
                self.start_commit();
                continue;
            }
            let next = match self.transaction {
                Some(_) => self.transaction_queue.pop_front(),
                None => queue.pop_front(),
            };
            let Some(job) = next else {
                // Nothing more to add right now: make the batch durable once it
                // has been open long enough. Writes arriving meanwhile join it,
                // or queue up for the next one. A reader lets go of its
                // snapshot.
                if self.commit_due_at().is_none_or(|due| now >= due) {
                    self.start_commit();
                }
                self.end_read_batch();
                return;
            };
            if job.request().meta().is_cancelled() {
                self.answer(job);
                continue;
            }
            self.idle_since = None;
            if self.batches && self.transaction.is_none() {
                let statement = job.request().statement();
                if statement.controls_transaction() {
                    // It would end or nest inside the batch: commit the batch
                    // first, then run it on its own.
                    if matches!(self.batch, Some(Batch::Open(_))) {
                        queue.push_front(job);
                        self.start_commit();
                        continue;
                    }
                } else if let Err(err) = self.enter_batch(now) {
                    self.fail(job, Error::Turso(err));
                    continue;
                }
            }
            if !self.batches && self.transaction.is_none() {
                self.read_batch_for(&job, queue.len(), commits);
            }
            self.start(job);
        }
    }

    /// Makes `job` the running request, or finishes it if it could not start.
    fn start(&mut self, job: Job<H>) {
        // SAFETY: rule 2 — ours until we hand it back.
        let body = unsafe { job.request().body_mut() };
        match self.conn.start(job.request().statement(), body) {
            Step::Done => self.finish(job),
            _ => {
                self.running = Some(job);
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
    /// database to retry, an open batch to commit, or an idle transaction to
    /// roll back.
    fn deadline(&self, transaction_timeout: Duration) -> Option<Instant> {
        match (&self.running, self.wait) {
            (Some(_), Wait::Until(deadline)) => Some(deadline),
            (Some(_), _) => None,
            (None, _) => {
                let idle = self.idle_since.map(|since| since + transaction_timeout);
                idle.into_iter().chain(self.commit_due_at()).min()
            }
        }
    }

    /// When the open batch is due to commit, if one is open.
    fn commit_due_at(&self) -> Option<Instant> {
        match (&self.batch, self.batch_opened) {
            (Some(Batch::Open(_)), Some(opened)) => Some(opened + self.commit_interval),
            _ => None,
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
        let rollback = Job::Own(Request::rollback());
        self.transaction_queue.push_back(rollback);
    }

    /// Runs requests from `queue` one after the other, until one waits, the
    /// queue runs dry, or the budget is spent.
    fn take_turns(&mut self, now: Instant, queue: &mut VecDeque<Job<H>>, commits: u64) {
        for _ in 0..REQUESTS_PER_TURN {
            self.take_turn(now);
            if self.running.is_some() {
                return;
            }
            self.fill(queue, commits, now);
            if self.running.is_none() {
                return;
            }
        }
    }

    /// Steps the running request until it waits, finishes, or its turn ends.
    fn take_turn(&mut self, now: Instant) {
        for _ in 0..STEPS_PER_TURN {
            if !self.is_runnable(now) {
                return;
            }
            let job = self.running.as_ref().unwrap();
            // SAFETY: rule 2 — ours until we hand it back.
            let body = unsafe { job.request().body_mut() };
            self.io_done.0.store(false, Ordering::Release);
            match self
                .conn
                .step(body, job.request().meta().cancel_flag(), &self.waker)
            {
                Step::Ready => self.wait = Wait::Ready,
                Step::Io => self.wait = Wait::Io,
                Step::Sleep(duration) => self.wait = Wait::Until(now + duration),
                Step::Done => {
                    let job = self.running.take().unwrap();
                    self.finish(job);
                }
            }
        }
    }

    /// Opens the batch if none is, then a savepoint for the next write in it.
    fn enter_batch(&mut self, now: Instant) -> Result<(), LimboError> {
        if self.batch.is_none() {
            self.conn.run_internal(&BEGIN_BATCH)?;
            self.batch = Some(Batch::Open(Vec::new()));
            self.batch_opened = Some(now);
        }
        self.conn.run_internal(&SAVEPOINT)
    }

    /// Readers only: gets the shared read transaction right for `job`, with
    /// `queued` reads behind it. A statement that controls transactions runs
    /// outside it; a transaction older than the writer's last commit is
    /// replaced; one opens when enough reads wait to pay for it.
    fn read_batch_for(&mut self, job: &Job<H>, queued: usize, commits: u64) {
        if job.request().statement().controls_transaction() {
            self.end_read_batch();
            return;
        }
        if self.read_batch.is_some_and(|since| since != commits) {
            self.end_read_batch();
        }
        if self.read_batch.is_none()
            && queued >= MIN_READ_BATCH
            && self.conn.run_internal(&BEGIN_READ).is_ok()
        {
            self.read_batch = Some(commits);
        }
    }

    /// Closes the shared read transaction, if open. It only read, so there is
    /// nothing to lose: if `COMMIT` fails, `ROLLBACK` ends it all the same.
    fn end_read_batch(&mut self) {
        if self.read_batch.take().is_some() && self.conn.run_internal(&COMMIT).is_err() {
            let _ = self.conn.run_internal(&ROLLBACK);
        }
    }

    /// Settles a write that ran in the batch: a failed one is undone and
    /// answered at once; a successful one waits for the commit.
    fn leave_batch(&mut self, job: Job<H>) {
        // SAFETY: rule 2 — ours until we hand it back.
        let failed = matches!(unsafe { job.request().body_mut() }, Body::Error(_));
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
            (Ok(()), true) => self.answer(job),
            (Ok(()), false) => held.push(job),
            // The batch's state is unknown: give it up, this write with it.
            (Err(err), _) => {
                held.push(job);
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
        self.batch_opened = None;
        self.batch = Some(Batch::Committing(held));
        self.start(Job::Own(Request::commit()));
    }

    /// The batch's `COMMIT` ended: answer every write in it, or fail them all
    /// if it failed, since none of them is durable then.
    fn end_commit(&mut self, commit: Job<H>, held: Vec<Job<H>>) {
        // SAFETY: our own request, shared with no one.
        match unsafe { commit.request().body_mut() } {
            Body::Error(err) => {
                self.batch = Some(Batch::Committing(held));
                self.abort_batch(err.clone());
            }
            _ => {
                // Counted before the writes are answered: a read that arrives
                // after their answer finds a reader's older snapshot stale.
                self.commits += 1;
                held.into_iter().for_each(|job| self.answer(job));
            }
        }
    }

    /// Rolls the batch back and fails every write in it with `err`.
    fn abort_batch(&mut self, err: Error) {
        let _ = self.conn.run_internal(&ROLLBACK);
        self.batch_opened = None;
        let held = match self.batch.take() {
            Some(Batch::Open(held) | Batch::Committing(held)) => held,
            None => Vec::new(),
        };
        for job in held {
            self.fail(job, err.clone());
        }
    }

    /// Answers, after pinning the connection if the request opened a
    /// transaction, or unpinning it if the request ended one.
    fn finish(&mut self, job: Job<H>) {
        // Our own `BEGIN` holds the connection in a transaction: not the
        // caller's to pin.
        match self.batch.take() {
            Some(Batch::Committing(held)) => return self.end_commit(job, held),
            Some(open) => {
                self.batch = Some(open);
                return self.leave_batch(job);
            }
            None => {}
        }
        if self.read_batch.is_some() {
            // Our own read transaction: not the caller's to pin. A failed read
            // may have ended it.
            if !self.conn.in_transaction() {
                self.read_batch = None;
            }
            return self.answer(job);
        }
        // Outside a batch, the writer committed whatever it wrote once it is
        // out of any transaction.
        if self.batches && !self.conn.in_transaction() {
            self.commits += 1;
        }
        match (self.transaction, self.conn.in_transaction()) {
            (None, true) => {
                let id = TransactionId::compose(self.index, self.next_transaction);
                self.next_transaction = self.next_transaction.wrapping_add(1);
                self.transaction = Some(id);
                // SAFETY: rule 2 — ours until we hand it back.
                unsafe { job.request().body_mut() }.set_transaction(id);
            }
            (Some(_), false) => {
                self.transaction = None;
                for queued in std::mem::take(&mut self.transaction_queue) {
                    self.fail(queued, Error::TransactionEnded);
                }
            }
            _ => {}
        }
        self.idle_since = self.transaction.map(|_| Instant::now());
        self.answer(job);
    }

    /// The IO backend failed: a request waiting on it ends with `err`.
    fn io_failed(&mut self, err: &LimboError) {
        if !self.waits_on_disk() {
            return;
        }
        self.wait = Wait::Ready;
        let job = self.running.take().expect("a request waits on IO");
        // SAFETY: rule 2 — ours until we hand it back.
        self.conn
            .abort(unsafe { job.request().body_mut() }, err.clone());
        self.end_read_batch();
        self.finish(job);
    }

    /// Answers `job` with `err` without running it.
    fn fail(&mut self, job: Job<H>, err: Error) {
        // SAFETY: rule 2 — ours until we hand it back.
        unsafe { job.request().body_mut() }.set_error(err);
        self.answer(job);
    }

    /// Hands a host's request back at the end of the turn; drops our own.
    fn answer(&mut self, job: Job<H>) {
        if let Job::Client(handle) = job {
            self.finished.push(handle);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::{Numeric, OpenOptions, SqliteDialect, ValueRef};

    static CREATE: Statement = Statement::new("CREATE TABLE t (x INTEGER)");
    static INSERT: Statement = Statement::new("INSERT INTO t VALUES (1)");
    static COUNT: Statement = Statement::new("SELECT count(*) FROM t");

    struct Fixture {
        path: String,
        io: Arc<RingIo>,
        engine: Engine<Box<Request>>,
    }

    impl Fixture {
        fn new(name: &str) -> Self {
            Fixture::with_config(name, &Config::default())
        }

        fn with_config(name: &str, config: &Config) -> Self {
            let path = format!("/tmp/slopio-turso-engine-{name}-{}.db", std::process::id());
            let io = Arc::new(RingIo::new().unwrap());
            let options = OpenOptions::new(Arc::new(SqliteDialect));
            let db = Database::open(io.clone(), &path, options).unwrap();
            let engine = Engine::new(&db, config).unwrap();
            Fixture { path, io, engine }
        }

        fn submit(&mut self, statement: &'static Statement) {
            self.engine.submit(Box::new(Request::new(statement, &[])));
        }

        /// One turn; the requests it answered. Boxed: each keeps its address,
        /// which the tests use to tell requests apart.
        #[allow(clippy::vec_box)]
        fn turn(&mut self) -> Vec<Box<Request>> {
            let mut done = Vec::new();
            self.engine.turn(&self.io, Instant::now(), &mut done);
            done
        }

        /// Runs `statement` alone to its answer.
        fn run(&mut self, statement: &'static Statement) -> Box<Request> {
            self.submit(statement);
            loop {
                if let Some(done) = self.turn().pop() {
                    return done;
                }
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.path));
            }
        }
    }

    fn count(request: &Request) -> i64 {
        match request.body().rows().next().unwrap().get(0).unwrap() {
            ValueRef::Numeric(Numeric::Integer(n)) => n,
            other => panic!("not a count: {other:?}"),
        }
    }

    /// A reader keeps a read transaction open across turns while reads queue
    /// up; a write is answered meanwhile. A read submitted after that answer
    /// must see the write, not the reader's older snapshot.
    #[test]
    fn a_read_sees_every_write_answered_before_it() {
        let mut fx = Fixture::new("fresh");
        fx.run(&CREATE);

        // More reads than the readers finish in one turn: their transaction
        // stays open into the next.
        for _ in 0..4 * REQUESTS_PER_TURN {
            fx.submit(&COUNT);
        }
        fx.turn();
        assert!(fx.engine.readers.iter().any(|r| r.read_batch.is_some()));

        fx.submit(&INSERT);
        let mut written = false;
        while !written {
            for done in fx.turn() {
                match done.statement() {
                    s if std::ptr::eq(s, &INSERT) => written = true,
                    _ => fx.submit(&COUNT), // keep the readers' queue full
                }
            }
        }
        assert!(fx.engine.readers.iter().any(|r| r.read_batch.is_some()));

        // Every read submitted from here on must see the write, whatever the
        // older reads still queued around them see.
        let mut after = std::collections::HashSet::new();
        for _ in 0..REQUESTS_PER_TURN {
            let request = Box::new(Request::new(&COUNT, &[]));
            after.insert(&*request as *const Request);
            fx.engine.submit(request);
        }
        while !after.is_empty() {
            for done in fx.turn() {
                if after.remove(&(&*done as *const Request)) {
                    assert_eq!(count(&done), 1, "a read missed an answered write");
                }
            }
        }
    }

    /// A client's `BEGIN` reaching a reader in the middle of its shared read
    /// transaction still opens a transaction of its own, which ends with the
    /// client's `COMMIT`.
    #[test]
    fn a_client_transaction_does_not_nest_in_a_read_batch() {
        let mut fx = Fixture::new("nest");
        fx.run(&CREATE);
        for _ in 0..4 * REQUESTS_PER_TURN {
            fx.submit(&COUNT);
        }
        fx.turn();
        assert!(fx.engine.readers.iter().any(|r| r.read_batch.is_some()));

        fx.engine.submit(Box::new(Request::begin_read()));
        let id = loop {
            let begun = fx.turn().into_iter().find_map(|done| match done.body() {
                Body::Transaction(id) => Some(*id),
                Body::Error(err) => panic!("BEGIN failed: {err:?}"),
                _ => None,
            });
            if let Some(id) = begun {
                break id;
            }
        };
        fx.engine
            .submit(Box::new(Request::new(&COUNT, &[]).in_transaction(id)));
        fx.engine
            .submit(Box::new(Request::commit().in_transaction(id)));
        let mut ended = false;
        while !ended {
            for done in fx.turn() {
                if done.transaction() == Some(id) {
                    assert!(!matches!(done.body(), Body::Error(_)), "{:?}", done.body());
                    ended |= std::ptr::eq(done.statement(), Request::commit().statement());
                }
            }
        }
        let reader = fx.engine.slot_mut(id.connection()).unwrap();
        assert_eq!(reader.transaction, None, "COMMIT unpinned the reader");
    }

    /// With a commit interval, a write is answered only once its batch has been
    /// open that long, and the engine says when to come back for it.
    #[test]
    fn a_batch_commits_once_its_interval_is_over() {
        let interval = Duration::from_millis(50);
        let config = Config {
            commit_interval: interval,
            ..Config::default()
        };
        let mut fx = Fixture::with_config("interval", &config);
        fx.run(&CREATE);

        let submitted = Instant::now();
        fx.submit(&INSERT);
        let mut answered = None;
        while answered.is_none() {
            if !fx.turn().is_empty() {
                answered = Some(Instant::now());
                break;
            }
            match fx.engine.next_wake(Instant::now()) {
                // The disk: the batch's statements or its `COMMIT`.
                Next::Now | Next::Sleep { disk: true, .. } => {}
                Next::Sleep { deadline, disk: false } => {
                    let due = deadline.expect("an open batch has a commit deadline");
                    assert!(due <= submitted + interval + Duration::from_millis(5));
                    std::thread::sleep(due.saturating_duration_since(Instant::now()));
                }
            }
        }
        assert!(answered.unwrap() >= submitted + interval, "answered before the interval");
        assert_eq!(count(&fx.run(&COUNT)), 1);
    }
}
