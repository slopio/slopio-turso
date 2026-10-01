//! Experiment: the engine fed over plain mutex queues, no io_uring messaging.
//!
//! One DB thread runs the `Engine`; each client thread keeps `--tasks`
//! requests in flight. Every client shares two queues with the DB thread:
//! requests one way, answered requests the other. Requests are moved, not
//! shared: a `Box` goes out and comes back.
//!
//! A side with nothing to do waits `--spin-us` watching its queues' `ready`
//! flags, then parks.
//! The other side unparks it after pushing; `unpark` only costs a syscall when
//! the thread really is parked.
//!
//! With `--waker 1`, the DB thread does not unpark clients itself: it flags
//! them, and a waker thread that never sleeps — as an io_uring SQPOLL thread
//! does under load — unparks them, paying the wakeups in its place.
//!
//! ```text
//! cargo run --release --example queues -- [--seconds 5] [--clients 9] [--tasks 4] [--reads 100] [--spin-us 0] [--waker 0]
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use slopio_turso::{Body, Config, Engine, Handle, Next, Request, RingIo, Statement};
use turso_core::{Database, IO, OpenOptions, SqliteDialect, Value, ValueRef};

static GET_USER: Statement = Statement::new("SELECT id, name FROM users WHERE id = ?1");
static INSERT_USER: Statement = Statement::new("INSERT INTO users (name) VALUES (?1)");
const PATH: &str = "/tmp/slopio-turso-queues.db";
const ROWS: u64 = 100_000;

#[derive(Clone, Copy)]
struct Args {
    seconds: u64,
    clients: usize,
    tasks: usize,
    reads: u64,
    spin: Duration,
    waker: bool,
}

/// A request, which client sent it, and when.
struct Job {
    client: usize,
    sent: Instant,
    request: Request,
}

impl Handle for Box<Job> {
    fn request(&self) -> &Request {
        &self.request
    }
}

/// A queue one side fills and the other takes whole. `ready` mirrors "not
/// empty": written under the lock, read without it, so a side can look at the
/// queue, or skip an empty one, without taking the lock.
///
/// A stale `false` loses nothing: the side that fills the queue unparks the
/// other afterwards, and an `unpark` before a `park` makes it return at once.
struct Queue<T> {
    items: Mutex<Vec<T>>,
    ready: AtomicBool,
}

impl<T> Queue<T> {
    fn new() -> Self {
        Queue {
            items: Mutex::new(Vec::new()),
            ready: AtomicBool::new(false),
        }
    }

    /// Moves all of `items` in.
    fn push(&self, items: &mut Vec<T>) {
        if items.is_empty() {
            return;
        }
        let mut queued = self.items.lock().unwrap();
        queued.append(items);
        self.ready.store(true, Ordering::Release);
    }

    /// Swaps everything queued into `out`, which must be empty; takes the lock
    /// only if something is there.
    fn take(&self, out: &mut Vec<T>) {
        if !self.is_ready() {
            return;
        }
        let mut items = self.items.lock().unwrap();
        std::mem::swap(&mut *items, out);
        self.ready.store(false, Ordering::Relaxed);
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
}

/// What one client thread shares with the DB thread.
struct Link {
    requests: Queue<Box<Job>>,
    replies: Queue<Box<Job>>,
    /// The client thread, to unpark once its replies are queued.
    client: OnceLock<Thread>,
}

struct Shared {
    links: Vec<Link>,
    db: OnceLock<Thread>,
    stop: AtomicBool,
    /// With a waker thread: which clients it must unpark. Set by the DB thread
    /// after queuing their replies.
    to_wake: Option<Vec<AtomicBool>>,
}

/// Unparks the clients the DB thread flagged, until told to stop. Never
/// sleeps: spinning is what keeps the wakeups off the DB thread.
fn waker_thread(shared: &Shared) {
    let to_wake = shared.to_wake.as_ref().unwrap();
    while !shared.stop.load(Ordering::Relaxed) {
        for (link, flag) in shared.links.iter().zip(to_wake) {
            if flag.load(Ordering::Relaxed) && flag.swap(false, Ordering::Acquire) {
                unpark(&link.client);
            }
        }
        std::hint::spin_loop();
    }
}

fn unpark(thread: &OnceLock<Thread>) {
    if let Some(thread) = thread.get() {
        thread.unpark();
    }
}

/// Watches `has_work` for `spin`, then parks until unparked or `timeout`.
fn idle(spin: Duration, timeout: Option<Duration>, has_work: impl Fn() -> bool) {
    let until = Instant::now() + spin;
    while Instant::now() < until {
        if has_work() {
            return;
        }
        std::hint::spin_loop();
    }
    match timeout {
        Some(timeout) => thread::park_timeout(timeout),
        None => thread::park(),
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn open(io: &Arc<RingIo>) -> Arc<Database> {
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io.clone(), PATH, options).unwrap();
    let setup = db.connect().unwrap();
    setup
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    setup
        .execute(format!(
            "INSERT INTO users (id, name) WITH RECURSIVE c(x) AS \
             (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {ROWS}) SELECT x, 'user' || x FROM c"
        ))
        .unwrap();
    db
}

fn db_thread(shared: &Shared, db: &Arc<Database>, io: &RingIo, spin: Duration) {
    let mut engine = Engine::new(db, &Config::default()).unwrap();
    let mut incoming = Vec::new();
    let mut done = Vec::new();
    let mut outgoing = Vec::new();
    while !shared.stop.load(Ordering::Relaxed) {
        for link in &shared.links {
            link.requests.take(&mut incoming);
            for job in incoming.drain(..) {
                engine.submit(job);
            }
        }
        engine.turn(io, Instant::now(), &mut done);
        if !done.is_empty() {
            // Per client: one lock, one unpark for all its replies.
            for (index, link) in shared.links.iter().enumerate() {
                outgoing.extend(done.extract_if(.., |job: &mut Box<Job>| job.client == index));
                if !outgoing.is_empty() {
                    link.replies.push(&mut outgoing);
                    match &shared.to_wake {
                        Some(to_wake) => to_wake[index].store(true, Ordering::Release),
                        None => unpark(&link.client),
                    }
                }
            }
        }
        match engine.next_wake(Instant::now()) {
            Next::Now => {}
            // Waiting on the disk: step its ring, which blocks until it answers.
            Next::Sleep { disk: true, .. } => io.step().unwrap(),
            Next::Sleep { deadline, disk: false } => {
                let timeout = deadline.map(|d| d.saturating_duration_since(Instant::now()));
                idle(spin, timeout, || {
                    shared.stop.load(Ordering::Relaxed)
                        || shared.links.iter().any(|l| l.requests.is_ready())
                });
            }
        }
    }
}

#[derive(Default)]
struct Stats {
    reads: Vec<Duration>,
    writes: Vec<Duration>,
}

fn client_thread(shared: &Shared, index: usize, args: Args) -> Stats {
    let link = &shared.links[index];
    link.client.set(thread::current()).unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (index as u64 + 1));
    let mut next = || {
        let n = rng.next();
        let request = if n % 100 < args.reads {
            let id = (n / 100 % ROWS) as i64 + 1;
            Request::new(&GET_USER, &[ValueRef::from_i64(id)])
        } else {
            let name = Value::from_text(format!("new{}", n % 1_000_000));
            Request::new(&INSERT_USER, &[name.as_ref()])
        };
        Box::new(Job {
            client: index,
            sent: Instant::now(),
            request,
        })
    };
    let send = |jobs: &mut Vec<Box<Job>>| {
        link.requests.push(jobs);
        unpark(&shared.db);
    };
    let mut batch: Vec<_> = (0..args.tasks).map(|_| next()).collect();
    send(&mut batch);

    let deadline = Instant::now() + Duration::from_secs(args.seconds);
    let mut stats = Stats::default();
    let mut replies = Vec::new();
    while Instant::now() < deadline {
        link.replies.take(&mut replies);
        if replies.is_empty() {
            idle(args.spin, Some(Duration::from_millis(10)), || link.replies.is_ready());
            continue;
        }
        let now = Instant::now();
        for job in replies.drain(..) {
            let latency = now - job.sent;
            match job.request.body() {
                Body::Rows(_) => stats.reads.push(latency),
                Body::Command { .. } => stats.writes.push(latency),
                other => panic!("request failed: {other:?}"),
            }
            batch.push(next());
        }
        send(&mut batch);
    }
    stats
}

fn report(name: &str, mut latencies: Vec<Duration>, seconds: u64) {
    if latencies.is_empty() {
        return;
    }
    latencies.sort_unstable();
    let n = latencies.len();
    let at = |q: f64| latencies[((n as f64 * q) as usize).min(n - 1)];
    println!(
        "{name:<6} {n:>9} req {:>9.0}/s  p50 {:>9.1?}  p99 {:>9.1?}",
        n as f64 / seconds as f64,
        at(0.50),
        at(0.99),
    );
}

fn main() {
    let mut args = Args {
        seconds: 5,
        clients: 9,
        tasks: 4,
        reads: 100,
        spin: Duration::ZERO,
        waker: false,
    };
    let raw: Vec<String> = std::env::args().skip(1).collect();
    for pair in raw.chunks(2) {
        let [flag, value] = pair else {
            panic!("expected `--flag value` pairs");
        };
        match flag.as_str() {
            "--seconds" => args.seconds = value.parse().unwrap(),
            "--clients" => args.clients = value.parse().unwrap(),
            "--tasks" => args.tasks = value.parse().unwrap(),
            "--reads" => args.reads = value.parse().unwrap(),
            "--spin-us" => args.spin = Duration::from_micros(value.parse().unwrap()),
            "--waker" => args.waker = value != "0",
            _ => panic!("unknown flag {flag}"),
        }
    }
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{PATH}{suffix}"));
    }
    let io = Arc::new(RingIo::new().unwrap());
    let db = open(&io);
    let shared = Shared {
        links: (0..args.clients)
            .map(|_| Link {
                requests: Queue::new(),
                replies: Queue::new(),
                client: OnceLock::new(),
            })
            .collect(),
        db: OnceLock::new(),
        stop: AtomicBool::new(false),
        to_wake: args
            .waker
            .then(|| (0..args.clients).map(|_| AtomicBool::new(false)).collect()),
    };
    let mut all = Stats::default();
    thread::scope(|s| {
        let db_handle = s.spawn(|| {
            shared.db.set(thread::current()).unwrap();
            db_thread(&shared, &db, &io, args.spin);
        });
        if args.waker {
            s.spawn(|| waker_thread(&shared));
        }
        let clients: Vec<_> = (0..args.clients)
            .map(|index| {
                let shared = &shared;
                s.spawn(move || client_thread(shared, index, args))
            })
            .collect();
        for client in clients {
            let stats = client.join().unwrap();
            all.reads.extend(stats.reads);
            all.writes.extend(stats.writes);
        }
        shared.stop.store(true, Ordering::Relaxed);
        db_handle.thread().unpark();
    });
    println!(
        "{} s, {} clients × {} tasks, {}% reads, spin {:?}, waker {}, mutex queues",
        args.seconds, args.clients, args.tasks, args.reads, args.spin, args.waker
    );
    report("reads", all.reads, args.seconds);
    report("writes", all.writes, args.seconds);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{PATH}{suffix}"));
    }
}
