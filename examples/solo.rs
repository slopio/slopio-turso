//! What one thread can do alone: the same point reads as `load`, with no other
//! thread involved, so no wakeups between threads.
//!
//! - `raw`: turso directly, one connection, the statement prepared once and
//!   rebound for each read, the row read in place and dropped. The ceiling.
//! - `parallel`: `raw` on `--threads` threads at once, each with its own
//!   connection to the one database: how turso's reads scale across cores.
//!   With `--batch` above 1, each thread's reads share read transactions of
//!   that many reads.
//! - `mixed`: `parallel` readers (`--threads`, `--batch`) while one writer
//!   thread commits `--rows` inserts every `--commit-every-us` (0: back to
//!   back), with `--synchronous full|normal`. turso's synchronous IO backend,
//!   so each thread reads the pages it misses itself. With `--mvcc 1`, the
//!   database is switched to turso's experimental MVCC journal mode, and the
//!   writer uses `BEGIN CONCURRENT`.
//! - `batch`: `raw`, with `--batch` reads sharing one read transaction
//!   (`BEGIN` … `COMMIT`) instead of one each.
//! - `copy`: `raw`, keeping each result about as the engine does: a `Request`
//!   per read, its arguments decoded to bind, the row encoded into a `RowBuf`.
//! - `engine`: our `Engine`, fed directly: `--tasks` requests in flight, each
//!   answered one replaced by a new one. No messages, no runtime. `--writes`
//!   is the share of single-row inserts, in percent (0 by default).
//! - `server`: our `Server`, with the client tasks on its own thread: the
//!   engine plus the messaging, sending to itself.
//!
//! ```text
//! cargo run --release --example solo -- [raw|parallel|mixed|batch|copy|engine|server] [--seconds 5] [--tasks 36] [--batch 64] [--writes 0] [--threads 4] [--rows 50] [--commit-every-us 1000] [--synchronous normal] [--mvcc 0]
//! ```

use std::cell::Cell;
use std::num::NonZero;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use slopio::runtime::spawn;
use slopio::thread_per_core;
use slopio_turso::{
    Body, Client, Config, Engine, Next, Request, RingIo, RowBuf, Server, Statement,
};
use turso_core::{
    Database, IO, LimboError, Numeric, OpenOptions, SqliteDialect, StepResult, SyncMode, UnixIO,
    Value, ValueRef,
};

static GET_USER: Statement = Statement::new("SELECT id, name FROM users WHERE id = ?1");
static INSERT_USER: Statement = Statement::new("INSERT INTO users (name) VALUES (?1)");
const PATH: &str = "/tmp/slopio-turso-solo.db";
const ROWS: u64 = 100_000;

struct Rng(u64);

impl Rng {
    fn id(&mut self) -> i64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % ROWS) as i64 + 1
    }
}

fn open(io: Arc<dyn IO>) -> Arc<Database> {
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io, PATH, options).unwrap();
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

/// turso alone: bind, step to the row, read it, reset.
fn raw(seconds: u64) -> u64 {
    let io = Arc::new(RingIo::new().unwrap());
    let db = open(io.clone());
    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare(GET_USER.sql()).unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut reads = 0;
    while Instant::now() < deadline {
        // Check the clock every 1000 reads only: it would cost as much as a read.
        for _ in 0..1000 {
            let id = Value::Numeric(Numeric::Integer(rng.id()));
            stmt.bind_at(NonZero::new(1).unwrap(), id).unwrap();
            let mut len = 0;
            loop {
                match stmt.step().unwrap() {
                    StepResult::Row => {
                        let row = stmt.row().unwrap();
                        len = row.get_value(1).to_text().map_or(0, str::len);
                    }
                    StepResult::Done => break,
                    StepResult::IO => io.step().unwrap(),
                    _ => {}
                }
            }
            assert!(len > 0);
            stmt.reset().unwrap();
        }
        reads += 1000;
    }
    reads
}

/// `raw` on `threads` threads, each with its own connection, `batch` reads per
/// read transaction (1: each read its own, as autocommit does).
fn parallel(seconds: u64, threads: usize, batch: usize) -> u64 {
    let io = Arc::new(RingIo::new().unwrap());
    let db = open(io.clone());
    let deadline = Instant::now() + Duration::from_secs(seconds);
    std::thread::scope(|s| {
        let workers: Vec<_> = (0..threads)
            .map(|t| {
                let (db, io) = (&db, &io);
                s.spawn(move || {
                    let conn = db.connect().unwrap();
                    let mut stmt = conn.prepare(GET_USER.sql()).unwrap();
                    let mut begin = conn.prepare("BEGIN").unwrap();
                    let mut commit = conn.prepare("COMMIT").unwrap();
                    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (t as u64 + 1));
                    let mut reads = 0;
                    while Instant::now() < deadline {
                        for i in 0..1000 {
                            if batch > 1 && i % batch == 0 {
                                begin.run_ignore_rows().unwrap();
                                begin.reset().unwrap();
                            }
                            let id = Value::Numeric(Numeric::Integer(rng.id()));
                            stmt.bind_at(NonZero::new(1).unwrap(), id).unwrap();
                            let mut len = 0;
                            loop {
                                match stmt.step().unwrap() {
                                    StepResult::Row => {
                                        let row = stmt.row().unwrap();
                                        len = row.get_value(1).to_text().map_or(0, str::len);
                                    }
                                    StepResult::Done => break,
                                    StepResult::IO => io.step().unwrap(),
                                    _ => {}
                                }
                            }
                            assert!(len > 0);
                            stmt.reset().unwrap();
                            if batch > 1 && (i % batch == batch - 1 || i == 999) {
                                commit.run_ignore_rows().unwrap();
                                commit.reset().unwrap();
                            }
                        }
                        reads += 1000;
                    }
                    reads
                })
            })
            .collect();
        workers.into_iter().map(|w| w.join().unwrap()).sum()
    })
}

/// Runs `stmt` to completion, retrying while the database is busy.
fn run_retrying(stmt: &mut turso_core::Statement) {
    loop {
        let ran = stmt.run_ignore_rows();
        stmt.reset().unwrap();
        match ran {
            Ok(()) => return,
            Err(LimboError::Busy) => std::thread::yield_now(),
            Err(err) => panic!("{err}"),
        }
    }
}

/// `threads` readers, `batch` reads per read transaction, while one writer
/// commits `rows` inserts every `every` (zero: back to back). Prints reads and
/// writes apart; returns the reads.
fn mixed(
    seconds: u64,
    threads: usize,
    batch: usize,
    rows: usize,
    every: Duration,
    sync: SyncMode,
    mvcc: bool,
) -> u64 {
    let io: Arc<dyn IO> = Arc::new(UnixIO::new().unwrap());
    let db = open(io);
    if mvcc {
        db.connect()
            .unwrap()
            .execute("PRAGMA journal_mode = 'mvcc'")
            .unwrap();
        assert!(db.mvcc_enabled());
    }
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let (reads, written) = std::thread::scope(|s| {
        let writer = s.spawn(|| {
            let conn = db.connect().unwrap();
            conn.set_sync_mode(sync);
            let begin_sql = if mvcc { "BEGIN CONCURRENT" } else { "BEGIN IMMEDIATE" };
            let mut begin = conn.prepare(begin_sql).unwrap();
            let mut commit = conn.prepare("COMMIT").unwrap();
            let mut insert = conn.prepare(INSERT_USER.sql()).unwrap();
            let start = Instant::now();
            let mut written = 0u64;
            let mut commits = 0u32;
            while Instant::now() < deadline {
                run_retrying(&mut begin);
                for _ in 0..rows {
                    let name = Value::from_text(format!("new{written}"));
                    insert.bind_at(NonZero::new(1).unwrap(), name).unwrap();
                    run_retrying(&mut insert);
                    written += 1;
                }
                run_retrying(&mut commit);
                commits += 1;
                let next = start + every * commits;
                if let Some(wait) = next.checked_duration_since(Instant::now()) {
                    std::thread::sleep(wait);
                }
            }
            written
        });
        let readers: Vec<_> = (0..threads)
            .map(|t| {
                let db = &db;
                s.spawn(move || {
                    let conn = db.connect().unwrap();
                    let mut stmt = conn.prepare(GET_USER.sql()).unwrap();
                    let mut begin = conn.prepare("BEGIN").unwrap();
                    let mut commit = conn.prepare("COMMIT").unwrap();
                    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (t as u64 + 1));
                    let mut reads = 0;
                    while Instant::now() < deadline {
                        for i in 0..1000 {
                            if batch > 1 && i % batch == 0 {
                                run_retrying(&mut begin);
                            }
                            let id = Value::Numeric(Numeric::Integer(rng.id()));
                            stmt.bind_at(NonZero::new(1).unwrap(), id).unwrap();
                            let mut len = 0;
                            loop {
                                match stmt.step().unwrap() {
                                    StepResult::Row => {
                                        let row = stmt.row().unwrap();
                                        len = row.get_value(1).to_text().map_or(0, str::len);
                                    }
                                    StepResult::Done => break,
                                    // Synchronous IO: nothing is ever pending.
                                    StepResult::IO => {}
                                    StepResult::Busy => std::thread::yield_now(),
                                    _ => {}
                                }
                            }
                            assert!(len > 0);
                            stmt.reset().unwrap();
                            if batch > 1 && (i % batch == batch - 1 || i == 999) {
                                run_retrying(&mut commit);
                            }
                        }
                        reads += 1000;
                    }
                    reads
                })
            })
            .collect();
        let reads: u64 = readers.into_iter().map(|r| r.join().unwrap()).sum();
        (reads, writer.join().unwrap())
    });
    println!(
        "  reads {:.0}/s, writes {:.0}/s",
        reads as f64 / seconds as f64,
        written as f64 / seconds as f64
    );
    reads
}

/// `raw`, with `batch` reads per read transaction.
fn batch(seconds: u64, batch: usize) -> u64 {
    let io = Arc::new(RingIo::new().unwrap());
    let db = open(io.clone());
    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare(GET_USER.sql()).unwrap();
    let mut begin = conn.prepare("BEGIN").unwrap();
    let mut commit = conn.prepare("COMMIT").unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut reads = 0;
    while Instant::now() < deadline {
        for _ in 0..1000 / batch.max(1) {
            begin.run_ignore_rows().unwrap();
            begin.reset().unwrap();
            for _ in 0..batch {
                let id = Value::Numeric(Numeric::Integer(rng.id()));
                stmt.bind_at(NonZero::new(1).unwrap(), id).unwrap();
                let mut len = 0;
                loop {
                    match stmt.step().unwrap() {
                        StepResult::Row => {
                            let row = stmt.row().unwrap();
                            len = row.get_value(1).to_text().map_or(0, str::len);
                        }
                        StepResult::Done => break,
                        StepResult::IO => io.step().unwrap(),
                        _ => {}
                    }
                }
                assert!(len > 0);
                stmt.reset().unwrap();
            }
            commit.run_ignore_rows().unwrap();
            commit.reset().unwrap();
            reads += batch as u64;
        }
    }
    reads
}

/// `raw`, keeping each result in a `Request` as the engine does.
fn copy(seconds: u64) -> u64 {
    let io = Arc::new(RingIo::new().unwrap());
    let db = open(io.clone());
    let conn = db.connect().unwrap();
    let mut stmt = conn.prepare(GET_USER.sql()).unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut reads = 0;
    while Instant::now() < deadline {
        for _ in 0..1000 {
            let params = [ValueRef::from_i64(rng.id())];
            let request = Request::new(&GET_USER, &params);
            for (index, arg) in (1..).zip(request.body().args()) {
                stmt.bind_at(NonZero::new(index).unwrap(), arg.to_owned().unwrap())
                    .unwrap();
            }
            let mut rows = RowBuf::new();
            loop {
                match stmt.step().unwrap() {
                    StepResult::Row => rows.push_row(stmt.row().unwrap().get_values()),
                    StepResult::Done => break,
                    StepResult::IO => io.step().unwrap(),
                    _ => {}
                }
            }
            assert_eq!(rows.len(), 1);
            stmt.reset().unwrap();
        }
        reads += 1000;
    }
    reads
}

/// Our engine alone, `tasks` requests in flight, `writes` percent of them
/// inserts. Prints reads and writes apart; returns them together.
fn engine(seconds: u64, tasks: usize, writes: u64) -> u64 {
    let io = Arc::new(RingIo::new().unwrap());
    let db = open(io.clone());
    let mut engine = Engine::new(&db, &Config::default()).unwrap();
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let mut next = || {
        let id = rng.id();
        if (id as u64) % 100 < writes {
            let name = Value::from_text(format!("new{id}"));
            Box::new(Request::new(&INSERT_USER, &[name.as_ref()]))
        } else {
            Box::new(Request::new(&GET_USER, &[ValueRef::from_i64(id)]))
        }
    };
    for _ in 0..tasks {
        engine.submit(next());
    }
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let mut done = Vec::new();
    let (mut reads, mut inserts) = (0, 0);
    loop {
        // A turn answers many reads: one clock read per turn is cheap.
        let now = Instant::now();
        if now >= deadline {
            let per_s = |n: u64| n as f64 / seconds as f64;
            println!("  reads {:.0}/s, writes {:.0}/s", per_s(reads), per_s(inserts));
            return reads + inserts;
        }
        engine.turn(&io, now, &mut done);
        for request in done.drain(..) {
            assert!(!matches!(request.body(), Body::Error(_)), "{:?}", request.body());
            match request.body() {
                Body::Rows(_) => reads += 1,
                _ => inserts += 1,
            }
            engine.submit(next());
        }
        // Writes wait on fsync: sleep on the ring rather than spin.
        if let Next::Sleep { disk: true, .. } = engine.next_wake(Instant::now()) {
            io.step().unwrap();
        }
    }
}

/// Our server, with `tasks` client tasks on its own thread.
async fn server(seconds: u64, tasks: usize) -> u64 {
    let io = Arc::new(RingIo::new().unwrap());
    let db = open(io.clone());
    let config = Config::default();
    let serving = spawn(Server::new(&db, &config).unwrap().serve(io));
    let client = Client::new(0);
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let reads = Rc::new(Cell::new(0));
    let tasks: Vec<_> = (0..tasks)
        .map(|task| {
            let reads = Rc::clone(&reads);
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (task as u64 + 1));
            spawn(async move {
                while Instant::now() < deadline {
                    let params = [ValueRef::from_i64(rng.id())];
                    let done = client.query(Request::new(&GET_USER, &params)).await;
                    assert!(!matches!(done.body(), Body::Error(_)));
                    reads.set(reads.get() + 1);
                }
            })
        })
        .collect();
    for task in tasks {
        task.await;
    }
    drop(serving);
    reads.get()
}

fn remove_files() {
    for suffix in ["", "-wal", "-shm", "-log"] {
        let _ = std::fs::remove_file(format!("{PATH}{suffix}"));
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "raw".into());
    let (mut seconds, mut tasks, mut batch_size, mut writes, mut threads) = (5, 36, 64, 0, 4);
    let (mut rows, mut every, mut sync) = (50, Duration::from_millis(1), SyncMode::Normal);
    let mut mvcc = false;
    while let (Some(flag), Some(value)) = (args.next(), args.next()) {
        match flag.as_str() {
            "--seconds" => seconds = value.parse().unwrap(),
            "--tasks" => tasks = value.parse().unwrap(),
            "--batch" => batch_size = value.parse().unwrap(),
            "--writes" => writes = value.parse().unwrap(),
            "--threads" => threads = value.parse().unwrap(),
            "--rows" => rows = value.parse().unwrap(),
            "--mvcc" => mvcc = value != "0",
            "--commit-every-us" => every = Duration::from_micros(value.parse().unwrap()),
            "--synchronous" => {
                sync = match value.as_str() {
                    "full" => SyncMode::Full,
                    "normal" => SyncMode::Normal,
                    _ => panic!("--synchronous is full or normal"),
                }
            }
            _ => panic!("unknown flag {flag}"),
        }
    }
    remove_files();
    let reads = match mode.as_str() {
        "raw" => raw(seconds),
        "parallel" => parallel(seconds, threads, batch_size),
        "mixed" => mixed(seconds, threads, batch_size, rows, every, sync, mvcc),
        "batch" => batch(seconds, batch_size),
        "copy" => copy(seconds),
        "engine" => engine(seconds, tasks, writes),
        "server" => {
            let reads = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let total = Arc::clone(&reads);
            thread_per_core(move |ctx| {
                let reads = Arc::clone(&total);
                Box::pin(async move {
                    if ctx.thread_id == 0 {
                        let n = server(seconds, tasks).await;
                        reads.store(n, std::sync::atomic::Ordering::Relaxed);
                    }
                })
            });
            reads.load(std::sync::atomic::Ordering::Relaxed)
        }
        _ => panic!("mode is raw, parallel, mixed, batch, copy, engine or server"),
    };
    remove_files();
    println!(
        "{mode}: {reads} reads in {seconds} s, {:.0}/s, {:.2} µs/read",
        reads as f64 / seconds as f64,
        seconds as f64 * 1e6 / reads as f64
    );
}
