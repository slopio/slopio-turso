//! Load test: worker 0 runs the DB thread on a real file with `RingIo`, every
//! other worker is a client thread running a few tasks that send requests in a
//! loop. Prints throughput and latency for reads and writes.
//!
//! ```text
//! cargo run --release --example load -- [--seconds 10] [--tasks 4] [--reads 90] [--rows 100000] [--readers 2] [--work-us 0]
//! ```
//!
//! `--work-us` is CPU work each task does after every reply, standing in for
//! handling it, so client threads are not only ever waiting on the database.
//!
//! `--reads` is the share of reads in percent: point lookups by random id. The
//! rest are single-row inserts, each its own transaction, so each pays a commit.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use slopio::runtime::{recv_from_peer, send_to_peer, spawn};
use slopio::thread_per_core;
use slopio_turso::client::Client;
use slopio_turso::io::RingIo;
use slopio_turso::request::{Request, ResultSet};
use slopio_turso::server::{Config, Server};
use slopio_turso::statement::Statement;
use turso_core::{Database, OpenOptions, SqliteDialect, Value, ValueRef};

const DB_THREAD: usize = 0;

static GET_USER: Statement = Statement::new("SELECT id, name FROM users WHERE id = ?1");
static INSERT_USER: Statement = Statement::new("INSERT INTO users (name) VALUES (?1)");
const PATH: &str = "/tmp/slopio-turso-load.db";

#[derive(Clone, Copy)]
struct Args {
    seconds: u64,
    tasks: usize,
    reads: u64,
    rows: i64,
    readers: u16,
    work_us: u64,
}

impl Args {
    fn parse() -> Self {
        let mut args = Args {
            seconds: 10,
            tasks: 4,
            reads: 90,
            rows: 100_000,
            readers: 2,
            work_us: 0,
        };
        let raw: Vec<String> = std::env::args().skip(1).collect();
        for pair in raw.chunks(2) {
            let [flag, value] = pair else {
                panic!("expected `--flag value` pairs");
            };
            match flag.as_str() {
                "--seconds" => args.seconds = parse(flag, value),
                "--tasks" => args.tasks = parse(flag, value),
                "--reads" => args.reads = parse(flag, value),
                "--rows" => args.rows = parse(flag, value),
                "--readers" => args.readers = parse(flag, value),
                "--work-us" => args.work_us = parse(flag, value),
                _ => panic!("unknown flag {flag}"),
            }
        }
        args
    }
}

fn parse<T: std::str::FromStr>(flag: &str, value: &str) -> T {
    value
        .parse()
        .unwrap_or_else(|_| panic!("bad value for {flag}: {value}"))
}

/// Latencies of one kind of request, plus how many failed.
#[derive(Default)]
struct Samples {
    latencies: Vec<Duration>,
    errors: usize,
}

impl Samples {
    fn merge(&mut self, other: Samples) {
        self.latencies.extend(other.latencies);
        self.errors += other.errors;
    }

    fn report(&mut self, kind: &str, elapsed: Duration) {
        self.latencies.sort_unstable();
        let n = self.latencies.len();
        let at = |q: f64| {
            self.latencies
                .get(((n as f64 * q) as usize).min(n.saturating_sub(1)))
        };
        let fmt = |d: Option<&Duration>| d.map_or("-".into(), |d| format!("{d:.1?}"));
        println!(
            "{kind:6} {n:>9} req  {:>9.0}/s  p50 {:>9}  p99 {:>9}  max {:>9}  errors {}",
            n as f64 / elapsed.as_secs_f64(),
            fmt(at(0.50)),
            fmt(at(0.99)),
            fmt(self.latencies.last()),
            self.errors,
        );
    }
}

#[derive(Default)]
struct Stats {
    reads: Samples,
    writes: Samples,
}

/// A small xorshift, so each task draws its own sequence without a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn remove_files() {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{PATH}{suffix}"));
    }
}

async fn db_thread(args: Args, clients: usize) {
    let io = Arc::new(RingIo::new().unwrap());
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io.clone(), PATH, options).unwrap();

    let setup = db.connect().unwrap();
    setup
        .execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    setup
        .execute(format!(
            "INSERT INTO users (id, name) WITH RECURSIVE c(x) AS \
             (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {}) SELECT x, 'user' || x FROM c",
            args.rows
        ))
        .unwrap();
    drop(setup);

    let config = Config {
        readers: args.readers,
        ..Config::default()
    };
    let serving = spawn(Server::new(&db, &config).unwrap().serve(io));
    for client in 1..=clients {
        send_to_peer(client, 0, 0);
    }
    // One stop message per client, then the runtime can wind down.
    for _ in 0..clients {
        recv_from_peer().await;
    }
    drop(serving);
}

async fn client_thread(args: Args, thread: usize, stats: Arc<Mutex<Stats>>) {
    recv_from_peer().await; // the table is ready
    let client = Client::new(DB_THREAD);
    let deadline = Instant::now() + Duration::from_secs(args.seconds);
    let local = Rc::new(RefCell::new(Stats::default()));
    let tasks: Vec<_> = (0..args.tasks)
        .map(|task| {
            let local = Rc::clone(&local);
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ ((thread * 1000 + task) as u64 + 1));
            spawn(async move {
                while Instant::now() < deadline {
                    let read = rng.next() % 100 < args.reads;
                    let start = Instant::now();
                    let done = if read {
                        let id = (rng.next() % args.rows as u64) as i64 + 1;
                        let params = [ValueRef::from_i64(id)];
                        client.query(Request::prepared(&GET_USER, &params)).await
                    } else {
                        let name = Value::from_text(format!("new{}", rng.next() % 1_000_000));
                        client
                            .query(Request::prepared(&INSERT_USER, &[name.as_ref()]))
                            .await
                    };
                    let elapsed = start.elapsed();
                    let mut local = local.borrow_mut();
                    let samples = if read {
                        &mut local.reads
                    } else {
                        &mut local.writes
                    };
                    samples.latencies.push(elapsed);
                    if matches!(done.result(), ResultSet::Error(_)) {
                        samples.errors += 1;
                    }
                    drop(local);
                    // Handle the reply: busy, as real work would keep the thread.
                    let work = Instant::now() + Duration::from_micros(args.work_us);
                    while Instant::now() < work {
                        std::hint::spin_loop();
                    }
                }
            })
        })
        .collect();
    for task in tasks {
        task.await;
    }
    let local = Rc::try_unwrap(local).ok().unwrap().into_inner();
    let mut stats = stats.lock().unwrap();
    stats.reads.merge(local.reads);
    stats.writes.merge(local.writes);
    drop(stats);
    send_to_peer(DB_THREAD, 0, 0);
}

fn main() {
    let args = Args::parse();
    remove_files();
    let stats = Arc::new(Mutex::new(Stats::default()));
    let shared = Arc::clone(&stats);
    let started = Instant::now();
    thread_per_core(move |ctx| {
        let stats = Arc::clone(&shared);
        Box::pin(async move {
            assert!(ctx.num_threads >= 2, "need at least 2 cores");
            match ctx.thread_id {
                DB_THREAD => db_thread(args, ctx.num_threads - 1).await,
                thread => client_thread(args, thread, stats).await,
            }
        })
    });
    let elapsed = started.elapsed().min(Duration::from_secs(args.seconds));
    remove_files();

    let mut stats = stats.lock().unwrap();
    println!(
        "{} s, {} readers, {} tasks on each client thread, {}% reads, {}µs of work per reply",
        args.seconds, args.readers, args.tasks, args.reads, args.work_us
    );
    stats.reads.report("reads", elapsed);
    stats.writes.report("writes", elapsed);
}
