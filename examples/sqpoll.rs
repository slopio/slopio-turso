//! The load of `load`, on an explicit topology, with or without SQPOLL on the
//! DB thread's ring.
//!
//! - The DB thread is a dedicated thread on a CPU of its own. With
//!   `--sqpoll-idle-ms`, its ring gets a kernel SQ polling thread on another CPU
//!   of its own, which spins that long on an empty queue before it sleeps: that
//!   thread, not the DB thread, then submits the replies' MSG_RINGs and pays for
//!   waking the clients.
//! - The clients are the per-core workers on the CPUs left. Only the first
//!   `--clients` run, so both setups compare with as many clients.
//!
//! ```text
//! cargo run --release --example sqpoll -- [--sqpoll-idle-ms 10] [--clients 8] [--seconds 10] [--tasks 4] [--reads 90] [--work-us 0] [--synchronous full|normal] [--commit-interval-us 0]
//! ```

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use slopio::runtime::{recv_from_peer, send_to_peer, spawn};
use slopio::{Cpu, Dedicated, SqPollPlacement, Topology, WorkerContext};
use slopio_turso::{Body, Client, Config, Request, RingIo, Server, Statement, Synchronous};
use turso_core::{Database, OpenOptions, SqliteDialect, Value, ValueRef};

static GET_USER: Statement = Statement::new("SELECT id, name FROM users WHERE id = ?1");
static INSERT_USER: Statement = Statement::new("INSERT INTO users (name) VALUES (?1)");
const PATH: &str = "/tmp/slopio-turso-sqpoll.db";

#[derive(Clone, Copy)]
struct Args {
    seconds: u64,
    clients: usize,
    tasks: usize,
    reads: u64,
    rows: i64,
    readers: u16,
    work_us: u64,
    /// `None`: no SQPOLL.
    sqpoll_idle_ms: Option<u64>,
    synchronous: Synchronous,
    commit_interval: Duration,
}

impl Args {
    fn parse() -> Self {
        let mut args = Args {
            seconds: 10,
            clients: 8,
            tasks: 4,
            reads: 90,
            rows: 100_000,
            readers: 2,
            work_us: 0,
            sqpoll_idle_ms: None,
            synchronous: Synchronous::Full,
            commit_interval: Duration::ZERO,
        };
        let raw: Vec<String> = std::env::args().skip(1).collect();
        for pair in raw.chunks(2) {
            let [flag, value] = pair else {
                panic!("expected `--flag value` pairs");
            };
            match flag.as_str() {
                "--seconds" => args.seconds = parse(flag, value),
                "--clients" => args.clients = parse(flag, value),
                "--tasks" => args.tasks = parse(flag, value),
                "--reads" => args.reads = parse(flag, value),
                "--rows" => args.rows = parse(flag, value),
                "--readers" => args.readers = parse(flag, value),
                "--work-us" => args.work_us = parse(flag, value),
                "--sqpoll-idle-ms" => args.sqpoll_idle_ms = Some(parse(flag, value)),
                "--commit-interval-us" => {
                    args.commit_interval = Duration::from_micros(parse(flag, value))
                }
                "--synchronous" => {
                    args.synchronous = match value.as_str() {
                        "full" => Synchronous::Full,
                        "normal" => Synchronous::Normal,
                        _ => panic!("--synchronous is full or normal"),
                    }
                }
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

/// The peers that run as clients: the first `--clients` workers.
fn clients(args: Args, ctx: &WorkerContext) -> std::ops::Range<usize> {
    assert!(
        ctx.workers.len() >= args.clients,
        "{} clients asked, {} CPUs left for them",
        args.clients,
        ctx.workers.len()
    );
    ctx.workers.start..ctx.workers.start + args.clients
}

async fn db_thread(args: Args, ctx: WorkerContext) {
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
        synchronous: args.synchronous,
        commit_interval: args.commit_interval,
        ..Config::default()
    };
    let serving = spawn(Server::new(&db, &config).unwrap().serve(io));
    let clients = clients(args, &ctx);
    for client in clients.clone() {
        send_to_peer(client, 0, 0); // the table is ready
    }
    // One stop message per client, then the runtime can wind down.
    for _ in clients {
        recv_from_peer().await;
    }
    drop(serving);
}

async fn client_thread(args: Args, db: usize, thread: usize, stats: Arc<Mutex<Stats>>) {
    recv_from_peer().await; // the table is ready
    let client = Client::new(db);
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
                        client
                            .query(Request::new(&GET_USER, &[ValueRef::from_i64(id)]))
                            .await
                    } else {
                        let name = Value::from_text(format!("new{}", rng.next() % 1_000_000));
                        client
                            .query(Request::new(&INSERT_USER, &[name.as_ref()]))
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
                    if matches!(done.body(), Body::Error(_)) {
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
    send_to_peer(db, 0, 0);
}

fn main() {
    let args = Args::parse();
    remove_files();
    let stats = Arc::new(Mutex::new(Stats::default()));

    let mut topology = Topology::new();
    let db = topology.dedicated::<(), _, _>(
        Dedicated {
            cpu: Cpu::Auto,
            sqpoll: args.sqpoll_idle_ms.map(|ms| SqPollPlacement {
                idle: Duration::from_millis(ms),
                cpu: Cpu::Auto,
            }),
        },
        move |ctx| db_thread(args, ctx),
    );
    let shared = Arc::clone(&stats);
    topology.per_core::<(), _, _>(move |ctx| {
        let stats = Arc::clone(&shared);
        async move {
            if clients(args, &ctx).contains(&ctx.thread_id) {
                client_thread(args, db, ctx.thread_id, stats).await;
            }
        }
    });
    let started = Instant::now();
    topology.run();
    let elapsed = started.elapsed().min(Duration::from_secs(args.seconds));
    remove_files();

    let mut stats = stats.lock().unwrap();
    let sqpoll = match args.sqpoll_idle_ms {
        Some(ms) => format!("SQPOLL (idle {ms} ms) on a CPU of its own"),
        None => "no SQPOLL".into(),
    };
    println!(
        "{} s, DB thread on a CPU of its own, {sqpoll}; {} clients × {} tasks, {}% reads, {}µs of work per reply, synchronous {:?}, commit interval {:?}",
        args.seconds, args.clients, args.tasks, args.reads, args.work_us, args.synchronous, args.commit_interval
    );
    stats.reads.report("reads", elapsed);
    stats.writes.report("writes", elapsed);
}
