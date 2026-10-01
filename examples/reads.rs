//! Load: one DB thread and `--workers` threads, each running `--tasks` tasks
//! that read, and write `--writes` percent of the time, through the worker's
//! [`Client`]. The DB thread only hands out the database: every worker reads
//! and writes on connections of its own.
//!
//! ```text
//! cargo run --release --example reads -- [--workers 3] [--tasks 64] [--seconds 5] [--writes 0] [--split 0]
//!     [--synchronous full|normal] [--interval-us 0]
//! ```
//!
//! `--split 1`: instead of every task writing `--writes` percent of the time,
//! that percentage of the tasks only write and the others only read.
//!
//! Each worker's runners serve what its tasks queued in one transaction, so
//! `--tasks` is the most a batch can hold (at most 64). Writers of different
//! workers take turns: one connection writes at a time.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use slopio::runtime::select::select;
use slopio::runtime::{recv_from_peer, send_to_peer};
use slopio::{Cpu, Dedicated, Topology};
use slopio_turso::db::{Config, Synchronous};
use slopio_turso::{Client, Error, FromRow, Read, Row, Write};
use turso_core::{Database, Numeric, OpenOptions, SqliteDialect, UnixIO, Value};

const PATH: &str = "/tmp/slopio-turso-reads.db";
const ROWS: u64 = 100_000;
/// One read in this many has its latency recorded.
const READ_SAMPLE: u64 = 64;

static GET_USER: Read = Read::new("SELECT id, name FROM users WHERE id = ?1");
static RENAME_USER: Write = Write::new("UPDATE users SET name = ?2 WHERE id = ?1");

struct User {
    id: i64,
    name: String,
}

impl FromRow for User {
    fn from_row(row: &Row<'_>) -> Result<Self, Error> {
        Ok(User {
            id: row.get(0)?,
            name: row.get(1)?,
        })
    }
}

struct Args {
    workers: usize,
    tasks: usize,
    seconds: u64,
    writes: u64,
    /// Tasks either only read or only write, `writes` percent of them writing.
    split: bool,
    config: Config,
}

/// What tasks did: how many reads and writes, and the latencies recorded.
#[derive(Default)]
struct Tally {
    reads: u64,
    writes: u64,
    read_latencies: Vec<Duration>,
    write_latencies: Vec<Duration>,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        self.reads += other.reads;
        self.writes += other.writes;
        self.read_latencies.extend(other.read_latencies);
        self.write_latencies.extend(other.write_latencies);
    }
}

fn main() {
    let args = parse_args();
    remove_files();
    seed();

    let anywhere = Dedicated {
        cpu: Cpu::Auto,
        sqpoll: None,
    };
    let (tx, rx) = mpsc::channel();
    let mut topology = Topology::new();
    let workers = args.workers;
    let config = args.config;
    let db_thread = topology.dedicated::<(), _, _>(anywhere, move |_| async move {
        // Serves until every worker said it is done.
        let stop = async {
            for _ in 0..workers {
                recv_from_peer().await;
            }
        };
        select(slopio_turso::db::serve(PATH, config), stop).await;
    });
    for _ in 0..args.workers {
        let tx = tx.clone();
        let (tasks, seconds, writes, split) = (args.tasks, args.seconds, args.writes, args.split);
        topology.dedicated::<(), _, _>(anywhere, move |ctx| async move {
            let client = Client::connect(db_thread).await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(seconds);
            let handles: Vec<_> = (0..tasks)
                .map(|t| {
                    let seed = (ctx.thread_id * tasks + t) as u64;
                    let writes = match split {
                        false => writes,
                        true if (t as u64) < (tasks as u64 * writes).div_ceil(100) => 100,
                        true => 0,
                    };
                    slopio::spawn(work(client.clone(), seed, deadline, writes))
                })
                .collect();
            let mut tally = Tally::default();
            for handle in handles {
                tally.add(handle.await);
            }
            send_to_peer(db_thread, 0, 0);
            tx.send((ctx.thread_id, tally)).unwrap();
        });
    }
    drop(tx);
    topology.run();

    let mut results: Vec<_> = rx.iter().collect();
    results.sort_by_key(|(thread, _)| *thread);
    let seconds = args.seconds as f64;
    let mut total = Tally::default();
    for (thread, tally) in results {
        println!(
            "worker {thread}: {:>10.0} reads/s {:>8.0} writes/s",
            tally.reads as f64 / seconds,
            tally.writes as f64 / seconds,
        );
        total.add(tally);
    }
    println!(
        "total:    {:>10.0} reads/s {:>8.0} writes/s ({} workers, {} tasks each, {}% {})",
        total.reads as f64 / seconds,
        total.writes as f64 / seconds,
        args.workers,
        args.tasks,
        args.writes,
        if args.split {
            "of tasks write"
        } else {
            "writes"
        },
    );
    println!("reads:  {}", percentiles(&mut total.read_latencies));
    println!("writes: {}", percentiles(&mut total.write_latencies));
    remove_files();
}

/// Reads random users, or renames one `writes` percent of the time, until
/// `deadline`.
async fn work(client: Client, seed: u64, deadline: Instant, writes: u64) -> Tally {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (seed + 1));
    let mut tally = Tally::default();
    let mut n = 0u64;
    while Instant::now() < deadline {
        n += 1;
        let id = rng.id();
        let id_param = Value::Numeric(Numeric::Integer(id));
        if rng.next() % 100 < writes {
            let start = Instant::now();
            let name = Value::build_text(format!("renamed{n}"));
            client
                .write(&RENAME_USER, vec![id_param, name])
                .await
                .unwrap();
            tally.write_latencies.push(start.elapsed());
            tally.writes += 1;
        } else {
            let start = Instant::now();
            let user: User = client
                .read_one(&GET_USER, vec![id_param])
                .await
                .unwrap()
                .unwrap();
            assert!(user.id == id && !user.name.is_empty());
            if tally.reads % READ_SAMPLE == 0 {
                tally.read_latencies.push(start.elapsed());
            }
            tally.reads += 1;
        }
    }
    tally
}

fn percentiles(latencies: &mut [Duration]) -> String {
    if latencies.is_empty() {
        return "-".into();
    }
    latencies.sort();
    let at = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    format!(
        "p50 {:?}  p99 {:?}  max {:?}",
        at(0.5),
        at(0.99),
        latencies[latencies.len() - 1]
    )
}

/// Creates the users table, before any thread starts.
fn seed() {
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(Arc::new(UnixIO::new().unwrap()), PATH, options).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(format!(
        "INSERT INTO users (id, name) WITH RECURSIVE c(x) AS \
         (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {ROWS}) SELECT x, 'user' || x FROM c"
    ))
    .unwrap();
}

fn remove_files() {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{PATH}{suffix}"));
    }
}

/// xorshift: cheap enough not to show in the measure.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn id(&mut self) -> i64 {
        (self.next() % ROWS) as i64 + 1
    }
}

fn parse_args() -> Args {
    let mut args = Args {
        workers: 3,
        tasks: 64,
        seconds: 5,
        writes: 0,
        split: false,
        config: Config::default(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let flag_value = it.next().expect("a flag takes a value");
        let value = || flag_value.parse().expect("a number");
        match flag.as_str() {
            "--workers" => args.workers = value(),
            "--tasks" => args.tasks = value(),
            "--seconds" => args.seconds = value() as u64,
            "--writes" => args.writes = value() as u64,
            "--split" => args.split = value() != 0,
            "--interval-us" => args.config.commit_interval = Duration::from_micros(value() as u64),
            "--synchronous" => {
                args.config.synchronous = match flag_value.as_str() {
                    "full" => Synchronous::Full,
                    "normal" => Synchronous::Normal,
                    other => panic!("--synchronous full|normal, not {other}"),
                }
            }
            _ => panic!("unknown flag {flag}"),
        }
    }
    assert!(args.tasks > 0, "--tasks must be at least 1");
    assert!(args.writes <= 100, "--writes is a percentage");
    args
}
