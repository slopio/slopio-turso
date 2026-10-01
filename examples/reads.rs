//! Read-only load: one DB thread and `--workers` threads, each running `--tasks`
//! tasks that read through the worker's [`Client`]. The DB thread only hands out
//! the database; no read crosses threads.
//!
//! ```text
//! cargo run --release --example reads -- [--workers 3] [--tasks 64] [--seconds 5]
//! ```
//!
//! Each worker's reader serves the reads its tasks queued in one read
//! transaction, so `--tasks` is the most a batch can hold (at most 64).

use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use slopio::runtime::select::select;
use slopio::runtime::{recv_from_peer, send_to_peer};
use slopio::{Cpu, Dedicated, Topology};
use slopio_turso::{Client, Error, FromRow, Read, Row};
use turso_core::{Database, Numeric, OpenOptions, SqliteDialect, UnixIO, Value};

const PATH: &str = "/tmp/slopio-turso-reads.db";
const ROWS: u64 = 100_000;

static GET_USER: Read = Read::new("SELECT id, name FROM users WHERE id = ?1");

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
    let db_thread = topology.dedicated::<(), _, _>(anywhere, move |_| async move {
        // Serves until every worker said it is done.
        let stop = async {
            for _ in 0..workers {
                recv_from_peer().await;
            }
        };
        select(slopio_turso::db::serve(PATH), stop).await;
    });
    for _ in 0..args.workers {
        let tx = tx.clone();
        let (tasks, seconds) = (args.tasks, args.seconds);
        topology.dedicated::<(), _, _>(anywhere, move |ctx| async move {
            let client = Client::connect(db_thread).await.unwrap();
            let deadline = Instant::now() + Duration::from_secs(seconds);
            let handles: Vec<_> = (0..tasks)
                .map(|t| {
                    let seed = (ctx.thread_id * tasks + t) as u64;
                    slopio::spawn(read(client.clone(), seed, deadline))
                })
                .collect();
            let mut reads = 0;
            for handle in handles {
                reads += handle.await;
            }
            send_to_peer(db_thread, 0, 0);
            tx.send((ctx.thread_id, reads)).unwrap();
        });
    }
    drop(tx);
    topology.run();

    let mut results: Vec<_> = rx.iter().collect();
    results.sort();
    let mut total = 0;
    for (thread, reads) in results {
        println!(
            "worker {thread}: {:>10.0} reads/s",
            reads as f64 / args.seconds as f64
        );
        total += reads;
    }
    let per_second = total as f64 / args.seconds as f64;
    println!(
        "total:    {per_second:>10.0} reads/s ({} workers, {} tasks each), {:.0} ns/read per worker",
        args.workers,
        args.tasks,
        1e9 * args.workers as f64 / per_second,
    );
    remove_files();
}

/// Point reads of random users until `deadline`. Returns how many were done.
async fn read(client: Client, seed: u64, deadline: Instant) -> u64 {
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (seed + 1));
    let mut reads = 0;
    while Instant::now() < deadline {
        // Check the clock every 64 reads only.
        for _ in 0..64 {
            let id = rng.id();
            let params = vec![Value::Numeric(Numeric::Integer(id))];
            let user: User = client.read_one(&GET_USER, params).await.unwrap().unwrap();
            assert!(user.id == id && !user.name.is_empty());
        }
        reads += 64;
    }
    reads
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
    fn id(&mut self) -> i64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % ROWS) as i64 + 1
    }
}

fn parse_args() -> Args {
    let mut args = Args {
        workers: 3,
        tasks: 64,
        seconds: 5,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it.next().expect("a flag takes a value");
        let value = || value.parse().expect("a number");
        match flag.as_str() {
            "--workers" => args.workers = value(),
            "--tasks" => args.tasks = value(),
            "--seconds" => args.seconds = value() as u64,
            _ => panic!("unknown flag {flag}"),
        }
    }
    assert!(args.tasks > 0, "--tasks must be at least 1");
    args
}
