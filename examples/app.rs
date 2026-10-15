//! A web API's load: requests arrive on their own at a target rate, whatever
//! the database's speed, and each request's latency counts from when it was due
//! to arrive. A database falling behind shows as latency, then as requests shed.
//!
//! ```text
//! cargo run --release --example app -- [--workers 3] [--rates 50000,100000] [--seconds 3]
//!     [--work-us 5] [--writes 15] [--synchronous full|normal] [--interval-us 0] [--wal-limit 16000]
//! ```
//!
//! The data: 100k users, 300k posts indexed by author. Users are not equally
//! popular: a few get most of the traffic. The requests:
//!
//! - 70% profile: a user, then their 20 latest posts;
//! - 15% feed: the 20 latest posts;
//! - 10% post: a user publishes a post (an insert at the end of the table);
//! - 5% seen: a user's `last_seen` is updated.
//!
//! `--writes PCT` (15 by default) changes the share of the last two, keeping
//! profile:feed at 70:15 and post:seen at 2:1.
//!
//! Each request also spends `--work-us` of CPU, as a handler would.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use slopio::runtime::select::select;
use slopio::runtime::{recv_from_peer, send_to_peer};
use slopio::{RingOptions, Threads, Topology};
use slopio_turso::db::{Config, Synchronous};
use slopio_turso::{Client, Error, FromRow, Read, Row, Write, read_sql, write_sql};
use turso_core::{Database, Numeric, OpenOptions, SqliteDialect, UnixIO, Value};

const PATH: &str = "/tmp/slopio-turso-app.db";
const USERS: u64 = 100_000;
const POSTS: u64 = 300_000;
/// Requests in progress past which a worker sheds new ones, as an overloaded
/// server would rather than queue without end.
const MAX_IN_FLIGHT: usize = 10_000;

static GET_USER: Read = read_sql!("SELECT id, name, email, last_seen FROM users WHERE id = ?1");
static USER_POSTS: Read = read_sql!(
    "SELECT id, user_id, created_at, body FROM posts WHERE user_id = ?1 ORDER BY id DESC LIMIT 20",
);
static RECENT_POSTS: Read =
    read_sql!("SELECT id, user_id, created_at, body FROM posts ORDER BY id DESC LIMIT 20");
static NEW_POST: Write =
    write_sql!("INSERT INTO posts (user_id, created_at, body) VALUES (?1, ?2, ?3)");
static SEEN: Write = write_sql!("UPDATE users SET last_seen = ?2 WHERE id = ?1");

#[allow(dead_code)]
struct User {
    id: i64,
    name: String,
    email: String,
    last_seen: i64,
}

impl FromRow for User {
    fn from_row(row: &Row<'_>) -> Result<Self, Error> {
        Ok(User {
            id: row.get(0)?,
            name: row.get(1)?,
            email: row.get(2)?,
            last_seen: row.get(3)?,
        })
    }
}

#[allow(dead_code)]
struct Post {
    id: i64,
    user_id: i64,
    created_at: i64,
    body: String,
}

impl FromRow for Post {
    fn from_row(row: &Row<'_>) -> Result<Self, Error> {
        Ok(Post {
            id: row.get(0)?,
            user_id: row.get(1)?,
            created_at: row.get(2)?,
            body: row.get(3)?,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Kind {
    Profile,
    Feed,
    Post,
    Seen,
}

const KINDS: [Kind; 4] = [Kind::Profile, Kind::Feed, Kind::Post, Kind::Seen];

impl Kind {
    /// A request, `writes` percent of them writing.
    fn pick(rng: &mut Rng, writes: u64) -> Kind {
        // In thousandths, for the shares to split finely.
        let reads = 1000 - 10 * writes;
        let profile = reads * 70 / 85;
        let post = reads + 10 * writes * 2 / 3;
        match rng.next() % 1000 {
            x if x < profile => Kind::Profile,
            x if x < reads => Kind::Feed,
            x if x < post => Kind::Post,
            _ => Kind::Seen,
        }
    }
}

#[derive(Clone)]
struct Args {
    workers: usize,
    rates: Vec<u64>,
    seconds: u64,
    work: Duration,
    /// Percent of requests that write.
    writes: u64,
    config: Config,
}

/// What a worker saw: each request's latency by kind, and the requests shed.
#[derive(Default)]
struct Tally {
    latencies: [Vec<Duration>; 4],
    shed: u64,
    unfinished: u64,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        for (mine, theirs) in self.latencies.iter_mut().zip(other.latencies) {
            mine.extend(theirs);
        }
        self.shed += other.shed;
        self.unfinished += other.unfinished;
    }
}

fn main() {
    let args = parse_args();
    remove_files();
    let start = Instant::now();
    seed();
    println!(
        "seeded {USERS} users, {POSTS} posts in {:.1?}; {} workers, {:?} of work per request, {:?}, commit interval {:?}, wal limit {} pages",
        start.elapsed(),
        args.workers,
        args.work,
        args.config.synchronous,
        args.config.commit_interval,
        args.config.wal_limit,
    );
    for &rate in &args.rates {
        run(&args, rate);
    }
    remove_files();
}

/// One run at `rate` requests per second, spread over the workers.
fn run(args: &Args, rate: u64) {
    // No DEFER_TASKRUN: a busy worker still gets its replies promptly.
    let ring = RingOptions {
        defer_taskrun: false,
        ..RingOptions::default()
    };
    let (tx, rx) = mpsc::channel();
    let mut topology = Topology::new();
    let (workers, config) = (args.workers, args.config);
    let db_thread = topology
        .group::<(), _, _>(Threads::Auto(1), ring, move |_| async move {
            let stop = async {
                for _ in 0..workers {
                    recv_from_peer().await;
                }
            };
            select(slopio_turso::db::serve(PATH, config), stop).await;
        })
        .start;
    let per_worker = rate as f64 / args.workers as f64;
    let (seconds, work, writes) = (args.seconds, args.work, args.writes);
    topology.group::<(), _, _>(Threads::Auto(args.workers), ring, move |ctx| {
        let tx = tx.clone();
        async move {
            // SAFETY: `db_thread` is the group that runs `serve`.
            let client = unsafe { Client::connect(db_thread) }.await.unwrap();
            let tally = generate(
                client,
                per_worker,
                seconds,
                work,
                writes,
                ctx.thread_id as u64,
            )
            .await;
            send_to_peer(db_thread, 0, 0);
            tx.send(tally).unwrap();
        }
    });
    topology.run();

    let mut total = Tally::default();
    for tally in rx.iter() {
        total.add(tally);
    }
    let done: usize = total.latencies.iter().map(Vec::len).sum();
    let wal = std::fs::metadata(format!("{PATH}-wal")).map_or(0, |m| m.len());
    println!(
        "\ntarget {rate:>7} req/s: done {:>7.0} req/s, shed {}, unfinished {}, wal {:.1} MB",
        done as f64 / args.seconds as f64,
        total.shed,
        total.unfinished,
        wal as f64 / 1e6,
    );
    for (kind, latencies) in KINDS.iter().zip(&mut total.latencies) {
        println!(
            "  {:<8} {:>7} {}",
            format!("{kind:?}"),
            latencies.len(),
            percentiles(latencies)
        );
    }
}

/// Starts requests as they come due, `rate` per second on average with random
/// gaps, for `seconds`; then waits for the ones in progress.
async fn generate(
    client: Client,
    rate: f64,
    seconds: u64,
    work: Duration,
    writes: u64,
    seed: u64,
) -> Tally {
    let tally = Rc::new(RefCell::new(Tally::default()));
    let in_flight = Rc::new(Cell::new(0usize));
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (seed + 1));
    let start = Instant::now();
    let end = start + Duration::from_secs(seconds);
    let mut due = start;
    while due < end {
        let now = Instant::now();
        while due <= now && due < end {
            if in_flight.get() >= MAX_IN_FLIGHT {
                tally.borrow_mut().shed += 1;
            } else {
                let kind = Kind::pick(&mut rng, writes);
                let user = rng.popular_user();
                in_flight.set(in_flight.get() + 1);
                let (client, tally, in_flight) = (client.clone(), tally.clone(), in_flight.clone());
                slopio::spawn(async move {
                    request(&client, kind, user, due, work).await;
                    tally.borrow_mut().latencies[kind as usize].push(due.elapsed());
                    in_flight.set(in_flight.get() - 1);
                });
            }
            due += rng.gap(rate);
        }
        slopio::yield_now().await;
    }
    // Lets the requests in progress finish, a while at most.
    let patience = Instant::now() + Duration::from_secs(5);
    while in_flight.get() > 0 && Instant::now() < patience {
        slopio::yield_now().await;
    }
    let mut tally = tally.take();
    tally.unfinished = in_flight.get() as u64;
    tally
}

async fn request(client: &Client, kind: Kind, user: i64, due: Instant, work: Duration) {
    let id = || Value::Numeric(Numeric::Integer(user));
    let now = || Value::Numeric(Numeric::Integer(due.elapsed().as_nanos() as i64));
    match kind {
        Kind::Profile => {
            let found: Option<User> = client.read_one(&GET_USER, vec![id()]).await.unwrap();
            assert!(found.is_some());
            let posts: Vec<Post> = client.read(&USER_POSTS, vec![id()]).await.unwrap();
            assert!(posts.len() <= 20);
        }
        Kind::Feed => {
            let posts: Vec<Post> = client.read(&RECENT_POSTS, vec![]).await.unwrap();
            assert_eq!(posts.len(), 20);
        }
        Kind::Post => {
            let body =
                Value::build_text(format!("a new post by user {user}, with a few words in it"));
            client
                .write(&NEW_POST, vec![id(), now(), body])
                .await
                .unwrap();
        }
        Kind::Seen => client.write(&SEEN, vec![id(), now()]).await.unwrap(),
    }
    // The handler's own work: decoding, rendering, and so on.
    let until = Instant::now() + work;
    while Instant::now() < until {
        std::hint::spin_loop();
    }
}

fn percentiles(latencies: &mut [Duration]) -> String {
    if latencies.is_empty() {
        return "-".into();
    }
    latencies.sort();
    let at = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    format!(
        "p50 {:>9.1?}  p99 {:>9.1?}  p99.9 {:>9.1?}  max {:>9.1?}",
        at(0.5),
        at(0.99),
        at(0.999),
        latencies[latencies.len() - 1]
    )
}

/// Creates the tables, before any thread starts.
fn seed() {
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(Arc::new(UnixIO::new().unwrap()), PATH, options).unwrap();
    let conn = db.connect().unwrap();
    for sql in [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, last_seen INTEGER)".to_owned(),
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, created_at INTEGER, body TEXT)"
            .to_owned(),
        format!(
            "INSERT INTO users WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {USERS}) \
             SELECT x, 'user' || x, 'user' || x || '@example.com', 0 FROM c"
        ),
        // Authors spread over the users, popular ones included.
        format!(
            "INSERT INTO posts WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {POSTS}) \
             SELECT x, (x * 7919) % {USERS} + 1, x, 'post ' || x || ': some words about something, as posts go' FROM c"
        ),
        "CREATE INDEX posts_by_user ON posts (user_id, id)".to_owned(),
    ] {
        conn.execute(sql).unwrap();
    }
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

    /// In [0, 1).
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// A user, the low ids far more often than the high ones: about as many
    /// requests go to users 1–10 as to users 10–100, 100–1000, and so on.
    fn popular_user(&mut self) -> i64 {
        ((USERS as f64).powf(self.unit()) as i64).clamp(1, USERS as i64)
    }

    /// The time to the next request when they come `rate` per second at
    /// random.
    fn gap(&mut self, rate: f64) -> Duration {
        Duration::from_secs_f64(-(1.0 - self.unit()).ln() / rate)
    }
}

fn parse_args() -> Args {
    let mut args = Args {
        workers: 3,
        rates: vec![50_000, 100_000, 200_000],
        seconds: 3,
        work: Duration::from_micros(5),
        writes: 15,
        config: Config {
            synchronous: Synchronous::Normal,
            ..Config::default()
        },
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let flag_value = it.next().expect("a flag takes a value");
        let value = || flag_value.parse::<u64>().expect("a number");
        match flag.as_str() {
            "--workers" => args.workers = value() as usize,
            "--rates" => {
                args.rates = flag_value
                    .split(',')
                    .map(|r| r.parse().expect("a number"))
                    .collect()
            }
            "--seconds" => args.seconds = value(),
            "--work-us" => args.work = Duration::from_micros(value()),
            "--writes" => args.writes = value(),
            "--interval-us" => args.config.commit_interval = Duration::from_micros(value()),
            "--wal-limit" => args.config.wal_limit = value(),
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
    args
}
