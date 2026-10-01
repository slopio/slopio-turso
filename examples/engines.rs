//! turso against SQLite on our workload, without our plumbing: plain threads,
//! one connection each, the data and queries of `app.rs`.
//!
//! ```text
//! cargo run --release --example engines
//! ```
//!
//! Readers run "profile" requests (a user, then their 20 latest posts) in read
//! transactions of 64, as our worker's reader batches them. One writer inserts
//! posts and updates users, 20 writes per commit, at a fixed commit rate or as
//! fast as it can. Every connection is `synchronous = NORMAL`.

use std::num::NonZero;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::thread;
use std::time::{Duration, Instant};

use turso_core::{
    CheckpointMode, Connection, Database, IO, Numeric, OpenOptions, SqliteDialect, Statement,
    StepResult, SyncMode, UnixIO, Value,
};

const USERS: i64 = 100_000;
const POSTS: i64 = 300_000;
const READ_BATCH: usize = 64;
const WRITES_PER_COMMIT: usize = 20;
const SECONDS: u64 = 2;

const GET_USER: &str = "SELECT id, name, email, last_seen FROM users WHERE id = ?1";
const USER_POSTS: &str =
    "SELECT id, user_id, created_at, body FROM posts WHERE user_id = ?1 ORDER BY id DESC LIMIT 20";
const NEW_POST: &str = "INSERT INTO posts (user_id, created_at, body) VALUES (?1, ?2, ?3)";
const SEEN: &str = "UPDATE users SET last_seen = ?2 WHERE id = ?1";
const BODY: &str = "a new post by some user, with a few words in it";

fn schema() -> Vec<String> {
    vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, last_seen INTEGER)".into(),
        "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, created_at INTEGER, body TEXT)".into(),
        format!(
            "INSERT INTO users WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {USERS}) \
             SELECT x, 'user' || x, 'user' || x || '@example.com', 0 FROM c"
        ),
        format!(
            "INSERT INTO posts WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {POSTS}) \
             SELECT x, (x * 7919) % {USERS} + 1, x, 'post ' || x || ': some words about something, as posts go' FROM c"
        ),
        "CREATE INDEX posts_by_user ON posts (user_id, id)".into(),
    ]
}

/// A write: a new post by `user`, or `user` seen.
#[derive(Clone, Copy)]
struct Write {
    user: i64,
    now: i64,
    post: bool,
}

trait Engine: Send + Sync {
    fn name(&self) -> String;
    fn connect(&self, writer: bool) -> Box<dyn Conn>;
    fn wal_path(&self) -> String;
    /// Empties the WAL between runs, with nobody else connected.
    fn truncate_wal(&self);
}

trait Conn {
    /// One read transaction, a profile per user; returns the bytes read, so
    /// that nothing is optimized away.
    fn read_batch(&mut self, users: &[i64]) -> usize;
    /// One write transaction.
    fn write_batch(&mut self, writes: &[Write]);
}

// ---- turso ----

struct Turso {
    db: Arc<Database>,
    path: String,
    /// turso's experimental MVCC journal instead of the WAL.
    mvcc: bool,
}

impl Turso {
    fn open(path: &str, mvcc: bool) -> Self {
        remove_files(path);
        let options = OpenOptions::new(Arc::new(SqliteDialect));
        let db = Database::open(Arc::new(UnixIO::new().unwrap()), path, options).unwrap();
        let conn = db.connect().unwrap();
        for sql in schema() {
            conn.execute(sql).unwrap();
        }
        if mvcc {
            conn.execute("PRAGMA journal_mode = 'mvcc'").unwrap();
            assert!(db.mvcc_enabled());
        }
        Turso {
            db,
            path: path.into(),
            mvcc,
        }
    }
}

impl Engine for Turso {
    fn name(&self) -> String {
        if self.mvcc {
            "turso mvcc".into()
        } else {
            "turso wal".into()
        }
    }

    fn connect(&self, writer: bool) -> Box<dyn Conn> {
        let conn = self.db.connect().unwrap();
        if writer {
            conn.set_sync_mode(SyncMode::Normal);
        }
        let prepare = |sql: &str| conn.prepare(sql).unwrap();
        Box::new(TursoConn {
            io: self.db.io.clone(),
            begin: prepare(match (writer, self.mvcc) {
                (true, true) => "BEGIN CONCURRENT",
                (true, false) => "BEGIN IMMEDIATE",
                (false, _) => "BEGIN",
            }),
            commit: prepare("COMMIT"),
            get_user: prepare(GET_USER),
            user_posts: prepare(USER_POSTS),
            new_post: prepare(NEW_POST),
            seen: prepare(SEEN),
            _conn: conn,
        })
    }

    fn wal_path(&self) -> String {
        // MVCC logs rows, in a log of its own.
        let suffix = if self.mvcc { "-log" } else { "-wal" };
        format!("{}{suffix}", self.path)
    }

    fn truncate_wal(&self) {
        let conn = self.db.connect().unwrap();
        let mode = CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        };
        conn.checkpoint(mode).unwrap();
    }
}

struct TursoConn {
    _conn: Arc<Connection>,
    io: Arc<dyn IO>,
    begin: Statement,
    commit: Statement,
    get_user: Statement,
    user_posts: Statement,
    new_post: Statement,
    seen: Statement,
}

/// Binds `params`, steps `stmt` to its end handing every row to `f`, resets it.
fn turso_run(
    stmt: &mut Statement,
    io: &Arc<dyn IO>,
    params: &[Value],
    mut f: impl FnMut(&turso_core::Row),
) {
    for (i, param) in params.iter().enumerate() {
        stmt.bind_at(NonZero::new(i + 1).unwrap(), param.clone())
            .unwrap();
    }
    loop {
        match stmt.step() {
            Ok(StepResult::Row) => f(stmt.row().unwrap()),
            Ok(StepResult::Done) => break,
            // MVCC may ask to come back later.
            Ok(StepResult::Busy) | Err(turso_core::LimboError::Busy) => thread::yield_now(),
            Ok(StepResult::Interrupt) => panic!("turso: interrupted"),
            Ok(_) => io.step().unwrap(),
            Err(e) => panic!("turso: {e}"),
        }
    }
    stmt.reset().unwrap();
}

fn int(i: i64) -> Value {
    Value::Numeric(Numeric::Integer(i))
}

impl Conn for TursoConn {
    fn read_batch(&mut self, users: &[i64]) -> usize {
        let mut bytes = 0;
        turso_run(&mut self.begin, &self.io, &[], |_| {});
        for &user in users {
            turso_run(&mut self.get_user, &self.io, &[int(user)], |row| {
                let (_id, name, email, _seen): (i64, String, String, i64) = (
                    row.get(0).unwrap(),
                    row.get(1).unwrap(),
                    row.get(2).unwrap(),
                    row.get(3).unwrap(),
                );
                bytes += name.len() + email.len();
            });
            turso_run(&mut self.user_posts, &self.io, &[int(user)], |row| {
                let (_id, _user, _at, body): (i64, i64, i64, String) = (
                    row.get(0).unwrap(),
                    row.get(1).unwrap(),
                    row.get(2).unwrap(),
                    row.get(3).unwrap(),
                );
                bytes += body.len();
            });
        }
        turso_run(&mut self.commit, &self.io, &[], |_| {});
        bytes
    }

    fn write_batch(&mut self, writes: &[Write]) {
        turso_run(&mut self.begin, &self.io, &[], |_| {});
        for w in writes {
            if w.post {
                let params = [int(w.user), int(w.now), Value::build_text(BODY)];
                turso_run(&mut self.new_post, &self.io, &params, |_| {});
            } else {
                turso_run(&mut self.seen, &self.io, &[int(w.user), int(w.now)], |_| {});
            }
        }
        turso_run(&mut self.commit, &self.io, &[], |_| {});
    }
}

// ---- SQLite ----

struct Sqlite {
    path: String,
    /// `PRAGMA mmap_size` of every connection: 0 reads with `pread`.
    mmap: u64,
    /// `PRAGMA cache_size` of every connection, in KiB.
    cache_kib: u64,
}

impl Sqlite {
    fn open(path: &str, mmap: u64, cache_kib: u64) -> Self {
        remove_files(path);
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        for sql in schema() {
            conn.execute_batch(&sql).unwrap();
        }
        Sqlite {
            path: path.into(),
            mmap,
            cache_kib,
        }
    }
}

impl Engine for Sqlite {
    fn name(&self) -> String {
        let mmap = match self.mmap {
            0 => String::new(),
            n => format!(", mmap {} MB", n >> 20),
        };
        format!("sqlite, cache {} MB{mmap}", self.cache_kib >> 10)
    }

    fn connect(&self, writer: bool) -> Box<dyn Conn> {
        let conn = rusqlite::Connection::open(&self.path).unwrap();
        conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
        conn.pragma_update(None, "mmap_size", self.mmap).unwrap();
        conn.pragma_update(None, "cache_size", -(self.cache_kib as i64))
            .unwrap();
        conn.busy_timeout(Duration::from_secs(5)).unwrap();
        Box::new(SqliteConn {
            conn,
            begin: if writer { "BEGIN IMMEDIATE" } else { "BEGIN" },
        })
    }

    fn wal_path(&self) -> String {
        format!("{}-wal", self.path)
    }

    fn truncate_wal(&self) {
        let conn = rusqlite::Connection::open(&self.path).unwrap();
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
            .unwrap();
    }
}

struct SqliteConn {
    conn: rusqlite::Connection,
    begin: &'static str,
}

impl Conn for SqliteConn {
    fn read_batch(&mut self, users: &[i64]) -> usize {
        let mut bytes = 0;
        self.conn
            .prepare_cached(self.begin)
            .unwrap()
            .execute([])
            .unwrap();
        for &user in users {
            let mut stmt = self.conn.prepare_cached(GET_USER).unwrap();
            let mut rows = stmt.query([user]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                let (_id, name, email, _seen): (i64, String, String, i64) = (
                    row.get(0).unwrap(),
                    row.get(1).unwrap(),
                    row.get(2).unwrap(),
                    row.get(3).unwrap(),
                );
                bytes += name.len() + email.len();
            }
            drop(rows);
            drop(stmt);
            let mut stmt = self.conn.prepare_cached(USER_POSTS).unwrap();
            let mut rows = stmt.query([user]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                let (_id, _user, _at, body): (i64, i64, i64, String) = (
                    row.get(0).unwrap(),
                    row.get(1).unwrap(),
                    row.get(2).unwrap(),
                    row.get(3).unwrap(),
                );
                bytes += body.len();
            }
        }
        self.conn
            .prepare_cached("COMMIT")
            .unwrap()
            .execute([])
            .unwrap();
        bytes
    }

    fn write_batch(&mut self, writes: &[Write]) {
        self.conn
            .prepare_cached(self.begin)
            .unwrap()
            .execute([])
            .unwrap();
        for w in writes {
            if w.post {
                let mut stmt = self.conn.prepare_cached(NEW_POST).unwrap();
                stmt.execute(rusqlite::params![w.user, w.now, BODY])
                    .unwrap();
            } else {
                let mut stmt = self.conn.prepare_cached(SEEN).unwrap();
                stmt.execute(rusqlite::params![w.user, w.now]).unwrap();
            }
        }
        self.conn
            .prepare_cached("COMMIT")
            .unwrap()
            .execute([])
            .unwrap();
    }
}

// ---- the runs ----

/// `readers` threads reading and one writer committing `commits` times a
/// second (`None`: no writer; `Some(u64::MAX)`: as fast as it can).
fn run(engine: &Arc<dyn Engine>, readers: usize, commits: Option<u64>) {
    let stop = Arc::new(AtomicBool::new(false));
    let profiles = Arc::new(AtomicU64::new(0));
    let writes = Arc::new(AtomicU64::new(0));
    let mut threads = Vec::new();
    for r in 0..readers {
        let (engine, stop, profiles) = (engine.clone(), stop.clone(), profiles.clone());
        threads.push(thread::spawn(move || {
            let mut conn = engine.connect(false);
            let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (r as u64 + 1));
            let mut users = [0; READ_BATCH];
            let mut bytes = 0;
            while !stop.load(Relaxed) {
                users.iter_mut().for_each(|u| *u = rng.popular_user());
                bytes += conn.read_batch(&users);
                profiles.fetch_add(READ_BATCH as u64, Relaxed);
            }
            assert!(bytes > 0);
        }));
    }
    if let Some(rate) = commits {
        let (engine, stop, writes) = (engine.clone(), stop.clone(), writes.clone());
        threads.push(thread::spawn(move || {
            let mut conn = engine.connect(true);
            let mut rng = Rng(0xDEAD_BEEF);
            let start = Instant::now();
            let mut done = 0u64;
            while !stop.load(Relaxed) {
                if rate != u64::MAX {
                    let due = start + Duration::from_secs_f64(done as f64 / rate as f64);
                    while Instant::now() < due && !stop.load(Relaxed) {
                        std::hint::spin_loop();
                    }
                }
                let batch: Vec<Write> = (0..WRITES_PER_COMMIT)
                    .map(|i| Write {
                        user: rng.popular_user(),
                        now: done as i64,
                        // As in app.rs: two new posts for one user seen.
                        post: i % 3 != 2,
                    })
                    .collect();
                conn.write_batch(&batch);
                done += 1;
                writes.fetch_add(WRITES_PER_COMMIT as u64, Relaxed);
            }
        }));
    }
    thread::sleep(Duration::from_secs(SECONDS));
    stop.store(true, Relaxed);
    for t in threads {
        t.join().unwrap();
    }
    let wal = std::fs::metadata(engine.wal_path()).map_or(0, |m| m.len());
    let secs = SECONDS as f64;
    let writer = match commits {
        None => "no writer".to_owned(),
        Some(u64::MAX) => "writer flat out".to_owned(),
        Some(rate) => format!("writer {rate} commits/s"),
    };
    println!(
        "  {readers} readers, {writer:<22} | {:>9.0} profiles/s ({:>8.0} per reader) | {:>7.0} writes/s ({:>5.0} commits/s) | wal {:>6.1} MB",
        profiles.load(Relaxed) as f64 / secs,
        profiles.load(Relaxed) as f64 / secs / readers.max(1) as f64,
        writes.load(Relaxed) as f64 / secs,
        writes.load(Relaxed) as f64 / secs / WRITES_PER_COMMIT as f64,
        wal as f64 / 1e6,
    );
    engine.truncate_wal();
}

fn main() {
    let scenarios: [(usize, Option<u64>); 7] = [
        (3, None),
        (9, None),
        (3, Some(500)),
        (3, Some(2000)),
        (9, Some(500)),
        (9, Some(2000)),
        (0, Some(u64::MAX)),
    ];
    const SQLITE: &str = "/tmp/slopio-turso-engines-sqlite.db";
    let sqlite = |mmap, cache_kib| -> Arc<dyn Engine> {
        Arc::new(Sqlite {
            path: SQLITE.into(),
            mmap,
            cache_kib,
        })
    };
    let only: Option<String> = std::env::args().nth(1);
    let engines: Vec<Arc<dyn Engine>> = vec![
        Arc::new(Turso::open("/tmp/slopio-turso-engines-turso.db", false)),
        Arc::new(Turso::open("/tmp/slopio-turso-engines-mvcc.db", true)),
        // Seeds the file the others share; SQLite's default cache is 2 MB.
        Arc::new(Sqlite::open(SQLITE, 0, 2 << 10)),
        sqlite(0, 64 << 10),
        sqlite(1 << 30, 64 << 10),
    ];
    for engine in &engines {
        if only
            .as_ref()
            .is_some_and(|o| !engine.name().contains(o.as_str()))
        {
            continue;
        }
        println!("== {}", engine.name());
        for &(readers, commits) in &scenarios {
            run(engine, readers, commits);
        }
    }
    for path in [
        "/tmp/slopio-turso-engines-turso.db",
        "/tmp/slopio-turso-engines-mvcc.db",
        "/tmp/slopio-turso-engines-sqlite.db",
    ] {
        remove_files(path);
    }
}

fn remove_files(path: &str) {
    for suffix in ["", "-wal", "-shm", "-log"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
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

    /// A user, the low ids far more often than the high ones, as in app.rs.
    fn popular_user(&mut self) -> i64 {
        let unit = (self.next() >> 11) as f64 / (1u64 << 53) as f64;
        ((USERS as f64).powf(unit) as i64).clamp(1, USERS)
    }
}
