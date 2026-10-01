//! End to end over MSG_RING: worker 0 runs the DB thread, worker 1 is a client.
//!
//! The DB thread serves until the client sends it a raw peer message (a
//! separate inbox from the typed requests), so the test can finish. Each side
//! is guarded by a timeout so a missing wakeup fails instead of hanging.

use std::sync::Arc;
use std::time::Duration;

use slopio::runtime::join::join4;
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_from_peer, send_to_peer};
use slopio::thread_per_core;
use slopio_turso::Client;
use slopio_turso::RingIo;
use slopio_turso::Statement;
use slopio_turso::client::query::{DecodeError, Query, get_str};
use slopio_turso::{Config, Server};
use slopio_turso::{Body, Error, Request};
use turso_core::{Database, Numeric, OpenOptions, SqliteDialect, Value, ValueRef};

const DB_THREAD: usize = 0;
const CLIENT_THREAD: usize = 1;
const TIMEOUT: Duration = Duration::from_secs(10);
/// Short, so the expiry test is quick; the other transactions never idle this long.
const TRANSACTION_TIMEOUT: Duration = Duration::from_millis(200);

/// A real file with `RingIo`, as in production: statements then wait on disk,
/// which an in-memory database never does.
fn db_path() -> String {
    format!("/tmp/slopio-turso-serve-{}.db", std::process::id())
}

fn remove_db() {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db_path()));
    }
}

static CREATE_USERS: Statement = Statement::new("CREATE TABLE users (name TEXT)");
static INSERT_USER: Statement = Statement::new("INSERT INTO users VALUES (?1)");
static MISSING: Statement = Statement::new("SELECT * FROM missing");
static COUNT_FOREVER: Statement = Statement::new(
    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000000000) SELECT count(*) FROM c",
);
static NAMES: Statement = Statement::new("SELECT name FROM users ORDER BY name");
static CREATE_TAGS: Statement = Statement::new("CREATE TABLE tags (name TEXT UNIQUE)");
static INSERT_TAG: Statement = Statement::new("INSERT INTO tags VALUES (?1)");
static COUNT_TAGS: Statement = Statement::new("SELECT count(*) FROM tags");
static COUNT_USERS: Statement = Statement::new("SELECT count(*) FROM users");

/// A typed query: rows are decoded on the client, borrowing the encoded rows.
struct Name<'a>(&'a str);

impl Query for Name<'static> {
    type Row<'a> = Name<'a>;
    const STATEMENT: &'static Statement = &NAMES;
    fn map<'a>(row: &[ValueRef<'a>]) -> Result<Name<'a>, DecodeError> {
        Ok(Name(get_str(row, 0)?))
    }
}

async fn db_thread() {
    let io = Arc::new(RingIo::new().unwrap());
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io.clone(), &db_path(), options).unwrap();
    let config = Config {
        readers: 2,
        max_rows: 100,
        transaction_timeout: TRANSACTION_TIMEOUT,
        ..Config::default()
    };
    let serving = Server::new(&db, &config).unwrap().serve(io);
    match select(serving, select(recv_from_peer(), Sleep::during(TIMEOUT))).await {
        SelectResult::First(()) => unreachable!("serve never returns"),
        SelectResult::Second(SelectResult::First(_stop)) => {}
        SelectResult::Second(SelectResult::Second(())) => panic!("DB thread: timed out"),
    }
}

async fn client_thread() {
    let client = Client::new(DB_THREAD);
    let all = async {
        queries(&client).await;
        transactions(&client).await;
    };
    match select(all, Sleep::during(TIMEOUT)).await {
        SelectResult::First(()) => send_to_peer(DB_THREAD, 0, 0),
        SelectResult::Second(()) => panic!("client: timed out"),
    }
}

async fn queries(client: &Client) {
    let run = |statement: &'static Statement, params: &[ValueRef<'_>]| {
        client.query(Request::new(statement, params))
    };

    let done = run(&CREATE_USERS, &[]).await;
    assert!(matches!(done.body(), Body::Command { .. }));
    for name in ["ada", "bob"] {
        let done = run(&INSERT_USER, &[Value::from_text(name).as_ref()]).await;
        assert!(matches!(
            done.body(),
            Body::Command { changes: 1, .. }
        ));
    }

    let names = client.query_typed::<Name>(&[]).await.unwrap();
    let names: Vec<_> = names.iter().map(|name| name.unwrap().0).collect();
    assert_eq!(names, ["ada", "bob"]);

    let done = run(&MISSING, &[]).await;
    assert!(matches!(done.body(), Body::Error(Error::Turso(_))));

    // Abandoning the future cancels the request on the DB thread, which still
    // signals and moves on to the next one.
    let forever = run(&COUNT_FOREVER, &[]);
    match select(forever, Sleep::during(Duration::from_millis(50))).await {
        SelectResult::First(_) => panic!("the query should still be running"),
        SelectResult::Second(()) => {}
    }
    // Several requests in flight at once: the writes queue for the writer,
    // the reads spread over the readers.
    let (a, b, c, d) = join4(
        run(&INSERT_USER, &[Value::from_text("cy").as_ref()]),
        run(&NAMES, &[]),
        run(&INSERT_USER, &[Value::from_text("dee").as_ref()]),
        run(&NAMES, &[]),
    )
    .await;
    for write in [a, c] {
        assert!(matches!(
            write.body(),
            Body::Command { changes: 1, .. }
        ));
    }
    for read in [b, d] {
        assert!(matches!(read.body(), Body::Rows(_)));
    }

    // Writes in flight together share a batch: a failing one leaves the others
    // alone, and each reports its own result.
    run(&CREATE_TAGS, &[]).await;
    let [x, y, z] = ["x", "y", "z"].map(Value::from_text);
    let tag = |name: &Value| run(&INSERT_TAG, &[name.as_ref()]);
    let (a, b, c, d) = join4(tag(&x), tag(&y), tag(&x), tag(&z)).await;
    let failed = [&a, &b, &c, &d].map(|done| matches!(done.body(), Body::Error(_)));
    assert_eq!(
        failed.iter().filter(|&&f| f).count(),
        1,
        "only the duplicate fails"
    );
    assert!(!failed[1] && !failed[3]);
    let done = run(&COUNT_TAGS, &[]).await;
    let tags = done.body().rows().next().unwrap().get(0).unwrap();
    assert!(matches!(tags, ValueRef::Numeric(Numeric::Integer(3))));

    let done = run(&COUNT_USERS, &[]).await;
    let count = done.body().rows().next().unwrap().get(0).unwrap();
    assert!(matches!(count, ValueRef::Numeric(Numeric::Integer(4))));
}

/// Interactive transactions through their token.
async fn transactions(client: &Client) {
    let count = || async move {
        let done = client.query(Request::new(&COUNT_USERS, &[])).await;
        match done.body().rows().next().unwrap().get(0).unwrap() {
            ValueRef::Numeric(Numeric::Integer(n)) => n,
            other => panic!("not a count: {other:?}"),
        }
    };
    let insert = |name: &str| {
        let name = Value::from_text(name.to_owned());
        Request::new(&INSERT_USER, &[name.as_ref()])
    };
    let before = count().await;

    // Commit: readers see the rows only afterwards.
    let tx = client.begin_write().await.unwrap();
    assert_eq!(
        tx.id().connection(),
        0,
        "write transactions run on the writer"
    );
    let (tx, done) = client.query_in(tx, insert("eve")).await;
    assert!(matches!(
        done.body(),
        Body::Command { changes: 1, .. }
    ));
    // In turso a failed statement leaves the transaction open.
    let (tx, done) = client.query_in(tx, Request::new(&MISSING, &[])).await;
    assert!(matches!(done.body(), Body::Error(_)));
    assert_eq!(count().await, before, "readers don't see uncommitted rows");
    client.commit(tx).await.unwrap();
    assert_eq!(count().await, before + 1);

    // Dropping the token rolls back, and frees the writer for other writes.
    let tx = client.begin_write().await.unwrap();
    let (tx, _) = client.query_in(tx, insert("fay")).await;
    drop(tx);
    let done = client.query(insert("gus")).await;
    assert!(matches!(
        done.body(),
        Body::Command { changes: 1, .. }
    ));
    assert_eq!(count().await, before + 2, "fay rolled back, gus inserted");

    // Read transaction: on a reader, which refuses writes.
    let tx = client.begin_read().await.unwrap();
    assert_ne!(tx.id().connection(), 0, "read transactions run on a reader");
    let (tx, done) = client.query_in(tx, insert("hal")).await;
    assert!(matches!(done.body(), Body::Error(Error::Turso(_))));
    client.rollback(tx).await.unwrap();

    // A request naming a transaction that is over is refused, not run.
    let tx = client.begin_write().await.unwrap();
    let id = tx.id();
    client.commit(tx).await.unwrap();
    let done = client.query(insert("ivy").in_transaction(id)).await;
    assert!(matches!(
        done.body(),
        Body::Error(Error::TransactionEnded)
    ));
    assert_eq!(count().await, before + 2);

    // Idle past the timeout: the DB thread rolls it back and frees the writer.
    let tx = client.begin_write().await.unwrap();
    let (tx, _) = client.query_in(tx, insert("jo")).await;
    Sleep::during(TRANSACTION_TIMEOUT * 3).await;
    let (_tx, done) = client.query_in(tx, insert("kim")).await;
    assert!(matches!(
        done.body(),
        Body::Error(Error::TransactionEnded)
    ));
    let done = client.query(insert("lu")).await;
    assert!(matches!(
        done.body(),
        Body::Command { changes: 1, .. }
    ));
    assert_eq!(count().await, before + 3, "jo rolled back, lu inserted");
}

#[test]
fn serve_requests_from_another_thread() {
    remove_db();
    thread_per_core(|ctx| {
        Box::pin(async move {
            if ctx.num_threads < 2 {
                eprintln!("skipping: need >= 2 cores, have {}", ctx.num_threads);
                return;
            }
            match ctx.thread_id {
                DB_THREAD => db_thread().await,
                CLIENT_THREAD => client_thread().await,
                _ => {}
            }
        })
    });
    remove_db();
}
