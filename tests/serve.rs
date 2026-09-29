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
use slopio_turso::client::Client;
use slopio_turso::request::{Error, Request, ResultSet};
use slopio_turso::server::Server;
use turso_core::{Database, MemoryIO, Numeric, OpenOptions, SqliteDialect, Value, ValueRef};

const DB_THREAD: usize = 0;
const CLIENT_THREAD: usize = 1;
const TIMEOUT: Duration = Duration::from_secs(10);

async fn db_thread() {
    let io = Arc::new(MemoryIO::new());
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io.clone(), "serve.db", options).unwrap();
    let serving = Server::new(&db, 2, 100).unwrap().serve(io);
    match select(serving, select(recv_from_peer(), Sleep::during(TIMEOUT))).await {
        SelectResult::First(()) => unreachable!("serve never returns"),
        SelectResult::Second(SelectResult::First(_stop)) => {}
        SelectResult::Second(SelectResult::Second(())) => panic!("DB thread: timed out"),
    }
}

async fn client_thread() {
    let all = async {
        queries().await;
        transactions().await;
    };
    match select(all, Sleep::during(TIMEOUT)).await {
        SelectResult::First(()) => send_to_peer(DB_THREAD, 0, 0),
        SelectResult::Second(()) => panic!("client: timed out"),
    }
}

async fn queries() {
    let client = Client::new(DB_THREAD);
    let run =
        |name, sql, params: &[ValueRef<'_>]| client.query(Request::prepared(name, sql, params));

    let done = run("create", "CREATE TABLE users (name TEXT)", &[]).await;
    assert!(matches!(done.result(), ResultSet::Command { .. }));
    for name in ["ada", "bob"] {
        let done = run(
            "insert",
            "INSERT INTO users VALUES (?1)",
            &[Value::from_text(name).as_ref()],
        )
        .await;
        assert!(matches!(
            done.result(),
            ResultSet::Command { changes: 1, .. }
        ));
    }

    let done = run("names", "SELECT name FROM users ORDER BY name", &[]).await;
    let names: Vec<_> = done
        .rows_iter()
        .map(|row| row[0].to_text().unwrap().to_owned())
        .collect();
    assert_eq!(names, ["ada", "bob"]);

    let done = run("missing", "SELECT * FROM missing", &[]).await;
    assert!(matches!(done.result(), ResultSet::Error(Error::Turso(_))));

    // Abandoning the future cancels the request on the DB thread, which still
    // signals and moves on to the next one.
    let forever = run(
        "forever",
        "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000000000) SELECT count(*) FROM c",
        &[],
    );
    match select(forever, Sleep::during(Duration::from_millis(50))).await {
        SelectResult::First(_) => panic!("the query should still be running"),
        SelectResult::Second(()) => {}
    }
    // Several requests in flight at once: the writes queue for the writer,
    // the reads spread over the readers.
    let (a, b, c, d) = join4(
        run(
            "insert",
            "INSERT INTO users VALUES (?1)",
            &[Value::from_text("cy").as_ref()],
        ),
        run("names", "SELECT name FROM users ORDER BY name", &[]),
        run(
            "insert",
            "INSERT INTO users VALUES (?1)",
            &[Value::from_text("dee").as_ref()],
        ),
        run("names", "SELECT name FROM users ORDER BY name", &[]),
    )
    .await;
    for write in [a, c] {
        assert!(matches!(
            write.result(),
            ResultSet::Command { changes: 1, .. }
        ));
    }
    for read in [b, d] {
        assert!(matches!(read.result(), ResultSet::Rows(_)));
    }

    let done = run("count", "SELECT count(*) FROM users", &[]).await;
    let count = done.rows_iter().next().unwrap()[0];
    assert!(matches!(count, ValueRef::Numeric(Numeric::Integer(4))));
}

/// Interactive transactions, driven by hand: the id comes back on the `BEGIN`
/// and is stamped on every request that belongs to it.
async fn transactions() {
    let client = Client::new(DB_THREAD);
    let query = |name, sql, params: &[ValueRef<'_>]| Request::prepared(name, sql, params);
    let count = || async move {
        let done = client
            .query(query("count", "SELECT count(*) FROM users", &[]))
            .await;
        match done.rows_iter().next().unwrap()[0] {
            ValueRef::Numeric(Numeric::Integer(n)) => n,
            other => panic!("not a count: {other:?}"),
        }
    };
    let eve = Value::from_text("eve");
    let insert_eve = || query("insert", "INSERT INTO users VALUES (?1)", &[eve.as_ref()]);
    let before = count().await;

    // Write transaction: pinned to the writer.
    let begin = client
        .query(query("begin_write", "BEGIN IMMEDIATE", &[]))
        .await;
    let id = begin
        .transaction()
        .expect("BEGIN IMMEDIATE opened a transaction");
    assert_eq!(id.connection(), 0);
    let done = client.query(insert_eve().in_transaction(id)).await;
    assert!(matches!(
        done.result(),
        ResultSet::Command { changes: 1, .. }
    ));
    assert_eq!(count().await, before, "readers don't see uncommitted rows");
    let done = client
        .query(query("commit", "COMMIT", &[]).in_transaction(id))
        .await;
    assert!(matches!(done.result(), ResultSet::Command { .. }));
    assert_eq!(count().await, before + 1);

    // Over: its id is refused rather than run outside the transaction.
    let done = client.query(insert_eve().in_transaction(id)).await;
    assert!(matches!(
        done.result(),
        ResultSet::Error(Error::TransactionEnded)
    ));

    // Read transaction: pinned to a reader, which refuses writes.
    let begin = client.query(query("begin_read", "BEGIN", &[])).await;
    let id = begin.transaction().expect("BEGIN opened a transaction");
    assert_ne!(id.connection(), 0);
    let done = client.query(insert_eve().in_transaction(id)).await;
    assert!(matches!(done.result(), ResultSet::Error(Error::Turso(_))));
    let done = client
        .query(query("rollback", "ROLLBACK", &[]).in_transaction(id))
        .await;
    assert!(matches!(done.result(), ResultSet::Command { .. }));
    assert_eq!(count().await, before + 1);
}

#[test]
fn serve_requests_from_another_thread() {
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
}
