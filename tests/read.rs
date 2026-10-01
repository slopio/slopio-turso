//! Reads decoded into structs, and the reads that fail, all in one batch: a
//! failing read gets its error and the others their rows.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_from_peer, send_to_peer};
use slopio::{RingOptions, Threads, Topology};
use slopio_turso::{Client, Error, FromRow, Read, Row};
use turso_core::{Database, Numeric, OpenOptions, SqliteDialect, UnixIO, Value};

const DB_THREAD: usize = 0;
const TIMEOUT: Duration = Duration::from_secs(10);

static GET_USER: Read = Read::new("SELECT id, name FROM users WHERE id = ?1");
static ALL_USERS: Read = Read::new("SELECT id, name FROM users ORDER BY id");
static MISSING: Read = Read::new("SELECT * FROM missing");
static BAD_JSON: Read = Read::new("SELECT json_extract('{oops', '$'), 'x'");

#[derive(Debug, PartialEq)]
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

/// Reads a column the statements do not have.
#[derive(Debug)]
struct Third;

impl FromRow for Third {
    fn from_row(row: &Row<'_>) -> Result<Self, Error> {
        row.value(2)?;
        Ok(Third)
    }
}

fn id(id: i64) -> Vec<Value> {
    vec![Value::Numeric(Numeric::Integer(id))]
}

fn user(id: i64, name: &str) -> User {
    User {
        id,
        name: name.to_owned(),
    }
}

#[test]
fn each_read_of_a_batch_gets_its_own_answer() {
    let path = format!("/tmp/slopio-turso-read-{}.db", std::process::id());
    seed(&path);

    let (tx, rx) = mpsc::channel();
    // No DEFER_TASKRUN: a busy thread still gets its messages promptly.
    let ring = RingOptions {
        defer_taskrun: false,
        ..RingOptions::default()
    };
    let mut topology = Topology::new();
    let db_path = path.clone();
    topology.group::<(), _, _>(Threads::Auto(1), ring, move |_| {
        let db_path = db_path.clone();
        async move {
            let stop = select(recv_from_peer(), Sleep::during(TIMEOUT));
            match select(slopio_turso::db::serve(&db_path, Default::default()), stop).await {
                SelectResult::First(()) => unreachable!("serve never returns"),
                SelectResult::Second(SelectResult::First(_stop)) => {}
                SelectResult::Second(SelectResult::Second(())) => panic!("DB thread: timed out"),
            }
        }
    });
    topology.group::<(), _, _>(Threads::Auto(1), ring, move |_| {
        let tx = tx.clone();
        async move {
            let client = Client::connect(DB_THREAD).await.unwrap();
            // Every read is spawned before any runs: they reach the reader together.
            let c = client.clone();
            let one = slopio::spawn(async move { c.read_one::<User>(&GET_USER, id(2)).await });
            let c = client.clone();
            let none = slopio::spawn(async move { c.read_one::<User>(&GET_USER, id(9)).await });
            let c = client.clone();
            let all = slopio::spawn(async move { c.read::<User>(&ALL_USERS, vec![]).await });
            let c = client.clone();
            let too_many =
                slopio::spawn(async move { c.read_one::<User>(&ALL_USERS, vec![]).await });
            let c = client.clone();
            let no_param =
                slopio::spawn(async move { c.read_one::<User>(&GET_USER, vec![]).await });
            let c = client.clone();
            let two_params = slopio::spawn(async move {
                let params = vec![id(1).remove(0), id(1).remove(0)];
                c.read_one::<User>(&GET_USER, params).await
            });
            let c = client.clone();
            let no_column = slopio::spawn(async move { c.read::<Third>(&ALL_USERS, vec![]).await });
            let c = client.clone();
            let missing = slopio::spawn(async move { c.read::<User>(&MISSING, vec![]).await });
            let c = client.clone();
            let bad_json = slopio::spawn(async move { c.read::<User>(&BAD_JSON, vec![]).await });
            let c = client.clone();
            let after = slopio::spawn(async move { c.read_one::<User>(&GET_USER, id(3)).await });

            let work = async {
                let one = one.await.unwrap();
                assert_eq!(one, Some(user(2, "b")));
                assert_eq!(none.await.unwrap(), None);
                let all = all.await.unwrap();
                assert_eq!(all, vec![user(1, "a"), user(2, "b"), user(3, "c")]);
                assert!(matches!(too_many.await, Err(Error::TooManyRows)));
                let no_param = no_param.await;
                assert!(matches!(
                    no_param,
                    Err(Error::Parameters {
                        expected: 1,
                        given: 0
                    })
                ));
                let two_params = two_params.await;
                assert!(matches!(
                    two_params,
                    Err(Error::Parameters {
                        expected: 1,
                        given: 2
                    })
                ));
                assert!(matches!(no_column.await, Err(Error::NoColumn(2))));
                assert!(matches!(missing.await, Err(Error::Turso(_))));
                assert!(matches!(bad_json.await, Err(Error::Turso(_))));
                // turso rolled back the shared transaction on that error; the rest
                // of the batch still reads.
                assert_eq!(after.await.unwrap(), Some(user(3, "c")));
            };
            let finished = matches!(
                select(work, Sleep::during(TIMEOUT)).await,
                SelectResult::First(())
            );
            send_to_peer(DB_THREAD, 0, 0);
            tx.send(finished).unwrap();
        }
    });
    topology.run();
    remove(&path);
    assert!(rx.recv().unwrap(), "client: timed out");
}

fn seed(path: &str) {
    remove(path);
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(Arc::new(UnixIO::new().unwrap()), path, options).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute("INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .unwrap();
}

fn remove(path: &str) {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
}
