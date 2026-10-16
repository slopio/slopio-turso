//! Writes cross to the DB thread's writer; each answer tells the truth: a write
//! answered `Ok` is in the database, one answered `Err` is not, even when
//! another write of its batch failed. A read after a write sees it. A
//! transaction lands whole or not at all, and only its own writes with it. A
//! write with `RETURNING` answers its own rows.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_from_peer, send_to_peer};
use slopio::{RingOptions, Threads, Topology};
use slopio_turso::{Client, Error, FromRow, Read, Row, Write, read_sql, write_sql};
use turso_core::{Database, Numeric, OpenOptions, SqliteDialect, UnixIO, Value};

const DB_THREAD: usize = 0;
const TIMEOUT: Duration = Duration::from_secs(10);

static INSERT_USER: Write = write_sql!("INSERT INTO users (id, name) VALUES (?1, ?2)");
static GET_USER: Read<Option<User>> = read_sql!("SELECT id, name FROM users WHERE id = ?1");
static ADD_USER: Write<Option<User>> =
    write_sql!("INSERT INTO users (name) VALUES (?1) RETURNING id, name");
static SHOUT: Write<Vec<User>> =
    write_sql!("UPDATE users SET name = upper(name) WHERE id IN (40, 41) RETURNING id, name");

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

fn int(i: i64) -> Value {
    Value::Numeric(Numeric::Integer(i))
}

#[test]
fn every_write_is_answered_with_what_happened() {
    let path = format!("/tmp/slopio-turso-write-{}.db", std::process::id());
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
            // SAFETY: the first group, thread DB_THREAD, runs `serve`.
            let client = unsafe { Client::connect(DB_THREAD) }.await.unwrap();
            let work = async {
                // A write, then a read of it from the same task.
                let name = Value::build_text("alice");
                client
                    .write(&INSERT_USER, vec![int(10), name])
                    .await
                    .unwrap();
                let alice = client.read(&GET_USER, vec![int(10)]).await;
                assert_eq!(alice.unwrap().unwrap().name, "alice");

                // Sent together, so they likely share a batch; id 1 already exists.
                let writes: Vec<_> = [20, 1, 21]
                    .into_iter()
                    .map(|id| {
                        let c = client.clone();
                        let name = Value::build_text(format!("user{id}-new"));
                        slopio::spawn(async move {
                            (id, c.write(&INSERT_USER, vec![int(id), name]).await)
                        })
                    })
                    .collect();
                for write in writes {
                    let (id, written) = write.await;
                    let row = client.read(&GET_USER, vec![int(id)]).await.unwrap();
                    let new = row.is_some_and(|user| user.name.ends_with("-new"));
                    assert_eq!(written.is_ok(), new, "write {id} answered {written:?}");
                    if id == 1 {
                        assert!(matches!(written, Err(Error::Turso(_))), "{written:?}");
                    }
                }

                // A transaction whose second write fails, id 2 existing, and a
                // write sent with it, likely in the same batch.
                let c = client.clone();
                let failed = slopio::spawn(async move {
                    c.transaction(vec![
                        (&INSERT_USER, vec![int(30), Value::build_text("in tx")]),
                        (&INSERT_USER, vec![int(2), Value::build_text("in tx")]),
                    ])
                    .await
                });
                let c = client.clone();
                let neighbour = slopio::spawn(async move {
                    let name = Value::build_text("neighbour");
                    c.write(&INSERT_USER, vec![int(31), name]).await
                });
                let failed = failed.await;
                assert!(matches!(failed, Err(Error::Turso(_))), "{failed:?}");
                neighbour.await.unwrap();
                let undone = client.read(&GET_USER, vec![int(30)]).await;
                assert_eq!(undone.unwrap(), None);
                let kept = client.read(&GET_USER, vec![int(31)]).await;
                assert_eq!(kept.unwrap().unwrap().name, "neighbour");

                // One that succeeds lands whole.
                client
                    .transaction(vec![
                        (&INSERT_USER, vec![int(40), Value::build_text("first")]),
                        (&INSERT_USER, vec![int(41), Value::build_text("second")]),
                    ])
                    .await
                    .unwrap();
                for id in [40, 41] {
                    let row = client.read(&GET_USER, vec![int(id)]).await;
                    assert!(row.unwrap().is_some(), "transaction write {id}");
                }
                assert!(client.transaction(vec![]).await.is_ok());

                // Sent together, likely in one batch: each gets its own row back.
                let adds: Vec<_> = ["dora", "emil"]
                    .into_iter()
                    .map(|name| {
                        let c = client.clone();
                        slopio::spawn(async move {
                            let added = c.write(&ADD_USER, vec![Value::build_text(name)]).await;
                            (name, added.unwrap().unwrap())
                        })
                    })
                    .collect();
                let mut ids = Vec::new();
                for add in adds {
                    let (name, added) = add.await;
                    assert_eq!(added.name, name);
                    let row = client.read(&GET_USER, vec![int(added.id)]).await;
                    ids.push(added.id);
                    assert_eq!(row.unwrap(), Some(added));
                }
                assert_ne!(ids[0], ids[1]);

                // One row back per row changed.
                let mut shouted = client.write(&SHOUT, vec![]).await.unwrap();
                shouted.sort_by_key(|user| user.id);
                assert_eq!(
                    shouted,
                    [
                        User {
                            id: 40,
                            name: "FIRST".into()
                        },
                        User {
                            id: 41,
                            name: "SECOND".into()
                        },
                    ]
                );
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
