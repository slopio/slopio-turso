//! Writes cross to the DB thread's writer; each answer tells the truth: a write
//! answered `Ok` is in the database, one answered `Err` is not, even when
//! another write of its batch failed. A read after a write sees it.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_from_peer, send_to_peer};
use slopio::{Cpu, Dedicated, Topology};
use slopio_turso::{Client, Error, FromRow, Read, Row, Write};
use turso_core::{Database, Numeric, OpenOptions, SqliteDialect, UnixIO, Value};

const DB_THREAD: usize = 0;
const TIMEOUT: Duration = Duration::from_secs(10);

static INSERT_USER: Write = Write::new("INSERT INTO users (id, name) VALUES (?1, ?2)");
static GET_USER: Read = Read::new("SELECT id, name FROM users WHERE id = ?1");

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
    let anywhere = Dedicated {
        cpu: Cpu::Auto,
        sqpoll: None,
    };
    let mut topology = Topology::new();
    let db_path = path.clone();
    topology.dedicated::<(), _, _>(anywhere, move |_| async move {
        let stop = select(recv_from_peer(), Sleep::during(TIMEOUT));
        match select(slopio_turso::db::serve(&db_path, Default::default()), stop).await {
            SelectResult::First(()) => unreachable!("serve never returns"),
            SelectResult::Second(SelectResult::First(_stop)) => {}
            SelectResult::Second(SelectResult::Second(())) => panic!("DB thread: timed out"),
        }
    });
    topology.dedicated::<(), _, _>(anywhere, move |_| async move {
        let client = Client::connect(DB_THREAD).await.unwrap();
        let work = async {
            // A write, then a read of it from the same task.
            let name = Value::build_text("alice");
            client
                .write(&INSERT_USER, vec![int(10), name])
                .await
                .unwrap();
            let alice = client.read_one::<User>(&GET_USER, vec![int(10)]).await;
            assert_eq!(alice.unwrap().unwrap().name, "alice");

            // Sent together, so they likely share a batch; id 1 already exists.
            let writes: Vec<_> = [20, 1, 21]
                .into_iter()
                .map(|id| {
                    let c = client.clone();
                    let name = Value::build_text(format!("user{id}-new"));
                    slopio::spawn(
                        async move { (id, c.write(&INSERT_USER, vec![int(id), name]).await) },
                    )
                })
                .collect();
            for write in writes {
                let (id, written) = write.await;
                let row = client
                    .read_one::<User>(&GET_USER, vec![int(id)])
                    .await
                    .unwrap();
                let new = row.is_some_and(|user| user.name.ends_with("-new"));
                assert_eq!(written.is_ok(), new, "write {id} answered {written:?}");
                if id == 1 {
                    assert!(matches!(written, Err(Error::Turso(_))), "{written:?}");
                }
            }
        };
        let finished = matches!(
            select(work, Sleep::during(TIMEOUT)).await,
            SelectResult::First(())
        );
        send_to_peer(DB_THREAD, 0, 0);
        tx.send(finished).unwrap();
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
