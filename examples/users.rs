//! A tour of the client API: worker 0 runs the DB thread, worker 1 creates a
//! table, writes to it, reads it back with a typed query, and runs a
//! transaction. Other workers stay idle.
//!
//! ```text
//! cargo run --example users
//! ```

use std::sync::Arc;

use slopio::runtime::{recv_from_peer, send_to_peer, spawn};
use slopio::thread_per_core;
use slopio_turso::Client;
use slopio_turso::RingIo;
use slopio_turso::Statement;
use slopio_turso::client::query::{DecodeError, Query, get_i64, get_opt_str, get_str};
use slopio_turso::{Config, Server};
use slopio_turso::{Body, Request};
use turso_core::{Database, OpenOptions, SqliteDialect, Value, ValueRef};

const DB_THREAD: usize = 0;
const APP_THREAD: usize = 1;
const PATH: &str = "/tmp/slopio-turso-users.db";

// Every statement is a `static`: its address is its identity, the key of each
// connection's statement cache, so it is prepared once per connection.
static CREATE_USERS: Statement =
    Statement::new("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)");
static INSERT_USER: Statement = Statement::new("INSERT INTO users (name, email) VALUES (?1, ?2)");
static USERS_BY_NAME: Statement = Statement::new("SELECT id, name, email FROM users ORDER BY name");
static COUNT_USERS_LIKE: Statement =
    Statement::new("SELECT count(*) FROM users WHERE name LIKE ?1");

/// One row of the `users` table. `&str` fields borrow the result's rows: no
/// copy, but the row cannot outlive the `Rows` it came from.
struct User<'a> {
    id: i64,
    name: &'a str,
    email: Option<&'a str>,
}

/// The query *is* the row type, tied to its statement.
impl Query for User<'static> {
    type Row<'a> = User<'a>;
    const STATEMENT: &'static Statement = &USERS_BY_NAME;
    fn map<'a>(row: &[ValueRef<'a>]) -> Result<User<'a>, DecodeError> {
        Ok(User {
            id: get_i64(row, 0)?,
            name: get_str(row, 1)?,
            email: get_opt_str(row, 2)?,
        })
    }
}

/// A query with a parameter and a single-column row.
struct Count(i64);

impl Query for Count {
    type Row<'a> = Count;
    const STATEMENT: &'static Statement = &COUNT_USERS_LIKE;
    fn map<'a>(row: &[ValueRef<'a>]) -> Result<Count, DecodeError> {
        Ok(Count(get_i64(row, 0)?))
    }
}

/// Writes return no rows: read `changes` and `last_insert_rowid` instead.
fn insert(name: &str, email: Option<&str>) -> Request {
    let name = Value::from_text(name.to_owned());
    let email = email.map_or(Value::Null, |e| Value::from_text(e.to_owned()));
    Request::new(&INSERT_USER, &[name.as_ref(), email.as_ref()])
}

async fn app() {
    let client = Client::new(DB_THREAD);

    client.query(Request::new(&CREATE_USERS, &[])).await;

    for (name, email) in [("ada", Some("ada@example.com")), ("bob", None)] {
        let done = client.query(insert(name, email)).await;
        if let Body::Command {
            last_insert_rowid, ..
        } = done.body()
        {
            println!("inserted {name} as id {last_insert_rowid}");
        }
    }

    // A transaction: both inserts land together, or neither does.
    let tx = client.begin_write().await.expect("BEGIN failed");
    let (tx, _) = client.query_in(tx, insert("cy", None)).await;
    let (tx, _) = client
        .query_in(tx, insert("dee", Some("dee@example.com")))
        .await;
    client.commit(tx).await.expect("COMMIT failed");

    // A transaction dropped without commit rolls back.
    let tx = client.begin_write().await.expect("BEGIN failed");
    let (tx, _) = client.query_in(tx, insert("eve", None)).await;
    drop(tx);

    // Typed reads: rows are decoded here, on this thread, as they are iterated.
    let users = client
        .query_typed::<User>(&[])
        .await
        .expect("SELECT failed");
    println!("{} users:", users.len());
    for user in users.iter() {
        match user {
            Ok(user) => println!(
                "  #{} {} <{}>",
                user.id,
                user.name,
                user.email.unwrap_or("-")
            ),
            Err(err) => println!("  bad row: {err}"),
        }
    }

    let pattern = Value::from_text("%e%");
    let count = client.query_typed::<Count>(&[pattern.as_ref()]).await;
    let count = count
        .expect("SELECT failed")
        .iter()
        .next()
        .unwrap()
        .unwrap();
    println!("{} names contain an 'e'", count.0);
}

async fn db_thread() {
    let io = Arc::new(RingIo::new().unwrap());
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io.clone(), PATH, options).unwrap();
    let serving = spawn(Server::new(&db, &Config::default()).unwrap().serve(io));
    recv_from_peer().await; // the app is done
    drop(serving);
}

fn remove_files() {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{PATH}{suffix}"));
    }
}

fn main() {
    remove_files();
    thread_per_core(|ctx| {
        Box::pin(async move {
            match ctx.thread_id {
                DB_THREAD => db_thread().await,
                APP_THREAD => {
                    app().await;
                    send_to_peer(DB_THREAD, 0, 0);
                }
                _ => {}
            }
        })
    });
    remove_files();
}
