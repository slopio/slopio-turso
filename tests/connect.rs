//! A client gets the database from the DB thread, or the error its open
//! failed with.
//!
//! Thread 0 serves until thread 1 sends it a raw peer message (an inbox apart
//! from the typed requests). Waits are bounded so a lost wakeup fails the test
//! instead of hanging it.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_from_peer, send_to_peer};
use slopio::{RingOptions, Threads, Topology};
use slopio_turso::{Client, Error};
use turso_core::Database;

const DB_THREAD: usize = 0;
const TIMEOUT: Duration = Duration::from_secs(10);

type Connected = Result<Arc<Database>, Error>;

/// Runs the DB thread on `path` and a client that connects twice, and hands
/// back what both connects got.
fn connect_twice(path: &str) -> (Connected, Connected) {
    let path = path.to_owned();
    let (tx, rx) = mpsc::channel();
    // No DEFER_TASKRUN: a busy thread still gets its messages promptly.
    let ring = RingOptions {
        defer_taskrun: false,
        ..RingOptions::default()
    };
    let mut topology = Topology::new();
    let db_thread = topology.group::<(), _, _>(Threads::Auto(1), ring, move |_| {
        let path = path.clone();
        async move {
            let stop = select(recv_from_peer(), Sleep::during(TIMEOUT));
            match select(slopio_turso::db::serve(&path, Default::default()), stop).await {
                SelectResult::First(()) => unreachable!("serve never returns"),
                SelectResult::Second(SelectResult::First(_stop)) => {}
                SelectResult::Second(SelectResult::Second(())) => panic!("DB thread: timed out"),
            }
        }
    });
    assert_eq!(db_thread.start, DB_THREAD);
    topology.group::<(), _, _>(Threads::Auto(1), ring, move |_| {
        let tx = tx.clone();
        async move {
            let first = connect().await;
            let second = connect().await;
            send_to_peer(DB_THREAD, 0, 0);
            tx.send((first, second)).unwrap();
        }
    });
    topology.run();
    rx.recv().unwrap()
}

async fn connect() -> Connected {
    match select(Client::connect(DB_THREAD), Sleep::during(TIMEOUT)).await {
        SelectResult::First(client) => client.map(|c| c.database().clone()),
        SelectResult::Second(()) => panic!("client: timed out"),
    }
}

#[test]
fn every_client_gets_the_same_database() {
    let path = format!("/tmp/slopio-turso-connect-{}.db", std::process::id());
    let (first, second) = connect_twice(&path);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{path}{suffix}"));
    }
    assert!(Arc::ptr_eq(&first.unwrap(), &second.unwrap()));
}

#[test]
fn every_client_gets_the_open_error() {
    let (first, second) = connect_twice("/nonexistent/slopio-turso.db");
    assert!(first.is_err());
    assert!(second.is_err());
}
