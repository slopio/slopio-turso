//! The DB thread: receives requests from other threads over MSG_RING and runs
//! them on a turso connection, one at a time.
//!
//! This is the other half of the handoff protocol in [`crate::pending`].

use std::sync::Arc;

use slopio::io_driver::Message;
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_msg_from_peer, signal_reply_to_peer};
use turso_core::{Connection, IO};

use crate::conn::{Conn, Step};
use crate::pending::Pending;

/// Serves requests forever. Run it as the root future of the DB thread.
///
/// `io` must be the backend `conn`'s database was opened with. Waiting on disk
/// blocks the thread (turso's backends wait inside `IO::step`); requests that
/// arrive meanwhile queue up in the inbox.
pub async fn serve(io: Arc<dyn IO>, conn: Arc<Connection>, max_rows: usize) {
    let mut conn = Conn::new(conn, max_rows);
    loop {
        // SAFETY: the DB thread only ever receives `Message<Pending>` — the
        // requests `Client`s send it.
        let msg = unsafe { recv_msg_from_peer::<Pending>() }.await;
        if !msg.payload.is_cancelled() {
            run(&mut conn, io.as_ref(), &msg.payload).await;
        }
        reply(msg);
    }
}

/// Runs one request to completion on `conn`.
async fn run(conn: &mut Conn, io: &dyn IO, pending: &Pending) {
    // SAFETY: rule 2 — sole access from here until `reply` signals.
    let request = unsafe { pending.request_mut() };
    let mut step = conn.start(request);
    loop {
        match step {
            Step::Done => return,
            Step::Ready => {}
            Step::Io => {
                if let Err(err) = io.step() {
                    conn.abort(request, err);
                    return;
                }
            }
            Step::Sleep(duration) => Sleep::during(duration).await,
        }
        step = conn.step(request, pending.cancel_flag());
    }
}

/// Wakes the caller. We release our clone *before* signalling, so the caller
/// wakes as the last holder and can take its request back out whole (rule 4).
/// A cancelled request is signalled too: that is what frees the caller's slot.
fn reply(msg: Arc<Message<Pending>>) {
    let header = msg.header;
    drop(msg);
    signal_reply_to_peer(&header, 0);
}
