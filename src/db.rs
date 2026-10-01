//! The DB thread: owns the database and hands it to the clients that ask.
//!
//! A client sends a [`Connect`] and awaits a wakeup. The DB thread opens the
//! database before it reads its inbox, so a `Connect` sent during the open
//! simply waits in the inbox: no queue of our own.

use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use slopio::runtime::{recv_msg_from_peer, signal_reply_to_peer};
use turso_core::{Database, LimboError, OpenOptions, SqliteDialect, UnixIO};

/// The database, or why it could not be opened.
pub(crate) type Opened = Result<Arc<Database>, LimboError>;

/// A client's request for the database.
///
/// Shared between the two threads: the client keeps a clone of the `Arc` and
/// touches only `cancelled`; the DB thread writes `answer`, then drops its clone,
/// then wakes the client. The wakeup is the client's proof that nobody else
/// holds the request anymore.
pub(crate) struct Connect {
    cancelled: AtomicBool,
    answer: UnsafeCell<Option<Opened>>,
}

impl Connect {
    pub(crate) fn new() -> Self {
        Connect {
            cancelled: AtomicBool::new(false),
            answer: UnsafeCell::new(None),
        }
    }

    /// The client no longer waits for the answer.
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// The answer, once the request is back in the client's hands alone.
    pub(crate) fn into_answer(self) -> Option<Opened> {
        self.answer.into_inner()
    }
}

/// Opens the database at `path`, then answers every [`Connect`] with it, or
/// with the error the open failed with. Run it as the root future of the DB
/// thread; it never returns.
pub async fn serve(path: &str) {
    let opened = open(path);
    loop {
        // SAFETY: the DB thread only ever receives `Message<Connect>`.
        let msg = unsafe { recv_msg_from_peer::<Connect>() }.await;
        let header = msg.header;
        if !msg.payload.cancelled.load(Ordering::Relaxed) {
            // SAFETY: until the wakeup below, the client never touches `answer`.
            unsafe { *msg.payload.answer.get() = Some(opened.clone()) };
        }
        drop(msg);
        // Sent even when cancelled: the client's reply slot stays reserved
        // until a wakeup lands in it.
        signal_reply_to_peer(&header, 0);
    }
}

fn open(path: &str) -> Opened {
    let io = Arc::new(UnixIO::new()?);
    Database::open(io, path, OpenOptions::new(Arc::new(SqliteDialect)))
}
