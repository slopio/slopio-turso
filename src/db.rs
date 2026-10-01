//! The DB thread: owns the database and its one writer.
//!
//! Workers send it a `Connect` over MSG_RING for the database, then push their
//! writes into the shared [`Inbox`] and ring its doorbell over MSG_RING. It
//! opens the database before it reads its messages, so a `Connect` sent during
//! the open simply waits: no queue of our own.
//!
//! Each turn it answers the `Connect`s that came, takes every write in the
//! inbox and runs them on the writer, which commits once its commit interval
//! allows it.

use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use slopio::runtime::recv_msg_from_peer;
use slopio::runtime::select::{SelectResult, select};
use turso_core::{Database, OpenOptions, SqliteDialect, UnixIO};

use crate::Error;
use crate::inbox::Inbox;
use crate::request::{Body, Msg, Request, reply};
use crate::timer::Timer;
use crate::writer::{MAX_BATCH, Writer};

#[derive(Clone, Copy, Debug, Default)]
pub struct Config {
    pub synchronous: Synchronous,
    /// The least time between two commits. Each commit wipes every reader's
    /// page cache, so fewer commits mean faster reads, for writes that may
    /// wait up to this long. Zero commits as soon as the inbox is drained.
    pub commit_interval: Duration,
}

/// What the DB thread shares with every worker.
pub(crate) struct Shared {
    pub(crate) db: Arc<Database>,
    /// Where workers push their writes.
    pub(crate) writes: Inbox,
}

/// When a commit is on disk. `OFF`, which can corrupt the database on a power
/// loss, is not offered.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Synchronous {
    /// Every commit waits for an fsync of the WAL: nothing is lost.
    #[default]
    Full,
    /// Only checkpoints fsync: a crash of the process loses nothing, a power
    /// loss may lose the last commits but never corrupts the database.
    Normal,
}

/// Opens the database at `path`, then serves the workers' requests. Run it as
/// the root future of the DB thread; it never returns.
///
/// If the open fails, every request is answered with its error.
pub async fn serve(path: &str, config: Config) {
    let mut opened = open(path, &config);
    let mut timer = Timer::new();
    let mut writes = Vec::with_capacity(MAX_BATCH);
    loop {
        let commit_at = match &opened {
            Ok((_, writer)) => writer.commit_at(),
            Err(_) => None,
        };
        // A request, or `None` once the open transaction is due.
        let first = match commit_at {
            None => Some(recv().await),
            Some(at) if at <= Instant::now() => None,
            Some(at) => match select(recv(), timer.until(at)).await {
                SelectResult::First(msg) => Some(msg),
                SelectResult::Second(()) => None,
            },
        };
        if let Some(msg) = first {
            take(&opened, msg, &mut writes);
            drain(&opened, &mut writes).await;
        }
        match &mut opened {
            Ok((shared, writer)) => {
                shared.writes.take(&mut writes);
                writer.write(&mut writes);
                if writer.commit_at().is_some_and(|at| at <= Instant::now()) {
                    writer.commit();
                }
            }
            // No inbox was handed out: no writes.
            Err(_) => {}
        }
    }
}

/// What the workers share, and the writer.
type Opened = Result<(Arc<Shared>, Writer), Error>;

fn open(path: &str, config: &Config) -> Opened {
    let io = Arc::new(UnixIO::new()?);
    let db = Database::open(io, path, OpenOptions::new(Arc::new(SqliteDialect)))?;
    let shared = Arc::new(Shared {
        db,
        writes: Inbox::new(),
    });
    let writer = Writer::new(&shared, config)?;
    Ok((shared, writer))
}

/// Takes every message already sent. The runtime only moves so many arrivals
/// into the thread's inbox per turn: while some came, yield to let it move the
/// next ones.
async fn drain(opened: &Opened, writes: &mut Vec<Msg>) {
    loop {
        let mut came = false;
        while writes.len() < MAX_BATCH
            && let Some(msg) = try_recv().await
        {
            take(opened, msg, writes);
            came = true;
        }
        if !came || writes.len() >= MAX_BATCH {
            return;
        }
        slopio::yield_now().await;
    }
}

/// Answers a `Connect` at once; a doorbell only woke the thread, the writes
/// are in the inbox.
fn take(opened: &Opened, msg: Msg, writes: &mut Vec<Msg>) {
    match msg.payload.body() {
        Body::Connect => {
            let shared = opened.as_ref().map(|(shared, _)| shared.clone());
            reply(msg, Body::Opened(shared.map_err(Error::clone)));
        }
        Body::Doorbell => {}
        Body::Write { .. } => writes.push(msg),
        Body::Opened(_) | Body::Written(_) => unreachable!("an answer sent as a request"),
    }
}

async fn recv() -> Msg {
    // SAFETY: the DB thread only ever receives `Message<Request>`.
    unsafe { recv_msg_from_peer::<Request>() }.await
}

/// The next request if one is already in the inbox, without waiting.
async fn try_recv() -> Option<Msg> {
    let mut recv = pin!(recv());
    poll_fn(|cx| {
        Poll::Ready(match recv.as_mut().poll(cx) {
            Poll::Ready(msg) => Some(msg),
            Poll::Pending => None,
        })
    })
    .await
}
