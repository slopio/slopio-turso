//! The DB thread: owns the database and its one writer.
//!
//! Workers send it [`Request`]s over MSG_RING: a `Connect` for the database,
//! then their writes. It opens the database before it reads its inbox, so a
//! request sent during the open simply waits there: no queue of our own.
//!
//! Each turn it takes every request already in its inbox: a `Connect` is
//! answered at once, the writes run on the writer, which commits once its
//! commit interval allows it.

use std::future::poll_fn;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use slopio::runtime::recv_msg_from_peer;
use slopio::runtime::select::{SelectResult, select};
use turso_core::{Database, OpenOptions, SqliteDialect, UnixIO};

use crate::Error;
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
            Ok((_, writer)) => {
                writer.write(&mut writes);
                if writer.commit_at().is_some_and(|at| at <= Instant::now()) {
                    writer.commit();
                }
            }
            Err(error) => {
                for msg in writes.drain(..) {
                    reply(msg, Body::Written(Err(error.clone())));
                }
            }
        }
    }
}

fn open(path: &str, config: &Config) -> Result<(Arc<Database>, Writer), Error> {
    let io = Arc::new(UnixIO::new()?);
    let db = Database::open(io, path, OpenOptions::new(Arc::new(SqliteDialect)))?;
    let writer = Writer::new(&db, config)?;
    Ok((db, writer))
}

/// Takes every request already sent, up to a batch of writes. The runtime only
/// moves so many arrivals into the inbox per turn: while some came, yield to
/// let it move the next ones.
async fn drain(opened: &Result<(Arc<Database>, Writer), Error>, writes: &mut Vec<Msg>) {
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

/// Answers a `Connect` at once; keeps a write for the writer.
fn take(opened: &Result<(Arc<Database>, Writer), Error>, msg: Msg, writes: &mut Vec<Msg>) {
    match msg.payload.body() {
        Body::Connect => {
            let db = opened.as_ref().map(|(db, _)| db.clone());
            reply(msg, Body::Opened(db.map_err(Error::clone)));
        }
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
