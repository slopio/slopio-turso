//! The DB thread on slopio: receives requests from other threads over
//! MSG_RING, runs them on the [`Engine`], and wakes each caller once its
//! request is answered. Sleeps only once nothing can run, until a request
//! arrives, the disk answers, or a deadline passes.
//!
//! This is the other half of the handoff protocol in [`crate::shared::request`].

use std::future::{self, poll_fn};
use std::os::fd::RawFd;
use std::pin::pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use io_uring::opcode;
use io_uring::types::Fd;
use slopio::io_driver::operations::OneshotToken;
use slopio::io_driver::{IO_DRIVER, Message};
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::time::Sleep;
use slopio::runtime::{recv_msg_from_peer, signal_reply_to_peer, yield_now};
use turso_core::{Database, LimboError};

use crate::db::engine::{Config, Engine, Handle, Next};
use crate::db::io::RingIo;
use crate::shared::request::Request;

type Msg = Arc<Message<Request>>;

/// The resolution of slopio's timers.
const TIMER_RESOLUTION: Duration = Duration::from_millis(10);

impl Handle for Msg {
    fn request(&self) -> &Request {
        &self.payload
    }
}

pub struct Server {
    engine: Engine<Msg>,
}

impl Server {
    pub fn new(db: &Arc<Database>, config: &Config) -> Result<Self, LimboError> {
        Ok(Server {
            engine: Engine::new(db, config)?,
        })
    }

    /// Serves requests forever. Run it as the root future of the DB thread.
    ///
    /// `io` must be the backend the database was opened with.
    pub async fn serve(mut self, io: Arc<RingIo>) {
        let mut disk = DiskWatch::new(io.ring_fd());
        let mut done = Vec::new();
        loop {
            // Let the runtime process its completions, so requests sent since
            // the last turn reach the inbox — and the replies queued last turn
            // leave — then take them all.
            yield_now().await;
            while let Some(msg) = try_recv().await {
                self.engine.submit(msg);
            }
            self.engine.turn(&io, Instant::now(), &mut done);
            done.drain(..).for_each(reply);
            let Next::Sleep { deadline, disk: on_disk } = self.engine.next_wake(Instant::now())
            else {
                continue;
            };
            // slopio's timers tick every 10ms: a deadline closer than that
            // would be served up to a tick late. Turn again until it passes.
            if deadline.is_some_and(|d| d <= Instant::now() + TIMER_RESOLUTION) {
                continue;
            }
            if let Some(msg) = recv_until(deadline, disk.readable(on_disk)).await {
                self.engine.submit(msg);
            }
        }
    }
}

async fn recv() -> Msg {
    // SAFETY: the DB thread only ever receives `Message<Request>` — the
    // requests `Client`s send it.
    unsafe { recv_msg_from_peer::<Request>() }.await
}

/// The next request, or `None` once `disk` fires or `deadline` passes.
async fn recv_until(deadline: Option<Instant>, disk: impl Future<Output = ()>) -> Option<Msg> {
    let deadline = async {
        match deadline {
            Some(deadline) => {
                Sleep::during(deadline.saturating_duration_since(Instant::now())).await
            }
            None => future::pending().await,
        }
    };
    match select(recv(), select(disk, deadline)).await {
        SelectResult::First(msg) => Some(msg),
        SelectResult::Second(_) => None,
    }
}

/// Watches the turso ring from slopio's: its fd turns readable once the disk
/// has finished something. The watch stays armed across the sleeps it did not
/// end, and is re-armed only once it fired.
struct DiskWatch {
    fd: RawFd,
    armed: Option<OneshotToken>,
}

impl DiskWatch {
    fn new(fd: RawFd) -> Self {
        DiskWatch { fd, armed: None }
    }

    /// Fires once the ring has completions; never, if `watch` is false.
    async fn readable(&mut self, watch: bool) {
        if !watch {
            return future::pending().await;
        }
        let fd = self.fd;
        let token = *self.armed.get_or_insert_with(|| {
            let poll = opcode::PollAdd::new(Fd(fd), libc::POLLIN as u32).build();
            IO_DRIVER.with(|d| d.push_op(poll))
        });
        poll_fn(|cx| IO_DRIVER.with(|d| d.poll_op(token, cx.waker()))).await;
        self.armed = None;
    }
}

impl Drop for DiskWatch {
    fn drop(&mut self) {
        if let Some(token) = self.armed.take() {
            IO_DRIVER.try_with(|d| {
                if let Some(d) = d {
                    d.cancel_single_op(token, None);
                }
            });
        }
    }
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

/// Wakes the caller. We release our clone *before* signalling, so the caller
/// wakes as the last holder and can take its request back out whole (rule 4).
/// A cancelled request is signalled too: that is what frees the caller's slot.
fn reply(msg: Msg) {
    let header = msg.header;
    drop(msg);
    signal_reply_to_peer(&header, 0);
}
