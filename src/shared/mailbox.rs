//! Where client threads leave their requests for the DB thread.
//!
//! Instead of one `MSG_RING` per request, each client thread has a mailbox the
//! DB thread drains once per turn. Only a request that finds no ring on its way
//! travels over `MSG_RING` as before: it is the ring, and wakes the DB thread,
//! which then drains the mailbox. Every request pushed until that drain rides
//! along for free. Replies still travel over `MSG_RING`.
//!
//! Order between requests does not matter: a task awaits its reply before it
//! sends its next request, so two requests in flight never come from the same
//! task.
//!
//! For requests left in a mailbox, the mutex takes over rule 5 of the handoff
//! protocol: the client writes the request before unlocking, the DB thread
//! reads it after locking, so it sees the request whole.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use slopio::io_driver::Message;

use crate::shared::request::Request;

type Msg = Arc<Message<Request>>;

/// A message on its way to the DB thread.
///
/// `Request` is not `Sync` — its body sits in an `UnsafeCell` — so an
/// `Arc` of it cannot cross threads on its own. The handoff protocol is what
/// makes sharing it sound, as it does for `send_shared_to_peer`.
pub(crate) struct Shared(Msg);

// SAFETY: only moved between threads by `Mailbox`, whose `push` makes the
// caller promise the handoff protocol.
unsafe impl Send for Shared {}

/// One mailbox per thread, indexed by slopio thread id: a client thread pushes
/// into its own, so it contends only with the DB thread, never with another
/// client.
///
/// Build it once, before the threads start, and hand a clone to the
/// [`Server`](crate::Server) and to every [`Client`](crate::Client).
#[derive(Clone)]
pub struct Mailboxes(Arc<[Mailbox]>);

impl Mailboxes {
    /// `threads` must cover every slopio thread id a client runs on.
    pub fn new(threads: usize) -> Self {
        Mailboxes((0..threads).map(|_| Mailbox::default()).collect())
    }

    pub(crate) fn get(&self, thread: usize) -> &Mailbox {
        self.0
            .get(thread)
            .expect("Mailboxes::new was given fewer threads than the runtime has")
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Mailbox> {
        self.0.iter()
    }
}

#[derive(Default)]
pub(crate) struct Mailbox {
    msgs: Mutex<Vec<Shared>>,
    /// A request went over `MSG_RING` since the last drain: the DB thread will
    /// drain again once it has it, so pushing is enough.
    ///
    /// Written only under the `msgs` lock. Read without it by the DB thread, to
    /// skip a mailbox that cannot hold anything: requests are only left while
    /// `rung` is set.
    rung: AtomicBool,
}

impl Mailbox {
    /// Leaves `msg` for the DB thread, or hands it back when no ring is on its
    /// way: the caller must then send it over `MSG_RING` itself, to wake the DB
    /// thread.
    ///
    /// # Safety
    /// The caller keeps its clone of `msg` and follows the handoff protocol in
    /// [`crate::shared::request`]: from here on it touches nothing but the
    /// cancellation flag until the reply lands.
    pub(crate) unsafe fn push(&self, msg: Msg) -> Option<Msg> {
        let mut msgs = self.msgs.lock().unwrap();
        if !self.rung.load(Ordering::Relaxed) {
            self.rung.store(true, Ordering::Relaxed);
            return Some(msg);
        }
        msgs.push(Shared(msg));
        None
    }

    /// Takes every message pushed so far, in order, and appends them to `out`.
    /// The next push rings again.
    ///
    /// The lock is held only to swap `spare` in: both vectors keep their
    /// capacity, so once warm neither side allocates. `spare` must be empty.
    pub(crate) fn drain(&self, spare: &mut Vec<Shared>, out: &mut impl Extend<Msg>) {
        debug_assert!(spare.is_empty());
        // Unset means empty, so no lock. Reading it stale is harmless: a push
        // sets it *before* sending its ring, so once the DB thread has that
        // ring it reads it set — or reset by a later drain, which took all.
        if !self.rung.load(Ordering::Relaxed) {
            return;
        }
        {
            let mut msgs = self.msgs.lock().unwrap();
            self.rung.store(false, Ordering::Relaxed);
            std::mem::swap(&mut *msgs, spare);
        }
        out.extend(spare.drain(..).map(|Shared(msg)| msg));
    }
}
