//! A write's request: shared by the worker that pushed it and the DB thread
//! that runs it, and typed by what it answers.
//!
//! The worker allocates a `Request<J>`, keeps one clone of its `Arc` and
//! pushes the other into the [`Inbox`]. The DB thread sees only its [`Head`]:
//! a `VTable` of `Request<J>`'s functions, made once per `J`, runs the job and
//! leaves its answer in place of it. Then the DB thread releases its clone and
//! wakes the worker, which finds its own the last one and takes the answer
//! without a lock.
//!
//! Until the wakeup the worker touches nothing but `cancelled`.

use std::cell::UnsafeCell;
use std::mem;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use slopio::io_driver::Header;
use slopio::runtime::{ReplySignal, signal_reply_to_peer};

use crate::Error;
use crate::inbox::Inbox;
use crate::request;
use crate::writer::Writer;

/// A write the writer runs, and what it answers.
pub(crate) trait Job: Send + 'static {
    type Answer: Send + 'static;

    fn run(self, writer: &mut Writer) -> Result<Self::Answer, Error>;
}

/// What the DB thread sees of a request, whatever its job: `repr(C)` keeps it
/// at the start of every `Request<J>`, so a pointer to one is a pointer to the
/// other.
#[repr(C)]
pub(crate) struct Head {
    /// In the inbox: the request pushed before this one. Written by the
    /// worker before the push publishes the request, read by the DB thread
    /// after its swap took it: never both at once.
    next: UnsafeCell<*mut Head>,
    /// The worker no longer waits for the answer.
    cancelled: AtomicBool,
    /// Where the answer's wakeup goes.
    reply_to: Header,
    vtable: &'static VTable,
}

impl Head {
    /// For the inbox's push.
    ///
    /// # Safety
    ///
    /// The request is not published yet: nobody else reads `next`.
    pub(crate) unsafe fn set_next(&self, next: *mut Head) {
        unsafe { *self.next.get() = next };
    }
}

/// `Request<J>`'s functions, for a `Head` that does not know its `J`. Each
/// takes the head of a `Request<J>` the DB thread holds a clone of.
struct VTable {
    /// Takes the job, runs it and leaves its answer: whether it succeeded.
    run: unsafe fn(NonNull<Head>, &mut Writer) -> bool,
    /// Answers `error` instead of the job, or of a successful answer: its
    /// batch was rolled back. A failed answer keeps its own error.
    fail: unsafe fn(NonNull<Head>, &Error),
    /// Drops the DB thread's clone of the `Arc<Request<J>>`.
    release: unsafe fn(NonNull<Head>),
}

#[repr(C)]
pub(crate) struct Request<J: Job> {
    head: Head,
    /// Only the DB thread reaches it until the wakeup, only the worker after.
    state: UnsafeCell<State<J>>,
}

enum State<J: Job> {
    Asked(J),
    /// The writer took the job and has not answered yet.
    Running,
    Answered(Result<J::Answer, Error>),
}

// SAFETY: shared by the worker and the DB thread, but `state` and `next` are
// only ever reached by one of them at a time, as their comments say;
// `cancelled` is atomic, and the rest never changes.
unsafe impl<J: Job> Send for Request<J> {}
unsafe impl<J: Job> Sync for Request<J> {}

impl<J: Job> Request<J> {
    const VTABLE: &'static VTable = &VTable {
        run: Self::run,
        fail: Self::fail,
        release: Self::release,
    };

    fn new(reply_to: Header, job: J) -> Self {
        Request {
            head: Head {
                next: UnsafeCell::new(std::ptr::null_mut()),
                cancelled: AtomicBool::new(false),
                reply_to,
                vtable: Self::VTABLE,
            },
            state: UnsafeCell::new(State::Asked(job)),
        }
    }

    /// # Safety
    ///
    /// `head` is the head of a live `Request<J>`, whose `state` only the
    /// caller reaches.
    unsafe fn state<'a>(head: NonNull<Head>) -> &'a mut State<J> {
        unsafe { &mut *head.cast::<Self>().as_ref().state.get() }
    }

    unsafe fn run(head: NonNull<Head>, writer: &mut Writer) -> bool {
        // SAFETY: the DB thread runs the job, before the wakeup.
        let state = unsafe { Self::state(head) };
        let State::Asked(job) = mem::replace(state, State::Running) else {
            unreachable!("a write runs once");
        };
        let answer = job.run(writer);
        let ran = answer.is_ok();
        *state = State::Answered(answer);
        ran
    }

    unsafe fn fail(head: NonNull<Head>, error: &Error) {
        // SAFETY: the DB thread fails the job, before the wakeup.
        let state = unsafe { Self::state(head) };
        if !matches!(state, State::Answered(Err(_))) {
            *state = State::Answered(Err(error.clone()));
        }
    }

    unsafe fn release(head: NonNull<Head>) {
        // SAFETY: the clone the inbox's push turned raw, released once.
        drop(unsafe { Arc::from_raw(head.cast::<Self>().as_ptr()) });
    }
}

/// The DB thread's clone of a request, taken from the inbox. Dropping it
/// answers the request: whatever its state, the worker is woken.
pub(crate) struct Held(NonNull<Head>);

impl Held {
    /// # Safety
    ///
    /// `head` is a request's clone the inbox's push turned raw, taken once.
    pub(crate) unsafe fn from_raw(head: *mut Head) -> Self {
        Held(NonNull::new(head).expect("a request is never null"))
    }

    fn head(&self) -> &Head {
        // SAFETY: our clone keeps the request alive.
        unsafe { self.0.as_ref() }
    }

    /// The request pushed before this one.
    pub(crate) fn next(&self) -> *mut Head {
        // SAFETY: the swap that took this request made us its only reader.
        unsafe { *self.head().next.get() }
    }

    /// Whether the worker still waits for the answer.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.head().cancelled.load(Ordering::Relaxed)
    }

    /// Runs the job and keeps its answer: whether it succeeded.
    pub(crate) fn run(&mut self, writer: &mut Writer) -> bool {
        // SAFETY: we hold a clone, and the DB thread alone reaches `state`.
        unsafe { (self.head().vtable.run)(self.0, writer) }
    }

    /// Answers `error`, unless the job already failed on its own.
    pub(crate) fn fail(&mut self, error: &Error) {
        // SAFETY: as for `run`.
        unsafe { (self.head().vtable.fail)(self.0, error) }
    }

    /// Wakes the worker with whatever was left in the request.
    pub(crate) fn reply(self) {
        drop(self);
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        let reply_to = self.head().reply_to;
        // Released before the wakeup: the worker must find its clone the last
        // one.
        // SAFETY: our clone, released once; the head is not touched again.
        unsafe { (self.head().vtable.release)(self.0) };
        // Sent even when cancelled: the worker's reply slot stays reserved
        // until a wakeup lands in it.
        signal_reply_to_peer(&reply_to, 0);
    }
}

/// Pushes `job` into the DB thread's `inbox`, rings its doorbell if nobody
/// did since it last looked, and waits for the answer.
pub(crate) async fn push<J: Job>(
    db_thread: usize,
    inbox: &Inbox,
    job: J,
) -> Result<J::Answer, Error> {
    let signal = ReplySignal::reserve();
    let request = Arc::new(Request::new(request::header(db_thread, &signal), job));
    if inbox.push(Arc::clone(&request)) {
        request::ring(db_thread);
    }
    let pending = Pending(Some(request));
    signal.await;
    pending.take()
}

/// A request in flight: dropped before the answer, it flags the request
/// cancelled so the DB thread does not bother running it.
struct Pending<J: Job>(Option<Arc<Request<J>>>);

impl<J: Job> Pending<J> {
    fn take(mut self) -> Result<J::Answer, Error> {
        let request = self.0.take().unwrap();
        let request =
            Arc::into_inner(request).expect("the DB thread released its clone before the wakeup");
        match request.state.into_inner() {
            State::Answered(answer) => answer,
            State::Asked(_) | State::Running => unreachable!("woken before the answer"),
        }
    }
}

impl<J: Job> Drop for Pending<J> {
    fn drop(&mut self) {
        if let Some(request) = &self.0 {
            request.head.cancelled.store(true, Ordering::Relaxed);
        }
    }
}
