//! The writes waiting for the DB thread, shared by every worker.
//!
//! A lock-free stack: workers push with a CAS, the DB thread takes everything
//! in one swap, so it needs no ABA care. MSG_RING only rings a doorbell, once
//! per emptying, instead of carrying every write: the runtime moves only so
//! many arrivals into a thread's inbox per turn, and each costs the kernel.

use std::ptr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering::SeqCst};

use slopio::io_driver::Message;

use crate::request::{Msg, Request};

pub(crate) struct Inbox {
    /// The last write pushed; each links to the one pushed before it.
    head: AtomicPtr<Message<Request>>,
    /// A doorbell was sent since the DB thread last emptied the stack: the
    /// next push need not send another.
    rung: AtomicBool,
}

impl Inbox {
    pub(crate) fn new() -> Self {
        Inbox {
            head: AtomicPtr::new(ptr::null_mut()),
            rung: AtomicBool::new(false),
        }
    }

    /// Pushes `msg`; true when the caller must ring the doorbell.
    pub(crate) fn push(&self, msg: Msg) -> bool {
        let node = Arc::into_raw(msg).cast_mut();
        let mut head = self.head.load(SeqCst);
        loop {
            // SAFETY: `node` is ours until the CAS publishes it.
            unsafe { (*node).payload.set_next(head) };
            match self.head.compare_exchange_weak(head, node, SeqCst, SeqCst) {
                Ok(_) => break,
                Err(current) => head = current,
            }
        }
        // After the push: a DB thread that clears the flag afterwards takes
        // this write; one that cleared it before gets a doorbell.
        !self.rung.swap(true, SeqCst)
    }

    /// For the DB thread: moves every write pushed so far into `into`, oldest
    /// first.
    pub(crate) fn take(&self, into: &mut Vec<Msg>) {
        // Before the swap: a push the swap misses rings again.
        self.rung.store(false, SeqCst);
        let mut node = self.head.swap(ptr::null_mut(), SeqCst);
        let start = into.len();
        while !node.is_null() {
            // SAFETY: every node was an `Arc` turned raw by `push`, and the
            // swap made the DB thread its only taker.
            let msg = unsafe { Arc::from_raw(node) };
            node = msg.payload.next();
            into.push(msg);
        }
        into[start..].reverse();
    }
}

impl Drop for Inbox {
    fn drop(&mut self) {
        let mut writes = Vec::new();
        self.take(&mut writes);
    }
}
