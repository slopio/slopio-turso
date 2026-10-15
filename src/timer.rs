//! A precise wakeup for the DB thread: slopio's timers tick every 10 ms, too
//! coarse for a commit interval of a millisecond or two. This one is an
//! io_uring timeout on the thread's own ring.

use std::any::Any;
use std::time::Instant;

use io_uring::opcode;
use io_uring::squeue::Entry;
use io_uring::types::Timespec;
use slopio::io_driver::operations::{Op, OpState};

/// The timespec the kernel reads, boxed so its address survives moves.
struct Timeout(Box<Timespec>);

// SAFETY: the entry points into the box, which moves with `self` but not its contents.
unsafe impl OpState for Timeout {
    fn entry(&mut self) -> Entry {
        opcode::Timeout::new(&*self.0).build()
    }

    fn into_keepalive(self) -> Option<Box<dyn Any>> {
        Some(self.0)
    }
}

/// One timeout at a time, kept armed until it fires.
pub(crate) struct Timer {
    armed: Option<Op<Timeout>>,
    /// The timespec while no timeout is armed.
    spare: Option<Timeout>,
}

impl Timer {
    pub(crate) fn new() -> Self {
        Timer {
            armed: None,
            spare: Some(Timeout(Box::new(Timespec::new()))),
        }
    }

    /// Fires at `deadline`, or earlier if it was armed for an earlier one: the
    /// caller checks the time again.
    pub(crate) async fn until(&mut self, deadline: Instant) {
        let op = self.armed.get_or_insert_with(|| {
            let mut timeout = self.spare.take().expect("spare while disarmed");
            *timeout.0 = Timespec::from(deadline.saturating_duration_since(Instant::now()));
            Op::new(timeout)
        });
        let (_, timeout) = op.await;
        self.armed = None;
        self.spare = Some(timeout);
    }
}
