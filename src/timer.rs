//! A precise wakeup for the DB thread: slopio's timers tick every 10 ms, too
//! coarse for a commit interval of a millisecond or two. This one is an
//! io_uring timeout on the thread's own ring.

use std::future::poll_fn;
use std::mem::ManuallyDrop;
use std::time::Instant;

use io_uring::opcode;
use io_uring::types::Timespec;
use slopio::io_driver::IO_DRIVER;
use slopio::io_driver::operations::OneshotToken;

/// One timeout at a time, kept armed until it fires.
pub(crate) struct Timer {
    /// Read by the kernel when the timeout is submitted, which may be after
    /// it was pushed: it must outlive the op.
    timespec: ManuallyDrop<Box<Timespec>>,
    armed: Option<OneshotToken>,
}

impl Timer {
    pub(crate) fn new() -> Self {
        Timer {
            timespec: ManuallyDrop::new(Box::new(Timespec::new())),
            armed: None,
        }
    }

    /// Fires at `deadline`, or earlier if it was armed for an earlier one: the
    /// caller checks the time again.
    pub(crate) async fn until(&mut self, deadline: Instant) {
        let token = match self.armed {
            Some(token) => token,
            None => {
                let wait = deadline.saturating_duration_since(Instant::now());
                **self.timespec = Timespec::from(wait);
                let timeout = opcode::Timeout::new(&**self.timespec).build();
                let token = IO_DRIVER.with(|d| d.push_op(timeout));
                self.armed = Some(token);
                token
            }
        };
        poll_fn(|cx| IO_DRIVER.with(|d| d.poll_op(token, cx.waker()))).await;
        self.armed = None;
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        match self.armed.take() {
            Some(token) => {
                IO_DRIVER.try_with(|d| {
                    if let Some(d) = d {
                        d.cancel_single_op(token, None);
                    }
                });
                // The kernel may not have read it yet: leaked, not freed.
            }
            // SAFETY: no op refers to it anymore, and it is not used again.
            None => unsafe { ManuallyDrop::drop(&mut self.timespec) },
        }
    }
}
