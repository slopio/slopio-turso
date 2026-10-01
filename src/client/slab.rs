//! Where a client thread keeps track of its requests in flight: one slot per
//! request, holding the waker of the task awaiting it until the reply is back.
//!
//! The slab belongs to its thread alone. A request carries its slot id, so the
//! thread's dispatcher can find the slot when the reply comes back — unless the
//! request was cancelled: its slot was freed then, possibly reused, and the
//! dispatcher must leave it alone.

use std::task::Waker;

#[derive(Debug)]
enum Slot {
    /// Free; the next free slot, if any.
    Vacant(Option<u32>),
    /// In flight; the waker to call once the reply is back, if polled yet.
    Waiting(Option<Waker>),
    /// The reply is back.
    Done,
}

#[derive(Debug, Default)]
pub(crate) struct Slab {
    slots: Vec<Slot>,
    /// Head of the free list threaded through the vacant slots.
    free: Option<u32>,
}

impl Slab {
    pub(crate) fn new() -> Self {
        Slab::default()
    }

    /// Reserves a slot for a request about to be sent.
    pub(crate) fn insert(&mut self) -> u32 {
        match self.free {
            Some(id) => {
                let Slot::Vacant(next) = self.slots[id as usize] else {
                    unreachable!("the free list holds vacant slots only");
                };
                self.free = next;
                self.slots[id as usize] = Slot::Waiting(None);
                id
            }
            None => {
                let id =
                    u32::try_from(self.slots.len()).expect("fewer than 2³² requests in flight");
                self.slots.push(Slot::Waiting(None));
                id
            }
        }
    }

    /// Whether the reply is back; if not, `waker` is the one to call once it is.
    pub(crate) fn poll(&mut self, id: u32, waker: &Waker) -> bool {
        match &mut self.slots[id as usize] {
            Slot::Done => true,
            Slot::Waiting(Some(old)) if old.will_wake(waker) => false,
            Slot::Waiting(stored) => {
                *stored = Some(waker.clone());
                false
            }
            Slot::Vacant(_) => unreachable!("polled a freed slot"),
        }
    }

    /// Marks the reply back. Returns the waker to call — outside any borrow of
    /// the slab, since waking may run code that uses it.
    pub(crate) fn complete(&mut self, id: u32) -> Option<Waker> {
        let slot = &mut self.slots[id as usize];
        let Slot::Waiting(waker) = slot else {
            unreachable!("completed a slot not in flight");
        };
        let waker = waker.take();
        *slot = Slot::Done;
        waker
    }

    /// Frees the slot: the reply was taken, or the request was cancelled.
    pub(crate) fn remove(&mut self, id: u32) {
        let slot = &mut self.slots[id as usize];
        debug_assert!(!matches!(slot, Slot::Vacant(_)), "freed twice");
        *slot = Slot::Vacant(self.free);
        self.free = Some(id);
    }

    /// Slots in use.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| !matches!(slot, Slot::Vacant(_)))
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Wake;

    /// Counts its wakes.
    struct Counter(AtomicUsize);

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn counter() -> (Arc<Counter>, Waker) {
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        (Arc::clone(&counter), Waker::from(counter))
    }

    #[test]
    fn a_reply_wakes_the_last_waker() {
        let mut slab = Slab::new();
        let id = slab.insert();
        let (first, first_waker) = counter();
        let (second, second_waker) = counter();
        assert!(!slab.poll(id, &first_waker));
        assert!(!slab.poll(id, &second_waker));

        slab.complete(id).unwrap().wake();
        assert_eq!(first.0.load(Ordering::Relaxed), 0);
        assert_eq!(second.0.load(Ordering::Relaxed), 1);
        assert!(slab.poll(id, &second_waker));
    }

    #[test]
    fn a_reply_before_any_poll_wakes_no_one() {
        let mut slab = Slab::new();
        let id = slab.insert();
        assert!(slab.complete(id).is_none());
        assert!(slab.poll(id, Waker::noop()));
    }

    #[test]
    fn freed_slots_are_reused() {
        let mut slab = Slab::new();
        let a = slab.insert();
        let b = slab.insert();
        slab.remove(a);
        assert_eq!(slab.len(), 1);
        assert_eq!(slab.insert(), a);
        assert_eq!(slab.insert(), 2);
        slab.remove(b);
        assert_eq!(slab.insert(), b);
        assert_eq!(slab.len(), 3);
    }
}
