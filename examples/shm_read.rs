//! Experiment: point reads over shared-memory queues instead of one MSG_RING
//! per request and per reply.
//!
//! Worker 0 is the DB thread, every other worker a client thread running a few
//! tasks. Each client thread shares two lock-free queues with the DB thread:
//! requests one way, replies the other. A queue's consumer that runs out of
//! work sets its `parked` flag, checks the queue once more, then sleeps on its
//! io_uring ring; a producer rings it — one MSG_RING — only if it finds the
//! flag set. Busy on both sides, no MSG_RING at all.
//!
//! Messages are minimal (task slot + id, task slot + name length): this
//! measures the transport, not the request arena or typed decoding.
//!
//! ```text
//! cargo run --release --example shm_read -- [--seconds 5] [--tasks 4]
//! ```

use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::num::NonZero;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering, fence};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use crossbeam_queue::ArrayQueue;
use slopio::runtime::select::{SelectResult, select};
use slopio::runtime::{recv_from_peer, send_to_peer, spawn, yield_now};
use slopio::thread_per_core;
use slopio_turso::RingIo;
use turso_core::{Database, IO, Numeric, OpenOptions, SqliteDialect, StepResult, Value};

const DB_THREAD: usize = 0;
const PATH: &str = "/tmp/slopio-turso-shm.db";
const ROWS: u64 = 100_000;
const QUEUE: usize = 1024;

struct Req {
    slot: u32,
    id: i64,
}

struct Reply {
    slot: u32,
    name_len: u32,
}

/// What one client thread shares with the DB thread.
struct Link {
    requests: ArrayQueue<Req>,
    replies: ArrayQueue<Reply>,
    /// The client's dispatcher sleeps until rung.
    client_parked: AtomicBool,
}

struct Shared {
    links: Vec<Link>,
    /// The DB thread sleeps until rung.
    db_parked: AtomicBool,
    clients_left: AtomicUsize,
    stop: AtomicBool,
    latencies: Mutex<Vec<Duration>>,
}

/// Wakes `thread` if `parked` says it sleeps, and only then. Call after
/// publishing work: the fence pairs with the one in `park`, so either this
/// sees the flag or the sleeper sees the work.
fn ring_if_parked(parked: &AtomicBool, thread: usize) {
    fence(Ordering::SeqCst);
    if parked.load(Ordering::Relaxed) && parked.swap(false, Ordering::SeqCst) {
        send_to_peer(thread, 0, 0);
    }
}

/// How long a consumer that ran dry watches its queue before parking, from
/// `SPIN_US` (0 by default): work arriving meanwhile needs no ring.
fn spin_budget() -> Duration {
    static BUDGET: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *BUDGET.get_or_init(|| {
        let us = std::env::var("SPIN_US").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
        Duration::from_micros(us)
    })
}

/// Raises `parked`, then looks at `has_work` once more: true means stay up.
/// Watches `has_work` for the spin budget first.
fn park(parked: &AtomicBool, has_work: impl Fn() -> bool) -> bool {
    let until = Instant::now() + spin_budget();
    while Instant::now() < until {
        if has_work() {
            return true;
        }
        std::hint::spin_loop();
    }
    parked.store(true, Ordering::SeqCst);
    fence(Ordering::SeqCst);
    if has_work() {
        parked.store(false, Ordering::SeqCst);
        return true;
    }
    false
}

async fn db_thread(shared: Arc<Shared>) {
    let io = Arc::new(RingIo::new().unwrap());
    let options = OpenOptions::new(Arc::new(SqliteDialect));
    let db = Database::open(io.clone(), PATH, options).unwrap();
    let conn = db.connect().unwrap();
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    conn.execute(format!(
        "INSERT INTO users (id, name) WITH RECURSIVE c(x) AS \
         (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < {ROWS}) SELECT x, 'user' || x FROM c"
    ))
    .unwrap();
    let mut stmt = conn
        .prepare("SELECT id, name FROM users WHERE id = ?1")
        .unwrap();
    for client in 1..=shared.links.len() {
        send_to_peer(client, 1, 0); // the table is ready
    }

    let mut dirty = vec![false; shared.links.len()];
    loop {
        let mut served = 0;
        for (i, link) in shared.links.iter().enumerate() {
            while let Some(req) = link.requests.pop() {
                stmt.bind_at(
                    NonZero::new(1).unwrap(),
                    Value::Numeric(Numeric::Integer(req.id)),
                )
                .unwrap();
                let mut name_len = 0;
                loop {
                    match stmt.step().unwrap() {
                        StepResult::Row => {
                            name_len = stmt
                                .row()
                                .unwrap()
                                .get_value(1)
                                .to_text()
                                .map_or(0, str::len);
                        }
                        StepResult::Done => break,
                        StepResult::IO => io.step().unwrap(),
                        _ => {}
                    }
                }
                stmt.reset().unwrap();
                let reply = Reply {
                    slot: req.slot,
                    name_len: name_len as u32,
                };
                assert!(link.replies.push(reply).is_ok(), "reply queue full");
                dirty[i] = true;
                served += 1;
            }
        }
        // One ring per sleeping client, however many replies it got.
        for (i, link) in shared.links.iter().enumerate() {
            if std::mem::take(&mut dirty[i]) {
                ring_if_parked(&link.client_parked, i + 1);
            }
        }
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        if served > 0 {
            // Lets slopio submit the rings queued above.
            yield_now().await;
            continue;
        }
        let pending = || {
            shared.stop.load(Ordering::Acquire)
                || shared.links.iter().any(|l| !l.requests.is_empty())
        };
        if !park(&shared.db_parked, pending) {
            recv_from_peer().await;
            shared.db_parked.store(false, Ordering::SeqCst);
        }
    }
}

/// Per client thread: where each task's in-flight request stands.
#[derive(Default)]
struct Slots {
    wakers: Vec<Option<Waker>>,
    replies: Vec<Option<u32>>,
}

async fn client_thread(shared: Arc<Shared>, thread: usize, tasks: usize, seconds: u64) {
    recv_from_peer().await; // the table is ready
    let link_index = thread - 1;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    let slots = Rc::new(RefCell::new(Slots {
        wakers: vec![None; tasks],
        replies: vec![None; tasks],
    }));
    let running = Rc::new(Cell::new(tasks));
    let dispatcher_waker: Rc<RefCell<Option<Waker>>> = Rc::default();

    for slot in 0..tasks {
        let (shared, slots, running, dispatcher_waker) = (
            Arc::clone(&shared),
            Rc::clone(&slots),
            Rc::clone(&running),
            Rc::clone(&dispatcher_waker),
        );
        spawn(async move {
            let link = &shared.links[link_index];
            let mut rng = 0x9E37_79B9_7F4A_7C15u64 ^ ((thread * 1000 + slot) as u64 + 1);
            let mut latencies = Vec::new();
            while Instant::now() < deadline {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                let start = Instant::now();
                let req = Req {
                    slot: slot as u32,
                    id: (rng % ROWS) as i64 + 1,
                };
                assert!(link.requests.push(req).is_ok(), "request queue full");
                ring_if_parked(&shared.db_parked, DB_THREAD);
                let name_len = poll_fn(|cx| {
                    let mut slots = slots.borrow_mut();
                    match slots.replies[slot].take() {
                        Some(len) => Poll::Ready(len),
                        None => {
                            slots.wakers[slot] = Some(cx.waker().clone());
                            Poll::Pending
                        }
                    }
                })
                .await;
                assert!(name_len > 0);
                latencies.push(start.elapsed());
            }
            shared.latencies.lock().unwrap().extend(latencies);
            running.set(running.get() - 1);
            if let Some(waker) = dispatcher_waker.borrow_mut().take() {
                waker.wake();
            }
        });
    }

    // Dispatcher: hands replies to their tasks, sleeps when there are none.
    let link = &shared.links[link_index];
    while running.get() > 0 {
        let mut got = false;
        while let Some(reply) = link.replies.pop() {
            let mut slots = slots.borrow_mut();
            slots.replies[reply.slot as usize] = Some(reply.name_len);
            if let Some(waker) = slots.wakers[reply.slot as usize].take() {
                waker.wake();
            }
            got = true;
        }
        if got {
            // Let the woken tasks run and send their next request.
            yield_now().await;
            continue;
        }
        if park(&link.client_parked, || !link.replies.is_empty()) {
            continue;
        }
        let tasks_done = poll_fn(|cx| {
            if running.get() == 0 {
                return Poll::Ready(());
            }
            *dispatcher_waker.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        });
        if let SelectResult::First(_) = select(recv_from_peer(), tasks_done).await {
            link.client_parked.store(false, Ordering::SeqCst);
        }
    }
    if shared.clients_left.fetch_sub(1, Ordering::AcqRel) == 1 {
        shared.stop.store(true, Ordering::Release);
        ring_if_parked(&shared.db_parked, DB_THREAD);
    }
}

fn remove_files() {
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{PATH}{suffix}"));
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let (mut seconds, mut tasks) = (5, 4);
    while let (Some(flag), Some(value)) = (args.next(), args.next()) {
        match flag.as_str() {
            "--seconds" => seconds = value.parse().unwrap(),
            "--tasks" => tasks = value.parse().unwrap(),
            _ => panic!("unknown flag {flag}"),
        }
    }
    remove_files();
    let clients = std::thread::available_parallelism().unwrap().get() - 1;
    let shared = Arc::new(Shared {
        links: (0..clients)
            .map(|_| Link {
                requests: ArrayQueue::new(QUEUE),
                replies: ArrayQueue::new(QUEUE),
                client_parked: AtomicBool::new(false),
            })
            .collect(),
        db_parked: AtomicBool::new(false),
        clients_left: AtomicUsize::new(clients),
        stop: AtomicBool::new(false),
        latencies: Mutex::default(),
    });
    let for_threads = Arc::clone(&shared);
    thread_per_core(move |ctx| {
        let shared = Arc::clone(&for_threads);
        Box::pin(async move {
            assert_eq!(ctx.num_threads, shared.links.len() + 1);
            match ctx.thread_id {
                DB_THREAD => db_thread(shared).await,
                thread => client_thread(shared, thread, tasks, seconds).await,
            }
        })
    });
    remove_files();

    let mut latencies = std::mem::take(&mut *shared.latencies.lock().unwrap());
    latencies.sort_unstable();
    let n = latencies.len();
    let at = |q: f64| latencies[((n as f64 * q) as usize).min(n - 1)];
    println!("{seconds} s, {clients} client threads × {tasks} tasks, shared-memory queues");
    println!(
        "reads {n:>9} req  {:>9.0}/s  p50 {:>9.1?}  p99 {:>9.1?}  max {:>9.1?}",
        n as f64 / seconds as f64,
        at(0.50),
        at(0.99),
        latencies[n - 1],
    );
}
