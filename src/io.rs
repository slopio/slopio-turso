//! Turso IO on an io_uring ring of our own.
//!
//! [`RingIo::step`] waits for at least one operation to finish when none has,
//! so turso's own blocking loops (opening the database, checkpoints…) work as
//! with its `UringIO`. [`RingIo::poll`] only reaps what is already done, and
//! [`RingIo::ring_fd`] lets the DB thread sleep until the kernel has something:
//! the ring's fd turns readable once completions are waiting.
//!
//! Each operation's [`Completion`] and the buffers the kernel reads from or
//! writes into are kept in a slot until its completion is reaped; the slot
//! index is the operation's `user_data`.

use std::io::ErrorKind;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::{Arc, Mutex};

use io_uring::types::Fd;
use io_uring::{IoUring, opcode, squeue};
use turso_core::io::FileSyncType;
use turso_core::io::clock::DefaultClock;
use turso_core::{
    Buffer, Clock, Completion, CompletionError, File, IO, LimboError, MonotonicInstant, OpenFlags,
    WallClockInstant,
};

/// Submission and completion queue size.
const ENTRIES: u32 = 1024;

/// Buffers one vectored write hands the kernel at most (`IOV_MAX`); the rest
/// follow as the write resumes.
const MAX_IOVECS: usize = 1024;

pub struct RingIo {
    ring: Arc<Ring>,
}

impl RingIo {
    pub fn new() -> std::io::Result<Self> {
        Ok(RingIo {
            ring: Arc::new(Ring {
                state: Mutex::new(State {
                    ring: IoUring::new(ENTRIES)?,
                    ops: Vec::new(),
                    free: Vec::new(),
                    in_flight: 0,
                }),
            }),
        })
    }

    /// Completes whatever operations are done, without waiting.
    pub fn poll(&self) -> turso_core::Result<()> {
        finish(self.ring.reap(false)?);
        Ok(())
    }

    /// Readable once completions are waiting to be reaped by [`poll`](Self::poll).
    pub fn ring_fd(&self) -> RawFd {
        self.ring.state.lock().unwrap().ring.as_raw_fd()
    }
}

impl IO for RingIo {
    fn open_file(
        &self,
        path: &str,
        flags: OpenFlags,
        _direct: bool,
    ) -> turso_core::Result<Arc<dyn File>> {
        let mut options = std::fs::File::options();
        options.read(true);
        if !flags.contains(OpenFlags::ReadOnly) {
            options.write(true);
            options.create(flags.contains(OpenFlags::Create));
        }
        let file = options.open(path).map_err(|e| io_error(e, "open"))?;
        let file = Arc::new(RingFile {
            file,
            ring: Arc::clone(&self.ring),
        });
        if !flags.intersects(OpenFlags::ReadOnly | OpenFlags::NoLock) {
            file.lock_file(true)?;
        }
        Ok(file)
    }

    fn remove_file(&self, path: &str) -> turso_core::Result<()> {
        std::fs::remove_file(path).map_err(|e| io_error(e, "remove_file"))
    }

    /// Makes progress: completes what is done, waiting for one operation to
    /// finish if none has yet.
    fn step(&self) -> turso_core::Result<()> {
        finish(self.ring.reap(true)?);
        Ok(())
    }
}

impl Clock for RingIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        DefaultClock.current_time_monotonic()
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        DefaultClock.current_time_wall_clock()
    }
}

/// `IO` and `File` must be `Send + Sync`; only the DB thread ever takes the
/// lock, so it is never contended.
struct Ring {
    state: Mutex<State>,
}

struct State {
    ring: IoUring,
    /// Operations in flight; the index is the `user_data`.
    ops: Vec<Option<Op>>,
    free: Vec<usize>,
    in_flight: usize,
}

struct Op {
    completion: Completion,
    /// What the kernel reads from or writes into, alive until it is done.
    _buffer: Option<Arc<Buffer>>,
    /// Set for a vectored write, resumed from where the kernel stopped.
    writev: Option<Writev>,
}

struct Writev {
    fd: RawFd,
    pos: u64,
    _buffers: Vec<Arc<Buffer>>,
    /// Still to write; the kernel reads it until the write completes.
    iovecs: Vec<libc::iovec>,
    written: usize,
    total: usize,
}

// SAFETY: the iovecs only point into `_buffers`, which the `Writev` owns and
// which are themselves `Send`; the raw pointers carry no other ownership.
unsafe impl Send for Writev {}

impl Writev {
    fn entry(&self) -> squeue::Entry {
        let count = self.iovecs.len().min(MAX_IOVECS);
        opcode::Writev::new(Fd(self.fd), self.iovecs.as_ptr(), count as u32)
            .offset(self.pos)
            .build()
    }

    /// Drops the `n` bytes the kernel wrote from the front of `iovecs`.
    fn advance(&mut self, n: usize) {
        self.written += n;
        self.pos += n as u64;
        let mut left = n;
        let whole = self
            .iovecs
            .iter()
            .take_while(|iov| {
                let fits = iov.iov_len <= left;
                if fits {
                    left -= iov.iov_len;
                }
                fits
            })
            .count();
        self.iovecs.drain(..whole);
        if let Some(first) = self.iovecs.first_mut() {
            // SAFETY: `left` < `iov_len`, so the pointer stays in the buffer.
            first.iov_base = unsafe { first.iov_base.cast::<u8>().add(left) }.cast();
            first.iov_len -= left;
        }
    }
}

impl Ring {
    /// Queues `entry`; `op` keeps its completion and buffers until it is reaped.
    fn push(&self, entry: squeue::Entry, op: Op) -> turso_core::Result<()> {
        self.state.lock().unwrap().push(entry, op)
    }

    /// Submits what is queued and takes the finished operations. With `wait`,
    /// blocks until at least one has finished, if any is in flight.
    fn reap(&self, wait: bool) -> turso_core::Result<Vec<(Completion, i32)>> {
        let mut state = self.state.lock().unwrap();
        let mut done = Vec::new();
        loop {
            let want = usize::from(wait && done.is_empty() && state.in_flight > 0);
            match state.ring.submit_and_wait(want) {
                Ok(_) => {}
                // A signal cut the wait short, or the completion queue is full:
                // reap below and go round again.
                Err(e) if matches!(e.kind(), ErrorKind::Interrupted | ErrorKind::ResourceBusy) => {}
                Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {}
                Err(e) => return Err(io_error(e, "io_uring_enter")),
            }
            state.drain(&mut done)?;
            if want == 0 || !done.is_empty() {
                return Ok(done);
            }
        }
    }
}

impl State {
    fn push(&mut self, entry: squeue::Entry, op: Op) -> turso_core::Result<()> {
        let key = self.free.pop().unwrap_or_else(|| {
            self.ops.push(None);
            self.ops.len() - 1
        });
        let entry = entry.user_data(key as u64);
        // SAFETY: the buffers `entry` points into live in `op`, stored below
        // until the kernel is done with them.
        while unsafe { self.ring.submission().push(&entry) }.is_err() {
            // Queue full: hand the kernel what is there to make room.
            self.ring
                .submit()
                .map_err(|e| io_error(e, "io_uring_enter"))?;
        }
        self.ops[key] = Some(op);
        self.in_flight += 1;
        Ok(())
    }

    /// Takes the finished operations, resubmitting vectored writes the kernel
    /// only partly did.
    fn drain(&mut self, done: &mut Vec<(Completion, i32)>) -> turso_core::Result<()> {
        let finished: Vec<(u64, i32)> = self
            .ring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect();
        for (key, result) in finished {
            let key = key as usize;
            let mut op = self.ops[key]
                .take()
                .expect("completion for an unknown operation");
            self.free.push(key);
            self.in_flight -= 1;
            let Some(writev) = op.writev.as_mut() else {
                done.push((op.completion, result));
                continue;
            };
            if result <= 0 {
                // An error, or the kernel wrote nothing: report it as is.
                done.push((op.completion, result));
                continue;
            }
            writev.advance(result as usize);
            if writev.written < writev.total {
                let entry = writev.entry();
                self.push(entry, op)?;
            } else {
                let total = i32::try_from(writev.total).unwrap_or(i32::MAX);
                done.push((op.completion, total));
            }
        }
        Ok(())
    }
}

/// Hands results to turso, outside the lock: a completion's callback may start
/// the next operation.
fn finish(done: Vec<(Completion, i32)>) {
    for (completion, result) in done {
        if result < 0 {
            let kind = std::io::Error::from_raw_os_error(-result).kind();
            completion.error(CompletionError::IOError(kind, "io_uring"));
        } else {
            completion.complete(result);
        }
    }
}

fn io_error(err: std::io::Error, op: &'static str) -> LimboError {
    turso_core::io_error(err, op)
}

pub struct RingFile {
    file: std::fs::File,
    ring: Arc<Ring>,
}

impl RingFile {
    fn fd(&self) -> Fd {
        Fd(self.file.as_raw_fd())
    }

    fn submit(
        &self,
        entry: squeue::Entry,
        c: Completion,
        buffer: Option<Arc<Buffer>>,
    ) -> turso_core::Result<Completion> {
        let op = Op {
            completion: c.clone(),
            _buffer: buffer,
            writev: None,
        };
        self.ring.push(entry, op)?;
        Ok(c)
    }
}

impl File for RingFile {
    fn lock_file(&self, exclusive: bool) -> turso_core::Result<()> {
        let locked = if exclusive {
            self.file.try_lock()
        } else {
            self.file.try_lock_shared()
        };
        locked.map_err(|e| {
            LimboError::LockingError(match e {
                std::fs::TryLockError::WouldBlock => {
                    "Failed locking file. File is locked by another process".to_string()
                }
                std::fs::TryLockError::Error(e) => format!("Failed locking file, {e}"),
            })
        })
    }

    fn unlock_file(&self) -> turso_core::Result<()> {
        self.file
            .unlock()
            .map_err(|e| LimboError::LockingError(format!("Failed to release file lock: {e}")))
    }

    fn pread(&self, pos: u64, c: Completion) -> turso_core::Result<Completion> {
        let buffer = Arc::clone(&c.as_read().buf);
        let entry = opcode::Read::new(self.fd(), buffer.as_mut_ptr(), buffer.len() as u32)
            .offset(pos)
            .build();
        self.submit(entry, c, Some(buffer))
    }

    fn pwrite(
        &self,
        pos: u64,
        buffer: Arc<Buffer>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        let entry = opcode::Write::new(self.fd(), buffer.as_ptr(), buffer.len() as u32)
            .offset(pos)
            .build();
        self.submit(entry, c, Some(buffer))
    }

    fn pwritev(
        &self,
        pos: u64,
        buffers: Vec<Arc<Buffer>>,
        c: Completion,
    ) -> turso_core::Result<Completion> {
        if buffers.is_empty() {
            c.complete(0);
            return Ok(c);
        }
        let iovecs: Vec<libc::iovec> = buffers
            .iter()
            .map(|buffer| libc::iovec {
                iov_base: buffer.as_ptr().cast_mut().cast(),
                iov_len: buffer.len(),
            })
            .collect();
        let writev = Writev {
            fd: self.file.as_raw_fd(),
            pos,
            total: iovecs.iter().map(|iov| iov.iov_len).sum(),
            _buffers: buffers,
            iovecs,
            written: 0,
        };
        let entry = writev.entry();
        let op = Op {
            completion: c.clone(),
            _buffer: None,
            writev: Some(writev),
        };
        self.ring.push(entry, op)?;
        Ok(c)
    }

    /// `FullFsync` only differs on macOS.
    fn sync(&self, c: Completion, _sync_type: FileSyncType) -> turso_core::Result<Completion> {
        self.submit(opcode::Fsync::new(self.fd()).build(), c, None)
    }

    fn size(&self) -> turso_core::Result<u64> {
        Ok(self
            .file
            .metadata()
            .map_err(|e| io_error(e, "metadata"))?
            .len())
    }

    fn truncate(&self, len: u64, c: Completion) -> turso_core::Result<Completion> {
        self.submit(opcode::Ftruncate::new(self.fd(), len).build(), c, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::{Database, OpenOptions, SqliteDialect, Value};

    fn path(name: &str) -> String {
        format!("/tmp/slopio-turso-io-{}-{name}.db", std::process::id())
    }

    fn remove(path: &str) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{path}{suffix}"));
        }
    }

    fn open(path: &str) -> Arc<turso_core::Connection> {
        let io = Arc::new(RingIo::new().unwrap());
        let options = OpenOptions::new(Arc::new(SqliteDialect));
        Database::open(io, path, options)
            .unwrap()
            .connect()
            .unwrap()
    }

    fn query(conn: &Arc<turso_core::Connection>, sql: &str) -> Vec<Vec<Value>> {
        conn.prepare(sql).unwrap().run_collect_rows().unwrap()
    }

    /// Everything turso does to a database through the ring: WAL appends
    /// (vectored writes), fsyncs, a checkpoint that truncates the WAL, and
    /// reads of every page once the database is opened again from disk.
    #[test]
    fn database_round_trips_through_the_ring() {
        let path = path("round_trip");
        remove(&path);
        let expected = {
            let conn = open(&path);
            conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")
                .unwrap();
            // ~8 MB: more than the page cache, across many WAL frames.
            conn.execute(
                "INSERT INTO t (data) WITH RECURSIVE c(x) AS \
                 (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 2000) \
                 SELECT randomblob(4000) FROM c",
            )
            .unwrap();
            query(&conn, "PRAGMA wal_checkpoint(TRUNCATE)");
            let wal = std::fs::metadata(format!("{path}-wal")).unwrap().len();
            assert_eq!(wal, 0, "the checkpoint truncated the WAL");
            query(
                &conn,
                "SELECT count(*), sum(length(data)), sum(hex(data) LIKE '%AB%') FROM t",
            )
        };

        // Nothing is cached anymore: every page comes back from disk.
        let conn = open(&path);
        let read = query(
            &conn,
            "SELECT count(*), sum(length(data)), sum(hex(data) LIKE '%AB%') FROM t",
        );
        assert_eq!(read, expected);
        assert!(matches!(
            read[0][0],
            Value::Numeric(turso_core::Numeric::Integer(2000))
        ));
        drop(conn);
        remove(&path);
    }
}
