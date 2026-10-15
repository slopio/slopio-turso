//! The DB thread's writer: the one connection that writes, for every worker.
//!
//! Writes run as they arrive, in one open transaction, and are answered once
//! it has committed: before that, a write is not durable. Each commit wipes
//! the page cache of every reader, so commits are spaced by at least
//! `commit_interval`: when the last commit is older than that, the writes
//! commit at once; otherwise they wait for the interval to pass, and so do the
//! writes that arrive meanwhile.
//!
//! Past `wal_limit` pages of WAL, after a commit, the writer closes the
//! readers' gate and empties the WAL.

use std::mem;
use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::SyncMode;

use crate::Error;
use crate::conn::{self, Conn};
use crate::db::{Config, Shared, Synchronous};
use crate::request::{Body, Msg, reply};

/// The most writes one transaction serves: a full batch commits at once.
pub(crate) const MAX_BATCH: usize = 256;

/// How long the writer waits for the read transactions in progress to end
/// before giving up on emptying the WAL until its next commit.
const READERS_PATIENCE: Duration = Duration::from_millis(5);

pub(crate) struct Writer {
    conn: Conn,
    shared: Arc<Shared>,
    commit_interval: Duration,
    wal_limit: u64,
    /// When the last commit started; `None` before the first.
    last_commit: Option<Instant>,
    /// The open transaction's writes with their outcome, held until it commits.
    held: Vec<(Msg, Result<(), Error>)>,
}

impl Writer {
    pub(crate) fn new(shared: &Arc<Shared>, config: &Config) -> Result<Self, Error> {
        let conn = Conn::new(&shared.db, "BEGIN IMMEDIATE")?;
        conn.set_sync_mode(match config.synchronous {
            Synchronous::Full => SyncMode::Full,
            Synchronous::Normal => SyncMode::Normal,
        });
        Ok(Writer {
            conn,
            shared: shared.clone(),
            commit_interval: config.commit_interval,
            wal_limit: config.wal_limit,
            last_commit: None,
            held: Vec::with_capacity(MAX_BATCH),
        })
    }

    /// Runs `writes` in the open transaction, opening one if needed. Their
    /// answers wait for the commit.
    pub(crate) fn write(&mut self, writes: &mut Vec<Msg>) {
        for msg in writes.drain(..) {
            // Nobody waits for this write anymore: never run it.
            if msg.payload.is_cancelled() {
                reply(msg, Body::Written(Ok(())));
                continue;
            }
            if !self.conn.in_transaction()
                && let Err(error) = self.conn.begin()
            {
                reply(msg, Body::Written(Err(error)));
                continue;
            }
            let written = self.run(&msg);
            let failed = written.is_err();
            self.held.push((msg, written));
            if failed && !self.conn.in_transaction() {
                // turso rolled the transaction back, and the earlier writes
                // with it: their answers are final. The next write opens a
                // new transaction.
                self.fail_held(Error::RolledBack);
                self.answer_held();
            }
        }
    }

    /// When the open transaction should commit; `None` when none is open.
    pub(crate) fn commit_at(&self) -> Option<Instant> {
        if self.held.is_empty() {
            return None;
        }
        match self.last_commit {
            Some(last) if self.held.len() < MAX_BATCH => Some(last + self.commit_interval),
            _ => Some(Instant::now()),
        }
    }

    /// Commits the open transaction and answers its writes.
    pub(crate) fn commit(&mut self) {
        self.last_commit = Some(Instant::now());
        if self.conn.in_transaction()
            && let Err(error) = self.conn.commit()
        {
            self.fail_held(error);
        }
        self.answer_held();
        self.empty_wal_past_limit();
    }

    /// Past `wal_limit`: restarts the WAL. The costly part, copying it into
    /// the database file and its fsyncs, runs while the readers go on; they
    /// only pause, about 2 ms, for the pages committed meanwhile and the
    /// restart. Gives up until the next commit if the readers take too long
    /// to pause.
    fn empty_wal_past_limit(&mut self) {
        let past = self
            .conn
            .wal_pages()
            .is_ok_and(|pages| self.wal_limit > 0 && pages >= self.wal_limit);
        if !past {
            return;
        }
        let _ = self.conn.checkpoint_wal();
        if self.shared.gate.close(READERS_PATIENCE) {
            let _ = self.conn.restart_wal();
            self.shared.gate.open();
        }
    }

    fn run(&mut self, msg: &Msg) -> Result<(), Error> {
        let Body::Write { statement, params } = msg.payload.body() else {
            unreachable!("only writes reach the writer");
        };
        let (statement, params) = (*statement, mem::take(params));
        self.conn
            .run(statement.key(), statement.sql(), params, conn::none)
    }

    /// The held writes that had succeeded are undone: they get `error`.
    fn fail_held(&mut self, error: Error) {
        for (_, written) in &mut self.held {
            if written.is_ok() {
                *written = Err(error.clone());
            }
        }
    }

    fn answer_held(&mut self) {
        for (msg, written) in self.held.drain(..) {
            reply(msg, Body::Written(written));
        }
    }
}
