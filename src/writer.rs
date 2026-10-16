//! The DB thread's writer: the one connection that writes, for every worker.
//!
//! Writes run as they arrive, in one open transaction, and are answered once
//! it has committed: before that, a write is not durable. Each commit wipes
//! the page cache of every reader, so commits are spaced by at least
//! `commit_interval`: when the last commit is older than that, the writes
//! commit at once; otherwise they wait for the interval to pass, and so do the
//! writes that arrive meanwhile.
//!
//! A client's transaction runs in a savepoint of the open transaction: when
//! one of its writes fails, the savepoint undoes the others, and only them.
//!
//! Past `wal_limit` pages of WAL, after a commit, the writer closes the
//! readers' gate and empties the WAL.

use std::sync::Arc;
use std::time::{Duration, Instant};

use turso_core::{Statement, SyncMode, Value};

use crate::conn::Conn;
use crate::db::{Config, Shared, Synchronous};
use crate::job::{Held, Job};
use crate::row::Returned;
use crate::{Error, Write};

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
    /// The open transaction's writes, their answers held until it commits.
    held: Vec<Held>,
    /// Around a client's transaction.
    savepoint: Statement,
    release: Statement,
    rollback_to: Statement,
}

impl Writer {
    pub(crate) fn new(shared: &Arc<Shared>, config: &Config) -> Result<Self, Error> {
        let conn = Conn::new(&shared.db, "BEGIN IMMEDIATE")?;
        conn.set_sync_mode(match config.synchronous {
            Synchronous::Full => SyncMode::Full,
            Synchronous::Normal => SyncMode::Normal,
        });
        Ok(Writer {
            savepoint: conn.prepare("SAVEPOINT tx")?,
            release: conn.prepare("RELEASE tx")?,
            rollback_to: conn.prepare("ROLLBACK TO tx")?,
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
    pub(crate) fn write(&mut self, writes: &mut Vec<Held>) {
        for mut held in writes.drain(..) {
            // Nobody waits for this write anymore: never run it.
            if held.is_cancelled() {
                held.reply();
                continue;
            }
            if !self.conn.in_transaction()
                && let Err(error) = self.conn.begin()
            {
                held.fail(&error);
                held.reply();
                continue;
            }
            let failed = !held.run(self);
            self.held.push(held);
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

    fn statement<R: Returned>(
        &mut self,
        statement: &'static Write<R>,
        params: Vec<Value>,
    ) -> Result<R, Error> {
        self.conn.run::<R>(statement.key(), statement.sql(), params)
    }

    /// Runs `writes` in order, in a savepoint: the first that fails undoes
    /// the others, and is the answer.
    fn transaction(&mut self, writes: Vec<(&'static Write, Vec<Value>)>) -> Result<(), Error> {
        self.conn.execute(&mut self.savepoint)?;
        let ran = writes
            .into_iter()
            .try_for_each(|(statement, params)| self.statement(statement, params));
        // turso ends the whole transaction on some failures: no savepoint is
        // left then, and the writer fails the batch.
        if self.conn.in_transaction() {
            let undone = match ran {
                Ok(()) => Ok(()),
                Err(_) => self.conn.execute(&mut self.rollback_to),
            };
            let released = undone.and_then(|()| self.conn.execute(&mut self.release));
            if let Err(error) = released {
                // The savepoint is in an unknown state: undo the batch rather
                // than commit half a transaction.
                self.conn.rollback();
                return Err(error);
            }
        }
        ran
    }

    /// The held writes that had succeeded are undone: they get `error`.
    fn fail_held(&mut self, error: Error) {
        for held in &mut self.held {
            held.fail(&error);
        }
    }

    fn answer_held(&mut self) {
        for held in self.held.drain(..) {
            held.reply();
        }
    }
}

/// One statement, answering what its rows decode into.
pub(crate) struct Single<R: 'static> {
    pub(crate) statement: &'static Write<R>,
    pub(crate) params: Vec<Value>,
}

impl<R: Returned + Send> Job for Single<R> {
    type Answer = R;

    fn run(self, writer: &mut Writer) -> Result<R, Error> {
        writer.statement(self.statement, self.params)
    }
}

/// Writes that commit together or not at all.
pub(crate) struct Transaction {
    pub(crate) writes: Vec<(&'static Write, Vec<Value>)>,
}

impl Job for Transaction {
    type Answer = ();

    fn run(self, writer: &mut Writer) -> Result<(), Error> {
        writer.transaction(self.writes)
    }
}
