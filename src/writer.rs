//! The DB thread's writer: the one connection that writes, for every worker.
//!
//! Writes run as they arrive, in one open transaction, and are answered once
//! it has committed: before that, a write is not durable. Each commit wipes
//! the page cache of every reader, so commits are spaced by at least
//! `commit_interval`: when the last commit is older than that, the writes
//! commit at once; otherwise they wait for the interval to pass, and so do the
//! writes that arrive meanwhile.

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

pub(crate) struct Writer {
    conn: Conn,
    commit_interval: Duration,
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
            commit_interval: config.commit_interval,
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
    }

    fn run(&mut self, msg: &Msg) -> Result<(), Error> {
        let Body::Write { statement, params } = msg.payload.body() else {
            unreachable!("only writes reach the writer");
        };
        let (statement, params) = (*statement, mem::take(params));
        self.conn
            .run(statement.key(), statement.sql(), params, conn::none)
            .map(drop)
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
