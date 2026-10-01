use std::ptr;

use super::sql::{Kind, classify};

/// A statement that only reads: it runs on the calling thread's own read
/// connection, inside a short read transaction shared with its neighbours.
#[derive(Debug)]
pub struct Read {
    sql: &'static str,
}

impl Read {
    /// Panics — a compile error when defining a `static` — on `PRAGMA`,
    /// `ATTACH`, `DETACH`, `VACUUM … INTO`, transaction control, and statements
    /// whose first keyword writes.
    pub const fn new(sql: &'static str) -> Self {
        match classify(sql) {
            Kind::Read | Kind::Unknown => Read { sql },
            Kind::Write => panic!("this statement writes: declare it as a `Write`"),
            Kind::Transaction => {
                panic!("transactions are opened and ended through the transaction API")
            }
            Kind::Forbidden => panic!(
                "PRAGMA, ATTACH, DETACH and VACUUM INTO statements could reconfigure the \
                 shared connections or reach other files"
            ),
        }
    }

    pub const fn sql(&self) -> &'static str {
        self.sql
    }

    /// Its identity: the address of the `static`.
    pub(crate) fn key(&'static self) -> usize {
        ptr::from_ref(self) as usize
    }
}
