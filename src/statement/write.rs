use std::ptr;

use super::sql::{Kind, classify};

/// A statement that writes: it crosses to the writer, runs in a batch of
/// writes, and is answered once that batch has committed — with the rows it
/// changed, the last rowid it inserted, and the rows of its `RETURNING` clause
/// if it has one.
#[derive(Debug)]
pub struct Write {
    sql: &'static str,
}

impl Write {
    /// For [`write_sql!`](crate::write_sql), which runs it at compile time.
    ///
    /// Panics on `PRAGMA`, `ATTACH`, `DETACH`, `VACUUM … INTO`, transaction
    /// control, and statements whose first keyword only reads.
    #[doc(hidden)]
    pub const fn __new(sql: &'static str) -> Self {
        match classify(sql) {
            Kind::Write | Kind::Unknown => Write { sql },
            Kind::Read => panic!("this statement only reads: declare it as a `Read`"),
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
