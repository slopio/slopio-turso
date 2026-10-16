use std::fmt;
use std::marker::PhantomData;
use std::ptr;

use super::sql::{Kind, classify};

/// A statement that writes: it crosses to the writer, runs in a batch of
/// writes, and is answered once that batch has committed, with an `R` its rows
/// decode into: `()` by default, `Option<T>` or `Vec<T>` for the rows of its
/// `RETURNING` clause.
///
/// ```
/// use slopio_turso::{Error, FromRow, Row, Write, write_sql};
///
/// struct Id(i64);
///
/// impl FromRow for Id {
///     fn from_row(row: &Row<'_>) -> Result<Self, Error> {
///         Ok(Id(row.get(0)?))
///     }
/// }
///
/// static RENAME: Write = write_sql!("UPDATE users SET name = ?2 WHERE id = ?1");
/// static ADD_USER: Write<Option<Id>> =
///     write_sql!("INSERT INTO users (name) VALUES (?1) RETURNING id");
/// ```
pub struct Write<R = ()> {
    sql: &'static str,
    /// Decoded on the DB thread, then sent: a `Write` itself holds no `R`.
    answer: PhantomData<fn() -> R>,
}

impl<R> fmt::Debug for Write<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Write").field("sql", &self.sql).finish()
    }
}

impl<R> Write<R> {
    /// For [`write_sql!`](crate::write_sql), which runs it at compile time.
    ///
    /// Panics on `PRAGMA`, `ATTACH`, `DETACH`, `VACUUM … INTO`, transaction
    /// control, and statements whose first keyword only reads.
    #[doc(hidden)]
    pub const fn __new(sql: &'static str) -> Self {
        match classify(sql) {
            Kind::Write | Kind::Unknown => Write {
                sql,
                answer: PhantomData,
            },
            Kind::Read => panic!("this statement only reads: declare it as a `Read`"),
            Kind::Transaction => {
                panic!("run writes together with `Client::transaction`")
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
