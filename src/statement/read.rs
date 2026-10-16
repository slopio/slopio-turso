use std::fmt;
use std::marker::PhantomData;
use std::ptr;

use super::sql::{Kind, classify};

/// A statement that only reads: it runs on the calling thread's own read
/// connection, inside a short read transaction shared with its neighbours,
/// and answers an `R` its rows decode into: `Option<T>` for at most one row,
/// `Vec<T>` for every one.
///
/// ```
/// use slopio_turso::{Error, FromRow, Read, Row, read_sql};
///
/// struct User {
///     id: i64,
///     name: String,
/// }
///
/// impl FromRow for User {
///     fn from_row(row: &Row<'_>) -> Result<Self, Error> {
///         Ok(User { id: row.get(0)?, name: row.get(1)? })
///     }
/// }
///
/// static GET_USER: Read<Option<User>> = read_sql!("SELECT id, name FROM users WHERE id = ?1");
/// static ALL_USERS: Read<Vec<User>> = read_sql!("SELECT id, name FROM users ORDER BY id");
/// ```
pub struct Read<R> {
    sql: &'static str,
    /// Decoded on the reader: a `Read` itself holds no `R`.
    answer: PhantomData<fn() -> R>,
}

impl<R> fmt::Debug for Read<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Read").field("sql", &self.sql).finish()
    }
}

impl<R> Read<R> {
    /// For [`read_sql!`](crate::read_sql), which runs it at compile time.
    ///
    /// Panics on `PRAGMA`, `ATTACH`, `DETACH`, `VACUUM … INTO`, transaction
    /// control, and statements whose first keyword writes.
    #[doc(hidden)]
    pub const fn __new(sql: &'static str) -> Self {
        match classify(sql) {
            Kind::Read | Kind::Unknown => Read {
                sql,
                answer: PhantomData,
            },
            Kind::Write => panic!("this statement writes: declare it as a `Write`"),
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
