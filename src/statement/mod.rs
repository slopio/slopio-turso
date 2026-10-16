//! SQL statements, fixed at compile time and typed by where they run.
//!
//! - A [`Read`] runs on the calling thread's own read connection.
//! - A [`Write`] crosses to the writer and is answered after its batch commits.
//!
//! Define each statement as a `static`, from a string literal, typed by what
//! its rows decode into:
//!
//! ```
//! use slopio_turso::{Error, FromRow, Read, Row, Write, read_sql, write_sql};
//!
//! struct Name(String);
//!
//! impl FromRow for Name {
//!     fn from_row(row: &Row<'_>) -> Result<Self, Error> {
//!         Ok(Name(row.get(0)?))
//!     }
//! }
//!
//! static GET_NAME: Read<Option<Name>> = read_sql!("SELECT name FROM users WHERE id = ?1");
//! static RENAME: Write = write_sql!("UPDATE users SET name = ?2 WHERE id = ?1");
//! ```
//!
//! Its address is its identity: the key of every connection's statement cache.
//! Two statics never share an address, so two statements can never be taken
//! for one another, whatever their SQL.
//!
//! The macros take nothing but a literal: no SQL built at run time, so none
//! built from what a user sent.
//!
//! ```compile_fail
//! use slopio_turso::{Read, read_sql};
//!
//! let table = String::from("users");
//! let sql: &'static str = format!("SELECT * FROM {table}").leak();
//! let read: Read<()> = read_sql!(sql);
//! ```
//!
//! They check the SQL at compile time, so what they refuse is a compile
//! error: statements that could reconfigure the shared connections or reach
//! other files, transaction control, and a statement declared on the wrong
//! side by its first keyword:
//!
//! ```compile_fail
//! use slopio_turso::{Read, read_sql};
//!
//! static SNEAKY: Read<()> = read_sql!("DELETE FROM users");
//! ```
//!
//! ```compile_fail
//! use slopio_turso::{Write, write_sql};
//!
//! static RECONFIGURE: Write = write_sql!("PRAGMA query_only = 0");
//! ```
//!
//! The first keyword is not the whole story: `WITH … INSERT` reads like a
//! `SELECT` until its end. Preparing the statement settles it: the read
//! connections are query-only, and refuse a `Read` that writes.

mod read;
mod sql;
mod write;

pub use read::Read;
pub use write::Write;

/// A [`Read`] from a string literal, checked at compile time.
#[macro_export]
macro_rules! read_sql {
    ($sql:literal $(,)?) => {
        const { $crate::Read::__new($sql) }
    };
}

/// A [`Write`] from a string literal, checked at compile time.
#[macro_export]
macro_rules! write_sql {
    ($sql:literal $(,)?) => {
        const { $crate::Write::__new($sql) }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_static_is_its_own_statement() {
        static A: Read<()> = read_sql!("SELECT 1");
        static B: Read<()> = read_sql!("SELECT 1");
        static C: Write = write_sql!("CREATE TABLE t (x)");
        assert_ne!(A.key(), B.key());
        assert_eq!(A.key(), A.key());
        assert_ne!(A.key(), C.key());
    }

    #[test]
    fn a_cte_is_accepted_on_either_side() {
        static READ: Read<()> = read_sql!("WITH x AS (SELECT 1) SELECT * FROM x");
        static WRITE: Write = write_sql!("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x");
        assert!(READ.sql().starts_with("WITH"));
        assert!(WRITE.sql().starts_with("WITH"));
    }
}
