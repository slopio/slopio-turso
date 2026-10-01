//! SQL statements, fixed at compile time and typed by where they run.
//!
//! - A [`Read`] runs on the calling thread's own read connection.
//! - A [`Write`] crosses to the writer and is answered after its batch commits.
//!
//! Define each statement as a `static`:
//!
//! ```
//! use slopio_turso::{Read, Write};
//!
//! static GET_USER: Read = Read::new("SELECT id, name FROM users WHERE id = ?1");
//! static ADD_USER: Write = Write::new("INSERT INTO users (name) VALUES (?1) RETURNING id");
//! ```
//!
//! Its address is its identity: the key of every connection's statement cache.
//! Two statics never share an address, so two statements can never be taken
//! for one another, whatever their SQL.
//!
//! Both constructors run at compile time for a `static`, so what they refuse
//! is a compile error: statements that could reconfigure the shared
//! connections or reach other files, transaction control, and a statement
//! declared on the wrong side by its first keyword:
//!
//! ```compile_fail
//! use slopio_turso::Read;
//!
//! static SNEAKY: Read = Read::new("DELETE FROM users");
//! ```
//!
//! ```compile_fail
//! use slopio_turso::Write;
//!
//! static RECONFIGURE: Write = Write::new("PRAGMA query_only = 0");
//! ```
//!
//! The first keyword is not the whole story: `WITH … INSERT` reads like a
//! `SELECT` until its end. Preparing the statement settles it, from what turso
//! compiled.

mod read;
mod sql;
mod write;

pub use read::Read;
pub use write::Write;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_static_is_its_own_statement() {
        static A: Read = Read::new("SELECT 1");
        static B: Read = Read::new("SELECT 1");
        static C: Write = Write::new("CREATE TABLE t (x)");
        assert_ne!(A.key(), B.key());
        assert_eq!(A.key(), A.key());
        assert_ne!(A.key(), C.key());
    }

    #[test]
    fn a_cte_is_accepted_on_either_side() {
        static READ: Read = Read::new("WITH x AS (SELECT 1) SELECT * FROM x");
        static WRITE: Write = Write::new("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x");
        assert!(READ.sql().starts_with("WITH"));
        assert!(WRITE.sql().starts_with("WITH"));
    }
}
