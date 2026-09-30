//! SQL statements, fixed at compile time.
//!
//! Define each statement as a `static`:
//!
//! ```
//! use slopio_turso::Statement;
//!
//! static GET_USER: Statement = Statement::new("SELECT id, name FROM users WHERE id = ?1");
//! ```
//!
//! Its address is its identity: the key of every connection's statement cache.
//! Two statics never share an address, so two statements can never be taken
//! for one another, whatever their SQL.
//!
//! [`Statement::new`] refuses statements that could reconfigure the shared
//! connections at prepare time or reach other files — `PRAGMA`, `ATTACH`,
//! `DETACH`, `VACUUM … INTO` — and, being evaluated at compile time for a
//! `static`, turns them into a compile error:
//!
//! ```compile_fail
//! use slopio_turso::Statement;
//!
//! static READ_WRITE: Statement = Statement::new("PRAGMA query_only = 0");
//! ```

use std::ptr;

#[derive(Debug)]
pub struct Statement {
    sql: &'static str,
    controls_transaction: bool,
}

impl Statement {
    /// Panics on `PRAGMA`, `ATTACH`, `DETACH` and `VACUUM … INTO`: a compile
    /// error when defining a `static`.
    pub const fn new(sql: &'static str) -> Self {
        match classify(sql.as_bytes()) {
            Ok(controls_transaction) => Statement {
                sql,
                controls_transaction,
            },
            Err(Forbidden) => panic!(
                "PRAGMA, ATTACH, DETACH and VACUUM INTO statements could reconfigure the \
                 shared connections or reach other files"
            ),
        }
    }

    pub const fn sql(&self) -> &'static str {
        self.sql
    }

    /// Whether it starts or ends a transaction or savepoint (`BEGIN`,
    /// `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`).
    pub const fn controls_transaction(&self) -> bool {
        self.controls_transaction
    }

    /// Its identity: the address of the `static`.
    pub(crate) fn key(&'static self) -> usize {
        ptr::from_ref(self) as usize
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Forbidden;

/// Whether `sql` controls a transaction, or `Forbidden`.
const fn classify(sql: &[u8]) -> Result<bool, Forbidden> {
    let (mut start, mut end) = word(sql, 0);
    if is(sql, start, end, b"EXPLAIN") {
        (start, end) = word(sql, end);
        if is(sql, start, end, b"QUERY") {
            // Past `PLAN`.
            (_, end) = word(sql, end);
            (start, end) = word(sql, end);
        }
    }
    if is(sql, start, end, b"PRAGMA")
        || is(sql, start, end, b"ATTACH")
        || is(sql, start, end, b"DETACH")
        || (is(sql, start, end, b"VACUUM") && has_word(sql, end, b"INTO"))
    {
        return Err(Forbidden);
    }
    Ok(is(sql, start, end, b"BEGIN")
        || is(sql, start, end, b"COMMIT")
        || is(sql, start, end, b"END")
        || is(sql, start, end, b"ROLLBACK")
        || is(sql, start, end, b"SAVEPOINT")
        || is(sql, start, end, b"RELEASE"))
}

/// Bounds of the next word at or after `at`, past spaces and comments.
const fn word(sql: &[u8], at: usize) -> (usize, usize) {
    let start = skip_blank(sql, at);
    let mut end = start;
    while end < sql.len() && (sql[end].is_ascii_alphanumeric() || sql[end] == b'_') {
        end += 1;
    }
    (start, end)
}

/// Skips spaces, `-- line` and `/* block */` comments.
const fn skip_blank(sql: &[u8], mut at: usize) -> usize {
    loop {
        while at < sql.len() && sql[at].is_ascii_whitespace() {
            at += 1;
        }
        if at + 1 < sql.len() && sql[at] == b'-' && sql[at + 1] == b'-' {
            while at < sql.len() && sql[at] != b'\n' {
                at += 1;
            }
        } else if at + 1 < sql.len() && sql[at] == b'/' && sql[at + 1] == b'*' {
            at += 2;
            while at + 1 < sql.len() && !(sql[at] == b'*' && sql[at + 1] == b'/') {
                at += 1;
            }
            at = if at + 2 < sql.len() {
                at + 2
            } else {
                sql.len()
            };
        } else {
            return at;
        }
    }
}

/// Whether `sql[start..end]` is `keyword`, ignoring ASCII case.
const fn is(sql: &[u8], start: usize, end: usize, keyword: &[u8]) -> bool {
    if end - start != keyword.len() {
        return false;
    }
    let mut i = 0;
    while i < keyword.len() {
        if sql[start + i].to_ascii_uppercase() != keyword[i] {
            return false;
        }
        i += 1;
    }
    true
}

/// Whether any word at or after `at` is `keyword`.
const fn has_word(sql: &[u8], mut at: usize, keyword: &[u8]) -> bool {
    while at < sql.len() {
        let (start, end) = word(sql, at);
        if start == end {
            // Not a word (a quote, a paren…): step over it.
            at = start + 1;
        } else if is(sql, start, end, keyword) {
            return true;
        } else {
            at = end;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(sql: &str) -> Result<bool, Forbidden> {
        classify(sql.as_bytes())
    }

    #[test]
    fn reconfiguring_or_file_statements_are_forbidden() {
        for sql in [
            "PRAGMA query_only = 0",
            "pragma journal_mode",
            "  /* hidden */ PRAGMA query_only = 0",
            "-- hidden\nPRAGMA query_only = 0",
            "EXPLAIN PRAGMA query_only = 0",
            "EXPLAIN QUERY PLAN PRAGMA query_only = 0",
            "ATTACH DATABASE '/etc/x' AS x",
            "DETACH x",
            "VACUUM INTO '/tmp/copy.db'",
            "VACUUM main INTO '/tmp/copy.db'",
        ] {
            assert_eq!(check(sql), Err(Forbidden), "{sql}");
        }
    }

    #[test]
    fn transaction_control_is_recognised() {
        for sql in [
            "BEGIN",
            "begin immediate",
            "  COMMIT",
            "/* c */ END TRANSACTION",
            "ROLLBACK TO s",
            "SAVEPOINT s",
            "RELEASE s",
        ] {
            assert_eq!(check(sql), Ok(true), "{sql}");
        }
        for sql in [
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET x = 1",
            "CREATE TABLE begin_log (x)",
            "SELECT 'PRAGMA'",
            "VACUUM",
            "",
        ] {
            assert_eq!(check(sql), Ok(false), "{sql}");
        }
    }

    #[test]
    fn each_static_is_its_own_statement() {
        static A: Statement = Statement::new("SELECT 1");
        static B: Statement = Statement::new("SELECT 1");
        assert_ne!(A.key(), B.key());
        assert_eq!(A.key(), A.key());
    }
}
