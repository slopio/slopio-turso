//! A look at a statement's SQL at compile time: enough to sort it by its first
//! keyword, not to parse it.

/// What a statement's first keyword says it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    /// `PRAGMA`, `ATTACH`, `DETACH`, `VACUUM … INTO`: could reconfigure the
    /// shared connections or reach other files.
    Forbidden,
    /// `BEGIN`, `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`.
    Transaction,
    /// `SELECT`, `VALUES`, or anything allowed under `EXPLAIN`, which runs
    /// nothing.
    Read,
    /// `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, and schema changes.
    Write,
    /// Anything else, `WITH` first among them: a CTE may lead to a `SELECT` or
    /// an `INSERT`. Only preparing it tells.
    Unknown,
}

pub(crate) const fn classify(sql: &str) -> Kind {
    let sql = sql.as_bytes();
    let (mut start, mut end) = word(sql, 0);
    let explain = is(sql, start, end, b"EXPLAIN");
    if explain {
        (start, end) = word(sql, end);
        if is(sql, start, end, b"QUERY") {
            // Past `PLAN`.
            (_, end) = word(sql, end);
            (start, end) = word(sql, end);
        }
    }
    // Checked under `EXPLAIN` too: some pragmas act as they are prepared.
    if is(sql, start, end, b"PRAGMA")
        || is(sql, start, end, b"ATTACH")
        || is(sql, start, end, b"DETACH")
        || (is(sql, start, end, b"VACUUM") && has_word(sql, end, b"INTO"))
    {
        return Kind::Forbidden;
    }
    if explain {
        return Kind::Read;
    }
    if is(sql, start, end, b"BEGIN")
        || is(sql, start, end, b"COMMIT")
        || is(sql, start, end, b"END")
        || is(sql, start, end, b"ROLLBACK")
        || is(sql, start, end, b"SAVEPOINT")
        || is(sql, start, end, b"RELEASE")
    {
        return Kind::Transaction;
    }
    if is(sql, start, end, b"SELECT") || is(sql, start, end, b"VALUES") {
        return Kind::Read;
    }
    if is(sql, start, end, b"INSERT")
        || is(sql, start, end, b"UPDATE")
        || is(sql, start, end, b"DELETE")
        || is(sql, start, end, b"REPLACE")
        || is(sql, start, end, b"CREATE")
        || is(sql, start, end, b"DROP")
        || is(sql, start, end, b"ALTER")
        || is(sql, start, end, b"REINDEX")
        || is(sql, start, end, b"ANALYZE")
        || is(sql, start, end, b"VACUUM")
    {
        return Kind::Write;
    }
    Kind::Unknown
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

    fn all(kind: Kind, sqls: &[&str]) {
        for sql in sqls {
            assert_eq!(classify(sql), kind, "{sql}");
        }
    }

    #[test]
    fn reconfiguring_or_file_statements_are_forbidden() {
        all(
            Kind::Forbidden,
            &[
                "PRAGMA query_only = 0",
                "pragma journal_mode",
                "  /* hidden */ PRAGMA query_only = 0",
                "-- hidden\nPRAGMA query_only = 0",
                "ATTACH DATABASE '/etc/x' AS x",
                "DETACH x",
                "VACUUM INTO '/tmp/copy.db'",
                "VACUUM main INTO '/tmp/copy.db'",
                "EXPLAIN PRAGMA query_only = 0",
                "EXPLAIN QUERY PLAN PRAGMA query_only = 0",
            ],
        );
    }

    #[test]
    fn transaction_control_is_recognised() {
        all(
            Kind::Transaction,
            &[
                "BEGIN",
                "begin immediate",
                "  COMMIT",
                "/* c */ END TRANSACTION",
                "ROLLBACK TO s",
                "SAVEPOINT s",
                "RELEASE s",
            ],
        );
    }

    #[test]
    fn reads_and_writes_by_first_keyword() {
        all(
            Kind::Read,
            &[
                "SELECT id FROM users",
                "  select 1",
                "VALUES (1), (2)",
                "EXPLAIN INSERT INTO t VALUES (1)",
                "EXPLAIN QUERY PLAN SELECT 1",
                "EXPLAIN BEGIN",
            ],
        );
        all(
            Kind::Write,
            &[
                "INSERT INTO t VALUES (1)",
                "UPDATE t SET x = 1",
                "DELETE FROM t",
                "REPLACE INTO t VALUES (1)",
                "CREATE TABLE begin_log (x)",
                "DROP TABLE t",
                "ALTER TABLE t ADD COLUMN y",
                "VACUUM",
            ],
        );
    }

    #[test]
    fn a_cte_is_left_to_prepare_time() {
        all(
            Kind::Unknown,
            &[
                "WITH x AS (SELECT 1) SELECT * FROM x",
                "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
                "",
            ],
        );
    }
}
