//! Typed query layer over the positional row API.
//!
//! Each query is a type implementing [`Query`]: its statement, and a
//! concrete row type with a **non-panicking** mapping from the raw positional
//! columns, built from the `get_*` helpers below. Decoding happens on the
//! caller's thread, lazily as rows are iterated: the DB thread only fills the
//! arena.
//!
//! The helpers take a row (`&[ValueRef]`) and a column index, and return a
//! [`DecodeError`] on a shape or type mismatch instead of panicking. `&str` and
//! `&[u8]` results borrow the arena (zero copy).

use std::fmt;
use std::marker::PhantomData;

use turso_core::{Numeric, ValueRef};

use crate::request::{Error, Request, ResultSet};
use crate::statement::Statement;

/// A statically-declared query: its identity, SQL, and row type + mapping.
pub trait Query {
    /// The concrete row type; borrows the arena (for `&str`/`&[u8]` fields).
    type Row<'a>;
    /// Its statement, a `static`, with `?1..` placeholders.
    const STATEMENT: &'static Statement;
    /// Map one row's positional columns into the row type. Never panics.
    fn map<'a>(row: &'a [ValueRef<'a>]) -> Result<Self::Row<'a>, DecodeError>;
}

/// A completed query result, typed by its [`Query`]. Owns the arena its rows
/// borrow from, so iterate it while it is alive.
pub struct Rows<Q: Query> {
    request: Request,
    _marker: PhantomData<fn() -> Q>,
}

impl<Q: Query> Rows<Q> {
    /// A filled request, or the error it failed with.
    pub(crate) fn new(request: Request) -> Result<Self, Error> {
        if let ResultSet::Error(err) = request.result() {
            return Err(err.clone());
        }
        Ok(Rows {
            request,
            _marker: PhantomData,
        })
    }

    /// Lazily decoded, each item a `Result` — decoding is fallible, never
    /// panicking.
    pub fn iter(&self) -> impl Iterator<Item = Result<Q::Row<'_>, DecodeError>> {
        self.request.rows_iter().map(|row| Q::map(row))
    }

    pub fn len(&self) -> usize {
        self.request.row_count().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn request(&self) -> &Request {
        &self.request
    }
}

/// A failure decoding a column into its target type. Returned instead of
/// panicking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// No column at that index in the row.
    MissingColumn { index: usize, len: usize },
    /// A SQL NULL where a value was required.
    UnexpectedNull { index: usize },
    /// The column holds another type than `expected`.
    WrongType {
        index: usize,
        expected: &'static str,
        found: &'static str,
    },
    /// The value does not fit in `target`.
    OutOfRange { index: usize, target: &'static str },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::MissingColumn { index, len } => {
                write!(f, "no column at index {index} (row has {len})")
            }
            DecodeError::UnexpectedNull { index } => write!(f, "column {index} is NULL"),
            DecodeError::WrongType {
                index,
                expected,
                found,
            } => write!(f, "column {index} is {found}, not {expected}"),
            DecodeError::OutOfRange { index, target } => {
                write!(f, "column {index} does not fit in {target}")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// Non-null value at `index`.
fn get<'a>(row: &[ValueRef<'a>], index: usize) -> Result<ValueRef<'a>, DecodeError> {
    match row.get(index).copied() {
        None => Err(DecodeError::MissingColumn {
            index,
            len: row.len(),
        }),
        Some(ValueRef::Null) => Err(DecodeError::UnexpectedNull { index }),
        Some(value) => Ok(value),
    }
}

/// `None` for SQL NULL, else `decode`. Errors only if the column is out of
/// range or `decode` fails.
fn opt<'a, T>(
    row: &[ValueRef<'a>],
    index: usize,
    decode: fn(&[ValueRef<'a>], usize) -> Result<T, DecodeError>,
) -> Result<Option<T>, DecodeError> {
    match row.get(index) {
        None => Err(DecodeError::MissingColumn {
            index,
            len: row.len(),
        }),
        Some(ValueRef::Null) => Ok(None),
        Some(_) => decode(row, index).map(Some),
    }
}

fn wrong_type(index: usize, expected: &'static str, found: ValueRef<'_>) -> DecodeError {
    let found = match found {
        ValueRef::Null => "null",
        ValueRef::Numeric(Numeric::Integer(_)) => "integer",
        ValueRef::Numeric(Numeric::Float(_)) => "real",
        ValueRef::Text(_) => "text",
        ValueRef::Blob(_) => "blob",
    };
    DecodeError::WrongType {
        index,
        expected,
        found,
    }
}

/// Non-null integer at `index`.
pub fn get_i64(row: &[ValueRef<'_>], index: usize) -> Result<i64, DecodeError> {
    match get(row, index)? {
        ValueRef::Numeric(Numeric::Integer(i)) => Ok(i),
        other => Err(wrong_type(index, "integer", other)),
    }
}

/// Non-null integer at `index` that fits in an `i32`.
pub fn get_i32(row: &[ValueRef<'_>], index: usize) -> Result<i32, DecodeError> {
    i32::try_from(get_i64(row, index)?).map_err(|_| DecodeError::OutOfRange {
        index,
        target: "i32",
    })
}

/// Non-null number at `index`; an integer is widened, as `sqlite3_column_double`
/// does.
pub fn get_f64(row: &[ValueRef<'_>], index: usize) -> Result<f64, DecodeError> {
    match get(row, index)? {
        ValueRef::Numeric(Numeric::Float(f)) => Ok(f64::from(f)),
        ValueRef::Numeric(Numeric::Integer(i)) => Ok(i as f64),
        other => Err(wrong_type(index, "real", other)),
    }
}

/// Non-null boolean at `index`: SQLite stores them as the integers 0 and 1.
pub fn get_bool(row: &[ValueRef<'_>], index: usize) -> Result<bool, DecodeError> {
    match get_i64(row, index)? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DecodeError::OutOfRange {
            index,
            target: "bool",
        }),
    }
}

/// Non-null `&str` at `index`, borrowing the arena.
pub fn get_str<'a>(row: &[ValueRef<'a>], index: usize) -> Result<&'a str, DecodeError> {
    match get(row, index)? {
        ValueRef::Text(text) => Ok(text.value),
        other => Err(wrong_type(index, "text", other)),
    }
}

/// Non-null bytes at `index`, borrowing the arena.
pub fn get_blob<'a>(row: &[ValueRef<'a>], index: usize) -> Result<&'a [u8], DecodeError> {
    match get(row, index)? {
        ValueRef::Blob(bytes) => Ok(bytes),
        other => Err(wrong_type(index, "blob", other)),
    }
}

/// Integer at `index`, or `None` for SQL NULL.
pub fn get_opt_i64(row: &[ValueRef<'_>], index: usize) -> Result<Option<i64>, DecodeError> {
    opt(row, index, get_i64)
}

/// Number at `index`, or `None` for SQL NULL.
pub fn get_opt_f64(row: &[ValueRef<'_>], index: usize) -> Result<Option<f64>, DecodeError> {
    opt(row, index, get_f64)
}

/// `&str` at `index`, or `None` for SQL NULL.
pub fn get_opt_str<'a>(row: &[ValueRef<'a>], index: usize) -> Result<Option<&'a str>, DecodeError> {
    opt(row, index, get_str)
}

/// Bytes at `index`, or `None` for SQL NULL.
pub fn get_opt_blob<'a>(
    row: &[ValueRef<'a>],
    index: usize,
) -> Result<Option<&'a [u8]>, DecodeError> {
    opt(row, index, get_blob)
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::Value;

    static USERS: Statement = Statement::new("SELECT id, name, email FROM users");

    /// The row type *is* the query: fields + a lifetime for the borrowed string.
    #[derive(Debug, PartialEq)]
    struct User<'a> {
        id: i64,
        name: &'a str,
        email: Option<&'a str>,
    }

    // `Query` is a lifetime-free identity, so the impl is tagged with `'static`;
    // the actual rows are `User<'shorter>` via the `Row<'a>` GAT.
    impl Query for User<'static> {
        type Row<'a> = User<'a>;
        const STATEMENT: &'static Statement = &USERS;
        fn map<'a>(row: &'a [ValueRef<'a>]) -> Result<User<'a>, DecodeError> {
            Ok(User {
                id: get_i64(row, 0)?,
                name: get_str(row, 1)?,
                email: get_opt_str(row, 2)?,
            })
        }
    }

    fn int(i: i64) -> Value {
        Value::Numeric(Numeric::Integer(i))
    }

    #[test]
    fn typed_rows_decode_positionally() {
        let mut request = Request::prepared(User::STATEMENT, &[]);
        request.begin_rows();
        let ada = [int(1), Value::from_text("ada"), Value::from_text("ada@x")];
        let bob = [int(2), Value::from_text("bob"), Value::Null];
        request.push_row(ada.iter());
        request.push_row(bob.iter());

        let rows = Rows::<User<'static>>::new(request).unwrap();
        assert_eq!(rows.len(), 2);
        let users: Vec<_> = rows.iter().map(Result::unwrap).collect();
        assert_eq!(
            users,
            [
                User {
                    id: 1,
                    name: "ada",
                    email: Some("ada@x")
                },
                User {
                    id: 2,
                    name: "bob",
                    email: None
                },
            ]
        );
    }

    #[test]
    fn a_failed_request_is_an_error() {
        let mut request = Request::prepared(User::STATEMENT, &[]);
        request.set_error(Error::TooManyRows);
        assert!(matches!(
            Rows::<User<'static>>::new(request),
            Err(Error::TooManyRows)
        ));
    }

    #[test]
    fn helpers_report_instead_of_panicking() {
        let values = [
            int(7),
            Value::from_f64(1.5),
            Value::from_text("x"),
            Value::Null,
            int(1 << 40),
            int(2),
        ];
        let row: Vec<_> = values.iter().map(Value::as_ref).collect();

        assert_eq!(get_i64(&row, 0), Ok(7));
        assert_eq!(get_f64(&row, 0), Ok(7.0), "integers widen to f64");
        assert_eq!(get_f64(&row, 1), Ok(1.5));
        assert_eq!(get_str(&row, 2), Ok("x"));
        assert_eq!(get_opt_str(&row, 3), Ok(None));
        assert_eq!(
            get_i64(&row, 2),
            Err(DecodeError::WrongType {
                index: 2,
                expected: "integer",
                found: "text"
            })
        );
        assert_eq!(
            get_i64(&row, 3),
            Err(DecodeError::UnexpectedNull { index: 3 })
        );
        assert_eq!(
            get_i64(&row, 9),
            Err(DecodeError::MissingColumn { index: 9, len: 6 })
        );
        assert_eq!(
            get_i32(&row, 4),
            Err(DecodeError::OutOfRange {
                index: 4,
                target: "i32"
            })
        );
        assert_eq!(
            get_bool(&row, 5),
            Err(DecodeError::OutOfRange {
                index: 5,
                target: "bool"
            })
        );
    }
}
