//! Decoding rows into the caller's structs, on the worker's reader, while the
//! row is still in turso's hands.
//!
//! ```
//! use slopio_turso::{Error, FromRow, Row};
//!
//! struct User {
//!     id: i64,
//!     name: String,
//! }
//!
//! impl FromRow for User {
//!     fn from_row(row: &Row<'_>) -> Result<Self, Error> {
//!         Ok(User { id: row.get(0)?, name: row.get(1)? })
//!     }
//! }
//! ```

use turso_core::{FromValueRow, Value};

use crate::Error;

/// Builds a value from one row of a read.
pub trait FromRow: Sized + 'static {
    fn from_row(row: &Row<'_>) -> Result<Self, Error>;
}

/// The row being read. Only valid while it is decoded: the next step of the
/// statement overwrites it.
pub struct Row<'a>(&'a turso_core::Row);

impl<'a> Row<'a> {
    pub(crate) fn new(row: &'a turso_core::Row) -> Self {
        Row(row)
    }

    /// Column `index`, converted to `T`.
    pub fn get<T: FromValueRow<'a> + 'a>(&self, index: usize) -> Result<T, Error> {
        self.check(index)?;
        Ok(self.0.get(index)?)
    }

    /// Column `index` as it is.
    pub fn value(&self, index: usize) -> Result<&'a Value, Error> {
        self.check(index)?;
        Ok(self.0.get_value(index))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// turso reads a column without checking it exists: past the end is out of
    /// bounds.
    fn check(&self, index: usize) -> Result<(), Error> {
        if index < self.0.len() {
            Ok(())
        } else {
            Err(Error::NoColumn(index))
        }
    }
}
