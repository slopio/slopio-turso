//! The rows of a result, encoded one after the other in a single byte buffer,
//! in the format of [`crate::shared::codec`].
//!
//! Rows follow one another with no header: every row of a result has the same
//! number of columns, stored once.

use turso_core::{Value, ValueRef};

use crate::shared::codec;

#[derive(Debug, Default)]
pub struct RowBuf {
    bytes: Vec<u8>,
    columns: usize,
    rows: usize,
}

impl RowBuf {
    pub fn new() -> Self {
        RowBuf::default()
    }

    /// Reuses `bytes`' allocation: it is cleared, its capacity kept.
    pub fn from_vec(mut bytes: Vec<u8>) -> Self {
        bytes.clear();
        RowBuf {
            bytes,
            columns: 0,
            rows: 0,
        }
    }

    /// Appends one row (`stmt.row().get_values()`). Every row must have as
    /// many values as the first.
    pub fn push_row<'v>(&mut self, values: impl IntoIterator<Item = &'v Value>) {
        let mut columns = 0;
        for value in values {
            codec::encode(&mut self.bytes, value.as_ref());
            columns += 1;
        }
        if self.rows == 0 {
            self.columns = columns;
        }
        debug_assert_eq!(columns, self.columns, "every row has the same columns");
        self.rows += 1;
    }

    /// The number of rows.
    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// The rows, in the order they were pushed.
    pub fn iter(&self) -> impl Iterator<Item = Row<'_>> {
        let columns = self.columns;
        let mut rest = &self.bytes[..];
        (0..self.rows).map(move |_| {
            let len = (0..columns).fold(0, |len, _| len + codec::encoded_len(&rest[len..]));
            let (bytes, after) = rest.split_at(len);
            rest = after;
            Row { bytes, columns }
        })
    }
}

/// One encoded row, borrowing its `RowBuf`.
#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    bytes: &'a [u8],
    columns: usize,
}

impl<'a> Row<'a> {
    /// The number of columns.
    pub fn len(&self) -> usize {
        self.columns
    }

    pub fn is_empty(&self) -> bool {
        self.columns == 0
    }

    /// The values, left to right.
    pub fn values(&self) -> impl Iterator<Item = ValueRef<'a>> {
        codec::values(self.bytes)
    }

    /// The value in column `index`: decodes the ones before it to get there.
    pub fn get(&self, index: usize) -> Option<ValueRef<'a>> {
        self.values().nth(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_come_back_in_order() {
        let mut buf = RowBuf::new();
        for (id, name) in [(1, "ada"), (2, "bob"), (3, "cy")] {
            buf.push_row(&[Value::from_i64(id), Value::from_text(name)]);
        }
        assert_eq!(buf.len(), 3);
        let names: Vec<_> = buf
            .iter()
            .map(|row| row.get(1).unwrap().to_text().unwrap())
            .collect();
        assert_eq!(names, ["ada", "bob", "cy"]);
    }

    #[test]
    fn a_row_ends_at_its_last_column() {
        let mut buf = RowBuf::new();
        buf.push_row(&[Value::from_i64(1), Value::Null]);
        buf.push_row(&[Value::from_i64(2), Value::from_text("bob")]);
        let first = buf.iter().next().unwrap();
        assert_eq!(first.values().count(), 2);
        assert!(first.get(2).is_none());
    }

    #[test]
    fn empty_buffer_has_no_rows() {
        let buf = RowBuf::new();
        assert!(buf.is_empty());
        assert_eq!(buf.iter().count(), 0);
    }

    #[test]
    fn from_vec_starts_empty_and_keeps_capacity() {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&[1, 2, 3]);
        let buf = RowBuf::from_vec(bytes);
        assert!(buf.is_empty());
        assert!(buf.bytes.capacity() >= 64);
    }

    #[test]
    fn values_are_packed() {
        let mut buf = RowBuf::new();
        buf.push_row(&[Value::from_i64(42), Value::from_text("user42")]);
        // integer: tag + 8, text: tag + 4 + 6
        assert_eq!(buf.bytes.len(), 9 + 11);
    }
}
