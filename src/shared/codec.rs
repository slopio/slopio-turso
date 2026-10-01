//! Values encoded one after the other in a byte buffer: the format of both a
//! request's arguments and its result rows.
//!
//! Each value is a tag byte, then its content:
//!
//! | tag | value   | content                           |
//! |-----|---------|-----------------------------------|
//! | 0   | NULL    | —                                 |
//! | 1   | integer | 8 bytes, `i64` little-endian      |
//! | 2   | real    | 8 bytes, `f64` little-endian      |
//! | 3   | text    | `u32` length little-endian, UTF-8 |
//! | 4   | JSON    | as text                           |
//! | 5   | blob    | `u32` length little-endian, bytes |
//!
//! Decoding copies nothing: text and blobs borrow the buffer. Buffers are only
//! ever written by [`encode`], so a malformed one is a bug: decoding panics.

use turso_core::types::{TextRef, TextSubtype};
use turso_core::{NonNan, Numeric, ValueRef};

const NULL: u8 = 0;
const INTEGER: u8 = 1;
const REAL: u8 = 2;
const TEXT: u8 = 3;
const JSON: u8 = 4;
const BLOB: u8 = 5;

/// Appends `value` to `bytes`.
pub(crate) fn encode(bytes: &mut Vec<u8>, value: ValueRef<'_>) {
    match value {
        ValueRef::Null => bytes.push(NULL),
        ValueRef::Numeric(Numeric::Integer(i)) => {
            bytes.push(INTEGER);
            bytes.extend_from_slice(&i.to_le_bytes());
        }
        ValueRef::Numeric(Numeric::Float(f)) => {
            bytes.push(REAL);
            bytes.extend_from_slice(&f64::from(f).to_le_bytes());
        }
        ValueRef::Text(text) => {
            let tag = match text.subtype {
                TextSubtype::Text => TEXT,
                TextSubtype::Json => JSON,
            };
            encode_bytes(bytes, tag, text.value.as_bytes());
        }
        ValueRef::Blob(blob) => encode_bytes(bytes, BLOB, blob),
    }
}

fn encode_bytes(bytes: &mut Vec<u8>, tag: u8, content: &[u8]) {
    let len = u32::try_from(content.len()).expect("a value is at most 4 GiB");
    bytes.push(tag);
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(content);
}

/// Every value in `bytes`, in order.
pub(crate) fn values(mut bytes: &[u8]) -> impl Iterator<Item = ValueRef<'_>> {
    std::iter::from_fn(move || {
        if bytes.is_empty() {
            return None;
        }
        let (value, rest) = decode(bytes);
        bytes = rest;
        Some(value)
    })
}

/// How many bytes the value at the start of `bytes` takes, tag included.
pub(crate) fn encoded_len(bytes: &[u8]) -> usize {
    match bytes[0] {
        NULL => 1,
        INTEGER | REAL => 1 + 8,
        TEXT | JSON | BLOB => {
            let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
            1 + 4 + len as usize
        }
        tag => unreachable!("unknown tag {tag}"),
    }
}

/// Decodes the value at the start of `bytes`, and returns it with what follows.
fn decode(bytes: &[u8]) -> (ValueRef<'_>, &[u8]) {
    let (&tag, rest) = bytes.split_first().unwrap();
    match tag {
        NULL => (ValueRef::Null, rest),
        INTEGER => {
            let (n, rest) = rest.split_first_chunk().unwrap();
            (
                ValueRef::Numeric(Numeric::Integer(i64::from_le_bytes(*n))),
                rest,
            )
        }
        REAL => {
            let (f, rest) = rest.split_first_chunk().unwrap();
            let f = NonNan::new(f64::from_le_bytes(*f)).expect("encoded from a NonNan");
            (ValueRef::Numeric(Numeric::Float(f)), rest)
        }
        TEXT | JSON => {
            let (content, rest) = split_len(rest);
            let text = std::str::from_utf8(content).expect("encoded from a str");
            let subtype = if tag == JSON {
                TextSubtype::Json
            } else {
                TextSubtype::Text
            };
            (ValueRef::Text(TextRef::new(text, subtype)), rest)
        }
        BLOB => {
            let (content, rest) = split_len(rest);
            (ValueRef::Blob(content), rest)
        }
        tag => unreachable!("unknown tag {tag}"),
    }
}

/// Splits off a `u32` length, then that many bytes.
fn split_len(bytes: &[u8]) -> (&[u8], &[u8]) {
    let (len, rest) = bytes.split_first_chunk().unwrap();
    rest.split_at(u32::from_le_bytes(*len) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use turso_core::Value;
    use turso_core::types::Text;

    #[test]
    fn every_type_round_trips() {
        let originals = [
            Value::Null,
            Value::from_i64(-42),
            Value::from_f64(1.5),
            Value::from_text("ada"),
            Value::Text(Text::json(r#"{"a":1}"#.to_owned())),
            Value::Blob(vec![0, 1, 255]),
            Value::from_text(""),
        ];
        let mut bytes = Vec::new();
        for value in &originals {
            encode(&mut bytes, value.as_ref());
        }
        let decoded: Vec<_> = values(&bytes).collect();
        assert_eq!(decoded.len(), originals.len());
        for (decoded, original) in decoded.iter().zip(&originals) {
            assert_eq!(decoded, original);
        }
        let ValueRef::Text(json) = decoded[4] else {
            panic!("JSON decodes as text");
        };
        assert_eq!(json.subtype, TextSubtype::Json);
    }

    #[test]
    fn encoded_len_matches_encode() {
        for value in [Value::Null, Value::from_i64(7), Value::from_text("user42")] {
            let mut bytes = Vec::new();
            encode(&mut bytes, value.as_ref());
            assert_eq!(encoded_len(&bytes), bytes.len());
        }
    }

    #[test]
    fn empty_buffer_has_no_values() {
        assert_eq!(values(&[]).count(), 0);
    }
}
