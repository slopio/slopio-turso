//! turso on slopio's thread-per-core runtime.
//!
//! Reads run where they are asked: every worker thread owns a read connection
//! and steps its statements itself, sharing short read transactions between
//! them. Writes cross to the one writer, which batches them into few commits.

mod client;
mod conn;
pub mod db;
mod error;
mod reader;
mod request;
mod row;
pub mod statement;
mod timer;
mod writer;

pub use client::Client;
pub use error::Error;
pub use row::{FromRow, Row};
pub use statement::{Read, Write};
