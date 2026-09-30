//! turso on a dedicated thread, fed by other threads over io_uring.
//!
//! - [`shared`]: what travels between threads, or both sides know — the
//!   request and its arena, the handoff protocol, statements, transaction ids.
//! - [`client`]: what runs on the application's threads — sending requests,
//!   transactions, decoding typed rows.
//! - [`db`]: what runs on the DB thread — the server loop, the turso
//!   connections, the IO backend.
//!
//! `client` and `db` only depend on `shared`, never on each other.

pub mod client;
pub mod db;
pub mod shared;

pub use client::query::{DecodeError, Query, Rows};
pub use client::{Client, TransactionToken};
pub use db::{Config, RingIo, Server};
pub use shared::request::{Error, Request, ResultSet};
pub use shared::statement::Statement;
pub use shared::transaction::TransactionId;
