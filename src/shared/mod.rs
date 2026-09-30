//! What travels between the application's threads and the DB thread, or what
//! both sides know.

pub(crate) mod pending;
pub mod request;
pub mod statement;
pub mod transaction;
