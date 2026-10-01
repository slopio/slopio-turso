//! What travels between the application's threads and the DB thread, or what
//! both sides know.

pub(crate) mod codec;
pub mod request;
pub mod response;
pub mod statement;
pub mod transaction;
