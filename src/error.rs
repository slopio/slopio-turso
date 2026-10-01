use std::fmt;

use turso_core::LimboError;

#[derive(Debug)]
pub enum Error {
    /// turso refused or failed the statement.
    Turso(LimboError),
    /// The statement takes `expected` parameters but was given `given`.
    Parameters { expected: usize, given: usize },
    /// The row has no column at this index.
    NoColumn(usize),
    /// `read_one` got more than one row.
    TooManyRows,
}

impl From<LimboError> for Error {
    fn from(error: LimboError) -> Self {
        Error::Turso(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Turso(error) => error.fmt(f),
            Error::Parameters { expected, given } => {
                write!(
                    f,
                    "the statement takes {expected} parameters, {given} given"
                )
            }
            Error::NoColumn(index) => write!(f, "the row has no column {index}"),
            Error::TooManyRows => f.write_str("more than one row for read_one"),
        }
    }
}

impl std::error::Error for Error {}
