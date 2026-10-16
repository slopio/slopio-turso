use std::fmt;

use turso_core::LimboError;

#[derive(Debug, Clone)]
pub enum Error {
    /// turso refused or failed the statement.
    Turso(LimboError),
    /// The statement takes `expected` parameters but was given `given`.
    Parameters { expected: usize, given: usize },
    /// The row has no column at this index.
    NoColumn(usize),
    /// A statement answering an `Option` gave more than one row.
    TooManyRows,
    /// The write succeeded, but its batch was not committed: a later write of
    /// it failed and turso rolled the whole transaction back, or the writer
    /// stopped before committing it.
    RolledBack,
}

impl Error {
    /// Another connection holds the lock this one needs.
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            Error::Turso(LimboError::Busy | LimboError::BusySnapshot)
        )
    }
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
            Error::TooManyRows => f.write_str("more than one row for an `Option`"),
            Error::RolledBack => f.write_str("rolled back with its batch"),
        }
    }
}

impl std::error::Error for Error {}
