#![forbid(unsafe_op_in_unsafe_fn)]

use std::{io, result, sync::Arc};

use thiserror::Error;

pub mod buffer_cache;
mod page_cache;
mod page_table;
// mod engine;
// mod store;
mod sys;
mod types;
mod util;

#[derive(Debug, Error)]
#[error(transparent)]
pub struct Error {
    // Use `Arc` here because concurrent code may need to share error values. Don't
    // expose this to public code.
    kind: Arc<ErrorKind>,
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum ErrorKind {
    #[error("broken wal segment")]
    BrokenWalSegment,
    #[error("broken page")]
    BrokenPage,
    #[error("a lock could not be acquired")]
    LockContended,
    #[error("page not found")]
    PageNotFound,
    /// A *crippling* I/O failure has occurred and it is no longer safe to perform
    /// any operations on the database.
    ///
    /// This error is 'fused'; once this has been returned all future operations will
    /// fail immediately. For more details on why this is important and unrecoverable
    /// can be found [here](https://lwn.net/Articles/752093/).
    #[error("a crippling io failure has occurred")]
    IoCrippled,
    #[error("page does not exist")]
    PageDoesNotExist,
    /// An unexpected I/O error occurred.
    #[error("unexpected io error occurred: {0:?}")]
    IoError(#[from] io::Error),
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorKind {
    pub(crate) fn is_recoverable(&self) -> bool {
        !matches!(self, ErrorKind::IoCrippled)
    }
}

impl Error {
    #[inline]
    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    /// Clone this error.
    ///
    /// This is not exposed publicly, so we can't use the standard Clone trait.
    pub(crate) fn clone(&self) -> Self {
        Self {
            kind: self.kind.clone(),
        }
    }
}

impl<E> From<E> for Error
where
    ErrorKind: From<E>,
{
    #[inline]
    fn from(error: E) -> Self {
        Error {
            kind: Arc::new(error.into()),
        }
    }
}

impl From<io::ErrorKind> for ErrorKind {
    fn from(kind: io::ErrorKind) -> Self {
        io::Error::from(kind).into()
    }
}
