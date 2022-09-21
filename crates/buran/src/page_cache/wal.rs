use std::{
    convert::TryInto,
    io::{self, BufReader, BufWriter, Read, Seek, Write},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use nonmax::NonMaxU64;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use crate::{
    sys::{self, StdIo, Tracked},
    types::{Lsn, PageId},
};

pub struct Wal {
    file: Arc<sys::File>,
    append: RwLock<Tracked<BufWriter<StdIo<'static>>>>,
    buffer_size: usize,
    size: AtomicU64,
    stable_size: AtomicU64,
    sync_mutex: Mutex<()>,
    rewind_mutex: Mutex<()>,
}

impl Wal {
    fn rewind(&self) -> io::Result<()> {
        let _guard = self.rewind_mutex.lock();
        self.append.write().rewind()?;
        self.file.set_len(0)?;
        self.size.store(0, Ordering::Release);
        self.stable_size.store(0, Ordering::Release);
        Ok(())
    }

    fn write(&self, page: PageId, buffer: &[u8]) -> io::Result<Lsn> {
        self.write_inner(|write| {
            write.write_all(&[RecordKind::PageWrite.into()])?;
            write.write_all(&page.0.get().to_le_bytes())?;
            leb128::write::unsigned(&mut *write, buffer.len() as u64)?;
            write.write_all(buffer)?;
            Ok(())
        })
    }

    fn free(&self, page: PageId) -> io::Result<Lsn> {
        self.write_inner(|write| {
            write.write_all(&[RecordKind::Free.into()])?;
            write.write_all(&page.0.get().to_le_bytes())?;
            Ok(())
        })
    }

    fn commit(&self) -> io::Result<Lsn> {
        self.write_inner(|write| {
            write.write_all(&[RecordKind::Commit.into()])?;
            Ok(())
        })
    }

    fn rollback(&self) -> io::Result<Lsn> {
        self.write_inner(|write| {
            write.write_all(&[RecordKind::Rollback.into()])?;
            Ok(())
        })
    }

    fn sync(&self) -> io::Result<()> {
        let _guard = self.sync_mutex.lock();
        if self.size.load(Ordering::Relaxed) <= self.stable_size.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.file.sync_all()
    }

    fn write_inner(
        &self,
        f: impl FnOnce(&mut Tracked<BufWriter<StdIo<'_>>>) -> io::Result<()>,
    ) -> io::Result<Lsn> {
        let mut write = self.append.write();
        let pos = write.position();

        f(&mut write)?;

        self.size.store(write.position(), Ordering::Release);
        Ok(NonMaxU64::new(pos).map(Lsn).unwrap())
    }
}

#[derive(Debug, Error)]
pub enum ReadRecordError {
    #[error("broken record")]
    Broken,
    #[error(transparent)]
    IoError(#[from] io::Error),
}

pub type ReadRecordResult<T> = std::result::Result<T, ReadRecordError>;

struct WalIter<'a> {
    read: Tracked<BufReader<StdIo<'a>>>,
    done: bool,
}

impl<'a> WalIter<'a> {
    fn read_inner(&mut self) -> ReadRecordResult<(Lsn, Record)> {
        let mut kind = [0];

        let lsn = Lsn(NonMaxU64::new(self.read.position()).unwrap());

        self.read.read_exact(&mut kind)?;

        let kind = kind[0].try_into().map_err(|_| ReadRecordError::Broken)?;

        let record = match kind {
            RecordKind::Commit => Record::Commit,
            RecordKind::Rollback => Record::Rollback,
            RecordKind::Free => {
                let mut page = [0; 8];
                self.read.read_exact(&mut page).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        ReadRecordError::Broken
                    } else {
                        e.into()
                    }
                })?;

                let page = u64::from_le_bytes(page);
                let page = NonMaxU64::new(page)
                    .map(PageId)
                    .ok_or(ReadRecordError::Broken)?;

                Record::Free { page }
            }
            RecordKind::PageWrite => {
                let mut page = [0; 8];
                self.read.read_exact(&mut page).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        ReadRecordError::Broken
                    } else {
                        e.into()
                    }
                })?;

                let page = u64::from_le_bytes(page);
                let page = NonMaxU64::new(page)
                    .map(PageId)
                    .ok_or(ReadRecordError::Broken)?;

                let size = leb128::read::unsigned(&mut self.read)
                    .map_err(|e| match e {
                        leb128::read::Error::IoError(io)
                            if io.kind() == io::ErrorKind::UnexpectedEof =>
                        {
                            ReadRecordError::Broken
                        }
                        leb128::read::Error::IoError(io) => ReadRecordError::IoError(io),
                        leb128::read::Error::Overflow => ReadRecordError::Broken,
                    })?
                    .try_into()
                    .map_err(|_| ReadRecordError::Broken)?;

                self.read.seek_relative(size).map_err(|e| {
                    if e.kind() == io::ErrorKind::UnexpectedEof {
                        ReadRecordError::Broken
                    } else {
                        e.into()
                    }
                })?;
                Record::PageWrite {
                    page,
                    size: size as u64,
                }
            }
        };

        Ok((lsn, record))
    }
}

impl<'a> Iterator for WalIter<'a> {
    type Item = ReadRecordResult<(Lsn, Record)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        match self.read_inner() {
            Ok((lsn, record)) => Some(Ok((lsn, record))),
            Err(ReadRecordError::IoError(err)) if err.kind() == io::ErrorKind::UnexpectedEof => {
                None
            }
            Err(err) => {
                self.done = true;
                Some(Err(err))
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Record {
    Commit,
    Rollback,
    Free { page: PageId },
    PageWrite { page: PageId, size: u64 },
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive)]
pub enum RecordKind {
    Commit,
    Rollback,
    PageWrite,
    Free,
}
