use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    mem::MaybeUninit,
    sync::Arc,
};

use either::Either;

use crate::util::{assume_init_slice_mut, slice_as_uninit_mut};

#[cfg(unix)]
use self::unix as imp;
#[cfg(windows)]
use self::windows::File;

mod buf;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

pub struct File(imp::File);

impl File {
    pub fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        todo!()
    }

    pub fn read_exact_at<'a>(
        &self,
        buf: &mut [MaybeUninit<u8>],
        offset: u64,
    ) -> io::Result<&'a mut [u8]> {
        todo!()
    }

    pub fn allocate(&self, size: u64) -> io::Result<()> {
        self.0.allocate(size)
    }

    pub fn seek(&self, whence: SeekFrom) -> io::Result<u64> {
        self.0.seek(whence)
    }

    pub fn read(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        self.0.read(buf)
    }

    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    pub fn read_exact<'a>(&self, buf: &'a mut [MaybeUninit<u8>]) -> io::Result<&'a mut [u8]> {
        let mut nread = 0;

        while nread < buf.len() {
            match self.read(&mut buf[nread..]) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    nread += n;
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(unsafe { assume_init_slice_mut(buf) })
    }

    pub fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            match self.0.write(buf) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    buf = &buf[n..];
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    pub fn sync_data(&self) -> io::Result<()> {
        self.0.sync_data()
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.0.sync_all()
    }
}

pub struct StdIo<'a>(Either<&'a File, Arc<File>>);

impl<'a> Read for StdIo<'a> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(slice_as_uninit_mut(buf))
    }
}

impl<'a> Write for StdIo<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.sync_data()
    }
}

impl<'a> Seek for StdIo<'a> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.0.seek(pos)
    }
}

impl<'a> From<Arc<File>> for StdIo<'a> {
    fn from(arc: Arc<File>) -> Self {
        Self(Either::Right(arc))
    }
}

impl<'a> From<&'a File> for StdIo<'a> {
    fn from(file: &'a File) -> Self {
        Self(Either::Left(file))
    }
}

pub struct Tracked<T>(T, u64);

impl<T> Tracked<T> {
    pub fn new(inner: T) -> Self {
        Self(inner, 0)
    }

    pub fn new_at_position(inner: T, pos: u64) -> Self {
        Self(inner, pos)
    }

    pub fn new_with_inferred_position(mut inner: T) -> io::Result<Self>
    where
        T: Seek,
    {
        let pos = inner.stream_position()?;
        Ok(Self(inner, pos))
    }

    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn position(&self) -> u64 {
        self.1
    }

    pub fn get_ref(&self) -> &T {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> Tracked<T>
where
    T: Seek,
{
    #[cfg(debug_assertions)]
    pub fn debug_ensure_position(&mut self) -> io::Result<()> {
        let pos = self.0.stream_position()?;
        assert_eq!(pos, self.position());
        Ok(())
    }

    #[inline]
    #[cfg(not(debug_assertions))]
    pub fn debug_ensure_position(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<T> Read for Tracked<T>
where
    T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.0.read(buf)?;
        self.1 += n as u64;
        Ok(n)
    }
}

impl<T> Write for Tracked<T>
where
    T: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.0.write(buf)?;
        self.1 += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<T> Seek for Tracked<T>
where
    T: Seek,
{
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.1 = self.0.seek(pos)?;
        Ok(self.1)
    }
}

enum FileImp {
    Sys(imp::File),
    Mem(buf::File),
}
