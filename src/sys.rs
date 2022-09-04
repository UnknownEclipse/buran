use std::io;

#[cfg(unix)]
use self::unix as imp;
#[cfg(windows)]
use self::windows::File;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

pub struct File(imp::File);

impl File {
    pub fn write_all_at(&self, buf: &[u8], offset: u64) -> io::Result<()> {
        todo!()
    }

    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> io::Result<()> {
        todo!()
    }
}
