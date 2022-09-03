use std::{
    collections::HashMap,
    fs::File,
    io,
    mem::ManuallyDrop,
    os::unix::prelude::{FileExt, MetadataExt, RawFd},
    sync::{Arc, Weak},
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use rustix::fd::{AsFd, AsRawFd};
use tracing::warn;

use crate::{device::RawDevice, Error, ErrorKind, Result};

pub struct FileDevice {
    file: ManuallyDrop<File>,
    data: Arc<Mutex<InodeData>>,
}

impl FileDevice {
    pub fn from_std_file(f: File) -> Result<Self> {
        let data = file_data(&f)?;
        let file = ManuallyDrop::new(f);
        Ok(Self { file, data })
    }

    // pub fn open(path: &Path) -> Result<Self> {
    //     let f = File::options().read(true).write(true).open(path)?;
    //     Self::from_std_file(f)
    // }

    // pub fn create(path: &Path, exclusive: bool) -> Result<Self> {
    //     let parent = path.parent().ok_or(io::ErrorKind::InvalidInput)?;
    //     let dir = File::open(parent)?;

    //     let mut name = path
    //         .file_name()
    //         .ok_or(io::ErrorKind::InvalidInput)?
    //         .as_bytes()
    //         .to_vec();
    //     name.push(b'\0');
    //     let path = CString::from_vec_with_nul(name).unwrap();

    //     let mode = 0o666;
    //     let mut flags = libc::O_CREAT | libc::O_RDWR;
    //     if exclusive {
    //         flags |= libc::O_EXCL;
    //     }

    //     let fd = unsafe { libc::openat(dir.as_raw_fd(), path.as_ptr(), flags, mode) };

    //     if fd < 0 {
    //         Err(io::Error::last_os_error().into())
    //     } else {
    //         let file = unsafe { File::from_raw_fd(fd) };
    //         dir.sync_all()?;
    //         Self::from_std_file(file)
    //     }
    // }
}

unsafe impl RawDevice for FileDevice {
    fn block_size(&self) -> Result<usize> {
        let meta = self.file.metadata()?;
        Ok(meta.blksize().try_into().unwrap())
    }

    fn allocate(&self, size: u64) -> Result<()> {
        use fs2::FileExt;

        Ok(self.file.allocate(size)?)
    }

    unsafe fn read_into(&self, buffer: &mut [u8], offset: u64) -> Result<()> {
        Ok(self.file.read_exact_at(buffer, offset)?)
    }

    unsafe fn write(&self, buf: &[u8], offset: u64) -> Result<()> {
        self.file.write_all_at(buf, offset)?;
        Ok(())
    }

    unsafe fn append(&self, mut buf: &[u8]) -> Result<()> {
        let fd = self.file.as_fd();

        while !buf.is_empty() {
            match rustix::io::write(fd, buf) {
                Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => {
                    buf = &buf[n..];
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(err) => return Err(io::Error::from(err).into()),
            }
        }
        Ok(())
    }

    fn set_len(&self, size: u64) -> Result<()> {
        self.file.set_len(size)?;
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    fn try_lock_shared(&self) -> Result<()> {
        let mut data = self.data.lock();
        match &mut data.state {
            LockState::Unlocked => {
                set_lock(&self.file, LockState::Shared(1))?;
                data.state = LockState::Shared(1);
                Ok(())
            }
            LockState::Shared(n) => {
                if *n > isize::MAX as usize {
                    panic!("lock count overflow")
                }
                Ok(())
            }
            LockState::Exclusive(_) => Err(ErrorKind::LockContended.into()),
        }
    }

    fn try_lock_exclusive(&self) -> Result<()> {
        let mut data = self.data.lock();
        match &mut data.state {
            LockState::Unlocked => {
                let state = LockState::Exclusive(self.file.as_raw_fd());
                set_lock(&self.file, state)?;
                data.state = state;
                Ok(())
            }
            LockState::Shared(_) | LockState::Exclusive(_) => Err(ErrorKind::LockContended.into()),
        }
    }

    unsafe fn unlock_shared(&self) -> Result<()> {
        let mut data = self.data.lock();
        match &mut data.state {
            LockState::Unlocked => {
                warn!("file.unlock_shared called on unlocked file");
                Ok(())
            }
            LockState::Shared(n) => {
                if *n <= 1 {
                    set_lock(&self.file, LockState::Unlocked)?;
                    data.state = LockState::Unlocked;
                } else {
                    *n -= 1;
                }
                Ok(())
            }
            LockState::Exclusive(_) => {
                panic!("file.unlock_shared: called on exclusively locked file")
            }
        }
    }

    unsafe fn unlock_exclusive(&self) -> Result<()> {
        let mut data = self.data.lock();
        match &mut data.state {
            LockState::Unlocked => {
                warn!("file.unlock_exclusive called on unlocked file");
                Ok(())
            }
            LockState::Exclusive(fd) => {
                if *fd != self.file.as_raw_fd() {
                    warn!("file.unlock_exclusive: called on different file descriptor than file.lock_exclusive");
                }
                set_lock(&self.file, LockState::Unlocked)?;
                data.state = LockState::Unlocked;
                Ok(())
            }
            LockState::Shared(_) => {
                panic!("file.unlock_exclusive: called on shared locked file")
            }
        }
    }

    fn len(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }
}

impl Drop for FileDevice {
    fn drop(&mut self) {
        let f = unsafe { ManuallyDrop::take(&mut self.file) };
        let mut data = self.data.lock();
        if data.is_locked() {
            data.retired.push(f);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct FileId {
    dev: u64,
    ino: u64,
}

#[derive(Debug, Default, Clone, Copy)]
enum LockState {
    #[default]
    Unlocked,
    Shared(usize),
    Exclusive(RawFd),
}

#[derive(Debug, Default)]
struct InodeData {
    state: LockState,
    retired: Vec<File>,
}

impl InodeData {
    pub fn is_locked(&self) -> bool {
        todo!()
    }
}

static INODE_TABLE: Lazy<Mutex<HashMap<FileId, Weak<Mutex<InodeData>>>>> =
    Lazy::new(Default::default);

fn file_data(f: &File) -> Result<Arc<Mutex<InodeData>>> {
    let meta = f.metadata()?;
    let id = FileId {
        dev: meta.dev(),
        ino: meta.ino(),
    };

    let mut table = INODE_TABLE.lock();
    let weak = table.entry(id).or_default();
    if let Some(arc) = weak.upgrade() {
        Ok(arc)
    } else {
        let arc = Default::default();
        *weak = Arc::downgrade(&arc);
        Ok(arc)
    }
}

fn set_lock(f: &File, state: LockState) -> Result<()> {
    todo!()
}
