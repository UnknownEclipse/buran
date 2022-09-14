use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{self, SeekFrom},
    mem::{ManuallyDrop, MaybeUninit},
    os::unix::prelude::{AsRawFd, FileExt, MetadataExt, RawFd},
    path::Path,
    sync::{Arc, Weak},
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tempfile::tempfile;
use tracing::warn;

use crate::Result;

pub struct File {
    file: ManuallyDrop<fs::File>,
    data: Arc<Mutex<InodeData>>,
}

impl File {
    pub fn temp() -> Result<Self> {
        tempfile().map_err(Into::into).and_then(Self::from_std_file)
    }

    pub fn open_with<P>(path: P, open_options: &OpenOptions) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        Self::_open_with(path.as_ref(), open_options.clone())
    }

    #[cfg(target_os = "linux")]
    fn _open_with(path: &Path, mut open_options: OpenOptions) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;

        Self::from_std_file(open_options.custom_flags(libc::O_DIRECT).open(path)?)
    }

    #[cfg(any(target_os = "macos", target_os = "ios"))]
    fn _open_with(path: &Path, open_options: OpenOptions) -> Result<Self> {
        let f = open_options.open(path)?;
        let rc = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_NOCACHE, 1) };
        if rc < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Self::from_std_file(f)
    }

    pub fn from_std_file(f: fs::File) -> Result<Self> {
        let data = file_data(&f)?;
        let file = ManuallyDrop::new(f);
        Ok(Self { file, data })
    }

    pub fn block_size(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.blksize())
    }

    pub fn read_at(&self, buffer: &mut [MaybeUninit<u8>], offset: u64) -> io::Result<usize> {
        let buffer = unsafe { &mut *(buffer as *mut [_] as *mut [u8]) };
        self.file.read_at(buffer, offset)
    }

    pub fn write_at(&self, buf: &[u8], offset: u64) -> io::Result<usize> {
        self.file.write_at(buf, offset)
    }

    pub fn set_len(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }

    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    pub fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }

    pub fn allocate(&self, size: u64) -> io::Result<()> {
        fs2::FileExt::allocate(&*self.file, size)
    }

    pub fn try_lock_shared(&self) -> io::Result<bool> {
        let mut data = self.data.lock();
        match &mut data.state {
            LockState::Unlocked => {
                set_lock(&self.file, LockState::Shared(1))?;
                data.state = LockState::Shared(1);
                Ok(true)
            }
            LockState::Shared(n) => {
                if *n > isize::MAX as usize {
                    panic!("lock count overflow")
                }
                Ok(true)
            }
            LockState::Exclusive(_) => Ok(false),
        }
    }

    pub fn try_lock_exclusive(&self) -> io::Result<bool> {
        let mut data = self.data.lock();
        match &mut data.state {
            LockState::Unlocked => {
                let state = LockState::Exclusive(self.file.as_raw_fd());
                set_lock(&self.file, state)?;
                data.state = state;
                Ok(true)
            }
            LockState::Shared(_) | LockState::Exclusive(_) => Ok(false),
        }
    }

    pub fn unlock_shared(&self) -> io::Result<()> {
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
                    data.retired.clear();
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

    pub fn unlock_exclusive(&self) -> io::Result<()> {
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
                data.retired.clear();
                Ok(())
            }
            LockState::Shared(_) => {
                panic!("file.unlock_exclusive: called on shared locked file")
            }
        }
    }

    pub fn len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub fn seek(&self, whence: SeekFrom) -> io::Result<u64> {
        #[cfg(not(target_os = "linux"))]
        use libc::lseek;
        #[cfg(target_os = "linux")]
        use libc::lseek64 as lseek;

        let (whence, pos) = match whence {
            SeekFrom::Start(off) => (libc::SEEK_SET, off as i64),
            SeekFrom::End(off) => (libc::SEEK_END, off),
            SeekFrom::Current(off) => (libc::SEEK_CUR, off),
        };

        let rc = unsafe { lseek(self.file.as_raw_fd(), pos as _, whence) };

        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc as u64)
        }
    }

    pub fn read(&self, buf: &mut [MaybeUninit<u8>]) -> io::Result<usize> {
        #[cfg(not(target_os = "linux"))]
        use libc::read;
        #[cfg(target_os = "linux")]
        use libc::read64 as read;

        let rc = unsafe { read(self.file.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };

        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc as usize)
        }
    }

    pub fn write(&self, buf: &[u8]) -> io::Result<usize> {
        #[cfg(not(target_os = "linux"))]
        use libc::write;
        #[cfg(target_os = "linux")]
        use libc::write64 as write;

        let rc = unsafe { write(self.file.as_raw_fd(), buf.as_ptr().cast(), buf.len()) };

        if rc < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(rc as usize)
        }
    }
}

impl Drop for File {
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
    retired: Vec<fs::File>,
}

impl InodeData {
    pub fn is_locked(&self) -> bool {
        todo!()
    }
}

static INODE_TABLE: Lazy<Mutex<HashMap<FileId, Weak<Mutex<InodeData>>>>> =
    Lazy::new(Default::default);

fn file_data(f: &fs::File) -> Result<Arc<Mutex<InodeData>>> {
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

fn set_lock(f: &fs::File, state: LockState) -> io::Result<bool> {
    unsafe { set_lock_inner(f.as_raw_fd(), libc::F_SETLK, state) }
}

unsafe fn set_lock_inner(fd: RawFd, cmd: libc::c_int, state: LockState) -> io::Result<bool> {
    let l_type = match state {
        LockState::Unlocked => libc::F_UNLCK,
        LockState::Shared(_) => libc::F_RDLCK,
        LockState::Exclusive(_) => libc::F_WRLCK,
    };

    let flock = libc::flock {
        l_whence: libc::SEEK_SET as _,
        l_len: 0,
        l_start: 0,
        l_pid: 0,
        l_type,
    };

    let rc = unsafe { libc::fcntl(fd, cmd, &flock) };
    if rc < 0 {
        let err = io::Error::last_os_error();
        if matches!(err.raw_os_error(), Some(libc::EAGAIN | libc::EACCES)) {
            Ok(false)
        } else {
            Err(err)
        }
    } else {
        Ok(true)
    }
}
