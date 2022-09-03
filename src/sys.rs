#[cfg(unix)]
pub use self::unix::File;
#[cfg(windows)]
pub use self::windows::File;

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;
