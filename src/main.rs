use std::{fs, io::Read};

use buran::Result;
use rustix::fs::FileExt;
use tempfile::tempfile;

fn main() -> Result<()> {
    let mut f = tempfile()?;
    // f.set_len(100)?;
    f.write_all_at(b"foo", 0)?;
    f.write_all_at(b"Hello, world!", 20)?;

    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    let s = String::from_utf8_lossy(&buf);
    dbg!(buf);
    Ok(())
}
