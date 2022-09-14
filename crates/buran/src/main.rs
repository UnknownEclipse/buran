use std::{
    fmt::{Debug, DebugStruct},
    fs,
    io::Read,
    sync::Barrier,
};

use ahash::AHashMap;
use buran::Result;
use rustix::fs::FileExt;
use tempfile::tempfile;

fn main() -> Result<()> {
    dbg!(A {});

    Ok(())
}

pub struct A {}

impl Debug for A {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("A").finish_non_exhaustive()
    }
}
