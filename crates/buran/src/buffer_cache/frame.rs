use std::sync::atomic::{AtomicU8, Ordering};

use bitflags::bitflags;
use num_enum::{IntoPrimitive, TryFromPrimitive};

pub struct Frame {
    flags: AtomicU8,
}

impl Frame {
    pub fn is_dirty(&self) -> bool {
        self.flags().contains(FrameFlags::DIRTY)
    }

    pub fn flags(&self) -> FrameFlags {
        let flags = self.flags.load(Ordering::Acquire);
        FrameFlags::from_bits(flags).unwrap()
    }
}

enum Inner {
    Free,
}

bitflags! {
    pub struct FrameFlags: u8 {
        const DIRTY = 1 << 0;
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
pub enum State {
    Free,
    Dirty,
}
