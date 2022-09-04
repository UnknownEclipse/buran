use std::{num::NonZeroU64, sync::atomic::Ordering};

use either::Either;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::util::{hash::TinyHashBuilder, hash_lru::HashLru};

use super::{Frame, FrameList, ListHead};

pub(super) struct Lru2QPolicy {
    am: ListHead,
    am_len: usize,
    am_capacity: usize,

    a1in: ListHead,
    a1in_len: usize,
    a1in_capacity: usize,

    a1out: HashLru<NonZeroU64, TinyHashBuilder>,
}

impl Lru2QPolicy {
    pub fn access(&mut self, frames: &[Frame], frame_index: usize) {
        let region = frames[frame_index].region.load(Ordering::Relaxed);
        let region = Region::try_from(region).expect("invalid cache region");

        match region {
            Region::Am => {
                let mut am = FrameList {
                    frames,
                    head: Either::Right(&mut self.am),
                };
                am.remove(frame_index);
                am.push_back(frame_index);
            }
            Region::A1In => {
                let mut a1in = FrameList {
                    frames,
                    head: Either::Right(&mut self.a1in),
                };
                a1in.remove(frame_index);
                a1in.push_back(frame_index);
            }
        }
    }

    pub fn insert(&mut self, frames: &[Frame], frame: usize) {
        let (q, region) = if self.a1out.deque.contains(frames[frame].page_number()) {
            (&mut self.am, Region::Am)
        } else {
            self.a1in_len += 1;
            (&mut self.a1in, Region::A1In)
        };

        let mut q = FrameList {
            frames,
            head: Either::Right(q),
        };

        frames[frame].region.store(region.into(), Ordering::Relaxed);
        q.push_back(frame);
    }

    pub fn evict(&mut self, frames: &[Frame]) -> Option<usize> {
        if self.a1in_capacity < self.a1in_len {
            let mut a1in = FrameList {
                frames,
                head: Either::Right(&mut self.a1in),
            };

            let i = a1in
                .pop_front()
                .expect("a1in will not be empty if it is full");
            let pgno = frames[i].page_number();
            self.a1out.insert(pgno);
            self.a1in_len -= 1;
            Some(i)
        } else {
            let mut am = FrameList {
                frames,
                head: Either::Right(&mut self.am),
            };
            am.pop_front()
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
enum Region {
    Am,
    A1In,
}
