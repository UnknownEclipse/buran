use std::{num::NonZeroU64, sync::atomic::Ordering};

use either::Either;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::util::{count_min::Sketch, hash::TinyHashBuilder};

use super::{Frame, FrameList, ListHead};

pub(super) struct TinyLfuPolicy {
    /// Window size as a fraction of total capacity
    window_frac: f32,

    window: ListHead,

    probation: ListHead,
    probation_size: usize,
    probation_capacity: usize,

    protected: ListHead,
    protected_size: usize,
    protected_capacity: usize,

    freq: Sketch<NonZeroU64, TinyHashBuilder>,
    freq_counter: usize,
}

impl TinyLfuPolicy {
    pub fn access(&mut self, frames: &[Frame], index: usize) {
        let region = frames[index].region.load(Ordering::Relaxed);
        let region = Region::try_from(region).expect("invalid cache region");

        self.increment_freq(frames[index].page_number());

        match region {
            Region::Protected => {
                let mut protected = self.protected(frames);
                protected.remove(index);
                protected.push_back(index);
            }
            Region::Probation => {
                self.probation(frames).remove(index);
                self.probation_size -= 1;

                self.protected(frames).push_back(index);
                self.protected_size += 1;

                frames[index]
                    .region
                    .store(Region::Protected.into(), Ordering::Relaxed);

                self.balance_protected(frames);
            }
            Region::Window => {
                self.window(frames).remove(index);

                if self.probation_size < self.probation_capacity {
                    self.probation(frames).push_back(index);
                    self.probation_size += 1;
                    frames[index]
                        .region
                        .store(Region::Probation.into(), Ordering::Relaxed);
                    return;
                }

                self.promote_or_window(frames, index);
            }
        }
    }

    pub fn evict(&mut self, frames: &[Frame]) -> Option<usize> {
        // Prefer if chain over if/else chain or `.or_else()` to satisfy the borrow
        // checker. Polonius can't come soon enough

        let popped = self.window(frames).pop_front();
        if let Some(i) = popped {
            return Some(i);
        }
        let popped = self.probation(frames).pop_front();
        if let Some(i) = popped {
            self.probation_size -= 1;
            return Some(i);
        }
        let popped = self.protected(frames).pop_front();
        if let Some(i) = popped {
            self.protected_size -= 1;
            return Some(i);
        }
        None
    }

    pub fn insert(&mut self, frames: &[Frame], index: usize) {
        self.increment_freq(frames[index].page_number());

        if self.protected_size < self.protected_capacity {
            self.protected(frames).push_back(index);
            self.protected_size += 1;
            frames[index]
                .region
                .store(Region::Protected.into(), Ordering::Relaxed);
            return;
        }
        if self.probation_size < self.probation_capacity {
            self.probation(frames).push_back(index);
            self.probation_size += 1;
            frames[index]
                .region
                .store(Region::Probation.into(), Ordering::Relaxed);
            return;
        }
        self.promote_or_window(frames, index);
    }

    /// Move any overflowing entries to the probation list, then balance the probation
    /// list.
    fn balance_protected(&mut self, frames: &[Frame]) {
        while self.protected_capacity < self.protected_size {
            let i = self
                .protected(frames)
                .pop_front()
                .expect("protected is full");
            self.probation(frames).push_back(i);

            frames[i]
                .region
                .store(Region::Probation.into(), Ordering::Relaxed);
            self.probation_size += 1;
            self.protected_size -= 1;
        }
        self.balance_probation(frames);
    }

    /// Move any overflowing entries from the probation list to the head of the window.
    /// This means that such entries will be evicted *first*.
    fn balance_probation(&mut self, frames: &[Frame]) {
        while self.probation_capacity < self.probation_size {
            let i = self
                .probation(frames)
                .pop_front()
                .expect("probation is full");
            self.window(frames).push_front(i);
            frames[i]
                .region
                .store(Region::Window.into(), Ordering::Relaxed);
            self.probation_size -= 1;
        }
    }

    fn increment_freq(&mut self, pg: NonZeroU64) {
        self.freq.increment(&pg);
        self.freq_counter += 1;

        if self.freq.sample_size() < self.freq_counter {
            self.freq.halve();
            self.freq_counter = 0;
        }
    }

    fn should_promote(&mut self, frames: &[Frame], index: usize) -> bool {
        let old_index = self
            .probation(frames)
            .first()
            .expect("probation should be full");

        let new = self.freq.get(&frames[index].page_number());
        let old = self.freq.get(&frames[old_index].page_number());

        old <= new
    }

    fn promote_or_window(&mut self, frames: &[Frame], index: usize) {
        if self.should_promote(frames, index) {
            let demoted = self
                .probation(frames)
                .pop_front()
                .expect("probation is full");

            self.window(frames).push_back(demoted);
            frames[demoted]
                .region
                .store(Region::Window.into(), Ordering::Relaxed);

            self.probation(frames).push_back(index);
            frames[index]
                .region
                .store(Region::Probation.into(), Ordering::Relaxed);
        } else {
            self.window(frames).push_back(index);
            frames[index]
                .region
                .store(Region::Window.into(), Ordering::Relaxed);
        }
    }

    fn push_window(&mut self, frames: &[Frame], index: usize) {}
    fn protected<'a>(&'a mut self, frames: &'a [Frame]) -> FrameList<'a> {
        self.list(frames, Region::Protected)
    }

    fn probation<'a>(&'a mut self, frames: &'a [Frame]) -> FrameList<'a> {
        self.list(frames, Region::Probation)
    }

    fn window<'a>(&'a mut self, frames: &'a [Frame]) -> FrameList<'a> {
        self.list(frames, Region::Window)
    }

    fn list<'a>(&'a mut self, frames: &'a [Frame], region: Region) -> FrameList<'a> {
        let q = match region {
            Region::Window => &mut self.window,
            Region::Probation => &mut self.probation,
            Region::Protected => &mut self.protected,
        };
        FrameList {
            frames,
            head: Either::Right(q),
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive)]
enum Region {
    Window,
    Probation,
    Protected,
}
