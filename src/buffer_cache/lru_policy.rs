use either::Either;
use parking_lot::Mutex;

use super::{Frame, FrameList, ListHead};

#[derive(Default)]
pub(super) struct LruPolicy {
    list: Mutex<ListHead>,
}

impl LruPolicy {
    pub fn access(&self, frames: &[Frame], frame: usize) {
        let mut list = self.list(frames);
        list.remove(frame);
        list.push_back(frame);
    }

    pub fn insert(&self, frames: &[Frame], frame: usize) {
        let mut list = self.list(frames);
        list.push_back(frame);
    }

    pub fn evict(&self, frames: &[Frame]) -> Option<usize> {
        let mut list = self.list(frames);
        list.pop_front()
    }

    fn list<'a>(&'a self, frames: &'a [Frame]) -> FrameList<'a> {
        FrameList {
            frames,
            head: Either::Left(self.list.lock()),
        }
    }
}
