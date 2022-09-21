use dashmap::DashMap;

use crate::types::{Lsn, PageId};

use super::wal::Record;

pub struct WalIndex {
    pages: DashMap<PageId, Vec<(Lsn, PageState)>>,
}

impl WalIndex {
    pub fn lookup(&self, page: PageId, lsn: Option<Lsn>) -> Option<(Lsn, PageState)> {
        let page = self.pages.get(&page)?;

        if let Some(lsn) = lsn {
            let (Ok(end) | Err(end)) = page
                .binary_search_by_key(&lsn, |(lsn, _)| *lsn)
                .map(|v| v + 1);
            let slice = &page[..end];
            slice.last().copied()
        } else {
            page.last().copied()
        }
    }

    pub fn push(&self, lsn: Lsn, record: Record) {
        let (page, state) = match record {
            Record::Free { page } => (page, PageState::Free),
            Record::PageWrite { page, size } => (page, PageState::Write(size)),
            Record::Commit | Record::Rollback => return,
        };
        self.pages.entry(page).or_default().push((lsn, state));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageState {
    Free,
    Write(u64),
}
