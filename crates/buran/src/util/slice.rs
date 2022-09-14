use std::{
    ops::{Deref, DerefMut},
    slice::SliceIndex,
};

use triomphe::ThinArc;

#[derive(Clone)]
pub struct Slice {
    data: ThinArc<(), u8>,
    start: u32,
    end: u32,
}

impl Slice {
    pub fn zeroed(size: usize) -> Self {
        if (u32::MAX as usize) < size {
            panic!()
        }
        let thin = ThinArc::from_header_and_iter((), (0..size).map(|_| 0));
        thin.try_into().unwrap()
    }

    pub fn slice<I>(&self, index: I) -> Option<Self>
    where
        I: SliceIndex<[u8], Output = [u8]>,
    {
        let slice = self.deref();
        let sub = slice.get(index)?;

        let offset = sub.as_ptr() as usize - slice.as_ptr() as usize;

        let mut new = self.clone();
        new.start += offset as u32;
        new.end = new.start + sub.len() as u32;

        Some(new)
    }
}

impl TryFrom<&[u8]> for Slice {
    type Error = ();

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        ThinArc::from_header_and_slice((), value).try_into()
    }
}

impl TryFrom<ThinArc<(), u8>> for Slice {
    type Error = ();

    fn try_from(data: ThinArc<(), u8>) -> Result<Self, Self::Error> {
        let start = 0;
        let end = data.slice.len().try_into().map_err(|_| ())?;
        Ok(Self { start, end, data })
    }
}

impl Deref for Slice {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        let i = self.start as usize..self.end as usize;
        &self.data.slice[i]
    }
}

pub struct UniqueSlice(Slice);

impl Deref for UniqueSlice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.deref()
    }
}

impl DerefMut for UniqueSlice {
    fn deref_mut(&mut self) -> &mut Self::Target {
        todo!()
    }
}
