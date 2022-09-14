use bytemuck::{Pod, Zeroable};

macro_rules! define_le_int {
    ($name:ident, $t:ty) => {
        #[repr(transparent)]
        #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Zeroable, Pod)]
        pub struct $name([u8; std::mem::size_of::<$t>()]);

        impl $name {
            #[inline]
            pub fn new(value: $t) -> Self {
                Self(value.to_le_bytes())
            }

            #[inline]
            pub fn get(self) -> $t {
                <$t>::from_le_bytes(self.0)
            }
        }
    };
}

define_le_int!(U16Le, u16);
define_le_int!(U32Le, u32);
define_le_int!(U64Le, u64);
