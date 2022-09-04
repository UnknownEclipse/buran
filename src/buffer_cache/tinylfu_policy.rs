use num_enum::{IntoPrimitive, TryFromPrimitive};

pub(super) struct TinyLfuPolicy {
    window: f32,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, IntoPrimitive, TryFromPrimitive)]
enum Region {
    Window,
    Probation,
    Protected,
}
