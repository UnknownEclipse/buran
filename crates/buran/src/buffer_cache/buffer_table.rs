use dashmap::DashMap;

use super::{BufId, MaybeFrame};

pub struct BufferTable {
    map: DashMap<BufId, MaybeFrame>,
}
