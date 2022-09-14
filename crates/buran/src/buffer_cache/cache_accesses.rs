use flume::Receiver;
use parking_lot::Mutex;
use thread_local::ThreadLocal;

use super::{BufId, FrameId};

pub(super) struct CacheAccesses {
    unused_shards: Receiver<AccessShard>,
    thread_local_shards: ThreadLocal<Mutex<AccessShard>>,
}

struct AccessShard(Vec<(BufId, FrameId)>);
