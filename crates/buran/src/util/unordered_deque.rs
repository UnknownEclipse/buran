use parking_lot::{Condvar, Mutex};
use thread_local::ThreadLocal;

pub struct UnorderedDeque<T>
where
    T: Send,
{
    thread_locals: ThreadLocal<Mutex<Vec<T>>>,
    main: Mutex<Vec<T>>,
    entries_waiting_cvar: Condvar,
    shard_capacity: usize,
    cross: Mutex<()>,
}

impl<T> UnorderedDeque<T>
where
    T: Send,
{
    pub fn push(&self, item: T) {
        let mut buf = self
            .thread_locals
            .get_or(|| Mutex::new(Vec::with_capacity(self.shard_capacity)))
            .lock();

        if buf.len() == buf.capacity() {
            let mut main = self.main.lock();
            main.extend(buf.drain(..));
            self.entries_waiting_cvar.notify_all();
        }

        buf.push(item);
    }

    pub fn pop(&self) -> T {
        todo!()
    }
}
