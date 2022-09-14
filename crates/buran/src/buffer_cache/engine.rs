use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use dashmap::DashMap;
use flume::{Receiver, SendError, Sender};
use nonmax::NonMaxUsize;
use parking_lot::{lock_api::RwLockReadGuard, Mutex};

use crate::{
    buffer_cache::{FrameState, WriteGuard},
    Result,
};

use super::{frames::Frames, wtinylfu::WTinyLfu, BufId, FrameId};

#[derive(Debug, Clone)]
pub(super) struct EngineHandle {
    /// The returned free frames. The returned frames are guaranteed to be unique.
    free: Receiver<Result<FrameId>>,
    /// Used to indicate how many freed frames are required. The responses will be
    /// sent back through the `free` channel.
    free_requests: Arc<AtomicUsize>,
    outgoing: Sender<Message>,
}

impl EngineHandle {
    /// Get a free frame from the engine, returning a preexisting freed frame if
    /// available, or evicting an unused frame if not. The error returned will be
    /// a response to the dirty page flush.
    pub fn get_free(&self) -> Result<FrameId> {
        // Add another free request, then receive a returned frame. The ordering
        // doesn't matter. A free frame is a free frame.
        self.free_requests.fetch_add(1, Ordering::Relaxed);
        self.free.recv().expect("broken channel")
    }

    pub async fn get_free_async(&self) -> Result<FrameId> {
        self.free_requests.fetch_add(1, Ordering::Relaxed);
        self.free.recv_async().await.expect("broken channel")
    }

    /// Submit a fetch request.
    ///
    /// There is no async equivalent, as this operation will not block and merely
    /// informs the engine that a buffer must be fetched into a set frame.
    pub fn fetch(&self, buf: BufId, frame: FrameId) {
        self.message(Message::Fetch(buf, frame))
    }

    /// Reclaim an unused frame
    pub fn reclaim(&self, frame: FrameId) {
        self.message(Message::Reclaim(frame))
    }

    pub fn marked_dirty(&self, frame: FrameId) {
        todo!()
    }

    fn message(&self, message: Message) {
        self.outgoing.send(message).expect("broken channel")
    }
}

enum Message {
    Fetch(BufId, FrameId),
    Dirty(FrameId),
    Reclaim(FrameId),
}

struct Engine {
    incoming: Receiver<Message>,
    free: Sender<Result<FrameId>>,
    /// It's the engine's job to send a freed frame for every request.
    free_requests: Arc<AtomicUsize>,
    cache_state: Arc<Mutex<WTinyLfu>>,
    free_stack: Vec<FrameId>,
    shutdown: bool,
    frames: Frames,
    errored: Vec<FrameId>,
    table: Arc<DashMap<BufId, FrameId>>,
}

impl Engine {
    pub fn run(&mut self) {}

    fn fetch(&mut self, buf: BufId, frame: FrameId) {
        todo!()
    }

    fn free(&mut self, count: usize) {
        let mut state = self.cache_state.lock();

        for i in 0..count {
            if let Some(frame) = self.free_stack.pop() {
                tracing::debug!("using freed frame {} ({}/{})", frame.0.get(), i, count);
                if self.free.send(Ok(frame)).is_err() {
                    tracing::debug!("channel disconnected, stopping...");
                    self.shutdown = true;
                    return;
                }
                continue;
            }

            let evicted = match state.evict() {
                Some(id) => id,
                None => panic!("too many active free requests"),
            };

            let frame = self
                .frames
                .get_ref(FrameId(NonMaxUsize::new(evicted).unwrap()));

            self.table.remove(&frame.buffer());
            let guard = WriteGuard::from_frame(frame);
            assert_eq!(guard.frame.refcount.load(Ordering::Relaxed), 1);

            tracing::debug!("beginning frame {} eviction ({}/{})", evicted, i, count);

            match self.io_write(guard.frame.buffer(), guard.deref()) {
                Ok(_) => {}
                Err(err) => {
                    tracing::debug!("failed to flush frame {} ({:?})", evicted, err);
                    self.errored.push(guard.frame.id);

                    if self.free.send(Err(err)).is_err() {
                        tracing::debug!("channel disconnected, stopping...");
                        self.shutdown = true;
                        return;
                    }
                }
            }

            unsafe {
                *guard.frame.response.get() = Default::default();
            }

            guard
                .frame
                .state
                .store(FrameState::Empty.into(), Ordering::Relaxed);

            if self.free.send(Ok(guard.frame.id)).is_err() {
                tracing::debug!("channel disconnected, stopping...");
                self.shutdown = true;
                return;
            }
        }
    }

    fn flush_frame(&mut self, frame: FrameId) -> Result<()> {
        let f = self.frames.read(frame);
        let data = f.deref();
        self.io_write(f.buffer(), data)
    }

    fn io_write(&self, buf: BufId, data: &[u8]) -> Result<()> {
        todo!()
    }
}

// struct EngineState {
//     requests: Receiver<Request>,
//     cache_policy: Arc<Mutex<WTinyLfu>>,
//     frames: Arc<Frames>,
//     free: Vec<FrameId>,
// }

// pub(super) fn start_engine(frames: Arc<Frames>) -> Result<IoEngineHandle> {
//     let (requests_tx, requests_rx) = unbounded();

//     let cache_policy = WTinyLfuBuilder::default().build(frames.len());

//     let frame_count = frames.len();
//     let state = EngineState {
//         requests: requests_rx,
//         frames,
//         cache_policy: Arc::new(Mutex::new(cache_policy)),
//         free: (0..frame_count)
//             .map(|i| NonMaxUsize::new(i).map(FrameId).unwrap())
//             .collect(),
//     };

//     let join_handle = thread::spawn(move || {
//         engine_run(state);
//     });

//     let handle = IoEngineHandle {
//         requests: requests_tx,
//         join_handle,
//     };
//     Ok(handle)
// }

// impl EngineState {
//     pub fn run(mut self) {
//         loop {
//             // Prioritize requests. If no request is present, do background maintenance
//             // tasks like preflushing and prefetching.
//             match self.requests.try_recv() {
//                 Ok(req) => {
//                     self.handle_request(req);
//                     continue;
//                 }
//                 Err(TryRecvError::Disconnected) => break,
//                 Err(TryRecvError::Empty) => {}
//             }
//             self.flush_dirty();
//             self.prefetch();
//         }
//     }

//     fn handle_request(&mut self, req: Request) {
//         todo!()
//     }

//     fn flush_dirty(&mut self) {
//         todo!()
//     }

//     fn prefetch(&mut self) {
//         todo!()
//     }
// }

// fn engine_run(mut state: EngineState) {
//     state.run();
// }
