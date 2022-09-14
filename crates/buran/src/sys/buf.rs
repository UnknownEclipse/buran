use parking_lot::RwLock;

pub struct File {
    vec: RwLock<Vec<u8>>,
}
