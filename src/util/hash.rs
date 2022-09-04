use std::hash::BuildHasherDefault;

use ahash::RandomState;
use rustc_hash::FxHasher;
use xxhash_rust::xxh3::Xxh3Builder;

pub type StableHashBuilder = Xxh3Builder;
pub type HashBuilder = RandomState;
pub type TinyHashBuilder = BuildHasherDefault<FxHasher>;
