use std::hash::BuildHasherDefault;

use rustc_hash::FxHasher;

pub type StableHashBuilder = xxhash_rust::xxh3::Xxh3Builder;
pub type HashBuilder = ahash::RandomState;
pub type TinyHashBuilder = BuildHasherDefault<FxHasher>;
