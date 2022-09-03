# Architecture

Buran is built in several layers that build on the abstractions provided by lower
layers.

## The Storage Layer

The storage layer manages the on-disk and cached in memory database pages. The storage
layer itself occupies two files: the heap and the write-ahead-log.

The API exposed by the storage layer is extremely simple, allowing easy modification
of the backend without any changes needed to higher layers.

```rust
pub struct Store { /* fields */ }

impl Store {
    pub fn read(&self) -> Reader<'_>;
    pub fn write(&self) -> Writer<'_>;
    pub fn sync(&self) -> Result<()>;
}

pub struct Reader<'store> { /* fields */ }

impl<'a> Reader<'a> {
    pub fn get(&self, page: PageId) -> Result<PageRef<'_>>;
}

pub struct Writer<'store> { /* fields */ }

impl<'a> Writer<'a> {
    pub fn get(&self, page: PageId) -> Result<PageRef<'_>>;
    pub fn get_mut(&mut self, page: PageId) -> Result<PageMut<'_>>;
    pub fn commit(self) -> Result<()>;
    pub fn rollback(self) -> Result<()> ;
}

pub struct PageRef<'r> { /* fields */ }

impl<'r> PageRef<'r> {
    pub fn bytes(&self) -> Result<&[u8]>;
}

pub struct PageMut<'w> { /* fields */ }

impl<'w> PageMut<'w> {
    pub fn bytes(&self) -> Result<&[u8]>;
    pub fn write(&mut self, data: &[u8], start: usize) -> Result<()>;
}
```

### The Heap

```text
page id --> mapping table -> (segment number, chunk index) -> page
```

The heap can also be considered the primary database file, and is where most data will
reside. It is structured as a group of `segment`s, where every segment is some multiple
of the filesystem block size. There are two main kinds of segments: Page segments, and
mapping table segments.

Page segments are divided into the actual database pages. Variable-sized (including
compressed) pages should be supported by some form
of chunk allocator. When a page is written, it is copied and written to a new segment.
In theory batches of writes can be accumulated and written as a single segment,
minimizing write amplification. Then, the mapping table (discussed below) is updated
to point to the new location and mark the old location as free.

The mapping table is an indirection layer between the high level `page id`s and lower
level segment numbers and chunk indexes within those segments. It also manages the
list of free chunks and segments. Updates to the mapping table are first logged to
the write-ahead-log as small records, before being later checkpointed into the heap.

Because we modify the mapping table as the final step in a write operation, the segment
writes themselves do not have to be atomic in any way. This allows us to bypass the
wal for the majority of data in each page write, only writing a small (~20 byte) record
to indicate the new location of a page and the newly freed page, which not only
reduces write amplification, but makes checkpoints less frequent.

### The Write-Ahead-Log

As discussed above, the WAL is only used to ensure the mapping table is updated
atomically even under power loss conditions. It consists of a sequence of simple
records. Current record types include:

```rust
enum Record {
    /// Allocate a new page
    Alloc(PageId),
    /// Free a page
    Free(PageId),
    /// Link a page to a new physical location
    Link {
        page: PageId,
        segment: u64,
        chunk: u32,
    }
    /// Decrement the reference count of a raw data chunk.
    Unref {
        segment: u64,
        chunk: u32,
    }
    /// Increment the reference count of a raw data chunk.
    Ref {
        segment: u64,
        chunk: u32,
    }
    /// Indicates the records between the last Commit/Rollback and this record
    /// should be considered committed and are now visible to readers.
    Commit,
    /// Indicates the records between the last Commit/Rollback and this record
    /// should be ignored in the future.
    Rollback,
}
```

### Diagrams

### Data Types

```rust
/// A unique reference to a database page.
pub struct PageId(u64);

/// The index of a segment in the heap
pub struct SegmentIndex(u64);

/// The index of a chunk within a segment
pub struct ChunkIndex(u32);
```

## The B-Tree Layer
