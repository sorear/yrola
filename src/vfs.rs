// inspired by the SQLite VFS, this trait abstracts I/O operations for mocking
// unlike its namesake, we cannot mock memory allocations or synchronization primitives in Rust
// so, no graceful OOM testing :(

pub struct File {
}
