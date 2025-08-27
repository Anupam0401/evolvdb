# DiskManager

Status: v0 (NIO-based), fixed page size.

Responsibilities:
- Allocate pages at file tail (zero-filled)
- Read and write exactly one page by PageId
- Sync durable writes (force)

API:
- `PageId allocatePage(FileId)`
- `void readPage(PageId, ByteBuffer dst)`
- `void writePage(PageId, ByteBuffer src, long lsn)`
- `void sync()`
- `close()`

Notes:
- File naming: `<name>.evolv` under `DbConfig.dataDir`
- Page numbers are 0-based
- No buffering; BufferPool handles caching
- Concurrency: coarse synchronization per FileChannel
- LSN ignored for now (WAL later)

Tests:
- Allocate, write/read patterns across pages
- Persist across manager instances
