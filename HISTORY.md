# History

Asyncband collects composable, runtime-agnostic concurrency building blocks informed by several existing implementations. Only components that draw on external designs or code are listed here.

- `barrier::Barrier` is inspired by [`std::sync::Barrier`](https://doc.rust-lang.org/std/sync/struct.Barrier.html) and [`tokio::sync::Barrier`](https://docs.rs/tokio/latest/tokio/sync/struct.Barrier.html), with a different implementation based on the internal `WaitSet` primitive.
- The single-future polling loop in `blocking` is adapted from [`pollster`](https://github.com/zesterer/pollster), its parker caching strategy follows [`futures-lite`](https://github.com/smol-rs/futures-lite), and its private parker state machine is adapted from [`parking`](https://github.com/smol-rs/parking) 2.2.1.
- `condvar::Condvar` is inspired by [`std::sync::Condvar`](https://doc.rust-lang.org/std/sync/struct.Condvar.html) and [`async_std::sync::Condvar`](https://docs.rs/async-std/latest/async_std/sync/struct.Condvar.html), with a fair FIFO waiter queue and standard non-buffered notification semantics.
- `latch::Latch` is inspired by [`latches`](https://github.com/mirromutth/latches), with a different implementation based on the internal `CountdownState` primitive.
- `mutex::Mutex` is derived from [`tokio::sync::Mutex`](https://docs.rs/tokio/latest/tokio/sync/struct.Mutex.html).
- `once::OnceCell` is derived from [`tokio::sync::OnceCell`](https://docs.rs/tokio/latest/tokio/sync/struct.OnceCell.html), but uses Asyncband's semaphore implementation.
- `once::OnceMap` is inspired by [`uv-once-map`](https://github.com/astral-sh/uv/tree/main/crates/uv-once-map), with a redesigned interface and implementation.
- `oneshot::channel` is derived from the [`oneshot`](https://github.com/faern/oneshot) crate, with significant simplifications because Asyncband does not provide synchronized receive operations.
- `pool` is ported from [`fastpool`](https://github.com/fast/fastpool), which is derived from [`deadpool`](https://github.com/bikeshedder/deadpool), while keeping Fastpool's runtime-agnostic design and caller-side timeout composition.
- `rwlock::RwLock` is derived from [`tokio::sync::RwLock`](https://docs.rs/tokio/latest/tokio/sync/struct.RwLock.html), but accepts any `NonZeroUsize` as `max_readers` instead of Tokio's restricted range.
- `semaphore::Semaphore` is derived from [`tokio::sync::Semaphore`](https://docs.rs/tokio/latest/tokio/sync/struct.Semaphore.html), but omits `close`, avoids Tokio's fixed maximum-permit constant, and adds operations such as `reduce_permits` for Asyncband's use cases.
- `waitgroup::WaitGroup` is inspired by [`waitgroup-rs`](https://github.com/laizy/waitgroup-rs), but treats every cloned handle as an RAII participant and supports cancellable multi-observer waits.
