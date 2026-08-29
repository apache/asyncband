# Apache Asyncband (Incubating)

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MSRV 1.86][msrv-badge]](https://www.whatrustisit.com)
[![Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/asyncband.svg
[crates-url]: https://crates.io/crates/asyncband
[docs-badge]: https://docs.rs/asyncband/badge.svg
[docs-url]: https://docs.rs/asyncband
[msrv-badge]: https://img.shields.io/badge/MSRV-1.86-green?logo=rust
[license-badge]: https://img.shields.io/crates/l/asyncband
[license-url]: LICENSE
[actions-badge]: https://github.com/apache/asyncband/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/apache/asyncband/actions/workflows/ci.yml

> [!IMPORTANT]
>
> Apache Asyncband (incubating) is an effort undergoing incubation at the Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC.
>
> Please read the [DISCLAIMER](DISCLAIMER) and a full explanation of ["incubating"](https://incubator.apache.org/policy/incubation.html).
>
> **Asyncband was formerly published as MEA.** The `mea` crate is deprecated and receives no further development. See the [migration guide](MIGRATE.md) for migration instructions and details about the rename.

## Overview

Asyncband is a focused collection of composable, runtime-agnostic concurrency building blocks for async Rust. It provides synchronization, initialization, task coordination, channels, resource reuse, and workload control without choosing an executor for the application.

Asyncband's async APIs are built on standard futures and wakers. The library does not spawn tasks, own worker threads, install timers, or require a reactor or I/O driver. Applications can poll its futures with Tokio, async-std, smol, a custom executor, or any other standards-based runtime, and compose runtime services such as deadlines around them.

### Project scope

The project is not limited to small or stateless synchronization primitives. Stateful utilities such as a singleflight group or an object pool fit when they provide a generally reusable coordination mechanism and remain independent of executor policy.

The boundary is mechanism versus policy. Task placement, timers, deadlines, retries, periodic maintenance, and application lifecycle orchestration stay with the caller and its runtime. Potential future-concurrency or scheduling APIs are evaluated against the same boundary: they must remain executor-independent and compose with caller-owned execution and timing.

## Getting started

The crate enables no APIs by default. Enable only the features your application uses:

```shell
cargo add asyncband --features mutex
```

```rust
use asyncband::mutex::Mutex;

async fn increment() {
    let counter = Mutex::new(0);
    *counter.lock().await += 1;
    assert_eq!(*counter.lock().await, 1);
}
```

Public modules stay at direct crate-root paths. Related variants are grouped beneath their semantic family—for example, initialization cells under `asyncband::once` and MPMC broadcast under `asyncband::broadcast::mpmc`. Cargo features select compiled APIs; topology and bounded or unbounded policies remain module or constructor choices rather than separate features.

## Examples

Runnable examples live in the [`examples`](examples) workspace crate. They demonstrate how to choose and compose Asyncband primitives in complete programs.

## API map

| Area                  | API                                                                                  | Feature        | Use                                                                                                    |
|-----------------------|--------------------------------------------------------------------------------------|----------------|--------------------------------------------------------------------------------------------------------|
| Shared state          | [`Mutex`](https://docs.rs/asyncband/*/asyncband/mutex/struct.Mutex.html)             | `mutex`        | Protect shared data with asynchronous mutual exclusion.                                                |
|                       | [`RwLock`](https://docs.rs/asyncband/*/asyncband/rwlock/struct.RwLock.html)          | `rwlock`       | Allow multiple readers or one writer.                                                                  |
|                       | [`Condvar`](https://docs.rs/asyncband/*/asyncband/condvar/struct.Condvar.html)       | `condvar`      | Wait for notifications while releasing a mutex.                                                        |
| Initialization        | [`Once`](https://docs.rs/asyncband/*/asyncband/once/struct.Once.html)                | `once`         | Run asynchronous initialization exactly once.                                                          |
|                       | [`OnceCell`](https://docs.rs/asyncband/*/asyncband/once/struct.OnceCell.html)        | `once-cell`    | Initialize and store one asynchronous value.                                                           |
|                       | [`LazyCell`](https://docs.rs/asyncband/*/asyncband/once/struct.LazyCell.html)        | `lazy-cell`    | Lazily initialize a value with a stored asynchronous function.                                         |
|                       | [`OnceMap`](https://docs.rs/asyncband/*/asyncband/once/struct.OnceMap.html)          | `once-map`     | Initialize and store one value per key.                                                                |
| Task coordination     | [`Barrier`](https://docs.rs/asyncband/*/asyncband/barrier/struct.Barrier.html)       | `barrier`      | Wait until all participants reach a synchronization point.                                             |
|                       | [`Latch`](https://docs.rs/asyncband/*/asyncband/latch/struct.Latch.html)             | `latch`        | Wait until a one-way countdown completes.                                                              |
|                       | [`WaitGroup`](https://docs.rs/asyncband/*/asyncband/waitgroup/struct.WaitGroup.html) | `waitgroup`    | Wait for a dynamic group of tasks to finish.                                                           |
|                       | [`Shutdown`](https://docs.rs/asyncband/*/asyncband/shutdown/struct.Shutdown.html)    | `shutdown`     | Coordinate shutdown signals and completion.                                                            |
| Channels              | [`oneshot`](https://docs.rs/asyncband/*/asyncband/oneshot/)                          | `oneshot`      | Send one value from one sender to one receiver.                                                         |
|                       | [`mpsc`](https://docs.rs/asyncband/*/asyncband/mpsc/)                                | `mpsc`         | Send each value from multiple producers to one receiver through a bounded or unbounded queue.           |
|                       | [`broadcast::mpmc`](https://docs.rs/asyncband/*/asyncband/broadcast/mpmc/)            | `broadcast`    | Broadcast every value from multiple producers to every active receiver with unbounded retention.        |
|                       | [`watch`](https://docs.rs/asyncband/*/asyncband/watch/)                              | `watch`        | Publish the latest state to independently tracked receivers and coalesce intermediate updates.          |
| Resource reuse        | [`pool`](https://docs.rs/asyncband/*/asyncband/pool/)                                | `pool`         | Reuse objects through bounded or unbounded pool variants.                                              |
| Workload coordination | [`Semaphore`](https://docs.rs/asyncband/*/asyncband/semaphore/struct.Semaphore.html) | `semaphore`    | Control concurrent access with permits.                                                                |
|                       | [`Group`](https://docs.rs/asyncband/*/asyncband/singleflight/struct.Group.html)      | `singleflight` | Coalesce concurrent calls for the same key.                                                            |
| Sync interop          | [`FutureExt`](https://docs.rs/asyncband/*/asyncband/blocking/trait.FutureExt.html)   | `blocking`     | Drive one runtime-agnostic future from a blocking thread.                                              |

## Synchronous interoperability

The optional `blocking` module is a boundary adapter for synchronous callers. It parks the calling thread while driving one future; it is not a general-purpose executor.

```shell
cargo add asyncband --features blocking,oneshot
```

```rust
use std::thread;

use asyncband::blocking::FutureExt as _;
use asyncband::oneshot;

let (sender, receiver) = oneshot::channel();
thread::spawn(move || {
    let result = 6 * 7;
    sender.send(result).unwrap();
});

assert_eq!(receiver.block_on(), Ok(42));
```

### Async first, blocking by adaptation

Async and synchronous synchronization primitives have different optimization constraints. Once an async operation is exposed as a runtime-agnostic future, synchronous code can usually drive that future through a `block_on` adapter. Asyncband therefore designs its primitives for async use and provides blocking interoperability at the boundary instead of duplicating synchronous methods across every type.

A sync-first implementation can exploit OS- or platform-specific facilities that an async implementation cannot assume. Libraries focused on synchronous code can therefore make different and sometimes better tradeoffs. Asyncband leaves those optimizations to dedicated libraries rather than treating blocking adaptation as a second family of primitives.

### Execution constraints

The `blocking` module is a lightweight, thread-parking single-future executor, not a general-purpose async runtime. `wait_timeout` drops the future on timeout. Futures that depend on a runtime-specific timer or I/O driver still need that runtime's driver to make progress, and blocking an executor thread can cause starvation or deadlocks. See the [`blocking` module documentation](https://docs.rs/asyncband/*/asyncband/blocking/) for the full contract.

## Thread safety

Asyncband types implement `Send` and `Sync` only when the protected, transferred, or managed value satisfies the necessary bounds. See each API's documentation for its exact contract.

## Minimum Supported Rust Version (MSRV)

Asyncband supports rustc 1.86.0 and newer. CI tests both 1.86.0 and the latest stable Rust release.

The minimum supported Rust version may increase in a minor release. Each increase is recorded as a breaking change in the changelog.

## License and Trademarks

This project is licensed under [Apache License, Version 2.0](LICENSE).

Apache Asyncband, Asyncband, and Apache are either registered trademarks or trademarks of The Apache Software Foundation in the United States and/or other countries.

## History

See [HISTORY.md](HISTORY.md) for the external implementations that informed Asyncband's APIs.
