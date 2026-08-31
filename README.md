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

Public paths stay direct—such as `asyncband::mutex`, `asyncband::pool`, and `asyncband::once::OnceCell`—while Cargo features keep unused implementations out of the build.

## Examples

Runnable examples live in the [`examples`](examples) workspace crate. They demonstrate how to choose and compose Asyncband primitives in complete programs.

## API map

| Area                       | API                                                                                            | Feature        | Use                                                                                                           |
|----------------------------|------------------------------------------------------------------------------------------------|----------------|---------------------------------------------------------------------------------------------------------------|
| Shared state               | [`Mutex`](https://docs.rs/asyncband/*/asyncband/mutex/struct.Mutex.html)                       | `mutex`        | Protect shared data with asynchronous mutual exclusion.                                                       |
|                            | [`RwLock`](https://docs.rs/asyncband/*/asyncband/rwlock/struct.RwLock.html)                    | `rwlock`       | Allow multiple readers or one writer.                                                                         |
|                            | [`Condvar`](https://docs.rs/asyncband/*/asyncband/condvar/struct.Condvar.html)                 | `condvar`      | Wait for notifications while releasing a mutex.                                                               |
| Initialization and caching | [`Once`](https://docs.rs/asyncband/*/asyncband/once/struct.Once.html)                          | `once`         | Complete one asynchronous initialization; cancelled or panicked attempts may be retried.                       |
|                            | [`OnceCell`](https://docs.rs/asyncband/*/asyncband/once/struct.OnceCell.html)                  | `once-cell`    | Store one value from an access-time initializer; failed, cancelled, or panicked attempts may be retried.       |
|                            | [`LazyCell`](https://docs.rs/asyncband/*/asyncband/once/struct.LazyCell.html)                  | `lazy-cell`    | Initialize one value with a stored function and resume the same in-flight future after caller cancellation.   |
|                            | [`OnceMap`](https://docs.rs/asyncband/*/asyncband/once/struct.OnceMap.html)                    | `once-map`     | Cache one successfully initialized value per key until explicitly removed.                                    |
| Task coordination          | [`Barrier`](https://docs.rs/asyncband/*/asyncband/barrier/struct.Barrier.html)                 | `barrier`      | Synchronize a fixed number of participants at a reusable rendezvous.                                          |
|                            | [`Completion`](https://docs.rs/asyncband/*/asyncband/completion/struct.Completion.html)       | `completion`   | Publish one shared result to any number of current and future observers.                                       |
|                            | [`ManualResetEvent`](https://docs.rs/asyncband/*/asyncband/event/struct.ManualResetEvent.html) | `event`        | Signal current and future waits until explicitly reset.                                                       |
|                            | [`Latch`](https://docs.rs/asyncband/*/asyncband/latch/struct.Latch.html)                       | `latch`        | Wait until a fixed one-way countdown reaches zero.                                                            |
|                            | [`WaitGroup`](https://docs.rs/asyncband/*/asyncband/waitgroup/struct.WaitGroup.html)           | `waitgroup`    | Wait until all cloned worker handles are dropped.                                                             |
|                            | [`Shutdown`](https://docs.rs/asyncband/*/asyncband/shutdown/struct.Shutdown.html)              | `shutdown`     | Request shutdown and wait until all completion guards are dropped.                                            |
| Channels                   | [`oneshot`](https://docs.rs/asyncband/*/asyncband/oneshot/)                                    | `oneshot`      | Send one value from one sender to one receiver.                                                               |
|                            | [`mpsc`](https://docs.rs/asyncband/*/asyncband/mpsc/)                                          | `mpsc`         | Send each value from multiple producers to one receiver with bounded backpressure or an unbounded queue.      |
|                            | [`broadcast`](https://docs.rs/asyncband/*/asyncband/broadcast/)                                | `broadcast`    | Deliver every value to receivers active at send time; retain an unbounded backlog until each consumes or drops. |
|                            | [`watch`](https://docs.rs/asyncband/*/asyncband/watch/)                                        | `watch`        | Publish the latest state to independently tracked receivers and coalesce intermediate updates.                |
| Resource reuse             | [`pool`](https://docs.rs/asyncband/*/asyncband/pool/)                                          | `pool`         | Reuse objects through bounded or unbounded pool variants.                                                     |
| Concurrency limiting       | [`Semaphore`](https://docs.rs/asyncband/*/asyncband/semaphore/struct.Semaphore.html)           | `semaphore`    | Limit concurrent work by acquiring permits.                                                                   |
| Duplicate suppression      | [`Group`](https://docs.rs/asyncband/*/asyncband/singleflight/struct.Group.html)                | `singleflight` | Coalesce overlapping calls for the same key without caching completed results.                                |
| Sync interop               | [`FutureExt`](https://docs.rs/asyncband/*/asyncband/blocking/trait.FutureExt.html)             | `blocking`     | Drive one runtime-agnostic future from a blocking thread.                                                     |

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

This crate is built against the latest stable release, and its minimum supported rustc version is 1.86.0.

The policy is that the minimum Rust version required to use this crate can be increased in minor version updates. For example, if Asyncband 1.0 requires Rust 1.20.0, then Asyncband 1.0.z for all values of z will also require Rust 1.20.0 or newer. However, Asyncband 1.y for y > 0 may require a newer minimum version of Rust.

## License and Trademarks

This project is licensed under [Apache License, Version 2.0](LICENSE).

Apache Asyncband, Asyncband, and Apache are either registered trademarks or trademarks of The Apache Software Foundation in the United States and/or other countries.

## History

See [HISTORY.md](HISTORY.md) for the external implementations that informed Asyncband's APIs.
