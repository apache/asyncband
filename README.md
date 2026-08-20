# Apache Asyncband (Incubating)

> [!IMPORTANT]
>
> Apache Asyncband (incubating) is an effort undergoing incubation at the Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC.
>
> Please read the [DISCLAIMER](DISCLAIMER) and a full explanation of ["incubating"](https://incubator.apache.org/policy/incubation.html).
>
> **Asyncband was formerly published as MEA.** The `mea` crate is deprecated and receives no further development. See the [migration guide](MIGRATE.md) for migration instructions and details about the rename.

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

## Overview

Asyncband is a runtime-agnostic library providing essential synchronization primitives for asynchronous Rust programming. The library offers a collection of well-tested, efficient synchronization tools that work with any async runtime.

## Available primitives

The crate enables no primitives by default. Categories describe each primitive's primary purpose and do not add another module level, so public paths remain concise, such as `asyncband::mutex` and `asyncband::once::OnceCell`.

| Category                | Primitive                                                                                    | Feature        | Purpose                                                                 |
| ----------------------- | -------------------------------------------------------------------------------------------- | -------------- | ----------------------------------------------------------------------- |
| Shared state            | [`Mutex`](https://docs.rs/asyncband/*/asyncband/mutex/struct.Mutex.html)                     | `mutex`        | Protect shared data with asynchronous mutual exclusion.                 |
|                         | [`RwLock`](https://docs.rs/asyncband/*/asyncband/rwlock/struct.RwLock.html)                  | `rwlock`       | Allow multiple readers or one writer.                                   |
|                         | [`Condvar`](https://docs.rs/asyncband/*/asyncband/condvar/struct.Condvar.html)               | `condvar`      | Wait for notifications while releasing a mutex.                         |
| One-time initialization | [`Once`](https://docs.rs/asyncband/*/asyncband/once/struct.Once.html)                        | `once`         | Run asynchronous initialization exactly once.                           |
|                         | [`OnceCell`](https://docs.rs/asyncband/*/asyncband/once/struct.OnceCell.html)                | `once-cell`    | Initialize and store one asynchronous value.                            |
|                         | [`OnceMap`](https://docs.rs/asyncband/*/asyncband/once/struct.OnceMap.html)                  | `once-map`     | Initialize and store one value per key.                                 |
| Task coordination       | [`Barrier`](https://docs.rs/asyncband/*/asyncband/barrier/struct.Barrier.html)               | `barrier`      | Wait until all participants reach a synchronization point.              |
|                         | [`Latch`](https://docs.rs/asyncband/*/asyncband/latch/struct.Latch.html)                     | `latch`        | Wait until a one-way countdown completes.                               |
|                         | [`WaitGroup`](https://docs.rs/asyncband/*/asyncband/waitgroup/struct.WaitGroup.html)         | `waitgroup`    | Wait for a dynamic group of tasks to finish.                            |
|                         | [`shutdown`](https://docs.rs/asyncband/*/asyncband/shutdown/)                                | `shutdown`     | Coordinate shutdown signals and completion.                             |
| Channels                | [`oneshot::channel`](https://docs.rs/asyncband/*/asyncband/oneshot/fn.channel.html)          | `oneshot`      | Send one value between two tasks.                                       |
|                         | [`mpsc::bounded`](https://docs.rs/asyncband/*/asyncband/mpsc/fn.bounded.html)                | `mpsc`         | Send values from multiple producers through a bounded channel.          |
|                         | [`mpsc::unbounded`](https://docs.rs/asyncband/*/asyncband/mpsc/fn.unbounded.html)            | `mpsc`         | Send values from multiple producers through an unbounded channel.       |
|                         | [`broadcast::overflow`](https://docs.rs/asyncband/*/asyncband/broadcast/overflow/)           | `broadcast`    | Broadcast values and report when slow receivers miss overwritten items. |
| Workload control        | [`Semaphore`](https://docs.rs/asyncband/*/asyncband/semaphore/struct.Semaphore.html)         | `semaphore`    | Control concurrent access with permits.                                 |
|                         | [`FairShare`](https://docs.rs/asyncband/*/asyncband/admission/struct.FairShare.html)         | `admission`    | Fairly share bounded concurrency across keys.                           |
|                         | [`PriorityShare`](https://docs.rs/asyncband/*/asyncband/admission/struct.PriorityShare.html) | `admission`    | Reserve shared capacity by priority while preserving per-key fairness.  |
|                         | [`Group`](https://docs.rs/asyncband/*/asyncband/singleflight/struct.Group.html)              | `singleflight` | Coalesce concurrent calls for the same key.                             |

## Installation

Add the dependency to your `Cargo.toml` via:

```shell
cargo add asyncband --features mutex,oneshot
```

List every primitive your application uses in `features`; a bare `cargo add asyncband` intentionally exposes no primitive modules.

## Synchronous interoperability

The optional `blocking` module bridges synchronous Rust code to runtime-agnostic futures. It is an interoperability utility rather than another async primitive, so it is documented separately from the table above.

```shell
cargo add asyncband --features blocking
```

```rust
use std::time::Duration;

use asyncband::blocking::FutureExt as _;

let value = async { 42 }.block_on();
assert_eq!(value, 42);

let value = async { 42 }.wait_timeout(Duration::ZERO);
assert_eq!(value, Some(42));
```

`asyncband::blocking::FutureExt::block_on(future)` is the equivalent UFCS spelling when function syntax is preferred; it calls the same trait method rather than a separate free function.

### Async first, blocking by adaptation

Async and synchronous synchronization primitives have different optimization constraints. Once an async primitive is runtime-agnostic, synchronous code can usually drive its future with a `block_on` adapter. Asyncband's `blocking` feature provides this adapter with a lightweight, thread-parking single-future executor: pending work parks the calling thread and its waker resumes it, providing practical blocking interoperability without busy-waiting or a full async runtime.

A sync-first implementation can still exploit OS- or platform-specific facilities for better performance. Asyncband therefore optimizes its primitives for async code and keeps blocking as a boundary adapter instead of duplicating sync and async methods across every type. This keeps the public API focused while leaving sync-oriented optimizations to dedicated libraries.

### Execution constraints

This is a minimal single-future executor, not a general-purpose async runtime. A timed-out `wait_timeout` drops the future. The implementation uses a private parker, so it does not consume wake-ups belonging to other parking operations on the same thread; recursive calls use a separate parker. Futures depending on a runtime-specific timer or I/O driver may not make progress, and blocking an executor thread can cause starvation or deadlocks. See [`asyncband::blocking`](https://docs.rs/asyncband/*/asyncband/blocking/index.html) for details.

## Runtime Agnostic

All synchronization primitives in this library are runtime-agnostic, meaning they can be used with any async runtime like Tokio, async-std, or others. This makes the library highly versatile and portable.

## Thread Safety

Asyncband primitives and guards implement `Send` and `Sync` only when the protected or transferred value satisfies the necessary bounds. In particular, owned read guards that may move destruction to another thread require the protected value to be `Send` as well as `Sync`. See each type's documentation for its exact bounds.

## Minimum Supported Rust Version (MSRV)

This crate is built against the latest stable release, and its minimum supported rustc version is 1.86.0.

The policy is that the minimum Rust version required to use this crate can be increased in minor version updates. For example, if Asyncband 1.0 requires Rust 1.20.0, then Asyncband 1.0.z for all values of z will also require Rust 1.20.0 or newer. However, Asyncband 1.y for y > 0 may require a newer minimum version of Rust.

## License and Trademarks

This project is licensed under [Apache License, Version 2.0](LICENSE).

Apache Asyncband, Asyncband, and Apache are either registered trademarks or trademarks of The Apache Software Foundation in the United States and/or other countries.

## History

See [HISTORY.md](HISTORY.md) for the external implementations that informed Asyncband's primitives.
