# Asyncband

> [!IMPORTANT]
> **Asyncband was formerly published as MEA.** The [`mea`](https://crates.io/crates/mea) crate is deprecated and receives no further development. New development and releases use the `asyncband` crate; no compatibility crate or re-export is provided under the old name. See [Migrating from MEA](#migrating-from-mea) and the [Asyncband proposal discussion](https://lists.apache.org/thread/f31qd3jm3odomjwy3lqkk21coyqsr9xs) for details.

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
[actions-badge]: https://github.com/fast/asyncband/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/fast/asyncband/actions/workflows/ci.yml

## Overview

Asyncband is a runtime-agnostic library providing essential synchronization primitives for asynchronous Rust programming. The library offers a collection of well-tested, efficient synchronization tools that work with any async runtime.

## Available primitives

Each module is an opt-in Cargo feature. The crate enables no primitives by default. Categories describe each primitive's primary purpose; they do not add another module level, so public paths continue to match feature names such as `asyncband::mutex` and `asyncband::mpsc`.

| Category | Primitive | Feature | Purpose |
| --- | --- | --- | --- |
| Shared state | [`Mutex`](https://docs.rs/asyncband/*/asyncband/mutex/struct.Mutex.html) | `mutex` | Protect shared data with asynchronous mutual exclusion. |
|  | [`RwLock`](https://docs.rs/asyncband/*/asyncband/rwlock/struct.RwLock.html) | `rwlock` | Allow multiple readers or one writer. |
|  | [`Condvar`](https://docs.rs/asyncband/*/asyncband/condvar/struct.Condvar.html) | `condvar` | Wait for notifications while releasing a mutex. |
| One-time initialization | [`Once`](https://docs.rs/asyncband/*/asyncband/once/struct.Once.html) | `once` | Run asynchronous initialization exactly once. |
|  | [`OnceCell`](https://docs.rs/asyncband/*/asyncband/once/struct.OnceCell.html) | `once` | Initialize and store one asynchronous value. |
|  | [`OnceMap`](https://docs.rs/asyncband/*/asyncband/once/struct.OnceMap.html) | `once` | Initialize and store one value per key. |
| Task coordination | [`Barrier`](https://docs.rs/asyncband/*/asyncband/barrier/struct.Barrier.html) | `barrier` | Wait until all participants reach a synchronization point. |
|  | [`Latch`](https://docs.rs/asyncband/*/asyncband/latch/struct.Latch.html) | `latch` | Wait until a one-way countdown completes. |
|  | [`WaitGroup`](https://docs.rs/asyncband/*/asyncband/waitgroup/struct.WaitGroup.html) | `waitgroup` | Wait for a dynamic group of tasks to finish. |
|  | [`shutdown`](https://docs.rs/asyncband/*/asyncband/shutdown/) | `shutdown` | Coordinate shutdown signals and completion. |
| Channels | [`oneshot::channel`](https://docs.rs/asyncband/*/asyncband/oneshot/fn.channel.html) | `oneshot` | Send one value between two tasks. |
|  | [`mpsc::bounded`](https://docs.rs/asyncband/*/asyncband/mpsc/fn.bounded.html) | `mpsc` | Send values from multiple producers through a bounded channel. |
|  | [`mpsc::unbounded`](https://docs.rs/asyncband/*/asyncband/mpsc/fn.unbounded.html) | `mpsc` | Send values from multiple producers through an unbounded channel. |
|  | [`broadcast::overflow`](https://docs.rs/asyncband/*/asyncband/broadcast/overflow/) | `broadcast` | Broadcast values and report when slow receivers miss overwritten items. |
| Workload control | [`Semaphore`](https://docs.rs/asyncband/*/asyncband/semaphore/struct.Semaphore.html) | `semaphore` | Control concurrent access with permits. |
|  | [`FairShare`](https://docs.rs/asyncband/*/asyncband/admission/struct.FairShare.html) | `admission` | Fairly share bounded concurrency across keys. |
|  | [`Group`](https://docs.rs/asyncband/*/asyncband/singleflight/struct.Group.html) | `singleflight` | Coalesce concurrent calls for the same key. |

Features that build on other primitives enable them automatically: `condvar` enables `mutex`, `once` enables `semaphore`, `shutdown` enables `latch` and `waitgroup`, and `singleflight` enables `once`.

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
use asyncband::blocking::{block_on, FutureExt as _};

let value = block_on(async { 42 });
assert_eq!(value, 42);

let value = async { 42 }.block_on();
assert_eq!(value, 42);
```

This is a minimal single-future executor, not a general-purpose async runtime. It uses a private parker, so it does not consume wake-ups belonging to other parking operations on the same thread; recursive calls use a separate parker. Futures depending on a runtime-specific timer or I/O driver may not make progress, and blocking an executor thread can cause starvation or deadlocks. See [`asyncband::blocking`](https://docs.rs/asyncband/*/asyncband/blocking/index.html) for details.

## Migrating from MEA

Asyncband continues the codebase formerly published as `mea`, but it uses a new Cargo package and Rust crate name. Remove the `mea` dependency, add `asyncband`, and update `mea::` paths to `asyncband::`. Existing `mea` releases remain available for builds that have not migrated, but they receive no further development.

No compatibility package or re-export is provided, so downstream crates must migrate their dependency declarations individually.

## Runtime Agnostic

All synchronization primitives in this library are runtime-agnostic, meaning they can be used with any async runtime like Tokio, async-std, or others. This makes the library highly versatile and portable.

## Thread Safety

Asyncband primitives and guards implement `Send` and `Sync` only when the protected or transferred value satisfies the necessary bounds. In particular, owned read guards that may move destruction to another thread require the protected value to be `Send` as well as `Sync`. See each type's documentation for its exact bounds.

## Minimum Supported Rust Version (MSRV)

This crate is built against the latest stable release, and its minimum supported rustc version is 1.86.0.

The policy is that the minimum Rust version required to use this crate can be increased in minor version updates. For example, if Asyncband 1.0 requires Rust 1.20.0, then Asyncband 1.0.z for all values of z will also require Rust 1.20.0 or newer. However, Asyncband 1.y for y > 0 may require a newer minimum version of Rust.

## License

This project is licensed under [Apache License, Version 2.0](LICENSE).

## History

This crate collects runtime-agnostic synchronization primitives from spare parts:

* **admission::FairShare** is written from scratch to bound global concurrency while balancing held permits across contending keys.
* **Barrier** is inspired by `std::sync::Barrier` and `tokio::sync::Barrier`, with a different implementation based on the internal `WaitSet` primitive.
* **Condvar** is inspired by `std::sync::Condvar` and `async_std::sync::Condvar`, with a fair FIFO waiter queue and standard non-buffered notification semantics.
* **Latch** is inspired by [`latches`](https://github.com/mirromutth/latches), with a different implementation based on the internal `CountdownState` primitive. No sync variant is provided, since it can be easily implemented with block_on of any runtime.
* **Mutex** is derived from `tokio::sync::Mutex`. No blocking method is provided, since it can be easily implemented with block_on of any runtime.
* **OnceCell** is derived from `tokio::sync::OnceCell`, but using our own semaphore implementation.
* **OnceMap** is inspired by `uv-once-map` but the interface and implementation are redesigned.
* **RwLock** is derived from `tokio::sync::RwLock`, but the `max_readers` can be any `NonZeroUsize` (effectively any positive `usize`) instead of `[0, u32::MAX >> 3]`. No blocking method is provided, since it can be easily implemented with block_on of any runtime.
* **Semaphore** is derived from `tokio::sync::Semaphore`, without `close` method since it is quite tricky to use. And thus, this semaphore doesn't have the limitation of max permits. Besides, new methods like `forget_exact` are added to fit the specific use case.
* **WaitGroup** is inspired by [`waitgroup-rs`](https://github.com/laizy/waitgroup-rs), providing different API flavor with a different implementation based on the internal `CountdownState` primitive.
* The internal atomic pointer slot used by MPSC is derived from [`atomicbox`](https://github.com/jorendorff/atomicbox/) at commit 07756444.
* The single-future polling loop in `blocking` is adapted from [`pollster`](https://github.com/zesterer/pollster), and its parker caching strategy follows [`futures-lite`](https://github.com/smol-rs/futures-lite).
* **broadcast::overflow::channel** is derived from `tokio::sync::broadcast::channel`, with a different implementation based on the internal `WaitSet` primitive.
* **oneshot::channel** is derived from [`oneshot`](https://github.com/faern/oneshot), with significant simplifications since we need not support synchronized receiving functions.

Other parts are written from scratch.

NB. The optimization considerations are different when implementing a sync primitive for sync code and async code. Generally speaking, once you have an async + runtime-agnostic implementation, you can immediately have a sync implementation by block_on any async runtime ([`pollster`](https://github.com/zesterer/pollster) is the most lightweight runtime that park the current thread). However, a sync-oriented implementation may leverage some platform-specific features to achieve better performance. This library is designed for async code, so it doesn't consider sync-oriented optimization. I often find libraries that try to provide both sync and async implementations end up with a clumsy API design. So I prefer to keep them separate.
