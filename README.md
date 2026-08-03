# MEA (Make Easy Async)

[![Crates.io][crates-badge]][crates-url]
[![Documentation][docs-badge]][docs-url]
[![MSRV 1.85][msrv-badge]](https://www.whatrustisit.com)
[![Apache 2.0 licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]

[crates-badge]: https://img.shields.io/crates/v/mea.svg
[crates-url]: https://crates.io/crates/mea
[docs-badge]: https://docs.rs/mea/badge.svg
[docs-url]: https://docs.rs/mea
[msrv-badge]: https://img.shields.io/badge/MSRV-1.85-green?logo=rust
[license-badge]: https://img.shields.io/crates/l/mea
[license-url]: LICENSE
[actions-badge]: https://github.com/fast/mea/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/fast/mea/actions/workflows/ci.yml

## Overview

MEA is a runtime-agnostic library providing essential synchronization primitives for asynchronous Rust programming. The library offers a collection of well-tested, efficient synchronization tools that work with any async runtime.

## Features

* [**Barrier**](https://docs.rs/mea/*/mea/barrier/struct.Barrier.html): A synchronization primitive that enables tasks to wait until all participants arrive.
* [**Condvar**](https://docs.rs/mea/*/mea/condvar/struct.Condvar.html): A condition variable that allows tasks to wait for a notification.
* [**Latch**](https://docs.rs/mea/*/mea/latch/struct.Latch.html): A synchronization primitive that allows one or more tasks to wait until a set of operations completes.
* [**Mutex**](https://docs.rs/mea/*/mea/mutex/struct.Mutex.html): A mutual exclusion primitive for protecting shared data.
* [**Once**](https://docs.rs/mea/*/mea/once/struct.Once.html): A primitive that ensures a one-time asynchronous operation runs at most once, even when called concurrently.
* [**OnceCell**](https://docs.rs/mea/*/mea/once/struct.OnceCell.html): A cell that can be written to at most once, providing safe, lazy initialization.
* [**OnceMap**](https://docs.rs/mea/*/mea/once/struct.OnceMap.html): A hash map that runs computation only once for each key and stores the result.
* [**RwLock**](https://docs.rs/mea/*/mea/rwlock/struct.RwLock.html): A reader-writer lock that allows multiple readers or a single writer at a time.
* [**Semaphore**](https://docs.rs/mea/*/mea/semaphore/struct.Semaphore.html): A synchronization primitive that controls access to a shared resource.
* [**WaitGroup**](https://docs.rs/mea/*/mea/waitgroup/struct.WaitGroup.html): A synchronization primitive that allows waiting for multiple tasks to complete.
* [**admission::FairShare**](https://docs.rs/mea/*/mea/admission/struct.FairShare.html): A work-conserving admission policy that fairly shares bounded concurrency across keys.
* [**atomicbox**](https://docs.rs/mea/*/mea/atomicbox/): A safe, owning version of AtomicPtr for heap-allocated data.
* [**broadcast**](https://docs.rs/mea/*/mea/broadcast/): A multi-producer, multi-consumer broadcast channel.
* [**mpsc::bounded**](https://docs.rs/mea/*/mea/mpsc/fn.bounded.html): A multi-producer, single-consumer bounded queue for sending values between asynchronous tasks.
* [**mpsc::unbounded**](https://docs.rs/mea/*/mea/mpsc/fn.unbounded.html): A multi-producer, single-consumer unbounded queue for sending values between asynchronous tasks.
* [**oneshot::channel**](https://docs.rs/mea/*/mea/oneshot/): A one-shot channel for sending a single value between tasks.
* [**shutdown**](https://docs.rs/mea/*/mea/shutdown/): A composite synchronization primitive for managing shutdown signals.
* [**singleflight::Group**](https://docs.rs/mea/*/mea/singleflight/): A duplicate function call suppression mechanism.
* [**time**](https://docs.rs/mea/*/mea/time/): Explicitly driven delays, timeouts, intervals, and scheduled actions that do not require a particular async runtime.

## Installation

Add the dependency to your `Cargo.toml` via:

```shell
cargo add mea
```

## Runtime Agnostic

All synchronization primitives in this library are runtime-agnostic, meaning they can be used with any async runtime like Tokio, async-std, or others. This makes the library highly versatile and portable.

## Thread Safety

All types in this library implement `Send` and `Sync`, making them safe to share across thread boundaries. This is essential for concurrent programming where data needs to be accessed from multiple threads.

## Minimum Supported Rust Version (MSRV)

This crate is built against the latest stable release, and its minimum supported rustc version is 1.85.0.

The policy is that the minimum Rust version required to use this crate can be increased in minor version updates. For example, if MEA 1.0 requires Rust 1.20.0, then MEA 1.0.z for all values of z will also require Rust 1.20.0 or newer. However, Mea 1.y for y > 0 may require a newer minimum version of Rust.

## License

This project is licensed under [Apache License, Version 2.0](LICENSE).

## History

This crate collects runtime-agnostic synchronization primitives from spare parts:

* **admission::FairShare** is written from scratch to bound global concurrency while balancing held permits across contending keys.
* **Barrier** is inspired by `std::sync::Barrier` and `tokio::sync::Barrier`, with a different implementation based on the internal `WaitSet` primitive.
* **Condvar** is inspired by `std::sync::Condvar` and `async_std::sync::Condvar`, with a fair FIFO waiter queue and standard non-buffered notification semantics.
* **Latch** is inspired by [`latches`](https://github.com/mirromutth/latches), with a different implementation based on the internal `CountdownState` primitive. No `wait` or `watch` method is provided, since it can be easily implemented by [composing delay futures](https://docs.rs/fastimer/*/fastimer/fn.timeout.html). No sync variant is provided, since it can be easily implemented with block_on of any runtime.
* **Mutex** is derived from `tokio::sync::Mutex`. No blocking method is provided, since it can be easily implemented with block_on of any runtime.
* **OnceCell** is derived from `tokio::sync::OnceCell`, but using our own semaphore implementation.
* **OnceMap** is inspired by `uv-once-map` but the interface and implementation are redesigned.
* **RwLock** is derived from `tokio::sync::RwLock`, but the `max_readers` can be any `NonZeroUsize` (effectively any positive `usize`) instead of `[0, u32::MAX >> 3]`. No blocking method is provided, since it can be easily implemented with block_on of any runtime.
* **Semaphore** is derived from `tokio::sync::Semaphore`, without `close` method since it is quite tricky to use. And thus, this semaphore doesn't have the limitation of max permits. Besides, new methods like `forget_exact` are added to fit the specific use case.
* **WaitGroup** is inspired by [`waitgroup-rs`](https://github.com/laizy/waitgroup-rs), providing different API flavor with a different implementation based on the internal `CountdownState` primitive.
* **atomicbox** is forked from [`atomicbox`](https://github.com/jorendorff/atomicbox/) at commit 07756444.
* **broadcast::channel** is derived from `tokio::sync::broadcast::channel`, with a different implementation based on the internal `WaitSet` primitive.
* **oneshot::channel** is derived from [`oneshot`](https://github.com/faern/oneshot), with significant simplifications since we need not support synchronized receiving functions.
* **time** is built around an explicit driver/context pair. The driver owns a private hierarchical timing wheel and is advanced by the integrating reactor; timeout, interval, and scheduling APIs compose over the context without spawning tasks or using a process-global runtime handle.

Other parts are written from scratch.

NB. The optimization considerations are different when implementing a sync primitive for sync code and async code. Generally speaking, once you have an async + runtime-agnostic implementation, you can immediately have a sync implementation by block_on any async runtime ([`pollster`](https://github.com/zesterer/pollster) is the most lightweight runtime that park the current thread). However, a sync-oriented implementation may leverage some platform-specific features to achieve better performance. This library is designed for async code, so it doesn't consider sync-oriented optimization. I often find libraries that try to provide both sync and async implementations end up with a clumsy API design. So I prefer to keep them separate.
