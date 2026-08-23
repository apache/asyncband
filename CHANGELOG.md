# Changelog

> Apache Asyncband (Incubating) is an effort undergoing incubation at the Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC. Please read the [DISCLAIMER](DISCLAIMER).

All notable changes to this project will be documented in this file.

## Unreleased

### New features

* Add an opt-in `asyncband::blocking::FutureExt` bridge with `block_on` and `wait_timeout` methods for waiting on runtime-agnostic futures from synchronous code.
* Add opt-in bounded and unbounded runtime-agnostic object pools under `asyncband::pool`.

### Breaking changes

* Gate all exported primitives behind opt-in Cargo features and enable no features by default; downstream dependencies must explicitly enable the APIs they use.
* Remove `admission::FairShare` and its `admission` Cargo feature from the feature set.
* Remove the `asyncband::atomicbox` module and its `AtomicBox` and `AtomicOptionBox` types from the public API.
* Remove the lossy `broadcast::overflow` channel and its `broadcast` Cargo feature; future broadcast APIs will use explicit bounded and unbounded lossless semantics.
* Remove `Semaphore::try_acquire_and_forget`, `Semaphore::acquire_and_forget`, `Semaphore::try_acquire_owned_and_forget`, and `Semaphore::acquire_owned_and_forget`; acquire a permit and call its `forget` method instead.
* Rename `oneshot::Sender::is_closed` and `oneshot::Receiver::is_closed` to `is_disconnected`.
* Replace `Semaphore::forget` with `Semaphore::drain_permits` and `Semaphore::forget_exact` with `Semaphore::reduce_permits`; permit-level `forget` methods are unchanged.
* Raise the minimum supported Rust version from 1.85.0 to 1.86.0.

### Bug fixes

* Release cancelled wait registrations promptly and reclaim fulfilled `Semaphore::reduce_permits` debt nodes.

### Improvements

* Remove the `slab` dependency in favor of a focused internal waiter arena.
* Refine object-pool lifecycle and maintenance APIs with exact idle status, return-time usage metadata, detachment hooks during retention, fallible `replenish_to`, and synchronous `try_get` for manually populated pools.
* Clarify Asyncband's scope as composable, runtime-agnostic concurrency building blocks that keep execution and timing policy with callers.
