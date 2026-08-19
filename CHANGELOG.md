# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### New features

* Add an opt-in `asyncband::blocking` bridge with `block_on`, `FutureExt::block_on`, and `FutureExt::wait_timeout` for waiting on runtime-agnostic futures from synchronous code.

### Breaking changes

* Gate all exported primitive modules behind same-named opt-in Cargo features and enable no features by default; downstream dependencies must explicitly enable the modules they use.
* Remove the `asyncband::atomicbox` module and its `AtomicBox` and `AtomicOptionBox` types from the public API.
* Rename `oneshot::Sender::is_closed` and `oneshot::Receiver::is_closed` to `is_disconnected`.
* Raise the minimum supported Rust version from 1.85.0 to 1.86.0.

### Bug fixes

* Release cancelled wait registrations promptly and reclaim fulfilled `Semaphore::forget_exact` debt nodes.

### Improvements

* Remove the `slab` dependency in favor of a focused internal waiter arena.
