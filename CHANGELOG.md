# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

## v0.7.0

### Breaking changes

* Gate all exported primitives behind opt-in Cargo features and enable no features by default; downstream dependencies must explicitly enable the APIs they use.
* Raise the minimum supported Rust version from 1.85.0 to 1.86.0.
* Remove the `admission` module, including `FairShare`, `FairSharePermit`, and `OwnedFairSharePermit`.
* Remove the `asyncband::atomicbox` module and its `AtomicBox` and `AtomicOptionBox` types from the public API.
* Remove the lossy `broadcast::overflow` channel; use the new lossless `broadcast::mpmc::unbounded` channel instead.
* Remove `OnceMap::with_capacity` and `OnceMap::with_capacity_and_hasher`; use `OnceMap::new` or `OnceMap::with_hasher`, which allocate the backing table lazily.
* Remove the unconstructible `LatchWait` and `OwnedLatchWait` types from the public API; `Latch::wait` and `Latch::wait_owned` continue to return anonymous futures through their `async fn` signatures.
* Rename `oneshot::Sender::is_closed` and `oneshot::Receiver::is_closed` to `is_disconnected`.
* Remove `Semaphore::try_acquire_and_forget`, `Semaphore::acquire_and_forget`, `Semaphore::try_acquire_owned_and_forget`, and `Semaphore::acquire_owned_and_forget`; acquire a permit and call its `forget` method instead.
* Replace `Semaphore::forget` with `Semaphore::drain_permits` and `Semaphore::forget_exact` with `Semaphore::reduce_permits`; permit-level `forget` methods are unchanged.
* Redesign graceful shutdown: rename `ShutdownSend` and `ShutdownRecv` to `Shutdown` and `ShutdownGuard`, and `shutdown::new_pair` to `shutdown::new`; replace `ShutdownSend::shutdown` with `Shutdown::request_shutdown` and `ShutdownSend::await_shutdown` with awaiting `Shutdown`; rename `is_shutdown_now`, `is_shutdown`, and `is_shutdown_owned` to `is_shutdown_requested`, `shutdown_requested`, and `shutdown_requested_owned`; and add `ShutdownGuard::into_watch`.

### New features

* Implement `broadcast::mpmc::unbounded`, an unbounded broadcast channel that retains messages until all active receivers consume them or are dropped.
* Add an opt-in clone-based latest-state channel under `asyncband::watch`, including retained replacement updates through `Sender::send_replace`.
* Add opt-in `asyncband::event::ManualResetEvent`, a reusable level-triggered signal that releases registered waits and remains ready for future waits until explicitly reset.
* Add an opt-in shared one-shot completion primitive under `asyncband::completion` with a single-use completer, cloneable observers, a retained borrowed result, and observable abandonment.
* Add opt-in `asyncband::once::LazyCell` for values that own one asynchronous initializer and preserve its in-flight future across caller cancellation.
* Add opt-in bounded and unbounded runtime-agnostic object pools under `asyncband::pool`.
* Add an opt-in `asyncband::blocking::FutureExt` bridge with `block_on` and `wait_timeout` methods for waiting on runtime-agnostic futures from synchronous code.

### Bug fixes

* Reject semaphore permit merges whose combined count exceeds `usize::MAX` instead of wrapping and losing permits.
* Release cancelled wait registrations promptly and reclaim fulfilled `Semaphore::reduce_permits` debt nodes.
* Preserve fan-out notifications, including semaphore permit grants, when one registered waker panics.

### Improvements

* Reduce fan-out notification overhead by avoiding heap allocation for a single waiter and transferring terminal waiter storage out of state locks.
* Reduce MPSC receiver registration and wake latency by storing receiver wakers inline instead of allocating them on the heap.
* Reduce semaphore and mutex hot-path overhead by avoiding wake-buffer allocation when no tasks are queued and batching queued wakes on the stack.
* Allocate `OnceMap` and `singleflight::Group` registries lazily to reduce construction overhead.
* Describe disconnected channel states consistently in channel error messages.
