<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Changelog

All significant changes to this project will be documented in this file.

## Unreleased

### Performance improvements

* Speed up `WaitGroup` handle cloning, worker completion, and waiting across single- and multi-observer workloads, while avoiding extra heap allocations when at most one observer waits before completion.

## v0.8.0

### Breaking changes

* `broadcast::mpmc::BoundedReceiver::{recv, try_recv}` and `UnboundedReceiver::{recv, try_recv}` now return `Arc<T>` instead of `T`, and `broadcast::mpmc::{bounded, unbounded}` no longer require `T: Clone`. Shared receives expose any interior mutability in `T` across receivers, and retaining an `Arc<T>` keeps its payload alive after it leaves the channel backlog, so bounded capacity limits queued messages rather than live payload memory. Use the new `recv_cloned` and `try_recv_cloned` methods to preserve owning `Clone` semantics (moving the payload without cloning when the receiver holds the last reference), or send `T` directly where `Arc<T>` was previously used only to avoid cloning.
* `MappedRwLockWriteGuard::downgrade` and `OwnedMappedRwLockWriteGuard::downgrade` are removed because downgrading a projected write guard could cause undefined behavior when concurrent readers can reach the projected component through interior mutability. Downgrade `RwLockWriteGuard` or `OwnedRwLockWriteGuard` first, then project the resulting read guard with `RwLockReadGuard::map` or `OwnedRwLockReadGuard::map`.

### New features

* Add `rwlock::UpgradableRwLock` and `rwlock::RwLockUpgradableReadGuard` to allow multiple ordinary readers alongside one upgradable reader that can atomically `upgrade`, `try_upgrade`, or `downgrade` to `RwLockReadGuard`. Cancelling a pending `upgrade` releases the consumed guard, and writers coordinate in FIFO order with upgradable readers so queued writers cannot deadlock an upgrade while ordinary readers may still acquire ahead of waiting writers.
* Add `is_disconnected` to `mpsc` and `mpmc` senders and receivers to check whether the peer endpoint has been dropped without sending or receiving; receivers report disconnection as soon as the last sender drops, even while buffered values remain available.
* Add `is_empty` to `mpsc` and `mpmc` receivers to check whether buffered values are queued without receiving; held reservation permits and sends waiting for capacity do not count as queued values.

### Performance improvements

* Speed up `mpsc::BoundedReceiver` receives on ready values by avoiding intermediate payload copies, particularly for larger inline message types; contended receive latency is roughly unchanged.
* Speed up `broadcast::mpmc` receives by avoiding an extra `Arc` clone when the slowest active receiver consumes the oldest retained message and skipping sender-wakeup checks on bounded receives that do not free capacity.
* Speed up uncontended fresh `OnceCell` initialization while keeping already-initialized reads and contended multi-waiter initialization unchanged.
* Speed up re-polling a pending `oneshot::Receiver` from the same task, such as across repeated `select!` iterations, by avoiding redundant waker clones while keeping first-poll and send-wakeup latency unchanged.
* Shrink the futures returned by `Mutex`, `RwLock`, `UpgradableRwLock`, `Condvar`, `Semaphore`, `Once`, `OnceCell`, and `broadcast::mpmc::BoundedSender::send`, speeding up uncontended lock reuse, upgradable lock operations, queued semaphore bursts, and single-waiter condition notifications.
* Speed up `Barrier::wait` for non-final arrivals and cancelled pending waits.

### Bug fixes

* `OnceMap::get` no longer retains an empty entry when an in-flight computation fails, panics, or is cancelled while a concurrent `get` inspects the same key.
* `OnceMap` and `singleflight::Group` now reclaim in-flight entries and cached value clones when dropping a duplicate key panics, including if the active computation is cancelled during unwinding.
* `Barrier::wait` on 32-bit targets no longer panics on overflow in debug builds or risks panics and missed follower wakeups in release builds after `2^32` completed generations.

## v0.7.4 (2026-10-25)

### Notable changes

* Executor waker operations, including cloning, waking, and dropping, are expected not to panic; recovery from panicking waker callbacks is no longer supported.

### New features

* Add `mpmc::BoundedSender::{reserve, try_reserve}` returning a borrowed `mpmc::Permit` so callers can wait for capacity before constructing a value. Pending sends and reservations receive capacity in wait-queue order, and dropping an unused permit releases its slot without claiming message order.

### Performance improvements

* Speed up `FutureExt::block_on` throughput and reduce context switches when multiple threads repeatedly block on a contended primitive such as `mpmc::bounded`; isolated cross-thread wakeups cost slightly more, and ready or timed waits are unchanged.
* Speed up `ManualResetEvent::is_set`, `ManualResetEvent::try_wait`, and already-set `ManualResetEvent::{wait, wait_owned}` calls, particularly under concurrent reads; replacing a pending waiter's waker costs slightly more.
* Speed up `Semaphore::release`, `RwLock` read-guard drops, and `pool::bounded` object returns when spare permits or capacity remain available; `Mutex` releases are unchanged.

### Bug fixes

* `singleflight::Group::{work, try_work}` no longer deadlock when dropping a duplicate key calls back into the same group.
* Fix undefined behavior in `oneshot` when sending on or dropping `Sender` races with `Receiver` completion or cancellation.

## v0.7.3 (2026-09-29)

### New features

* Add `event::AutoResetEvent` for releasing a single waiter per signal while retaining at most one unassigned signal that `reset` can clear and transferring assigned signals when waits are cancelled.
* Add `ManualResetEvent::try_wait` to check readiness without registering a waiter or consuming the set state.
* Add `broadcast::mpmc::bounded` alongside `BoundedSender` and `BoundedReceiver` for lossless bounded fan-out that retains at most the configured message count and applies backpressure from the slowest active receiver.
* Add `mpmc::{bounded, unbounded}` behind the `mpmc` feature, with cloneable `BoundedSender` and `UnboundedSender` handles and competing `BoundedReceiver` and `UnboundedReceiver` handles that deliver each accepted value to one receiver.
* Add `spmc::{bounded, unbounded}` behind the `spmc` feature, with an exclusive non-cloneable sender and cloneable competing receivers.
* Add `phaser::{Phaser, PhaserParticipant, PhaserParticipants, Closed}` behind the `phaser` feature for reusable phase synchronization with dynamic RAII participant registration, `u64` phase observations, split `arrive` and `wait` operations, observer handles, and explicit `close` signaling.
* Add `mpsc::BoundedSender::{reserve, try_reserve}` returning a borrowed `mpsc::Permit` so callers can wait for capacity before constructing a message. Pending sends and reservations receive capacity in wait-queue order, and dropping an unused permit releases its slot without claiming message order.

### Improvements

* Allow `Condvar` wait methods to accept `Mutex` guards over unsized values such as slices and trait objects, for both borrowed and owned guards.
* Allow manual `pool::unbounded` checkouts via `get_or_create` to use non-`Send` factory closures, including factories that borrow task-local state.
* Remove the `T: Clone` bound from `watch::channel` construction, publication, and change observation; only owning reads via `watch::Receiver::{get, recv}` require `Clone`.

### Performance improvements

* Speed up `Completion::wait` once a value has been published or the completer has been dropped, while keeping pending wait registration and cancellation roughly unchanged.
* Speed up `mpsc::unbounded` burst and multi-producer workloads while bounding retained empty buffer capacity independently of peak burst size, and speed up `mpsc::bounded` ready and multi-producer sends and receives.
* Speed up `broadcast::mpmc` message reclamation when subscriptions lag behind or are dropped, scaling cleanup with released messages rather than peak subscriber count.
* Avoid heap allocations when waking up to 32 tasks at once across `Barrier`, `broadcast`, `Condvar`, `event`, `mpsc`, and `watch` notifications.

### Bug fixes

* `Semaphore::release` and permit drops now finish releasing permits and waking remaining eligible waiters even if a waker callback panics.
* Dropping `mpsc::BoundedReceiver` or `UnboundedReceiver` now releases any stored receiver waker immediately, avoiding retained tasks and reference cycles when a waker holds a sender.
* Dropping `mpsc::BoundedReceiver` when a buffered message's destructor panics now finishes dropping the remaining buffered messages and wakes all blocked senders with disconnection errors.

## v0.7.2 (2026-09-11)

### Improvements

* Preserve third-party copyright and license notices on derived source files, and document provenance mappings in `LICENSE` and source comments.

## v0.7.1 (2026-09-04)

This is an interim non-ASF release. It has not been approved by the Apache Incubator PMC and is not an act of the Apache Software Foundation.

### Bug fixes

* Correct source license headers and make third-party derivation and test provenance records more precise in source distributions.

## v0.7.0 (2026-09-04)

This non-ASF release was not approved by the Apache Incubator PMC, is not an act of the Apache Software Foundation, and has been yanked from crates.io.

### Breaking changes

* Gate all public modules behind opt-in Cargo features with `default = []` (`barrier`, `blocking`, `broadcast`, `completion`, `condvar`, `event`, `latch`, `lazy-cell`, `mpsc`, `mutex`, `once`, `once-cell`, `once-map`, `oneshot`, `pool`, `rwlock`, `semaphore`, `shutdown`, `singleflight`, `waitgroup`, and `watch`). Downstream crates must explicitly enable the features for the APIs they use.
* Raise the minimum supported Rust version (MSRV) from 1.85.0 to 1.86.0.
* Remove the `admission` module (`FairShare`, `FairSharePermit`, and `OwnedFairSharePermit`) and the `atomicbox` module (`AtomicBox` and `AtomicOptionBox`) from the public API.
* Remove the lossy `broadcast::overflow` channel; use `broadcast::mpmc::unbounded` instead.
* Remove `OnceMap::with_capacity` and `OnceMap::with_capacity_and_hasher`; use `OnceMap::new` or `OnceMap::with_hasher`, which allocate backing storage lazily.
* Remove the unconstructible `LatchWait` and `OwnedLatchWait` types from the public API; `Latch::wait` and `Latch::wait_owned` continue to return anonymous futures via `async fn`.
* Rename `oneshot::Sender::is_closed` and `oneshot::Receiver::is_closed` to `is_disconnected`.
* Remove `Semaphore::{try_acquire_and_forget, acquire_and_forget, try_acquire_owned_and_forget, acquire_owned_and_forget}`, and replace `Semaphore::forget` with `Semaphore::drain_permits` and `Semaphore::forget_exact` with `Semaphore::reduce_permits`. Acquire a permit and call its `forget` method for queued acquisition without automatic release; permit-level `forget` methods are unchanged.
* Redesign `shutdown`: rename `shutdown::new_pair` to `shutdown::new`, `ShutdownSend` to `Shutdown`, and `ShutdownRecv` to `ShutdownGuard`; replace `ShutdownSend::shutdown` with `Shutdown::request_shutdown` and `ShutdownSend::await_shutdown` with awaiting `Shutdown`; rename `is_shutdown_now`, `is_shutdown`, and `is_shutdown_owned` to `is_shutdown_requested`, `shutdown_requested`, and `shutdown_requested_owned`; and add `ShutdownGuard::into_watch`.

### New features

* Add `broadcast::mpmc::unbounded` alongside `UnboundedSender` and `UnboundedReceiver` behind the `broadcast` feature for lossless broadcast channels that retain messages until all active receivers consume them or are dropped.
* Add `watch::{channel, Sender, Receiver, SendError, RecvError}` behind the `watch` feature for publishing latest state across independent receivers that coalesce intermediate updates, including in-place replacement via `Sender::send_replace`.
* Add `event::ManualResetEvent` behind the `event` feature for reusable level-triggered signaling that releases current and future waiters until explicitly reset.
* Add `completion::{new, Completer, Completion, Abandoned}` behind the `completion` feature for publishing a single shared result to cloneable observers with borrowed result access and observable producer abandonment.
* Add `once::LazyCell` behind the `lazy-cell` feature for asynchronous lazy initialization that preserves an in-flight future across caller cancellation and supports retryable fallible initialization via `try_force` and `try_force_with`.
* Add `pool::{bounded, unbounded}` alongside `ManageObject` and pool lifecycle policies behind the `pool` feature for runtime-agnostic asynchronous object pooling.
* Add `blocking::FutureExt` behind the `blocking` feature with `block_on` and `wait_timeout` for driving runtime-agnostic futures from synchronous code.

### Improvements

* Remove the `slab` dependency and make `hashbrown` optional (enabled only by `once-map` and `singleflight`), leaving all other features with no runtime dependencies.
* Format disconnected channel error messages consistently across channel modules.

### Performance improvements

* Speed up uncontended `Mutex` and `Semaphore` acquire-release cycles and queued `Semaphore` permit handoffs by avoiding wake-buffer heap allocations when no tasks are queued or up to 32 waiters are woken.
* Speed up pending `mpsc` receiver re-polls and wakeups by avoiding per-poll waker heap allocations.
* Speed up pending wait cancellation and multi-waiter fan-out across `Latch`, `Once`, and `WaitGroup`, along with `WaitGroup` handle clone/drop and multi-worker completion.
* Speed up single- and small-cohort fan-out notifications across `Completion`, `Condvar`, `ManualResetEvent`, `Barrier`, `Latch`, `Once`, and `watch` by avoiding single-waiter heap allocations.
* Speed up `OnceMap` and `singleflight::Group` empty construction and same-key coalescing via lazy table allocation; multi-threaded lookups and leader starts across many distinct keys contend on a single table lock instead of fixed shards.

### Bug fixes

* `SemaphorePermit::merge` and `OwnedSemaphorePermit::merge` now panic without losing permits from either guard if the combined count would exceed `usize::MAX`, instead of wrapping.
* Cancelled wait registrations now release their waiter slots immediately instead of retaining them until notification, and fulfilled `Semaphore::reduce_permits` (formerly `forget_exact`) obligations are reclaimed immediately.
* Fan-out notifications, including `Semaphore` permit grants, now continue waking remaining waiters when an earlier waker callback panics.
