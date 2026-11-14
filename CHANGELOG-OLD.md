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

# Historical Changelog

## v0.6.7 (2026-08-13)

### Notable changes

* Announce the upcoming rename from `mea` to `asyncband`.

## v0.6.6 (2026-08-13)

### Improvements

* Remove unnecessary `Clone` bounds on keys in `singleflight::Group` and on custom hashers in `OnceMap` and `singleflight::Group`.

### Bug fixes

* Make `OwnedMappedMutexGuard` invariant over its projected type to prevent unsound lifetime coercions.
* Require `T: Send + Sync` before `OwnedRwLockReadGuard<T>` implements `Send`.
* Wake waiters after releasing internal locks across `Barrier`, `Latch`, `WaitGroup`, `Once`, and `broadcast::overflow` so reentrant wakers cannot deadlock or poison state.
* Retry spurious compare-exchange failures when completing `Once` initialization instead of leaving concurrent waiters blocked.
* Panic on reference-count overflow when cloning `WaitGroup` instead of silently losing track of a handle.
* Notify only waiters already registered before `Condvar::{notify_one, notify_all}`, and forward a cancelled `notify_one` wakeup to another current waiter.
* Avoid waiting on sender progress when polling or cancelling a contended `oneshot::Receiver`.
* Remove uninitialized `OnceMap` and `singleflight::Group` entries once all callers leave after an error, panic, or cancellation.
* Leave the permit count unchanged when `Semaphore::release` panics on overflow.

## v0.6.5 (2026-07-30)

### New features

* Add `admission::{FairShare, FairSharePermit, OwnedFairSharePermit}` for work-conserving admission control across partitioned keys.

## v0.6.4 (2026-05-28)

### New features

* Add `shutdown::ShutdownWatch` to wait for the shutdown signal without delaying `ShutdownSend::await_shutdown`.

## v0.6.3 (2026-01-21)

### Improvements

* Remove the `K: Clone` bound from `OnceMap` construction and operations.

## v0.6.2 (2026-01-21)

### New features

* Add `once::OnceMap` to coalesce concurrent per-key computations and store completed values in a hash map.
* Support custom hashers on `singleflight::Group` via `Group::with_hasher`.

### Improvements

* Allow `singleflight::Group::forget` to accept borrowed key forms `&Q` where `K: Borrow<Q>` and `Q: ?Sized + Hash + Eq`.

## v0.6.1 (2026-01-11)

### New features

* Add `singleflight::Group` for coalescing concurrent duplicate requests by key.

## v0.6.0 (2026-01-04)

### Breaking changes

* Unify channel error types across `oneshot`, `mpsc`, and `broadcast`: `mpsc::{BoundedReceiver, UnboundedReceiver}::recv` now returns `Result<T, mpsc::RecvError>` instead of `Option<T>`, `oneshot::Receiver::try_recv` now returns `Result<T, oneshot::TryRecvError>` instead of `Result<Option<T>, oneshot::RecvError>`, and `oneshot::RecvError` is now an enum with a `Disconnected` variant.
* Move `broadcast::channel` and its sender, receiver, and error types into `broadcast::overflow`.

### Improvements

* Implement `Debug` unconditionally for `oneshot::Sender<T>`, `oneshot::Receiver<T>`, and `oneshot::Recv<T>` without requiring `T: Debug`.
