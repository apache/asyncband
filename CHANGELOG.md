# CHANGELOG

All notable changes to this project will be documented in this file.

## Unreleased

### Bug fixes

* Prevent `OwnedMappedMutexGuard` from allowing invalid lifetime coercions. ([#121](https://github.com/fast/mea/pull/121))
* Require the protected value of an `OwnedRwLockReadGuard` to be both `Send` and `Sync` before the guard can be sent across threads. ([#122](https://github.com/fast/mea/pull/122))
* Wake waiting tasks only after releasing internal locks. ([#125](https://github.com/fast/mea/pull/125))
* Retry spurious atomic failures when completing `Once` initialization. ([#126](https://github.com/fast/mea/pull/126))
* Make cloning a `WaitGroup` panic on counter overflow instead of silently losing track of a handle.
* Align `Condvar` with standard condition-variable semantics by notifying only current waiters and passing a cancelled `notify_one` wakeup to another current waiter instead of storing a permit.
* Clean up uninitialized `OnceMap` and `singleflight` entries once no callers remain after a failure, panic, or cancellation.
* Preserve semaphore permits when releasing permits panics on overflow.

### Improvements

* Remove unnecessary `Clone` bounds from `singleflight` keys and custom hashers used by keyed once primitives.

## v0.6.5 (2026-07-30)

### New features

* Implement `admission::FairShare` to provide a work-conserving admission policy for workloads partitioned by key.

## v0.6.4 (2026-05-28)

### New features

* Implement `shutdown::ShutdownWatch` to wait for the shutdown signal without blocking `ShutdownSend::await_shutdown`.

## v0.6.3 (2026-01-21)

### Improvements

* `OnceMap` no longer requires `K: Clone` everywhere.

## v0.6.2 (2026-01-21)

### New features

* Implement `once::OnceMap` to run computation only once and store the results in a hash map.
* `singleflight::Group` now supports custom hashers for keys.

### Improvements

* `singleflight::Group::forget` now accepts any `&Q` where `Q: ?Sized + Hash + Eq` and `K: Borrow<Q>` aligning with standard HashMap's interface.

## v0.6.1 (2026-01-11)

### New features

* Implement `singleflight` pattern for deduplicating concurrent requests.

## v0.6.0 (2026-01-04)

### Breaking changes

* All channel errors are now unified follow the same `[Try](Send|Recv)Error` pattern. ([#98](https://github.com/fast/mea/pull/98))
* `broadcast::channel` and the related types are moved to one level deeper module `broadcast::overflow`. ([#99](https://github.com/fast/mea/pull/99))

### Improvements

* `oneshot::Sender` and `oneshot::Receiver` now always implement `Debug`.
