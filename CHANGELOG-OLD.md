# Historical Changelog

## v0.6.7 (2026-08-13)

* Announce the upcoming `asyncband` rebrand.

## v0.6.6 (2026-08-13)

### Bug fixes

* Prevent `OwnedMappedMutexGuard` from allowing invalid lifetime coercions. ([#121](https://github.com/fast/mea/pull/121))
* Require the protected value of an `OwnedRwLockReadGuard` to be both `Send` and `Sync` before the guard can be sent across threads. ([#122](https://github.com/fast/mea/pull/122))
* Prevent reentrant wakers from deadlocking or poisoning `Barrier`, `Latch`, `WaitGroup`, `Once`, and `broadcast::overflow` operations. ([#125](https://github.com/fast/mea/pull/125))
* Retry spurious atomic failures when completing `Once` initialization. ([#126](https://github.com/fast/mea/pull/126))
* Make cloning a `WaitGroup` panic on counter overflow instead of silently losing track of a handle. ([#128](https://github.com/fast/mea/pull/128))
* Align `Condvar` with standard non-buffered notification semantics by notifying only current waiters and passing a cancelled `notify_one` wakeup to another current waiter. ([#132](https://github.com/fast/mea/pull/132))
* Ensure polling or cancelling a contended `oneshot` receive does not wait for the sender to make progress. ([#138](https://github.com/fast/mea/pull/138))
* Clean up uninitialized `OnceMap` and `singleflight::Group` entries once no callers remain after a failure, panic, or cancellation. ([#142](https://github.com/fast/mea/pull/142))
* Keep a semaphore's permit count unchanged when `Semaphore::release` panics on overflow. ([#144](https://github.com/fast/mea/pull/144))

### Improvements

* Remove unnecessary `Clone` bounds from `singleflight::Group` keys and custom hashers used by `OnceMap` and `singleflight::Group`. ([#142](https://github.com/fast/mea/pull/142))

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
