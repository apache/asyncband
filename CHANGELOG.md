# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Breaking changes

* Make every primitive an opt-in Cargo feature and enable no primitives by default; downstream dependencies must list the primitives they use.
* Rename `oneshot::Sender::is_closed` and `oneshot::Receiver::is_closed` to `is_disconnected`.
* Raise the minimum supported Rust version from 1.85.0 to 1.86.0.

### Bug fixes

* Release cancelled wait registrations promptly and reclaim fulfilled `Semaphore::forget_exact` debt nodes.

### Improvements

* Remove the `slab` dependency in favor of a focused internal waiter arena.
