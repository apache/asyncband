# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Breaking changes

* Rename `oneshot::Sender::is_closed` and `oneshot::Receiver::is_closed` to `is_disconnected`. ([#141](https://github.com/fast/asyncband/issues/141))

### Bug fixes

* Release cancelled wait registrations promptly and reclaim fulfilled `Semaphore::forget_exact` debt nodes.

### Improvements

* Remove the `slab` dependency in favor of a focused internal waiter arena.
