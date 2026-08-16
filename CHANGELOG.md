# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Features

* Add an opt-in `asyncband::block_on` module providing a minimal single-future blocking executor
  with `block_on`, `block_on_timeout`, and the `FutureExt` suffix methods.

### Bug fixes

* Release cancelled wait registrations promptly and reclaim fulfilled `Semaphore::forget_exact` debt nodes.

### Improvements

* Remove the `slab` dependency in favor of a focused internal waiter arena.
