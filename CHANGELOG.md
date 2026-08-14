# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

### Bug fixes

* Release cancelled wait registrations promptly and reclaim fulfilled `Semaphore::forget_exact` debt nodes.

### Improvements

* Remove the `slab` dependency in favor of a focused internal waiter arena.
