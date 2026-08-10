# Changelog

> Apache Asyncband (Incubating) is an effort undergoing incubation at the Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC. Please read the [DISCLAIMER](DISCLAIMER).

All notable changes to this project will be documented in this file.

## Unreleased

### New features

* Add an opt-in `asyncband::blocking::FutureExt` bridge with `block_on` and `wait_timeout` methods for waiting on runtime-agnostic futures from synchronous code.
* Add SPSC, MPSC, SPMC, and MPMC queues with rendezvous, bounded, and unbounded capacities.
* Add overflow, backpressure, and unbounded broadcast retention policies.
* Add coalescing watch channels and single-producer and multi-producer Disruptor-style multicast sequencers.
* Add `sync`, `channel`, and `coordination` public module and Cargo feature groups.

### Breaking changes

* Gate all exported primitives behind opt-in Cargo features and enable no features by default; downstream dependencies must explicitly enable the APIs they use.
* Group all channel families under `asyncband::channel` and remove the previous root-level oneshot, MPSC, and broadcast implementations.
* Remove `admission::FairShare` and its `admission` Cargo feature from the feature set.
* Remove the `asyncband::atomicbox` module and its `AtomicBox` and `AtomicOptionBox` types from the public API.
* Rename `oneshot::Sender::is_closed` and `oneshot::Receiver::is_closed` to `is_disconnected`.
* Raise the minimum supported Rust version from 1.85.0 to 1.86.0.

### Bug fixes

* Release cancelled wait registrations promptly and reclaim fulfilled `Semaphore::forget_exact` debt nodes.
* Serialize broadcast publication so receivers cannot observe reserved slots or messages overwritten out of sequence.

### Improvements

* Remove the `slab` dependency in favor of a focused internal waiter arena.
