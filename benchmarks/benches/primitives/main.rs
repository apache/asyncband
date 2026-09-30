// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Primitive operation costs, with hand-polled waiters unless a case explicitly uses threads.
//! Ready paths reuse initialized state; handoff/fanout cycles include waiter registration, wake
//! callbacks and repolling. Boxed waiter vectors in those cycles are timed too. The shared waker
//! accounts for reference counting but does not schedule tasks, so these are not executor
//! latencies.
//!
//! `diagnostics` groups are opt-in: cancellation, repeated Pending, construction bookkeeping,
//! no-consumer paths and large reclaim probes. Do not give them the weight of ordinary operations.
//!
//! Run `cargo x bench --bench primitives -- --list` to inspect scenarios, or pass a path filter.
//! Run `cargo x bench -- --test --include-ignored` to smoke every scenario without collecting
//! timings. Use `--include-ignored diagnostics` to measure diagnostic probes explicitly.

mod barrier;
mod blocking;
mod broadcast;
mod completion;
mod condvar;
mod event;
mod latch;
mod mpmc;
mod mpsc;
mod mutex;
mod once;
mod once_map;
mod oneshot;
mod phaser;
mod pool;
mod rwlock;
mod semaphore;
mod shutdown;
mod singleflight;
mod waitgroup;

fn main() {
    divan::main();
}
