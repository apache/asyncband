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

# Benchmarks

Following [Benchmark In Milliseconds](https://matklad.github.io/2026/10/05/benchmark-milliseconds.html), each scenario fixes a round count calibrated so the Asyncband reference finishes near 300 ms on a reference run. Every implementation and revision runs that exact same workload, so a faster implementation takes less wall time. Runs interleave peers and rotate which implementation starts first, reporting integer-millisecond medians alongside the full `min..max` range (and raw sample milliseconds with `--raw`).

## Timing Contract

`Bencher` times only the measured operation loop or barrier-gated parallel window. Runtime worker creation and teardown and reusable fixture setup happen outside the clock, whereas single-use primitives (`oneshot`, `Once`, `Completion`, `WaitGroup`, `Shutdown`) and per-batch task spawns execute inside the timed round as part of their workload contract, while each repetition still starts from fresh state. Hand-polled scenarios stack-pin waiter futures with `std::pin::pin!`, and wakeup scenarios poll all woken waiters to `Poll::Ready` unless cancellation itself is under test.

## Suite Roles

One `benchmarks` target (`cargo x bench`) unifies three complementary views:

1. **Self-comparison (`--baseline-only`)**: runs only the reference implementation on matching scenarios, covering fast-path operations, stack-pinned handoffs, and targeted cancellation or reclamation checks on Asyncband's own state machines.
2. **Classic concurrency workloads**: single-thread batch queues (`seq-batch`), scheduled Tokio `multi_thread` (`4w`) and `current_thread` topologies (`1p1c`, `4p1c`, `1p4c`, `4p4c`, `1p4r`, `request-reply`), and contended locks, semaphores, watch, and wait groups.
3. **Ecosystem comparisons (`--peers-only`)**: filters to multi-implementation comparisons using contract-matched adapters against `tokio::sync`, `flume`, `async-channel`, `async-broadcast`, and `tokio_util::task::TaskTracker`. Lossless bounded broadcast excludes Tokio's overwrite-on-full channel, unbackpressured broadcast sizes bounded rings for the full batch so no values drop, and `-shared` broadcast controls wrap owning peers' messages in `Arc` to match Asyncband's shared `Arc<T>` delivery contract.

## Usage

```shell
cargo x bench --no-run
cargo x bench -- --list
cargo x bench -- --test
cargo x bench -- mutex/uncontended --repeat 10 --raw
cargo x bench -- --peers-only --repeat 5
cargo x bench -- mutex/uncontended --baseline-only --rounds 50000000 --repeat 5
```

Substrings passed as positional filters must all match the scenario name. Use `--test` in CI and local smoke checks to execute every workload once per worker without timing claims.

## Calibrating Baselines

1. Run the scenario with `--baseline-only` and adjust `--rounds` until the Asyncband reference median lands near 300 ms on the target machine.
2. Record that round count in the scenario registration and run all peers under the same workload, toolchain, and hardware.
3. If a scenario's work unit, adapter contract, or timing boundary changes, recalibrate and treat the result as a new baseline.

## Adding Benchmarks

Add scenarios only for genuine hot paths, representative multi-task topologies, or targeted reclamation regressions. Avoid cold-path method completionism, allocator-dominated boxed payloads that mask queue or lock behavior, and issue-specific throwaway harnesses.
