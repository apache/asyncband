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

[Benchmark In Milliseconds](https://matklad.github.io/2026/10/05/benchmark-milliseconds.html) suggests timing enough useful work to take about 300 ms. Here that target applies to a **reference revision of Asyncband**: once a scenario has a fixed round count, every implementation and later revision runs that exact workload, and a faster implementation takes less time.

`cargo x bench` runs the `ecosystem` and `primitives` targets by default (`upgradable_stages` is selected explicitly with `--bench upgradable_stages`):

```shell
cargo x bench --no-run
cargo x bench -- --test
```

## Compare implementations (`ecosystem`)

```shell
cargo x bench --bench ecosystem -- --list
cargo x bench --bench ecosystem -- mpsc/bounded/try-roundtrip --repeat 10 --raw
cargo x bench --bench ecosystem -- tasks-1p1c-cap1024 --rounds 320 --repeat 10
```

Each scenario fixes the payload, topology, runtime configuration, observable delivery contract, and round count; filters are substrings that a scenario name must match in full. Each table reports the median elapsed time, full `min..max` range in integer milliseconds, and ratio against the scenario's reference implementation across five interleaved, order-rotated runs (`--raw` prints individual runs in measurement order). The clock covers fixture setup through worker shutdown and cleanup.

Independently sized scenarios are not a throughput ranking; pass `--rounds N` when comparing different scenarios whose work units match. Notable workload conventions:

- **Batch sizes**: a round transfers 16,384 messages for MPSC, MPMC, and SPMC, publishes 4,096 values to every broadcast subscriber, or performs one send and receive in `*-roundtrip`.
- **MPSC**: scheduled task cases share a Tokio runtime and run one priming batch inside the timed interval; `-inline1k` and `-boxed1k` compare inline copying against heap indirection (including box allocation and drop).
- **Broadcast**: lossless bounded cases exclude Tokio (which overwrites at capacity); no-backpressure cases size bounded peers for the entire batch, `-shared` controls require every peer to deliver a shared `Arc` per message, and `sequential-drain-4r` vs `drain-4r` separates single-thread cursor work from parallel receiver contention on a reused channel.
- **SPMC and WaitGroup**: SPMC uses Asyncband's SPMC channel as the reference with MPMC as a peer; WaitGroup's four-observer drain adapts single-waiter `waitgroup` via `FutureExt::shared` and Tokio via a closed `TaskTracker`.

## Set and preserve a baseline

1. Pick a fixed reference revision (currently `c37b1a4` for the initial ecosystem suite).
2. Run `--baseline-only --rounds <N> --repeat 5` and choose a round count that puts the reference median near 300 ms, checking the `min..max` range for noise.
3. Commit that count in the scenario registration so all peers and later revisions run the identical work size.
4. When a workload definition or timing boundary changes, recalibrate and treat both sides under the new definition as a new baseline.

## Diagnose Asyncband primitives (`primitives` and `upgradable_stages`)

```shell
cargo x bench --bench primitives -- --list
cargo x bench --bench primitives -- mutex::lock::uncontended_reuse --repeat 10
cargo x bench --bench upgradable_stages
```

`primitives` measures Asyncband-only operation cycles for before/after comparisons: hand-polled cases use a reference-counted non-scheduling waker, and `threads=N` cases synchronize at a barrier and split a constant total round count across workers. `upgradable_stages` breaks `UpgradableRwLock` down into hand-polled acquisition and promotion stages alongside an empty timing interval (`-- --test` runs one smoke pass in CI; use `upgradable_read_then_upgrade` in `primitives` for uninstrumented whole-cycle timing).
