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

[Benchmark In Milliseconds](https://matklad.github.io/2026/10/05/benchmark-milliseconds.html) suggests timing enough useful work to take about 300 ms. Here that target applies to a **reference revision of Asyncband**. Once a scenario has a round count, every implementation and every revision runs that exact workload. A faster peer should take less time.

`cargo x bench` runs both the `ecosystem` and `primitives` targets by default. To compile or smoke-test both:

```shell
cargo x bench --no-run
cargo x bench -- --test
```

## Compare implementations

```shell
cargo x bench --bench ecosystem -- --list
cargo x bench --bench ecosystem -- mpsc/bounded/try-roundtrip --repeat 10 --raw
cargo x bench --bench ecosystem -- tasks-1p1c-cap1024 --rounds 320 --repeat 10
```

The `ecosystem` target compares bounded and unbounded MPSC, MPMC and SPMC channels, broadcast, watch, reservation and wait groups. Select a target before filtering by scenario name: each executable checks its own matches. Filters are substrings and a scenario must match all of them. Each scenario declares its reference, adapters, topology and round count together. Each comparison fixes the payload, topology, runtime configuration and observable delivery contract; the adapters perform the equivalent operations on their library. For SPMC the reference is Asyncband's SPMC channel; Asyncband's general MPMC channel is one of the peers.

Each table reports a median and the full minimum-to-maximum range in integer milliseconds. The ratio is the implementation's median elapsed time divided by the reference's median, before rounding: `0.80x` means 20% less elapsed time for the same work. The default is five runs per implementation. The runner interleaves implementations and rotates which one runs first. It keeps the first measurement and every outlier; `--raw` includes the individual times in measurement order.

The clock starts before constructing the fixture and stops after cleanup, including joining workers. Each repetition builds fresh state. Some scenarios reuse channels or workers across rounds: the MPSC task cases also run one priming batch inside the timed interval. A task round transfers 16,384 messages for MPSC, MPMC and SPMC, or publishes 4,096 values to every participating broadcast subscriber. A round in `try-roundtrip` is one send and one receive. Scheduled MPSC comparisons use the same Tokio runtime for all adapters; single-threaded ready operations do not measure task scheduling. Lossless bounded broadcast excludes Tokio's overwrite-on-full channel; the no-backpressure comparison gives bounded peers room for the entire workload so no values are lost. `TokioArc` publishes a fresh `Arc<usize>` per message so Tokio pays for the allocation behind Asyncband's shared `Arc<T>` receive, and `sequential-drain-4r` reads four subscriptions on one thread to separate per-read work from contention between receivers.

MPSC task scenarios include both one and four producers, small and large buffers, and current-thread controls. Use the reported range to decide whether an apparent difference is reproducible. To compare *different* scenarios, set the same round count where their work units are equivalent; independently sized rows are not a throughput ranking.

## Set and preserve a baseline

1. Choose the scenario and a fixed reference revision. For the ecosystem workloads in this revision, the reference library is Asyncband at `c37b1a4`.
2. Run only the reference, for example `cargo x bench --bench ecosystem -- mpsc/bounded/try-roundtrip --baseline-only --rounds 30000000 --repeat 5`. Pick a convenient fixed count that puts the reference median in the hundreds of milliseconds, typically near 300 ms. Check the range as well: retuning a noisy scenario does not eliminate its variability.
3. Put that count in the scenario registration and run all implementations. Neither peers nor later revisions are calibrated independently. Compare the same scenario, work size, toolchain and machine; a change from 300 to 240 ms is a 20% reduction in elapsed time.
4. If the work size, an input or a timing boundary changes, rerun both sides under the new definition. Treat it as a new baseline. There is no automatic calibration or elapsed-time performance assertion in CI.

## Diagnose Asyncband primitives

```shell
cargo x bench --bench primitives -- --list
cargo x bench --bench primitives -- mutex::lock::uncontended_reuse --repeat 10
cargo x bench --bench primitives -- rwlock::upgradable --repeat 5
```

The `primitives` target measures Asyncband-only operation cycles for focused before/after comparisons, including ready paths, queued handoffs, wake registration, cancellation, fan-out, and contended tables. Hand-polled cases use a reference-counted waker and exercise bookkeeping without executor scheduling. For `threads=N` cases the workers share a fixture, start at a barrier, and divide a fixed total number of rounds, including the remainder. Smoke mode runs one outer round per worker, preserving inner message batches, topology and correctness checks without making timing claims.
