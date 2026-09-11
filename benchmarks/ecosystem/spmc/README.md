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

# SPMC competing queue benchmarks

These benchmarks exercise Issue [#212](https://github.com/apache/asyncband/issues/212): one producer owns a non-cloneable sender, and 1, 2, 4, or 8 receivers compete for values. The same harness compares Asyncband SPMC, Asyncband MPMC used with one producer, async-channel, and flume. Peer dependencies stay in the benchmark crate; their exact versions are recorded in `Cargo.lock`.

Every sample transfers 16,384 `usize` values; bounded queues have capacity 64. One sender moves into a producer task without cloning or a synchronization wrapper. Consumers drain freely until disconnection, with no equal-work quotas. All data operations run in spawned Tokio tasks, including on the current-thread runtime. The coordinator only releases the start barrier and joins the tasks. Each sample checks the total count and checksum.

The two runtime configurations are `WORKERS = 0` for Tokio current-thread and `WORKERS = 4` for four worker threads. Queue and runtime construction are outside the measured region; sending, receiving, terminal disconnection, and task completion are inside. Unbounded sending is synchronous for every implementation. Its producer can fill the queue before consumers run on a current-thread executor, so that configuration measures draining rather than parallel consumer contention. Use the four-worker results and the targeted-wakeup tests to evaluate the 1P/8C case.

Run the repository benchmark workflow:

```powershell
cargo x --help
cargo x bench --help
cargo x bench
```

For repeated focused measurements, use `cargo x bench --no-run`, then run the ecosystem executable printed by Cargo with `--bench --color never --sample-count 100 'spmc::'`. The default is 20 samples with one batch per sample. Run repeated comparisons serially, without concurrent builds or tests, and record the machine, OS, Rust version, commit, sample settings, and results for all consumer counts. Lower elapsed time is better. Investigate sustained gaps above 3x against a comparable peer; sustained order-of-magnitude gaps block acceptance under [#208](https://github.com/apache/asyncband/issues/208).

## Development measurements (2026-09-11)

Measured on an Intel Core i7-11700K (8 cores / 16 logical processors), 64-bit Windows 11 Pro 10.0.26200, Rust 1.96.1 (`31fca3adb`), and the default optimized bench profile, with this implementation based on `main` at `8204e14`. Peers: async-channel 2.5.0, flume 0.12.0; runtime: Tokio 1.53.1; harness: Divan 0.1.21. The full `cargo x bench` suite passed before three serial focused runs of 100 samples per case. No builds or tests ran alongside the measurements. Each value below is the median of three run medians in milliseconds per batch; `current` denotes Tokio current-thread. The ratio compares SPMC with the faster of async-channel and flume in that row.

| Queue     | Runtime | Consumers | SPMC    | MPMC    | async-channel | flume   | Peer ratio |
| --------- | ------- | --------: | ------: | ------: | ------------: | ------: | ---------: |
| bounded   | current | 1         | 0.819   | 0.875   | 1.207         | 0.652   | 1.25x      |
| bounded   | current | 2         | 0.811   | 0.866   | 1.200         | 0.645   | 1.26x      |
| bounded   | current | 4         | 0.873   | 0.931   | 1.469         | 0.689   | 1.27x      |
| bounded   | current | 8         | 0.999   | 1.041   | 1.492         | 0.770   | 1.30x      |
| bounded   | 4       | 1         | 1.054   | 1.123   | 1.480         | 0.898   | 1.17x      |
| bounded   | 4       | 2         | 3.830   | 4.002   | 2.021         | 3.193   | 1.90x      |
| bounded   | 4       | 4         | 8.008   | 7.979   | 6.074         | 4.889   | 1.64x      |
| bounded   | 4       | 8         | 11.570  | 11.400  | 10.610        | 6.926   | 1.67x      |
| unbounded | current | 1         | 0.565   | 0.613   | 1.314         | 0.521   | 1.08x      |
| unbounded | current | 2         | 0.543   | 0.588   | 1.262         | 0.495   | 1.10x      |
| unbounded | current | 4         | 0.542   | 0.588   | 1.254         | 0.498   | 1.09x      |
| unbounded | current | 8         | 0.546   | 0.596   | 1.263         | 0.506   | 1.08x      |
| unbounded | 4       | 1         | 0.567   | 0.597   | 1.298         | 0.524   | 1.08x      |
| unbounded | 4       | 2         | 3.003   | 3.250   | 1.529         | 2.542   | 1.96x      |
| unbounded | 4       | 4         | 6.751   | 6.537   | 2.543         | 3.418   | 2.65x      |
| unbounded | 4       | 8         | 10.360  | 9.824   | 2.667         | 3.816   | 3.88x      |

The largest sustained gap is unbounded 1P/8C with four workers: 10.360 ms versus async-channel at 2.667 ms (3.88x). The same harness measures the shared-core MPMC variant at 9.824 ms, only about 5% below SPMC. Both variants serialize storage and endpoint state under a mutex and use semaphore-backed receiver notifications; SPMC adds no extra queue lock or per-message allocation over MPMC. Together with the deterministic eight-waiter test proving one ordinary notification and cancellation handoff, this points to shared storage/waiter contention rather than a wake-all implementation. This is an inference from the implementation and comparison, not a lock profile. No topology reaches the 10x rejection threshold on this host, but the 3.88x gap remains a performance limitation. Reusing the core avoids a second backend while the exclusive, non-cloneable sender supplies the required static capability; these results are not a claim of general superiority over MPMC or peers.
