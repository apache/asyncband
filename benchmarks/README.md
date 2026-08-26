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

`benchmarks` measures Asyncband primitives in isolation. The `ecosystem` target compares channel operations with semantically similar Rust channels so a new implementation does not hide a large performance regression behind API differences.

## Running

Run the repository benchmark workflow, including the ecosystem target:

```shell
cargo x bench
```

For a shorter development loop, select the companion target and optionally a Divan name filter:

```shell
cargo bench -p benchmarks --bench ecosystem
cargo bench -p benchmarks --bench ecosystem -- mpsc::bounded_concurrent
```

Use a release build on an otherwise idle machine, record the CPU and operating system, and compare implementations in the same invocation. Absolute results from different machines are not directly comparable.

## Channel methodology

The current suite covers the MPSC API that Asyncband exposes today:

- bounded capacity is 64 messages;
- ready-path cases reuse an empty channel and measure one send/receive round trip;
- concurrent cases move 16,384 messages from 1, 2, 4, or 8 producer threads to one consumer;
- channel construction, thread spawning, and thread joining stay outside the timed section;
- async operations are driven by a minimal standards-based executor or a benchmark waker, so no peer gets a dedicated runtime;
- every implementation receives the same `usize` values and the consumer computes a checksum to keep the work observable.

The peer set is Asyncband from the current checkout, Tokio 1.53.1, async-channel 2.5.0, and flume 0.12.0. `Cargo.lock` records the exact resolved versions. Tokio has the same MPSC topology; async-channel and flume are MPMC implementations measured with one receiver, so their extra receiver capability is a documented semantic difference. async-channel exposes unbounded `send` as a future, so the unbounded cases use its non-waiting `try_send` path to match the other implementations' immediate sends. Benchmark-only peers are dev dependencies of the `benchmarks` package and do not become runtime dependencies of `asyncband`.

The suite is a regression signal rather than a fastest-wins contest. Investigate a sustained result above 3x the closest semantic peer. Treat an order-of-magnitude gap as blocking unless a documented semantic or resource tradeoff explains it.

## Extending the matrix

Add comparable cases when Asyncband exposes another topology; do not publish peer-only rows as an Asyncband baseline. SPMC and MPMC queue work should add competing-consumer and balanced producer/consumer cases. Broadcast work should add fanout and producer-contention cases while documenting delivery, overwrite, and lag semantics. Watch work should compare latest-state notification rather than queue throughput.
