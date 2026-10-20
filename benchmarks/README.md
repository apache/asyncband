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

These benchmarks follow the approach in [Benchmark In Milliseconds](https://matklad.github.io/2026/10/05/benchmark-milliseconds.html): run a fixed amount of useful work, aim for roughly 300 ms, and repeat it to develop intuition about a change. Work sizes are checked into the source. The runner never adjusts them in response to measured performance.

## Running

Use `cargo x bench` to build and run the release-profile benchmark executables. Select a target and filter cases for a short feedback loop:

```shell
# Discover the commands and available workloads.
cargo x bench --help
cargo x bench --bench primitives -- --help
cargo x bench -- --list

# Repeat one primitive workload and inspect every elapsed time.
cargo x bench --bench primitives -- mutex::lock::uncontended_reuse --repeat 10

# Compare all implementations of the same channel workload.
cargo x bench --bench ecosystem -- mpsc::bounded::try_send_receive --repeat 10

# Compile only, or exercise every case once per worker without timing.
cargo x bench --no-run
cargo x bench -- --test
```

Filters are case-sensitive substrings; multiple filters must all match. Use `--bench primitives` or `--bench ecosystem` with filters, since each executable rejects an empty selection. `--list` shows the resolved round counts without constructing fixtures or starting workers.

Every measured row prints the integer elapsed milliseconds, total rounds, case name, and repetition number. The default is one measurement per case; `--repeat 10` prints ten independent runs, including the first. There is no hidden warm-up run, averaging, outlier removal, or conversion to a per-operation time.

## Work and timing boundaries

`primitives` exercises operation cycles with controlled polling and reference-counted wakers. It covers ready paths, registration, handoff, cancellation, fan-in and fan-out; selected cases also exercise shared state from multiple OS threads. Hand-polled cases include waker bookkeeping but do not model executor scheduling.

`ecosystem` compares equivalent workloads across Asyncband and other libraries. It includes hand-polled paths, message batches on threads or Tokio tasks, bounded backpressure, retained backlogs, payload sizes, and notification fan-out. Existing semantic differences remain explicit: for example, Tokio broadcast does not participate in lossless bounded backpressure cases.

A round is one invocation of the case's operation closure. It may transfer one message, complete a group of waiters, or run a batch of messages. Channel batches use the message counts in their support modules. The `threads=N` variants divide the total rounds among N workers and start them at a barrier; they share the same fixture and do the same total work at every thread count. Other concurrency settings are part of the case name and describe the workers inside each round.

The clock surrounds the entire workload function, from fixture construction to cleanup. Outputs are consumed and dropped each round. Reusable fixtures survive across rounds, while cases that need fresh state construct and dispose of it inside the loop. Channel workloads that prime reusable workers or storage do so once inside this measured interval. Setup, teardown, allocation, key generation, coordination, and correctness checks are not separately subtracted. This makes the result a workload cost; it does not isolate the nanosecond cost of the operation in the function name.

Each repetition recreates the workload's fixture. Repeated rounds must return it to a usable state: drain messages, release permits, finish workers, and keep retained memory bounded. The smoke mode keeps each case's topology and inner message batch intact while reducing its outer loop to one round per worker.

## Sizing and comparing

Choose round counts that put representative cases in the hundreds of milliseconds on a development machine. Treat 300 ms as a sizing guide, not a deadline or an assertion: implementations, contention, hardware, and schedulers differ. A result below 10 ms needs more work before small differences are useful; a result above a second usually needs fewer rounds for convenient iteration.

Use `--rounds N` to try a different fixed count on a filtered workload, or `--scale N` to multiply the checked-in counts. These options do not change message capacities, payloads, or topology. Copy a useful count into the case's registration when changing the workload definition. Keep it identical for all implementations of the same case and parameter set; never calibrate each implementation or revision independently to the same elapsed time.

Compare the same workload definitions, parameters, round counts, toolchain, and machine. Run without competing builds or benchmarks, repeat a narrow selection, and inspect the spread before attributing a difference to the code. Changing a workload or its timing boundary establishes a new baseline; these results are not directly comparable with the former Divan per-operation measurements. The full matrix is useful for coverage, while a filtered set is the normal optimization workflow.
