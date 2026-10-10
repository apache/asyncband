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

# ManualResetEvent atomic-read experiment

This is the first experiment from [#252](https://github.com/apache/asyncband/issues/252), using the final manual-reset design in the closed, unmerged [#315](https://github.com/apache/asyncband/pull/315) as a reference. It retains `WaitList` and optimizes state observations; waiter-storage replacement remains a separate experiment.

## Design and correctness

`is_set` and `try_wait` use an Acquire load. An unregistered wait can complete from the same load when the event is set. `set` publishes true with Release; `set`, `reset`, and registration still hold the same waiter mutex. Reset clears the flag with Relaxed ordering because it does not publish data to successful waits. The locked recheck after a false fast probe prevents registering behind a set that has already finished detaching its cohort.

Registered waits still own their nodes. `set` marks and unlinks the entire cohort before releasing the lock and waking tasks. Polling an old node observes its committed state even after reset; cancellation removes that node without affecting another wait. A wait created but not registered before a set/reset pulse is still pending in the next period.

For the issue's callback requirement, initial waker cloning happens after the false probe but before locking. A repoll with an unchanged waker needs no clone. Replacing a waker checks its identity, unlocks to clone, and then rechecks commitment under the lock. Both the replaced waker and an unused prepared clone are dropped outside the lock. This adds a second lock acquisition only when preparing a changed waker; there is no panic-recovery machinery.

Focused schedules exercise set and set/reset during initial waker preparation, set/reset during replacement, cancellation before and after commitment, and callbacks that reset and register a new wait. The destruction callback test calls `reset`, which still locks; calling the now-lock-free `is_set` would no longer detect destruction under a lock. A repeated cross-thread publication test covers `wait`, `try_wait`, and `is_set` without using its round barrier to publish the payload.

## Measurements

Baseline: `44fc2d8` with only this change's benchmark additions copied into it. Candidate: the implementation accompanying this report. Both use identical benchmark sources, dependencies, compiler, and release settings, with separate build directories. The latest published tag, `v0.7.3` (`46972ddd371370d831c18301940d16848511816f`), also uses the mutex-protected boolean and `WaitList`.

Measured on an Intel Core i5-1135G7 (4 cores, 8 hardware threads), Linux x86_64, rustc 1.98.0 (`88d9e12ae`), Divan 0.1.21. Values below are medians of three run medians, alternating baseline/candidate order. CPU frequency was not fixed and these are local microbenchmarks, not application throughput measurements. The benchmark waker performs reference-counted task-waker operations but does not schedule an executor task.

| Operation                     | Baseline ns | Candidate ns | Baseline / candidate |
| ----------------------------- | ----------- | ------------ | -------------------- |
| is_set (unset, 1 thread)      | 11.71       | 1.04         | 11.27x               |
| is_set (set, 1 thread)        | 13.09       | 1.04         | 12.54x               |
| is_set (set, 4 threads)       | 221.20      | 2.40         | 91.98x               |
| is_set (set, 8 threads)       | 535.00      | 2.44         | 218.90x              |
| Already-set wait (1 thread)   | 15.03       | 5.83         | 2.58x                |
| Already-set wait (4 threads)  | 240.00      | 9.51         | 25.23x               |
| Already-set wait (8 threads)  | 715.20      | 13.23        | 54.06x               |
| Register/cancel, fresh event  | 93.36       | 92.43        | 1.01x                |
| Register/cancel, reused event | 45.01       | 44.40        | 1.01x                |
| Repoll with same waker        | 16.25       | 16.08        | 1.01x                |
| Repoll with changed waker     | 34.73       | 44.56        | 0.78x                |
| Register/set/complete/reset   | 84.41       | 76.49        | 1.10x                |
| Set/reset, no waiters         | 29.52       | 29.01        | 1.02x                |
| Full fan-out, 1 waiter        | 158.50      | 133.90       | 1.18x                |
| Full fan-out, 16 waiters      | 1301.00     | 1135.00      | 1.15x                |

Values above 1 in the last column favor the candidate. Single-thread reads improve about 12.5x and already-set waits about 2.6x. Concurrent readers avoid mutex contention. Replacing a pending waker is about 28% slower in these runs, consistent with the extra lock/recheck needed to clone outside the critical section. Unchanged-waker repolls and reused registration/cancellation are essentially unchanged. The smaller lifecycle and full fan-out differences are noisy and are not a claimed improvement.

A separate fan-out pass measures only set/detach/wake, excluding registration and future destruction. Using 200 samples of 64 iterations avoids the low sample count caused by setup costs under the general run's time limit. The median of run medians was 25.36 → 26.20 ns for one waiter, 154.70 → 153.10 ns for 16, and 2639 → 2561 ns for 256. The three candidate medians for 256 ranged from 2548 to 3122 ns, overlapping the baseline's 2611–2662 ns; this does not establish a fan-out improvement.

On this target, `size_of` measurements are unchanged: the event is 72 bytes, the borrowed wait future 32 bytes, and the owned wait future 56 bytes. The waiter node representation is unchanged. The atomic signal occupies space previously used by the locked flag and padding; this experiment does not claim a waiter-memory reduction.

## Reproduction

Use a separate checkout of `44fc2d8` with the candidate's `benchmarks/benches/primitives/event/wait.rs` copied into it, leaving its library implementation untouched. Build both checkouts before timing and keep their Cargo target directories separate. Run the following in each checkout, alternating their order for three trials:

```sh
cargo x --help
cargo x bench --help
cargo x bench --bench primitives --no-run
cargo x bench --bench primitives -- event::wait --sample-count 200 --sample-size 4096 --min-time 0.05 --max-time 0.15 --color never
cargo x bench --bench primitives -- event::wait::set_registered --sample-count 200 --sample-size 64 --color never
```

`cancel_pending` includes event allocation; `cancel_pending_reused` retains the event and reuses arena capacity. `waiter_fan_out` includes allocation, registration, set, and completion; `set_registered` isolates notification. The replacement benchmark alternates two distinct reference-counted wakers so that it cannot measure the unchanged-waker path by accident.

## Waiter-storage decision

The current shared type is `WakerSet`, whose one-word `WakerToken` is invalid after draining and has no epoch. It cannot recognize a committed wait after reset. Directly replacing `WaitList` would allow an old token to address a reused slot or would lose the old wait's commitment. Keeping `WaitList` avoids adding a generation protocol to this experiment.

A future storage prototype could pair a token with an event-owned generation identity. An integer generation needs an explicit overflow/aliasing policy for indefinitely suspended futures; simply wrapping or panicking does not preserve unlimited reuse. An `Arc` identity can stay unique while old waits retain it, at the cost of allocation and reference counting. Either alternative needs its own future-size, queued-memory, cancellation, and fan-out measurements. No storage alternative was benchmarked here, so this report makes no performance claim about replacing `WaitList`.

The measured state-read benefit is sufficient to retain the small atomic-read experiment for readiness-heavy use. Applications that frequently migrate pending waits between different wakers face the measured replacement cost. Lock-free state transitions and a new storage generation protocol are not necessary to obtain the read-path benefit.
