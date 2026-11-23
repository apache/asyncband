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

# Architectural Design Guidelines

This document sets the top-down architectural direction and decision criteria for Apache Asyncband (Incubating). Concrete data layouts and state transitions belong in the code; workspace workflows and coding conventions live in [AGENTS.md](AGENTS.md), and security boundaries live in [SECURITY-THREAT-MODEL.md](SECURITY-THREAT-MODEL.md).

## Scope and Composition

- Build exclusively on standard Rust `Future` and `Waker` contracts without binding to any concrete I/O reactor, timer driver, or executor.
- Treat any concurrency building block that stays independent of I/O reactors and timers as in scope, from locks, channels, initialization, and pools to direct multi-future coordination such as `join` or `select`; keep reactor- and clock-bound facilities (such as I/O drivers, sleeps, timeouts, and rate limiters) outside this crate.
- Prefer direct `async`/`await` control flow over callback chains and combinator pipelines (`map`, `and_then`, or `Stream`-style adapter nesting): add `Future`-level operations only when they express genuine concurrency rather than sequential wrapper layers that obscure control flow and inflate state or allocation cost.

## Async First, Modular Surface

- Shape public APIs around `async` operations, RAII guards and permits, and explicit `try_*` non-waiting operations rather than synchronous ports or exposed `poll_*` plumbing.
- Adapt synchronous callers at the boundary through the optional `blocking` module instead of duplicating blocking methods across primitive types.
- Gate every primitive family behind an opt-in Cargo feature with `default = []` and minimal dependencies so downstream builds pay only for what they enable.

## Cancellation and Failure Semantics

- Treat future cancellation (dropping a future at any suspension point) as ordinary control flow: waiting, reservation, and handoff protocols must conserve capacity, permits, documented ordering, and wakeups across cancellation.
- Uphold memory safety even when safe callers leak guards, permits, or futures with `mem::forget`.
- Do not poison locks or coordination primitives on panic by default, because both panics and future cancellations can interrupt multi-step caller work; reserve poisoning for APIs that consume a single-use initializer that cannot be retried.

## State Machines, Contention, and `unsafe` Restraint

- Separate uncontended transitions from contended wait queues: use direct atomic fast paths when they remove real synchronization work across workloads, avoiding busy-spin loops and arbitrary workload-tuned thresholds.
- Prefer safe, index-addressed, `Unpin`-friendly wait structures on contended paths so futures remain movable and cancellation stays local and predictable.
- Require every `unsafe` block to justify the invariant or ownership guarantee that safe Rust cannot express cleanly, keeping aliasing, lifetime, and variance boundaries small and explicit.

## Lock and Callback Boundaries

- Hold internal locks only across short, deterministic state transitions, and bound any cleanup work performed on hot operations so latency stays predictable without stop-the-world spikes.
- Always wake tasks and drop replaced or cancelled wakers outside internal locks; normal executor `Waker::clone` is permitted inside short critical sections (see the Waker Contract in [AGENTS.md](AGENTS.md)).
- Run caller closures, trait methods, and payload destructors outside internal locks by default; when an operation must inspect or clone state under an internal lock, keep the critical section minimal and document the non-reentrancy constraint under `# Deadlocks`.

## Cohesion Over Patchwork

- Extract shared internal components only when multiple primitives share the same state machine or ownership invariant, not merely superficial code similarity.
- Prefer tightening a state representation or ownership boundary so whole classes of glue states, redundant flags, and special cases disappear rather than adding defensive layers around them.
