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

This document states the high-level architectural direction for Apache Asyncband (Incubating). Concrete layouts, naming, and state transitions belong in the code; workspace conventions live in [AGENTS.md](AGENTS.md), and security boundaries live in [SECURITY-THREAT-MODEL.md](SECURITY-THREAT-MODEL.md).

## Scope and Composition

- Decouple concurrency building blocks from concrete executors, I/O reactors, and OS timer drivers by building on standard Rust `Future` and `Waker` contracts.
- Treat native `async`/`await` as the primary model for sequential control flow in Rust: introduce abstractions where execution genuinely coordinates or races concurrently, avoiding callback chains and nested combinator layers that obscure control flow and inflate state or allocation cost.

## Modular, Async-First Surface

- Center public APIs on idiomatic `async` operations, RAII resource ownership, and explicit non-waiting operations rather than synchronous ports or low-level polling plumbing.
- Isolate synchronous adaptation at the boundary instead of duplicating blocking methods across primitives.
- Keep capabilities modular and opt-in with minimal dependencies so downstream builds pay only for what they enable.

## Cancellation and Failure Semantics

- Treat future cancellation at any suspension point as ordinary control flow: dropping an in-flight future must preserve resource accounting, ordering guarantees, and liveness.
- Maintain memory safety even when safe callers leak values without running their destructors.
- Do not poison shared state on panic by default, since both panics and future cancellations can interrupt multi-step caller work; reserve poisoning for single-use initialization that consumes an unrepeatable closure.

## State Machines, Contention, and `unsafe` Restraint

- Separate uncontended fast paths from contended wait queues, avoiding busy-spin loops and workload-tuned magic constants.
- Keep futures `Unpin`-friendly and wait structures safe by default so waiting state remains movable and cancellation stays local.
- Use `unsafe` only where safe Rust cannot express a necessary ownership or performance invariant, keeping aliasing, lifetime, and variance boundaries small and explicit.

## Lock and Callback Boundaries

- Hold internal locks only across short, deterministic state transitions, keeping hot-path work bounded and predictable.
- Wake tasks and drop replaced or cancelled wakers outside internal locks; cloning an active waker inside a short critical section is acceptable (see [AGENTS.md](AGENTS.md)).
- Run user code and value destructors outside internal locks by default; when an API intentionally invokes caller logic under a lock, keep the critical section minimal and document the reentrancy constraint.

## Cohesion Over Patchwork

- Share internal abstractions around common invariants and state machines rather than superficial code similarity.
- Prefer cleaner state representations and ownership boundaries that eliminate edge cases at the root over defensive glue layers.
