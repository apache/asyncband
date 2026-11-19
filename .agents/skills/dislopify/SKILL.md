---
name: dislopify
description: Survey Apache Asyncband for simplifications in Rust primitives, tests, and benchmarks, or implement findings the maintainer selected from a previous round.
---

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

# Dislopify

## Survey

Treat “look for another round” as a read-only survey. Fetch and identify the current upstream main commit, inspect that tree, and record its short SHA. Check recent merges, relevant open PRs, and any review decisions the user supplied before reviving a proposal. Look beyond the last round's modules when the evidence points elsewhere.

Start from a maintenance question. What must two callers remember to do together? Which type already owns this resource? What timing or ownership detail would be lost by inlining a helper or merging two similar paths? Trace the operation through its real callers and an observable test before recommending a change.

- Follow who owns a waiter, slot, permit, or notification through polling, cancellation, handoff, and drop. Unlinking a waiter can leave a live slot owned by its future. Check that wake callbacks and the destruction of replaced or cancelled wakers happen after the primitive lock is released.
- Read names from the caller's perspective. Look for hidden effects such as consuming a signal or waking waiters on the final arrival. Judge a public trait method against its documented contract and real implementations before proposing a rename.
- For unsafe or projected guards, inspect the history of the exact type. Trace who holds the lock, which reference produced the projection, and which type enforces its lifetime and variance.
- In tests and benchmarks, check what the fixture keeps alive, what runs inside the timed interval, and whether the workload still exercises the advertised path.

Open the report with the criteria used in this round. Rank the worthwhile findings. For each, give the current code and call sites, a concrete maintenance cost, a proposed simplification, and the behavior or test that distinguishes a correct change. Link to the evidence. Surface suspected correctness bugs separately and label what has been verified. An empty or short list is a valid result. End the survey after reporting it.

## Implement selected findings

When the maintainer selects items for a PR, carry those items and their review constraints forward. Use one reviewable commit per item in one PR by default. Follow the repository's `cargo x` workflow. Confirm a suspected bug with a focused regression test before presenting it as a fix, and follow the maintainer's direction on a separate PR.

Choose extra verification for the claim at hand. Use Miri for the exercised ownership or aliasing paths and compile-fail tests for type constraints. For layout or benchmark claims, compare immediately before and after the relevant commit with the same target, toolchain, features, and workload. Record the measurements and their limits in the PR. Return the PR, commit list, and validation results for review.
