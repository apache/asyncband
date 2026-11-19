---
name: dislopify
description: Run a read-only simplification survey of Asyncband primitives, tests, benchmarks, and supporting code, or implement findings a maintainer selected from a previous round.
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

Default to a read-only survey for simplification requests (including "look for another round") without modifying the working tree or creating report files. Inspect the latest upstream `main` commit (fetching when remote access is available) or the caller-specified target, and record its short SHA. Check recent merges, relevant open PRs, and any review decisions the user supplied before reviving a proposal. Look beyond the last round's modules when the evidence points elsewhere.

Aim to lower cognitive load and remove redundant or dead complexity, not merely to reduce line count. Start from a concrete maintenance question: what must callers remember to coordinate, which type already owns a resource, or what timing and ownership detail would be lost by merging two paths? Unify or extract code only when the paths share intent and invariants; keep superficially similar paths separate when their lifecycles or contracts differ. Trace each candidate through its real callers and an observable test before recommending a change, and omit purely cosmetic churn.

- **Ownership and concurrency lifecycles**: trace waiters, slots, permits, and notifications across polling, cancellation, handoff, and drop. Check lock-release boundaries against the `Waker Contract` in `AGENTS.md` (keep wake callbacks and replaced or cancelled waker drops outside primitive locks, while allowing normal `Waker::clone` in short critical sections).
- **Names and contracts**: read APIs and internal helpers from the caller's perspective, watching for hidden state transitions or side effects. Judge any public API or trait change against its documented contract, real implementations, and semver impact before proposing it.
- **`unsafe` code and projected guards**: inspect the commit and PR history of the exact type before simplifying. Trace lock ownership, lifetime, aliasing, and variance invariants so guardrails are not removed as apparent boilerplate.
- **Tests and benchmarks**: check what fixtures keep alive, what runs inside timed intervals, and whether each workload still exercises the path it claims to cover.

Open the report in the conversation with the inspected short SHA, scope, and selection criteria for this round. Rank the worthwhile findings. For each, give the current code and call sites, concrete maintenance cost, proposed simplification, any public API or behavior impact, and the behavior or test that distinguishes a correct change. Cite exact file paths, line ranges, symbols, and relevant PRs or commits. Surface suspected correctness bugs separately and label what has been verified. An empty or short list is a valid result. End the survey after reporting it.

## Implement selected findings

When the maintainer selects items for a PR, carry those items and their review constraints forward. Use one reviewable commit per item in one PR by default. Follow the repository's `cargo x` workflow and `AGENTS.md` guidelines (updating `CHANGELOG.md` only for user-visible changes relative to the latest release). Confirm a suspected bug with a focused regression test before presenting it as a fix, and follow the maintainer's direction on a separate PR.

Choose extra verification for the claim at hand. Use Miri for exercised ownership or aliasing paths, compile-fail tests for type constraints, and `cargo x semver-check` when public APIs change. For layout or benchmark claims, compare immediately before and after the relevant commit with the same target, toolchain, features, and workload. Record measurements, their limits, and any skipped checks in the PR. Return the PR, commit list, and validation results for review.
