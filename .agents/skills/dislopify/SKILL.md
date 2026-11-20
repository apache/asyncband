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

Default to a read-only survey for simplification requests (including "look for another round") without modifying the working tree or creating report files. Inspect the latest upstream `main` commit (fetching when remote access is available) or the caller-specified target, reading the target tree via git without switching branches when it differs from the current checkout, and record the exact inspected short SHA. Check recent merges, relevant open PRs, and any review decisions the user supplied before reviving a proposal. Look beyond the last round's modules when the evidence points elsewhere.

Step back from the local history that produced the code and read the codebase as a whole. Incremental changes often leave behind patchwork states, redundant glue, or shortsighted abstractions that only look awkward from a global view. Aim for a cleaner mental model and less accidental complexity: both targeted cleanups and deeper redesigns are in scope when they remove real friction or reveal a stronger underlying invariant. Unify code around shared intent rather than superficial likeness, trace each candidate through real callers and observable tests, and skip purely cosmetic churn.

Useful starting lenses (not an exhaustive checklist):

- Look for cleaner state representations, ownership models, or cross-module abstractions that dissolve whole classes of glue code or edge cases, including design slop at abstraction boundaries or public APIs when the contract itself is the root cause.
- Trace ownership of waiters, slots, permits, and notifications across polling, cancellation, handoff, and drop, and check lock-release boundaries against the `Waker Contract` in `AGENTS.md`.
- Read APIs and internal helpers from the caller's perspective, watching for hidden state transitions or side effects. Evaluate any public API or trait change against its documented contract, real implementations, and semver impact.
- For `unsafe` code and projected guards, inspect the commit and PR history of the exact type before simplifying, tracing lock ownership, lifetime, aliasing, and variance invariants so guardrails are not removed as boilerplate.
- In tests and benchmarks, check what fixtures keep alive, what runs inside timed intervals, and whether each workload still exercises the path it claims to cover.

Open the report in the conversation with the inspected short SHA, scope, and any round-specific focus or constraints. Rank the worthwhile findings, separating straightforward internal simplifications from broader design or public API proposals. For each, give the current code and call sites, concrete maintenance cost, proposed simplification or redesign, any public API or behavior impact, and the behavior or test that distinguishes a correct change. Cite exact file paths, line ranges, symbols, and relevant PRs or commits. Surface suspected correctness bugs separately and label what has been verified. An empty or short list is a valid result. End the survey after reporting it.

## Implement selected findings

When the maintainer selects items for a PR, carry those items and their review constraints forward. Use one reviewable commit per item in one PR by default. Follow the repository's `cargo x` workflow and `AGENTS.md` guidelines (updating `CHANGELOG.md` only for user-visible changes relative to the latest release). Confirm a suspected bug with a focused regression test before presenting it as a fix, and follow the maintainer's direction on a separate PR.

Choose extra verification for the claim at hand. Use Miri for exercised ownership or aliasing paths, compile-fail tests for type constraints, and `cargo x semver-check` when public APIs change. For layout or benchmark claims, compare immediately before and after the relevant commit with the same target, toolchain, features, and workload. Record measurements, their limits, and any skipped checks in the PR. Return the PR, commit list, and validation results for review.
