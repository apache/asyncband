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

# Repository Guidelines

This document is the single source of truth for workspace structure, development commands, Rust style, waker and API documentation contracts, changelog rules, and pull request conventions for both human contributors and coding agents. Keep documented workflows synchronized with structural changes, and run the applicable `cargo x check`, `cargo x test`, and `cargo x lint` commands before submitting or handing work back.

For architectural direction and design principles, see [DESIGN.md](DESIGN.md). For first-time setup (`rustup`, MSRV, nightly toolchain) and community policies, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Workspace Layout

| Crate               | Purpose                                                                                              |
| ------------------- | ---------------------------------------------------------------------------------------------------- |
| `asyncband`         | Core library crate (`--no-default-features` by default; each primitive family is an opt-in feature). |
| `tests-integration` | End-to-end concurrency and primitive behavior integration tests (`asyncband` with all features).     |
| `benchmarks`        | Fixed-workload benchmark suite and runner; see [benchmarks/README.md](benchmarks/README.md).         |
| `examples`          | Standalone runnable end-to-end scenarios; see [examples/AGENTS.md](examples/AGENTS.md).              |
| `xtask` (`x`)       | Workspace automation entrypoint exposed via the `cargo x` alias defined in `.cargo/config.toml`.     |

## Workflows

Use `cargo x` as the single entrypoint for repository checks and automation (read `cargo x --help` and `<subcommand> --help` for options):

```shell
cargo x build              # Compile all workspace packages and targets with all features
cargo x check              # Check asyncband across --no-default-features, each feature, and all features
cargo x test               # Run workspace tests with all asyncband features enabled (--no-capture supported)
cargo x lint               # Run nightly clippy, rustfmt, taplo, typos, hawkeye, and docsrs rustdoc checks
cargo x lint --fix         # Automatically apply clippy, rustfmt, taplo, and hawkeye fixes
cargo x miri               # Run Miri memory-safety checks on unit and integration tests
cargo x bench -- --test    # Smoke-test default fixed-workload benchmarks (see benchmarks/README.md)
```

`cargo x check`, `cargo x lint`, and `cargo x miri` require a nightly toolchain; CI also verifies the workspace MSRV declared in `Cargo.toml`.

Use the shared [release skill](.agents/skills/release/SKILL.md) for release preparation, status checks, publication, and recovery. Its phase guides are the maintained release procedure; resume from the established candidate and completed steps. For an independent candidate check, use the [`release_verifier`](.codex/agents/release_verifier.toml) agent or follow the same verification guide directly.

Use the shared [license-audit skill](.agents/skills/license-audit/SKILL.md) for license audits and release licensing checks. For a delegated Codex review, use the [`license_auditor`](.codex/agents/license_auditor.toml) agent, which reads the same skill. Other agents can follow the skill directly.

Use the shared [deslopify skill](.agents/skills/deslopify/SKILL.md) to survey Asyncband for simplifications or implement findings a maintainer has selected.

## Rust Style

- **Modules and imports**: Order module headers as `//!` docs, `use` imports (`group_imports = "StdExternalCrate"`, `imports_granularity = "Item"`), out-of-line `mod` declarations, and items. Use `<module>/mod.rs` for multi-file modules, and do not split cohesive files prematurely. Group `mod` declarations with blank lines between `pub mod`, private `mod` with immediate `pub use self::...;` re-exports, and implementation-only private `mod` (`#[cfg(test)]` last), sorted alphabetically within each group; re-export subsystem interfaces from `mod.rs` and attach `#[cfg]` gates at module boundaries when possible. Import symbols at the top of the file from `crate::...` (avoiding `super::...` and private `self::...`, reserving `use super::*;` for `#[cfg(test)]` modules), preserve original names with meaningful module or crate qualification (such as `io::Error`, `pool::bounded`, `iter::once`, or `tokio::time::sleep`), and avoid `use ... as ...` aliases except `use ...::Trait as _;` (inline paths are fine for collisions such as `core::error::Error` or rare hooks such as `std::panic::catch_unwind`).
- **Visibility**: Use three visibility tiers: private by default, plain `pub` inside private ancestor module trees, and `pub(crate)` for internal fields, constructors, or helpers reachable through `pub mod` or `pub use` re-exported types. Avoid `pub(super)` and `pub(in ...)`, and never widen visibility solely for cross-module tests.
- **Types, traits, and lints**: Name items by concrete role, keep a type and its primary `impl` together (public API before private helpers), and reserve `len` and `is_empty` for collection or slice-like views. Use `new` (with `Default` when parameterless and `const fn` when trivial), `with_*` for alternative required inputs, and `*Builder` for multi-option construction. Do not prefix struct fields with `_` to silence warnings; keep descriptive names and add `#[expect(dead_code, reason = "...")]` on unread ownership or guard fields. Omit `PartialEq` and `Eq` when field equality would expose internal layout or history, avoid unnecessary generic bounds on custom `Debug` impls, reserve `#[must_use = "..."]` for lazy values or methods whose return value carries an ownership or drop obligation, and use `#[expect(..., reason = "...")]` instead of `#[allow(...)]` (retaining `#[allow(dead_code)]` only on shared helpers whose reachability varies across single-feature and all-feature builds).
- **Control flow and errors**: Keep abstractions at signatures, cohesive types, and module boundaries; inside functions, prefer linear control flow with guard returns, explicit `match` or `if let`, and explicit `for` loops for multi-step or fallible logic over premature helpers or uncontracted defensive glue. Use `Result` (keeping failing mutations atomic when practical) for recoverable errors, empty values or `Option` for absent queries, fail-fast panics for broken invariants, and structured error data rather than string parsing.

## Waker Contract

- Allow normal executor `Waker::clone` inside short state critical sections. Custom clone panic recovery and reentrancy are not general guarantees; do not require them in reviews unless an explicit local contract does.
- Reuse borrowed-waker registration and avoid redundant clones.
- Keep wake callbacks and replaced or cancelled waker destruction outside primitive locks.
- Executor waker operations, including cloning, waking, and dropping, are expected not to panic; do not add custom panic recovery.

Decision: [#257](https://github.com/apache/asyncband/pull/257).

## Documentation

Keep each Markdown prose paragraph and list item on one source line. Format Markdown tables so their columns and separators align in the source. Keep skill resource links within the skill directory; reference other skills or agents by name and resolve repository paths from the supplied repository root.

Write public API documentation from Asyncband's observable contracts. Start with a short action or query summary, then describe waiting conditions, ordering, ownership, and state changes relevant to callers. Keep implementation details out unless callers need them to use the API correctly. Use public API names as intra-doc link labels and hide private module paths in explicit link targets so internal layouts do not leak into rendered prose.

Use applicable sections in this order: `Returns`, `Errors`, `Cancellation`, `Panics`, `Deadlocks`, and `Examples`. Omit empty or redundant sections. Explain simple return values in the summary or body; reserve `Returns` for nontrivial outcomes, snapshots, or resource ownership. Document `Option::None` and boolean outcomes as return semantics, not errors. For `Result`, use `Errors` to explain failure conditions, returned ownership, and any state changes. Keep examples last and focused on useful behavior rather than mechanically demonstrating syntax, keeping `use` imports visible.

Document cancellation for public async operations, methods returning futures, and public `Future` or `IntoFuture` types. Explain when effects occur, what dropping a pending future retains or releases, and whether retrying restarts, resumes, or repeats an effect. Distinguish cancellation of active work from cancellation of an observer, and state when caller-owned inputs are dropped or external side effects are not rolled back. Do not use an unqualified cancellation-safety claim in place of these observable guarantees. Owned variants should include a `Cancellation` section and may link to shared semantics while explaining ownership differences.

Put concrete panic conditions and important unwind effects in `Panics`, and recursive acquisition or callback reentrancy hazards in `Deadlocks`. Do not enumerate arbitrary panics in caller-provided code unless their effect on the primitive matters. Apply the same conventions to public trait methods. Reserve inline `//` comments for non-obvious invariants, concurrency or `// SAFETY:` preconditions, and subtle quirks rather than restating code. Preserve existing provenance and license notices independently of documentation rewrites.

## Testing

- Exercise observable contracts through public APIs in `tests-integration`, and keep `#[cfg(test)]` unit tests next to the implementation strictly for private state machines or invariants that black-box tests cannot reach cleanly.
- Every test must guard a realistic regression without mirroring literals, trivial control flow, static type guarantees, or non-contract `Debug` wording.
- Avoid test-only production hooks, and coordinate concurrent tests deterministically instead of sleeping.

## Changelog

- Update `CHANGELOG.md` in the same pull request for significant user-visible changes by comparing the final behavior with the latest release tag rather than recording the sequence of commits that produced it.
- Before adding a bug-fix entry, verify from the latest release tag that the faulty behavior was shipped. If the affected API or behavior is itself unreleased, describe only its final contract in the relevant feature entry and omit the development-only correction.
- Include public API migrations, new capabilities, correctness or compatibility changes, and meaningful performance improvements. Exclude tests, internal refactors, documentation, CI, tooling, and dependency maintenance unless they change supported or observable behavior relative to the latest release.
- Keep the permanent `## Unreleased` section at the top. Group entries under `### Breaking changes`, `### Notable changes`, `### New features`, `### Improvements`, `### Performance improvements`, and `### Bug fixes` in that order, adding only categories that contain entries.
- Write one bullet for each coherent behavior from the user's perspective: name the affected public API or workload and its observable result, and give required migration guidance for breaking changes. Keep entries high-level and concise: omit pull request numbers, issue numbers, discarded intermediate APIs, internal data-structure or concurrency mechanics, and concrete byte sizes, slot counts, or target-specific layout numbers.
- Scope performance claims to the workload supported by evidence, distinguish broad improvements from scenario-specific benchmark results, and state material unchanged paths or memory and latency tradeoffs qualitatively rather than generalizing a microbenchmark or dumping benchmark and layout measurements.
- During release preparation, insert `## vX.Y.Z` without a date immediately below `## Unreleased` and move the accumulated entries into it. Add the actual UTC release date in `YYYY-MM-DD` format after publication; do not guess it in advance or remove `## Unreleased`.

## Pull Requests

Asyncband does not assign or reserve issues. Do not ask maintainers to assign an issue to you. Anyone can submit a pull request directly for review.

Format pull request titles according to `.github/semantic.yml` and keep the description concise. Use a `Summary` section for routine changes and add `Design Notes` only when the design needs explanation.
