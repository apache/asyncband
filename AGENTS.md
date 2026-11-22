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

For first-time setup (`rustup`, MSRV, nightly toolchain) and community policies, see [CONTRIBUTING.md](CONTRIBUTING.md).

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

Use the shared [dislopify skill](.agents/skills/dislopify/SKILL.md) to survey Asyncband for simplifications or implement findings a maintainer has selected.

## Rust Style

Declare restricted visibility at the module boundary and use `pub` for items in that module's API.

Use imports for referenced symbols and preserve their original names. Keep enough module or crate qualification to explain the operation, identify its source or execution context, and distinguish same-named symbols, such as `io::Error`, `pool::bounded`, `iter::once`, or `tokio::time::sleep`. Meaningful qualification is useful even without a name conflict. Avoid `use ... as ...` unless there is a compelling reason; aliases make symbols harder to discover.

Start ordinary intra-crate imports at `crate`; avoid explicit `super::...` paths. Reserve `use super::*` for test modules.

Keep `pub use self::...` when re-exporting a module's own children as its public API.

## Waker Contract

- Allow normal executor `Waker::clone` inside short state critical sections. Custom clone panic recovery and reentrancy are not general guarantees; do not require them in reviews unless an explicit local contract does.
- Reuse borrowed-waker registration and avoid redundant clones.
- Keep wake callbacks and replaced or cancelled waker destruction outside primitive locks.
- Executor waker operations, including cloning, waking, and dropping, are expected not to panic; do not add custom panic recovery.

Decision: [#257](https://github.com/apache/asyncband/pull/257).

## Documentation

Keep each Markdown prose paragraph and list item on one source line. Format Markdown tables so their columns and separators align in the source. Keep skill resource links within the skill directory; reference other skills or agents by name and resolve repository paths from the supplied repository root.

Write public API documentation from Asyncband's observable contracts. Start with a short action or query summary, then describe waiting conditions, ordering, ownership, and state changes relevant to callers. Keep implementation details out unless callers need them to use the API correctly.

Use applicable sections in this order: `Returns`, `Errors`, `Cancellation`, `Panics`, `Deadlocks`, and `Examples`. Omit empty or redundant sections. Explain simple return values in the summary or body; reserve `Returns` for nontrivial outcomes, snapshots, or resource ownership. Document `Option::None` and boolean outcomes as return semantics, not errors. For `Result`, use `Errors` to explain failure conditions, returned ownership, and any state changes. Keep examples last and focused on useful behavior rather than mechanically demonstrating syntax.

Document cancellation for public async operations, methods returning futures, and public `Future` or `IntoFuture` types. Explain when effects occur, what dropping a pending future retains or releases, and whether retrying restarts, resumes, or repeats an effect. Distinguish cancellation of active work from cancellation of an observer, and state when caller-owned inputs are dropped or external side effects are not rolled back. Do not use an unqualified cancellation-safety claim in place of these observable guarantees. Owned variants should include a `Cancellation` section and may link to shared semantics while explaining ownership differences.

Put concrete panic conditions and important unwind effects in `Panics`, and recursive acquisition or callback reentrancy hazards in `Deadlocks`. Do not enumerate arbitrary panics in caller-provided code unless their effect on the primitive matters. Apply the same conventions to public trait methods. Preserve existing provenance and license notices independently of documentation rewrites.

## Changelog

- Update `CHANGELOG.md` in the same pull request for significant user-visible changes by comparing the final behavior with the latest release tag rather than recording the sequence of commits that produced it.
- Before adding a bug-fix entry, verify from the latest release tag that the faulty behavior was shipped. If the affected API or behavior is itself unreleased, describe only its final contract in the relevant feature entry and omit the development-only correction.
- Include public API migrations, new capabilities, correctness or compatibility changes, and meaningful performance improvements. Exclude tests, internal refactors, documentation, CI, tooling, and dependency maintenance unless they change supported or observable behavior relative to the latest release.
- Keep the permanent `## Unreleased` section at the top. Group entries under `### Breaking changes`, `### Notable changes`, `### New features`, `### Improvements`, `### Performance improvements`, and `### Bug fixes` in that order, adding only categories that contain entries.
- Write one bullet for each coherent behavior from the user's perspective: name the affected public API or workload and its observable result, and give required migration guidance for breaking changes. Omit pull request numbers, issue numbers, discarded intermediate APIs, and implementation mechanics unless callers need them to migrate, understand compatibility, or assess risk.
- Scope performance claims to the workload supported by evidence, distinguish broad improvements from scenario-specific benchmark results, and note unchanged paths or tradeoffs when material rather than generalizing a microbenchmark into a library-wide claim.
- During release preparation, insert `## vX.Y.Z` without a date immediately below `## Unreleased` and move the accumulated entries into it. Add the actual UTC release date in `YYYY-MM-DD` format after publication; do not guess it in advance or remove `## Unreleased`.

## Pull Requests

Asyncband does not assign or reserve issues. Do not ask maintainers to assign an issue to you. Anyone can submit a pull request directly for review.

Format pull request titles according to `.github/semantic.yml` and keep the description concise. Use a `Summary` section for routine changes and add `Design Notes` only when the design needs explanation.
