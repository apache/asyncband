# Repository Guidelines

## Workflows

Use `cargo x` as the source of truth for repository workflows. Read `cargo x --help` and the relevant subcommand's `--help` before running build, test, lint, or formatting commands.

## Rust Style

Declare restricted visibility at the module boundary and use `pub` for items in that module's API.

## Waker Contract

- Allow normal executor `Waker::clone` inside short state critical sections. Custom clone panic recovery and reentrancy are not general guarantees; do not require them in reviews unless an explicit local contract does.
- Reuse borrowed-waker registration and avoid redundant clones.
- Keep wake callbacks and replaced or cancelled waker destruction outside primitive locks. If a batch wake panics, attempt the remaining wakes and propagate the first panic.

Decision: [#257](https://github.com/apache/asyncband/pull/257).

## Documentation

Keep each Markdown prose paragraph and list item on one source line.

## Changelog

- Update `CHANGELOG.md` for significant user-visible changes by comparing the final behavior with the latest release tag, not by recording the sequence of commits in the current development cycle.
- Before adding a bug-fix entry, verify from the latest release tag that the faulty behavior was shipped. If the affected API or behavior is itself unreleased, describe only its final contract in the relevant feature entry and omit the development-only correction.
- Include public API migrations, new capabilities, correctness or compatibility changes, and meaningful performance improvements. Exclude tests, internal refactors, documentation, CI, tooling, dependency maintenance, discarded intermediate APIs, and implementation history unless they change supported or observable behavior relative to the latest release.
- Write each entry from the user's perspective as one coherent observable change, including required migration guidance for breaking changes.

## Pull Requests

Format pull request titles according to `.github/semantic.yml` and keep the description concise. Use a `Summary` section for routine changes and add `Design Notes` only when the design needs explanation.
