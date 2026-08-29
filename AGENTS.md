# Repository Guidelines

## Workflows

Use `cargo x` as the source of truth for repository workflows. Read `cargo x --help` and the relevant subcommand's `--help` before running build, test, lint, or formatting commands.

## Rust Style

Declare restricted visibility at the module boundary and use `pub` for items in that module's API.

## Documentation

Keep each Markdown prose paragraph and list item on one source line.

## Pull Requests

Format pull request titles according to `.github/semantic.yml` and keep the description concise. Use a `Summary` section for routine changes and add `Design Notes` only when the design needs explanation.
