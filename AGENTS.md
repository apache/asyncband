# Agent Instructions

## Cargo XTask

Use `cargo x` as the source of truth for repository workflows.

- Run `cargo x --help` before choosing build, test, lint, or formatting commands.
- Run `cargo x <command> --help` for command-specific behavior.

## Code Style

- Express visibility at the module boundary. Inside a restricted module, use `pub` for its module API instead of repeating `pub(crate)`.

## Commits and Pull Requests

Follow the semantic definition at `.github/semantic.yml`.

- Keep title descriptions short.
- Simple PR descriptions should only include a `Summary` section.
- Complex PR descriptions may also include a `Design Notes` section.
