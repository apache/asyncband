# Migrating from MEA

Asyncband continues the codebase formerly published as [`mea`](https://crates.io/crates/mea), but it uses a new Cargo package and Rust crate name. Existing `mea` releases remain available for builds that have not migrated, but they receive no further development.

## Recommended migration path

First, upgrade the existing dependency to `mea` 0.6.7 and resolve any changes required by earlier MEA releases. The [historical changelog](CHANGELOG-OLD.md) documents those releases.

Next, switch from `mea` 0.6.7 to `asyncband` 0.6.7 without changing the dependency's feature configuration, and replace Rust paths from `mea::` to `asyncband::`:

```toml
# Before
mea = "0.6.7"

# After
asyncband = "0.6.7"
```

Asyncband 0.6.7 is the compatibility point for the rename, so the dependency name and Rust paths are the only changes expected in this step. No compatibility package or re-export keeps the `mea` crate name available; downstream crates must update those names directly.

Once the project builds with Asyncband 0.6.7, follow the [Asyncband changelog](CHANGELOG.md) when upgrading to later releases.

For the background to the rename, see the [Asyncband proposal discussion](https://lists.apache.org/thread/f31qd3jm3odomjwy3lqkk21coyqsr9xs).
