# Migrating from MEA

Asyncband continues the codebase formerly published as [`mea`](https://crates.io/crates/mea), but it uses a new Cargo package and Rust crate name. Existing `mea` releases remain available for builds that have not migrated, but they receive no further development.

To migrate, remove the `mea` dependency, add [`asyncband`](https://crates.io/crates/asyncband), and update Rust paths from `mea::` to `asyncband::`. Asyncband enables no primitive modules by default, so list every primitive your application uses in the dependency's Cargo features.

No compatibility package or re-export is provided under the old name, so downstream crates must migrate their dependency declarations and source paths individually.

For the background to the rename, see the [Asyncband proposal discussion](https://lists.apache.org/thread/f31qd3jm3odomjwy3lqkk21coyqsr9xs).
