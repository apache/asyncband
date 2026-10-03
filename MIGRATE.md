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
