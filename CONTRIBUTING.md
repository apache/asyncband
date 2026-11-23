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

# Contributing

Thank you for contributing to Apache Asyncband (Incubating)!

Architectural guidelines live in [DESIGN.md](DESIGN.md). Workspace structure, development commands, Rust style, waker and API documentation contracts, changelog rules, and pull request conventions for all contributors (human and AI) live in [AGENTS.md](AGENTS.md). Read both before planning or submitting changes.

## Your First Contribution

1. [Fork the Asyncband repository](https://github.com/apache/asyncband/fork) in your own GitHub account (issues are not assigned or reserved, so you can pick up an open issue directly).
2. [Create a new Git branch](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-and-deleting-branches-within-your-repository).
3. Make your changes following [AGENTS.md](AGENTS.md), and run `cargo x check`, `cargo x test`, and `cargo x lint`.
4. [Submit the branch as a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork) to the upstream repository.

## Setup

We recommend [rustup](https://rustup.rs/) to install Rust. On Linux or macOS:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

On Windows, download `rustup-init.exe` from [win.rustup.rs](https://win.rustup.rs/x86_64).

The workspace declares its minimum supported Rust version (MSRV) via `rust-version` in the root `Cargo.toml`; CI verifies it explicitly. Any toolchain at or above the MSRV works for building, testing, and benchmarking.

Feature-matrix checks, linting, and documentation checks additionally require a nightly toolchain (add `miri` as well when running `cargo x miri`):

```shell
rustup toolchain install --component rustfmt,clippy nightly
```

`cargo x lint` installs extra formatting and check tools (`taplo-cli`, `typos-cli`, `hawkeye`) automatically when missing.

## Community and Code of Conduct

See the [community guide](https://asyncband.apache.org/community/) for mailing lists, consensus and voting rules, and committer and PPMC guidelines. All community participation is governed by the [Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
