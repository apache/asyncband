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

# Source-release SBOM

Compose produces `apache-asyncband-${VERSION}-incubating-src.tar.gz.cdx.json` alongside the source archive. Its subject records the archive's SHA-512 and Git commit. The SBOM has its own checksum and automated project-key signature, and joins the candidate before voting. It is kept outside the archive to avoid a circular hash dependency. PR runs upload the unsigned archive, SBOM, and checksums as a GitHub Actions artifact for review; signing and ATR upload remain gated to RC tag runs.

## Scope and identity

This CycloneDX 1.5 document covers the published `asyncband` library's normal and build dependencies across all features and target platforms, plus the incorporated source recorded in LICENSE throughout the archive, including tests. It is a pre-build source inventory: conditional dependencies do not imply that every consumer uses them. External dependencies used only by tests, examples, benchmarks, or xtask are outside this scope, as are GitHub Actions, the Rust toolchain, and system libraries.

The Rust xtask consumes only the library BOM emitted by cargo-cyclonedx; it does not merge the other workspace members' BOMs or add dev-dependencies. Cargo metadata supplies the kind and target condition of edges already present in that graph, retained in `apache:asyncband:cargo-dependency` properties. The `apache:asyncband:distribution` property distinguishes bundled library source, external registry crates, and incorporated source. Registry checksums identify downloaded crates whose source is not included in the ASF archive.

The library's PURL identifies its source commit and relative path. For registry crates, `supplier` identifies the crates.io distribution service, not an inferred author or employer. Upstream author, repository, and license metadata are retained where supplied. Dependencies from other sources require a supplier review before generation supports them.

LICENSE is the only maintained source for incorporated-source metadata. Each TOML record between `BEGIN SOURCE` and `END SOURCE` binds `name`, `version`, `license`, exact `upstream` URLs, and a `files` map from repository-relative local paths to upstream paths. The record sits beside its modification description and applicable license text; update it there when changing incorporated portions. Generation rejects malformed records and missing local files, and emits each record's occurrences and upstream pedigree together. It does not infer provenance, prove the declared license, or detect undocumented copying; those remain part of the license audit. The file components describe adapted portions, not complete upstream crates.

## Reproduce and verify

Use the versions pinned by the candidate's Compose workflow: Rust 1.95.0, cargo-cyclonedx 0.5.9, and the native CycloneDX CLI 0.33.1 for schema validation. Install the CLI for your platform from its upstream release and verify its published checksum. Generation does not require Python.

```shell
rustup toolchain install 1.95.0 --profile minimal --no-self-update
cargo +1.95.0 install cargo-cyclonedx --version 0.5.9 --locked
cargo x sbom --help
RUSTUP_TOOLCHAIN=1.95.0 cargo x sbom \
  --archive "${SOURCE_ARCHIVE}" \
  --output "${SCRATCH_DIR}/reproduced.cdx.json"
cmp "${SOURCE_ARCHIVE}.cdx.json" "${SCRATCH_DIR}/reproduced.cdx.json"
cyclonedx validate --input-version v1_5 --fail-on-errors \
  --input-file "${SCRATCH_DIR}/reproduced.cdx.json"
```

Run from the candidate checkout or extracted source, keeping the original archive outside it. Generation extracts a fresh temporary copy, uses the commit/time recorded by `git archive`, resolves metadata with `--locked`, and checks that cargo-cyclonedx has not changed Cargo.lock. LICENSE and package metadata come from that extracted archive. Generation requires registry access for uncached Cargo metadata; after that resolution, cargo-cyclonedx runs offline.

The output removes machine-local package identifiers, sorts components and graph edges, and uses the source commit time. Generation checks graph closure. Compose validates the schema, compares two separate extractions, independently compares the package set with `cargo tree --edges normal,build`, and checks the subject hash against the actual archive checksum. These checks exercise the real release output. A voter must also reproduce the signed SBOM independently, along with the source archive, as described in [candidate verification](verification.md).

When changing the generator, run `cargo x lint`, `cargo x test`, and Compose on the PR. Inspect the uploaded SBOM's subject, pedigree, license metadata, dependency kinds, and suppliers. Review changes to LICENSE records and their surrounding text together. Never retrofit a sidecar into an active vote; preserve the voted manifest and use the updated workflow for a future candidate.
