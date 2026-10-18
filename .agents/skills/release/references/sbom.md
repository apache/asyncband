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

This CycloneDX 1.5 document describes the Cargo source workspace with all features and all target platforms. It includes every workspace member and resolved registry dependency, including build and dev dependencies. It is not a record of one executable's runtime dependencies. GitHub Actions, the Rust toolchain, system libraries, and the generator's own dependencies are outside this inventory.

`cargo-cyclonedx` supplies per-crate component metadata. Locked `cargo metadata` supplies the complete graph and dev-only packages omitted by that tool. Each dependency edge's Cargo kind and target expression is retained in an `apache:asyncband:cargo-dependency` component property, encoded as JSON. The `apache:asyncband:distribution` property distinguishes bundled workspace source, external registry crates, and bundled derived source. Registry checksums identify the downloaded crates; their source is not included in the ASF archive.

Workspace package identifiers are relative to the source tree and qualified by the source commit in their PURLs. For registry crates, `supplier` identifies the crates.io distribution service, not an inferred author or employer. Upstream author, repository, and license metadata are retained where supplied. Git dependencies, alternative registries, and non-workspace path packages require explicit source/supplier support before they can be released through this generator.

`xtask/sbom-sources.toml` indexes incorporated source documented in LICENSE. Generation verifies its version references, immutable source URLs, and complete set of local paths against LICENSE, and checks that those files exist. License texts and modification descriptions remain in LICENSE and still require review. The SBOM represents each set of adapted portions as a file component with occurrences and an upstream `pedigree.ancestors` entry. It does not claim that the whole upstream crate is bundled or that the upstream version describes the modified implementation.

## Reproduce and verify

Use the versions pinned by the candidate's Compose workflow: Rust 1.95.0, Python 3.12, cargo-cyclonedx 0.5.9, and cyclonedx-python-lib 11.12.0 with its JSON-validation extra. Install Python dependencies into a virtual environment outside the checkout and put its `python3` on PATH.

```shell
rustup toolchain install 1.95.0 --profile minimal --no-self-update
cargo +1.95.0 install cargo-cyclonedx --version 0.5.9 --locked
python3 -m pip install 'cyclonedx-python-lib[json-validation]==11.12.0'
cargo x sbom --help
cargo x sbom generate --help
cargo x sbom test
RUSTUP_TOOLCHAIN=1.95.0 cargo x sbom generate \
  --archive "${SOURCE_ARCHIVE}" \
  --output "${SCRATCH_DIR}/reproduced.cdx.json"
cmp "${SOURCE_ARCHIVE}.cdx.json" "${SCRATCH_DIR}/reproduced.cdx.json"
```

Run from the candidate checkout or extracted source, keeping the original archive outside it. Generation extracts a fresh temporary copy, uses the commit/time recorded by `git archive`, resolves metadata with `--locked`, and checks that cargo-cyclonedx has not changed Cargo.lock. The source index and package metadata come from that extracted archive. Generation requires registry access for uncached Cargo metadata; after that resolution, cargo-cyclonedx runs offline.

The output removes machine-local package identifiers and random document IDs, sorts components and graph edges, and uses the source commit time. It validates against the CycloneDX schema and checks graph closure before writing. Compose compares two separate extractions; a voter must also reproduce the signed SBOM independently, along with the source archive, as described in [candidate verification](verification.md).

When changing the generator, run `cargo x sbom test`, `cargo x lint`, and Compose on the PR. Inspect the uploaded SBOM's subjects, pedigree, license metadata, dependency kinds, and suppliers. Never retrofit a sidecar into an active vote; preserve the voted manifest and use the updated workflow for a future candidate.
