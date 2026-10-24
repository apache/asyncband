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

# Release dependency SBOM

Compose publishes `apache-asyncband-${VERSION}-incubating-src.tar.gz.cdx.json` beside the source archive. Both files have their own SHA-512 checksum and project-key signature. PR runs upload an unsigned bundle for review; RC tag runs also sign and upload it to ATR.

The CycloneDX 1.5 document describes the published `asyncband` library's Cargo normal and build dependencies with all features and targets enabled. It excludes dev-dependencies and other workspace members. This is a pre-build inventory, not a consumer-specific build or a complete inventory of the source archive. Conditional dependencies may not be used by every consumer.

`cargo x sbom` keeps cargo-cyclonedx's library dependency graph and adds the source revision, SBOM author, and suppliers. The supplier for crates.io dependencies identifies the distribution service, not the crate's author. Machine-local library identifiers are replaced with a commit-specific identity, and duplicate target components are removed. Upstream dependency identities, hashes, and declared licenses remain as generated.

Incorporated source portions are not separate Cargo dependencies and are not enumerated as SBOM components. Their provenance and license terms remain in the readable LICENSE file, linked at the candidate commit from the SBOM. There is no second attribution index, LICENSE parser, or feature metadata format to maintain. License audits still cover these portions.

## Reproduce and verify

Use Rust 1.95.0, cargo-cyclonedx 0.5.9, and CycloneDX CLI 0.33.1, as pinned in the candidate's Compose workflow. CI installs both SBOM tools with `taiki-e/install-action`. Locally, install those versions through the upstream release binaries or your package manager; cargo-cyclonedx also supports `cargo install cargo-cyclonedx --version 0.5.9 --locked`.

After verifying and extracting the archive as described in [candidate verification](verification.md), run from the candidate checkout. Set `RELEASE_COMMIT` to its verified commit and `SOURCE_ROOT` to the extracted archive root. Keep the output outside the source tree.

```shell
cargo x sbom --help
export SOURCE_DATE_EPOCH="$(git show -s --format=%ct "${RELEASE_COMMIT}")"
RUSTUP_TOOLCHAIN=1.95.0 cargo x sbom \
  --source "${SOURCE_ROOT}" --revision "${RELEASE_COMMIT}" \
  --output "${SCRATCH_DIR}/reproduced.cdx.json"
cmp "${SOURCE_ARCHIVE}.cdx.json" "${SCRATCH_DIR}/reproduced.cdx.json"
cyclonedx validate --input-version v1_5 --fail-on-errors \
  --input-file "${SCRATCH_DIR}/reproduced.cdx.json"
```

Generation requires registry access for uncached dependencies and rejects changes to the shipped Cargo.lock. Compose generates from two separate extractions, compares the JSON byte-for-byte, validates the schema, and independently compares the package set with `cargo tree --edges normal,build`. Review the actual output and run `cargo x lint` when changing this integration. Do not add an SBOM to an active vote; retain that candidate's original file set.
