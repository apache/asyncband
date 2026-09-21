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

# Verify an existing release candidate

Verify the supplied candidate and return results to the release manager. Preserve the original artifacts and use a scratch directory outside the checkout for extraction and builds.

## Inputs and scope

Use the supplied repository root, `VERSION`, `RC_TAG`, `RELEASE_COMMIT`, ATR URL and revision, compose run and attempt, archive/signature/checksum paths, expected signing fingerprints and their provenance, and requested verification scope.

Obtain signing fingerprints from trusted project key records and import verification keys into a temporary GPG home. Report any missing inputs or incomplete checks.

## Verify identity and source contents

1. Record the resolved commit, ATR revision, and artifact paths. Check the RC tag's commit and signature against `RELEASE_COMMIT` and the release manager's personal signing-key fingerprint recorded in the issue.
2. Verify the downloaded archive's SHA-512 against its checksum file and the compose summary. Verify its detached signature against the expected source-signing fingerprint, recording the primary fingerprint when a signing subkey is used. The archive signer can differ from the Git tag signer; check both identities.
3. Inspect the archive member list before extracting. Check the expected `apache-asyncband-${VERSION}-incubating-src/` root, reject paths that escape the extraction directory, and inspect symlinks without following them outside the extracted tree.
4. Compare the archived source inventory, file contents, executable bits, and symlink targets with `git archive` of the resolved candidate commit, using a separate temporary extraction. Report missing, added, or changed entries with concrete paths. Check the package version in the extracted manifest against `VERSION`.

For example, use `git -C "${REPO_ROOT}" rev-parse "${RC_TAG}^{commit}"` to resolve a candidate and `git -C "${REPO_ROOT}" verify-tag "${RC_TAG}"` to inspect its signature. Run `shasum -a 512 --check` from the artifact directory and `gpg --verify` with the explicitly supplied signature and archive. Inspect checksum filenames before using the checksum file so it checks the intended artifact.

On trusted hardware outside GitHub Actions, run the candidate's compose recipe in a Linux environment and compare the rebuilt archive's SHA-512 with the ATR artifact. Record this once per candidate to satisfy [ASF automated signing](https://infra.apache.org/release-signing.html#automated-release-signing). When diagnosing a mismatch, compare source contents separately from archive headers and compression.

## Verify the build and Cargo package

Run these checks when the caller requests build and packaging verification. Read `cargo x --help` and the relevant subcommand help in the extracted source first. The archive test uses Cargo's `--locked` mode to check the shipped lockfile; `cargo x test` does not expose that option.

```shell
cd "${EXTRACTED_SOURCE}"
cargo test --workspace --all-features --locked
cargo publish --package asyncband --locked --dry-run
```

Use the resulting `target/package/asyncband-${VERSION}.crate` for packaging verification and license review. Check its name, version, source contents, and provenance metadata where present. When comparing a separately supplied package, account for Cargo-generated manifests and metadata that depend on whether a Git checkout is available.

Keep the original archive inventory for license review; build outputs belong only to the verification workspace.

## Return the result

Return the checked candidate and ATR revision, signer fingerprints, signature/checksum results, source differences, independent rebuild checksum, build/package results, and any incomplete checks. Provide the extracted source and package paths for `license-audit` and retain them until that review is complete.
