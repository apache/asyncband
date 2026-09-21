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

Perform the requested checks against the supplied candidate and return evidence to the caller. This is a bounded technical check: preserve the supplied repository and artifacts, and create extraction or build outputs only in the caller's scratch directory or a new temporary directory outside the checkout. Do not change tags, sign replacement artifacts, stage, publish, send messages, or perform a separate license audit.

## Inputs and scope

Use the caller's repository root, `VERSION`, `RC_TAG`, `RELEASE_COMMIT`, ATR candidate URL and revision, compose run and attempt, absolute archive/signature/checksum paths, expected tag and source signing fingerprints with their provenance, and requested verification scope. The caller may supply an existing Cargo package as well. Resolve repository files from that explicit root; this guide's location does not identify the audited checkout.

Establish which checks are requested and which inputs are available. Continue independent checks when an input is missing and report the resulting gap. A key bundled with an archive is not independent evidence of signer identity. Use a temporary GPG home when importing supplied verification keys so the user's keyring stays unchanged.

## Verify identity and source contents

1. Record the resolved commit, ATR revision, and artifact paths. Check the supplied RC tag's commit and signature against `RELEASE_COMMIT` and the release manager's expected tag-signing fingerprint. Keep an existing candidate tied to that commit even if `main` has advanced.
2. Verify the SHA-512 checksum and detached archive signature on the original downloaded bytes against the expected source-signing fingerprint: the automated project key for CI candidates, or the recorded release manager's key for legacy candidates. For CI candidates, check the archive checksum against the compose summary as well. Record signature validity separately from signer identity, including the primary fingerprint when a signing subkey is used. The archive signer can differ from the Git tag signer; neither signature alone establishes agreement with the candidate commit.
3. Inspect the archive member list before extracting. Check the expected `apache-asyncband-${VERSION}-incubating-src/` root, reject paths that escape the extraction directory, and inspect symlinks without following them outside the extracted tree.
4. Compare the archived source inventory, file contents, executable bits, and symlink targets with `git archive` of the resolved candidate commit, using a separate temporary extraction. Report missing, added, or changed entries with concrete paths. Check the package version in the extracted manifest against `VERSION`.

For example, use `git -C "${REPO_ROOT}" rev-parse "${RC_TAG}^{commit}"` to resolve a candidate and `git -C "${REPO_ROOT}" verify-tag "${RC_TAG}"` to inspect its signature. Run `shasum -a 512 --check` from the artifact directory and `gpg --verify` with the explicitly supplied signature and archive. Inspect checksum filenames before using the checksum file so it checks the intended artifact.

## Independently reproduce the automated archive

For an automated source candidate, reproduce the archive using the recipe in its tagged `.github/workflows/release-compose.yml`, writing the result separately from the downloaded archive. The current recipe runs on Ubuntu 24.04 and archives the commit, not the annotated tag object:

```shell
set -euo pipefail
SOURCE_DIR="apache-asyncband-${VERSION}-incubating-src"
git -C "${REPO_ROOT}" -c core.attributesFile=/dev/null -c tar.umask=0022 archive \
  --format=tar --prefix="${SOURCE_DIR}/" "${RELEASE_COMMIT}" \
  | gzip -n -9 > "${VERIFY_DIR}/${SOURCE_DIR}.reproduced.tar.gz"
cmp "${ARCHIVE_PATH}" "${VERIFY_DIR}/${SOURCE_DIR}.reproduced.tar.gz"
```

Use a clean verification checkout and a scratch `VERIFY_DIR` outside it. Record the Git and gzip versions. If compressed bytes differ, compare logical archive entries to diagnose the difference and repeat with the workflow's Linux toolchain; logical equality alone does not establish reproducibility. Do not compare detached signature bytes from independently signed archives, since signature creation times can differ. Preserve and verify the original ATR signature. Record the independent reproduction result for the vote, following [ATR Trusted Publishing](https://releases.apache.org/docs/trusted-publishing#step-6-confirm-reproducibility-during-the-vote).

## Verify the build and Cargo package

Run these checks when the caller requests build and packaging verification. Read `cargo x --help` and the relevant subcommand help in the extracted source first. The archive test uses Cargo's `--locked` mode to check the shipped lockfile; `cargo x test` does not expose that option.

```shell
cd "${EXTRACTED_SOURCE}"
cargo test --workspace --all-features --locked
cargo publish --package asyncband --locked --dry-run
```

Use the resulting `target/package/asyncband-${VERSION}.crate` for packaging verification and the caller's license review. If a package is supplied separately, inspect its actual contents and compare it with the package generated from the candidate when available. Check the package name, version, packaged source, and Cargo provenance metadata where present. Account for Cargo-generated manifests and repository metadata when comparing; an absent commit field in a package built outside a Git checkout is not itself a mismatch.

Preserve the original archive inventory when builds add files to the extracted directory. Report build or dependency-fetch failures as observed failures; do not change the candidate to make verification pass.

## Return the result

Return a concise account of the checked candidate and ATR revision, the two signing identities, signature/checksum, source-comparison and byte-reproduction results, requested build/package results, concrete discrepancies, and checks not completed. Include commands and exit results when they matter. Give the caller the extracted source and package paths for `license-audit`; retain these scratch outputs until the caller has finished with them. Do not create a report file unless requested.
