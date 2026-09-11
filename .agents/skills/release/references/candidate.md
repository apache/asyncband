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

# Prepare and stage a release candidate

## Prepare the release pull request

For a new release, start from current `main` and choose `VERSION` from the changes since the latest crates.io release. If the release pull request or candidate already exists, resume its recorded version and commit.

1. Change `version` in `asyncband/Cargo.toml` and refresh `Cargo.lock` with Cargo.
2. Move the entries under `Unreleased` in `CHANGELOG.md` into an undated `v${VERSION}` section immediately below it, then restore an empty `Unreleased` section. Keep user-impacting sections ordered as breaking changes, new features, bug fixes, and improvements; add the actual release date only after publication.
3. Review `LICENSE`, `NOTICE`, `DISCLAIMER`, source headers, and bundled dependencies using the `license-audit` skill. Codex can delegate that review to `license_auditor`; provide the repository root and requested revision. Use its evidence and suggestions to decide what follow-up is needed during release preparation.
4. Run the release checks:

```shell
cargo x lint
cargo x check
cargo x test --no-capture
RUSTUP_TOOLCHAIN=1.86.0 cargo x test --no-capture
cargo x semver --release-version "${VERSION}"
cargo publish --package asyncband --locked --dry-run
```

For a semver-major release, including a pre-1.0 minor release such as `0.7.0`, the semver command uses minor compatibility rules to report breaking API changes. When it reports expected changes, record and review them in `CHANGELOG.md`, then rerun:

```shell
cargo x semver --release-version "${VERSION}" --acknowledge-breaking-changes
```

Merge the release pull request and record its merge commit as `RELEASE_COMMIT`. Every candidate artifact, the final tag, and the crates.io package use this exact commit.

## Create and validate a release candidate

For a new candidate, create a release directory outside the repository and a detached worktree at `RELEASE_COMMIT`. When resuming, reuse the recorded `RELEASE_DIR`, worktree, and RC tag. Run later Git and Cargo commands from that worktree root.

```shell
RELEASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/asyncband-release.XXXXXX")"
RC_TAG="v${VERSION}-rc.${RC}"
git fetch https://github.com/apache/asyncband.git main
git worktree add --detach "${RELEASE_DIR}/checkout" "${RELEASE_COMMIT}"
cd "${RELEASE_DIR}/checkout"
git grep -F "version = \"${VERSION}\"" -- asyncband/Cargo.toml Cargo.lock
git grep -Fx "## v${VERSION}" -- CHANGELOG.md
test -z "$(git status --porcelain)"
git tag --sign "${RC_TAG}" \
  --message "Apache Asyncband ${VERSION} release candidate ${RC}" \
  "${RELEASE_COMMIT}"
git push https://github.com/apache/asyncband.git "${RC_TAG}"
```

Wait for the `Release` GitHub Actions workflow to pass. The workflow validates the RC tag and runs `cargo publish --dry-run` against the unchanged `${VERSION}` package; it skips the crates.io publish job for RC tags. A candidate that needs a code change gets a new release pull request, merge commit, RC number, and signed tag.

## Build and verify the source archive

For a new candidate, build the source archive from the verified RC tag. Reuse existing signed artifacts for a retry of the same candidate. The `incubating` marker is required in the filename, and `gzip -n` keeps the gzip header independent of the local build time.

```shell
RC_TAG="v${VERSION}-rc.${RC}"
SOURCE_DIR="apache-asyncband-${VERSION}-incubating-src"
ARTIFACT_DIR="${RELEASE_DIR}/dist"
mkdir -p "${ARTIFACT_DIR}"
git verify-tag "${RC_TAG}"
git archive --format=tar --prefix="${SOURCE_DIR}/" "${RC_TAG}" \
  | gzip -n -9 > "${ARTIFACT_DIR}/${SOURCE_DIR}.tar.gz"
(
  cd "${ARTIFACT_DIR}"
  shasum -a 512 "${SOURCE_DIR}.tar.gz" > "${SOURCE_DIR}.tar.gz.sha512"
  gpg --armor --detach-sign --local-user "${ASF_GPG_FINGERPRINT}" \
    "${SOURCE_DIR}.tar.gz"
)
```

Verify the existing artifacts with the [candidate-verification guide](verification.md), directly or through `release_verifier`. Supply the candidate identity, signing-key fingerprint, repository root, and absolute artifact paths. Keep the signed archive unchanged throughout verification.

For the separate `license-audit` review, provide the original source archive, the extracted source, and the Cargo package produced by verification, together with `RELEASE_COMMIT`. Discuss its evidence and suggestions with the release manager. Keep the verification directory available until both reviews finish; then remove that disposable directory.

## Stage the candidate on ASF infrastructure

Check whether this candidate is already staged. For a new staging operation, check out a working copy under `RELEASE_DIR`, add the three candidate files, and commit them:

```shell
svn checkout --depth=empty \
  https://dist.apache.org/repos/dist/dev/incubator/asyncband "${RELEASE_DIR}/svn-dev"
mkdir "${RELEASE_DIR}/svn-dev/${VERSION}-rc.${RC}"
cp \
  "${ARTIFACT_DIR}/${SOURCE_DIR}.tar.gz" \
  "${ARTIFACT_DIR}/${SOURCE_DIR}.tar.gz.asc" \
  "${ARTIFACT_DIR}/${SOURCE_DIR}.tar.gz.sha512" \
  "${RELEASE_DIR}/svn-dev/${VERSION}-rc.${RC}/"
svn add "${RELEASE_DIR}/svn-dev/${VERSION}-rc.${RC}"
svn status "${RELEASE_DIR}/svn-dev"
svn commit "${RELEASE_DIR}/svn-dev" \
  -m "Stage Apache Asyncband ${VERSION} release candidate ${RC}"
```

Confirm the candidate at `https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}/` and verify every link prepared for the vote email.
