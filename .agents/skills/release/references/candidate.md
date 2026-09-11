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

## Stage the candidate in Apache Trusted Releases

Use [Apache Trusted Releases (ATR)](https://releases.apache.org/) for new candidates. Resume an existing candidate where it is already staged; moving a vote in progress between services would change its artifact links.

1. Open the `asyncband` project in ATR and locate the draft for `VERSION`, or create it if absent. Use the final version, such as `0.8.0`, without `v` or `-rc.N`. ATR assigns a new revision serial as files change; record that serial alongside `RC_TAG` and `RELEASE_COMMIT` rather than assuming it equals `RC`.
2. Upload the verified `.tar.gz`, `.tar.gz.asc`, and `.tar.gz.sha512` files through the browser, or use the rsync command provided by ATR. Use the release manager's existing signing identity. Keep verification reports and the crates.io convenience package outside the staged source bundle.
3. Inspect ATR's signature, checksum, archive, and license results for the resulting revision. Investigate concrete concerns with the `license-audit` skill where relevant; a scanner result is evidence to discuss, and a passing scan does not replace source or build verification. If the source archive is misclassified or a signing key is missing, consult [Infrastructure](infrastructure.md).
4. Download the staged files into a separate directory and compare them with the local verified originals. Record the candidate URL and revision, then prepare the vote on that exact set of bytes.

ATR holds the files through compose and vote, and pins the revision when voting starts. Use that candidate page as the voting artifact source. Its download commands are available to voters without committer access. See the official [staging and voting guide](https://releases.apache.org/docs/staging-and-voting); an additional `dist/dev` copy is unnecessary for this route.

If ATR cannot be used for a new candidate, agree on the existing SVN staging route before starting its vote: upload the same three files to `https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}/` and record that as the voting source. Preserve an existing SVN candidate through publication rather than silently switching it to ATR.
