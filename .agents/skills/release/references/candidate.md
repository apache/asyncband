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

For a new release, start from current `main` and choose `VERSION` from the changes since the latest crates.io release. For example, preparing `0.7.3` uses package version `0.7.3`, RC tag `v0.7.3-rc.1`, and ATR version `0.7.3`. If the release pull request or candidate already exists, resume its recorded version and commit.

1. Change `version` in `asyncband/Cargo.toml` and refresh `Cargo.lock` with Cargo.
2. Move the entries under `Unreleased` in `CHANGELOG.md` into an undated `v${VERSION}` section immediately below it, then restore an empty `Unreleased` section. Keep user-impacting sections ordered as breaking changes, new features, bug fixes, and improvements; add the actual release date only after publication.
3. Review `LICENSE`, `NOTICE`, `DISCLAIMER`, source headers, and bundled dependencies using the `license-audit` skill. Codex can delegate that review to `license_auditor`; provide the repository root and requested revision. Use its evidence and suggestions to decide what follow-up is needed during release preparation.
4. Read `cargo x --help` and the relevant subcommand help, then run the release checks:

```shell
cargo x lint
cargo x check
cargo x test --no-capture
RUSTUP_TOOLCHAIN=1.86.0 cargo x test --no-capture
cargo x semver --release-version "${VERSION}"
cargo publish --package asyncband --locked --dry-run --allow-dirty
```

The preparation dry run allows the edited version files; the RC workflow and downloaded candidate checks package committed or extracted sources without `--allow-dirty`.

For a semver-major release, including a pre-1.0 minor release such as `0.7.0`, the semver command uses minor compatibility rules to report breaking API changes. When it reports expected changes, record and review them in `CHANGELOG.md`, then rerun:

```shell
cargo x semver --release-version "${VERSION}" --acknowledge-breaking-changes
```

Before the first automated candidate, confirm the signing secrets, project public key, and ATR policy described in [Infrastructure](infrastructure.md). A successful PR compose check does not exercise signing or ATR authentication.

Merge the release pull request when authorized and record its merge commit as `RELEASE_COMMIT`. Every candidate artifact, the final tag, and the crates.io package use this exact commit.

## Create the RC tag

For a new candidate, create a release directory outside the repository and a detached worktree at `RELEASE_COMMIT`. When resuming, reuse the recorded `RELEASE_DIR`, worktree, and RC tag. Run later Git and Cargo commands from that worktree root. Set `TAG_SIGNING_FINGERPRINT` to the release manager's independently verified signing key; this is separate from the automated project key used by CI.

```shell
RELEASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/asyncband-release.XXXXXX")"
RC_TAG="v${VERSION}-rc.${RC}"
git fetch https://github.com/apache/asyncband.git main
git worktree add --detach "${RELEASE_DIR}/checkout" "${RELEASE_COMMIT}"
cd "${RELEASE_DIR}/checkout"
git grep -F "version = \"${VERSION}\"" -- asyncband/Cargo.toml Cargo.lock
git grep -Fx "## v${VERSION}" -- CHANGELOG.md
test -z "$(git status --porcelain)"
git tag --sign --local-user "${TAG_SIGNING_FINGERPRINT}" "${RC_TAG}" \
  --message "Apache Asyncband ${VERSION} release candidate ${RC}" \
  "${RELEASE_COMMIT}"
git verify-tag "${RC_TAG}"
git push https://github.com/apache/asyncband.git "${RC_TAG}"
```

## Follow both release workflows

The RC tag starts two independent workflows:

- `Release` (`release.yml`) validates the tag and runs `cargo publish --dry-run` against the unchanged `${VERSION}` package. Its crates.io publish job is skipped for RC tags.
- `Compose source release` (`release-compose.yml`) validates the RC version and ancestry on `main`, creates `apache-asyncband-${VERSION}-incubating-src.tar.gz` and its SHA-512 checksum, then waits for `release` environment approval. After review of the tag, commit, and checksum, `sign-and-upload` signs with the automated project key and uploads the three files to ATR project `asyncband`, version `${VERSION}`.

Record both run URLs and the compose run attempt. Require the package check to pass and confirm upload completion in the [Asyncband ATR project](https://releases.apache.org/projects/asyncband). Normally the compose run succeeds; if it failed after sending files, use the [recovery procedure](publication.md#recover-from-failures) to establish whether the complete revision is already present. Record the actual candidate URL, ATR revision, source checksum, and commit together. The pinned upload action creates revisions but does not return a candidate URL or revision as a workflow output; obtain those from ATR. The RC number and ATR revision number are independent.

A code change requires a new release pull request, merge commit, RC number, and signed tag. For an interrupted upload, follow [recovery](publication.md#recover-from-failures) before rerunning a job.

## Verify the ATR candidate before voting

ATR is the staging location. Download the archive, `.asc`, and `.sha512` from the recorded revision into `${RELEASE_DIR}/dist`, using the candidate page's download links. Confirm the archive checksum matches the compose summary and that all three files belong to that revision. Do not substitute a locally generated signature or the unsigned GitHub Actions bundle for the files voters will download.

Review ATR's checks, then follow the [candidate-verification guide](verification.md), directly or through `release_verifier`. Supply the tag and source signing identities separately. Verification includes independently reproducing the compressed source archive, as well as signatures, source contents, builds, and Cargo packaging.

For the separate `license-audit` review, provide the original source archive, the extracted source, and the Cargo package produced by verification, together with `RELEASE_COMMIT`. Discuss its evidence and suggestions with the release manager. Retain the verification directory until both reviews finish.

Once verification is complete, use the recorded ATR revision for [voting](publication.md). Link the ATR candidate as the artifact source in the vote; a second SVN staging copy is unnecessary. See ATR's [staging and voting guide](https://releases.apache.org/docs/staging-and-voting).
