---
name: release
description: Run or resume an Apache Asyncband release, from its tracking issue and frozen source through RC composition, ATR voting, publication, and follow-up; also verify an existing candidate.
---

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

# Release Apache Asyncband

Use the release tracking issue to coordinate the release and hand it to another PMC member. The normal path assumes signing, GitHub environments, ATR, and crates.io Trusted Publishing are already configured: GitHub builds and stages the candidate; ATR runs the votes and publishes the approved source; the final Git tag publishes the convenience crate.

For an actual release, follow the steps below. For a status check, resume from the issue and live workflow/ATR state. For an independent candidate check, use [verification](references/verification.md). A request to edit this skill does not start a release. Carry forward the user's authorization; prepare concrete tags, messages, and publication actions before asking only for authorization that is still missing. When the release manager operates ATR, provide the current page and next action, then confirm the result before proceeding.

## 1. Choose the version and open the tracking issue

Set `VERSION` to the target stable crate version, such as `0.7.3`, after comparing the previous published crate and changes since its release tag. Use the user's proposed version when compatible; `cargo x semver` below verifies the decision. Record a source cutoff commit and defer unrelated pending work so the release does not keep following `main`.

Find an existing `Tracking Issue to Release ${VERSION}` before creating one. Otherwise create it immediately from the [tracking issue template](references/tracking-issue.md). Use it as the release record: link the release PR, checked revisions, workflow runs, candidate, votes, and publication results; update completed items with evidence and keep the next action current.

## 2. Audit first, prepare the version, and freeze the source

Start with the `license-audit` skill on the selected checkout. Resolve release-blocking licensing or attribution findings before packaging. The configured `license_auditor` can perform this review independently. A checkout audit does not yet verify the distributed archive or Cargo package.

Prepare a release PR from the chosen scope: update `asyncband/Cargo.toml`, refresh `Cargo.lock` with Cargo, and move the final user-visible changelog entries from `Unreleased` into an undated `v${VERSION}` section, leaving `Unreleased` empty. Include necessary audit corrections; defer unrelated source changes. Respect required CI and merge authorization.

Before freezing, check that the merged PR's tree matches the reviewed release scope. If `main` advanced during preparation and the merge includes additional changes, review them and update the scope and affected audit results first. Record the PR's merge commit, not a later `main` HEAD, as `RELEASE_COMMIT` in the issue. This freezes the entire release tree, including manifests, licensing, and documentation. `main` may then advance without changing this release. Use a detached checkout for all remaining checks and tags:

```shell
RELEASE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/asyncband-release.XXXXXX")"
git fetch https://github.com/apache/asyncband.git main
git worktree add --detach "${RELEASE_DIR}/checkout" "${RELEASE_COMMIT}"
cd "${RELEASE_DIR}/checkout"
test -z "$(git status --porcelain)"
```

Keep scratch artifacts outside the repository. If a check requires a correction, prepare a new reviewed commit and replace the recorded snapshot explicitly; recheck affected results. After an RC tag exists, changed release contents require a new RC number. Never silently move an existing tag or change the files under a vote.

## 3. Check the frozen checkout

Read `cargo x --help` and each relevant subcommand's help, then run:

```shell
cargo x lint
cargo x check
cargo x test --no-capture
RUSTUP_TOOLCHAIN=1.86.0 cargo x test --no-capture
cargo x semver --release-version "${VERSION}"
cargo publish --package asyncband --locked --dry-run
```

Record results against `RELEASE_COMMIT`, including the required CI result for that commit. Do not use `--allow-dirty`. Pass the generated `target/package/asyncband-${VERSION}.crate` to the license review to check the actual Cargo distribution. If audit fixes changed the selected source, include those changes in the final review.

For a semver-major release, including a pre-1.0 minor bump, document and review expected API breaks before rerunning `cargo x semver --release-version "${VERSION}" --acknowledge-breaking-changes`. An incompatible patch release needs a corrected version or source before the snapshot can pass.

## 4. Push the RC and verify the ATR candidate

Set `RC` to the next unused positive candidate number and `TAG_SIGNING_FINGERPRINT` to the release manager's verified signing-key fingerprint. The package and ATR version remain `${VERSION}`; only the Git tag carries the RC suffix:

```shell
RC_TAG="v${VERSION}-rc.${RC}"
test "$(git rev-parse HEAD)" = "${RELEASE_COMMIT}"
test -z "$(git status --porcelain)"
git tag --sign --local-user "${TAG_SIGNING_FINGERPRINT}" "${RC_TAG}" \
  --message "Apache Asyncband ${VERSION} release candidate ${RC}" "${RELEASE_COMMIT}"
git verify-tag "${RC_TAG}"
git push https://github.com/apache/asyncband.git "${RC_TAG}"
```

Follow both workflows for this tag:

- `Release` checks the unchanged `${VERSION}` Cargo package and skips crates.io publication.
- `Compose source release` builds `apache-asyncband-${VERSION}-incubating-src.tar.gz` and its checksum. Approve its `release` environment job to sign with the automated project key and upload the archive, `.asc`, and `.sha512` to ATR project `asyncband`, version `${VERSION}`.

Open the uploaded candidate in [ATR](https://releases.apache.org/projects/asyncband), inspect its checks, and record its actual URL and revision with the tag, commit, workflow run, and SHA-512. The ATR revision is not the Git RC number. Download that revision and follow [verification](references/verification.md): check signatures and checksum, compare the source contents with the RC commit, and test the build and Cargo package. Use `release_verifier` for substantial independent verification, including the archive rebuild required for automated signing, and the `license-audit` skill (or `license_auditor` agent) for the actual distributions. Collect both results before voting; reuse completed checks for the same candidate.

## 5. Vote and publish through ATR

Follow the [ATR operations guide](references/atr.md). In the configured email-vote mode, the normal sequence is:

1. Review the candidate and vote email, then start the PPMC vote in ATR.
2. After at least 72 hours and sufficient PPMC votes, review the tally and resolve it as `Passed`. ATR sends the result and starts the IPMC vote automatically; check that thread and supply the PPMC result/tally link and any carried IPMC votes if missing.
3. After at least another 72 hours and sufficient binding IPMC votes, review and resolve that vote as `Passed`. ATR sends the result and moves the release to Finish.
4. In Finish, publish the exact approved revision to ASF distribution, or verify the completed automatic publication if it was enabled. Keep the candidate bytes unchanged.

Record both rounds' vote and result links in the issue. Do not send duplicate vote/result emails yourself. GitHub checks and a passing PPMC vote alone do not authorize an ASF release.

## 6. Publish the crate and close the issue

Once both votes have passed and the source publication is confirmed, create the signed final tag at the approved commit:

```shell
git verify-tag "${RC_TAG}"
test "$(git rev-parse "${RC_TAG}^{commit}")" = "${RELEASE_COMMIT}"
git tag --sign --local-user "${TAG_SIGNING_FINGERPRINT}" "v${VERSION}" \
  --message "Apache Asyncband ${VERSION}" "${RELEASE_COMMIT}"
git push https://github.com/apache/asyncband.git "v${VERSION}"
```

Approve the final tag's `release` environment deployment in `release.yml`, then verify crates.io, docs.rs, and the ASF downloads. Final tags do not compose another source candidate. Use ATR's Announce action after both distributions are available; review the message and recipients and record the sent announcement. Submit the changelog publication-date PR, confirm any superseded-release archival, and close the tracking issue only after its required items are complete. Remove the detached worktree and scratch directory when no longer needed.

## Resume or recover

Use the issue's frozen commit and candidate revision rather than the latest `main`. Reuse an existing tag after checking its signature and target. For uncertain uploads, votes, or publications, inspect ATR and the destination before repeating an action; signing/upload retries can create a new signature and revision. See [ATR recovery](references/atr.md#recover-without-replacing-voted-files). For a failed crates.io run, check whether that version already exists before retrying; a published package is immutable.

Consult [infrastructure](references/infrastructure.md) only for a concrete setup or access problem. Resolve repository paths from the caller's supplied repository root; skill reference links stay inside this directory. Discover `license-audit` by name, or use `.agents/skills/license-audit/SKILL.md` under that root.
