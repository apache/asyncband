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

# Vote, publish, and follow up

Resume from the recorded candidate and vote results. Check which publication steps already succeeded before running commands again.

## Hold the two-phase vote

Incubating releases use the [Incubator two-phase vote](https://incubator.apache.org/cookbook/#two-phase-vote-on-podling-releases). Each vote remains open for at least 72 hours.

For an ATR candidate, use its email vote mode with `dev@asyncband.apache.org` as the first-round recipient and `general@incubator.apache.org` as the second-round recipient. Use at least 72 hours and keep automatic SVN publication off for the initial ATR release so the release manager can inspect the destination before publishing. ATR pins the staged revision when voting starts.

When the staged candidate and supporting links have been verified, review the vote message before sending it through ATR. Use `[VOTE] Release Apache Asyncband (Incubating) ${VERSION} RC${RC}` as the subject and include:

- the ATR candidate page identifying the voted revision, or the staged SVN URL for a legacy candidate;
- the `KEYS` URL and signing-key fingerprint;
- the signed RC tag and commit hash;
- the changelog or comparison with the previous release;
- verification commands or a checklist for signatures, checksums, licensing, unexpected binaries, and the build;
- a closing time at least 72 hours after the vote starts.

The PPMC vote passes with at least three PPMC `+1` votes and more PPMC `+1` votes than `-1` votes. Record the voters, result, and archived thread. Resolving a passing first-round podling vote in ATR also starts the IPMC vote; prepare both actions with the release manager before resolving it. Do not send a duplicate IPMC proposal outside ATR.

The IPMC vote passes with at least three binding IPMC `+1` votes and more binding `+1` votes than `-1` votes. Confirm its result and record the archived vote and result links. ATR's checks and phase labels support the release manager's review; retain the evidence for both vote rounds.

For a candidate already staged in SVN, conduct the same two votes by email: send and resolve the PPMC proposal, then send the IPMC proposal with the PPMC result and archive link. Publish each result and retain its archive link.

Begin publication after the IPMC result records a passing vote.

## Promote and publish the approved release

For an ATR candidate, open its finish page after both rounds pass. Verify that the destination is `https://dist.apache.org/repos/dist/release/incubator/asyncband/${VERSION}/`, then use ATR's publish action to promote the exact voted artifacts. The `download_path_suffix` in `.asf.yaml` selects the version directory. Record the resulting SVN revision and URL; there is no separate `svn move` for this route. See [Promoting to release](https://releases.apache.org/docs/promoting-to-release).

For a legacy SVN candidate, promote the exact voted artifacts from its recorded staging area:

```shell
svn move \
  "https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}" \
  "https://dist.apache.org/repos/dist/release/incubator/asyncband/${VERSION}" \
  -m "Release Apache Asyncband ${VERSION}"
```

If the final tag already exists, verify that it points to the approved RC commit and continue with its workflow state. Otherwise, create the signed final tag from the verified RC tag and push it:

```shell
RC_TAG="v${VERSION}-rc.${RC}"
git verify-tag "${RC_TAG}"
git tag --sign "v${VERSION}" \
  --message "Apache Asyncband ${VERSION}" \
  "${RC_TAG}^{commit}"
git push https://github.com/apache/asyncband.git "v${VERSION}"
```

The final tag starts the crates.io publishing job. A configured reviewer compares the final tag with the approved RC, confirms the IPMC vote result, and approves the `release` environment deployment. The workflow verifies the package version and publishes with a short-lived crates.io token.

After publication:

1. Verify the version and metadata on crates.io and docs.rs.
2. After ASF distribution syncs, verify the source archive, checksum, and signature under `https://downloads.apache.org/incubator/asyncband/${VERSION}/` and the project `KEYS` file at `https://downloads.apache.org/incubator/asyncband/KEYS`.
3. Submit a post-release pull request that adds the actual publication date to the `v${VERSION}` changelog heading.
4. Announce the release on `dev@asyncband.apache.org` and other appropriate channels as Apache Asyncband (Incubating). For an ATR release, use its announcement action after checking crates.io and docs.rs; ATR also checks download availability and records the release in its catalog. Reuse an announcement already sent through ATR.
5. Remove superseded releases from `dist/release`; ASF retains them in the archive.

## Recover from failures

A transient CI, staging, or registry error can be retried against the same candidate after checking what already succeeded. For ATR, inspect the current phase, revision, vote tasks, and SVN publication result before repeating an upload or action; a lost response does not mean the operation failed.

If the community rejects a candidate or its content changes, coordinate a new candidate with an incremented `RC`. In ATR, end the affected vote and return the release to compose before uploading the replacement as a new revision; record its new Git tag, commit, and ATR revision. Keep the prior vote identity in the handoff. For a legacy SVN candidate, remove the rejected files from the development distribution area:

```shell
svn delete \
  "https://dist.apache.org/repos/dist/dev/incubator/asyncband/${VERSION}-rc.${RC}" \
  -m "Remove rejected Apache Asyncband ${VERSION} release candidate ${RC}"
```

For a failed final crates.io job, first check whether the version exists on crates.io. If it exists and its contents and metadata match the approved release, continue post-publication verification and follow-up; the upload may have succeeded before the job lost its response. If the version is absent, rerun the same final-tag workflow for a transient or publishing-infrastructure failure. A source or package change uses a new version and the full ASF vote process. A published version is immutable; if it has a confirmed problem, discuss any yank and follow-up release on the development list.

Once release follow-up is complete and the release manager no longer needs the local artifacts, remove the detached worktree with `git worktree remove "${RELEASE_DIR}/checkout"` and clean up the recorded release directory. Retain it while there are unresolved verification or recovery questions.
