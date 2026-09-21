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

Resume from the recorded RC commit, ATR candidate revision, and vote results. Check which publication steps already succeeded before running commands again.

## Hold the two-phase vote

Incubating releases use the [Incubator two-phase vote](https://incubator.apache.org/cookbook/#two-phase-vote-on-podling-releases). Each vote remains open for at least 72 hours, so allow at least six days for the two sequential votes, plus preparation and publication time.

Use the verified ATR revision for both votes. The project's email vote policy targets `dev@asyncband.apache.org`; it does not replace the subsequent vote on `general@incubator.apache.org`. Leave ATR's automatic SVN publication option disabled when starting the PPMC vote, and do not publish from the finish page merely because that first vote has passed.

When the candidate and supporting links have been verified and sending is authorized, start `[VOTE] Release Apache Asyncband (Incubating) ${VERSION} RC${RC}` on `dev@asyncband.apache.org`, using ATR's email vote flow. Review the generated message and include:

- the ATR candidate URL and revision as the source of the voted files;
- the `KEYS` URL and automated source-signing fingerprint;
- the signed RC tag, release manager's tag-signing fingerprint, and commit hash;
- the changelog or comparison with the previous release;
- the compose workflow run, archive checksum, and verification instructions for signatures, reproducibility, licensing, unexpected binaries, and the build;
- a closing time at least 72 hours after the vote starts.

The PPMC vote passes with at least three PPMC `+1` votes and more PPMC `+1` votes than `-1` votes. Publish a result email identifying the voters and linking the archived vote thread, and record the result with the candidate.

Then send the same candidate proposal to `general@incubator.apache.org`, including the PPMC result and archive link. Identify any PPMC voters who are also IPMC members so their binding votes can be carried forward. The IPMC vote passes with at least three binding IPMC `+1` votes and more binding `+1` votes than `-1` votes. Publish its result email and record the archive link alongside the PPMC result and ATR revision.

## Publish the approved source revision

After both vote results record approval and publication is authorized, open the candidate's ATR finish page. Confirm the revision and source checksum match the vote, then publish it to ASF distribution. With the repository's `download_path_suffix: "{{VERSION}}"`, the destination is `https://dist.apache.org/repos/dist/release/incubator/asyncband/${VERSION}/`.

ATR commits the approved artifacts to SVN. Record the resulting SVN revision and URL, and check the archive, signature, and checksum against the voted files. Do not rebuild or re-sign the release, and do not perform a second manual `svn move`. See [ATR promotion](https://releases.apache.org/docs/promoting-to-release).

## Publish the crates.io convenience package

If the final tag already exists, verify its signature and that it points to the approved RC commit, then continue with its workflow state. Otherwise, create the signed final tag from the verified RC tag and push it:

```shell
RC_TAG="v${VERSION}-rc.${RC}"
git verify-tag "${RC_TAG}"
test "$(git rev-parse "${RC_TAG}^{commit}")" = "${RELEASE_COMMIT}"
git tag --sign --local-user "${TAG_SIGNING_FINGERPRINT}" "v${VERSION}" \
  --message "Apache Asyncband ${VERSION}" \
  "${RELEASE_COMMIT}"
git push https://github.com/apache/asyncband.git "v${VERSION}"
```

The final tag starts `release.yml`; it does not run source composition again. A configured reviewer compares the final tag with the approved RC, confirms both vote results and source publication, and approves the `release` environment deployment. The workflow verifies the package version and publishes with a short-lived crates.io token. This approval is separate from the approval to sign and upload the RC.

After publication:

1. Verify the version and metadata on crates.io and docs.rs.
2. After ASF distribution syncs, verify the source archive, checksum, and signature under `https://downloads.apache.org/incubator/asyncband/${VERSION}/` and the project `KEYS` file at `https://downloads.apache.org/incubator/asyncband/KEYS`.
3. Submit a post-release pull request that adds the actual publication date to the `v${VERSION}` changelog heading.
4. Announce the release through ATR after the source downloads and crates.io package are available. Review recipients and text before sending, use Apache Asyncband (Incubating), and avoid a duplicate manual announcement to the same list.
5. Remove superseded releases from `dist/release` after confirming the replacement is available and the old files are archived. Check the actual distribution contents; an ATR catalog archive entry alone does not establish that distribution cleanup succeeded.

## Recover from failures

Inspect the recorded GitHub run, ATR revision and phase, vote threads, ASF distribution, and crates.io as relevant before retrying a failed operation.

- For a package-check failure, diagnose the check without changing the candidate. A source fix requires a new candidate.
- If signing failed before upload, repair the signing configuration and rerun the failed job against the same RC and stored source bundle. A workflow-code fix must be present in the tagged commit to take effect, so it requires a new merged commit and RC tag.
- If upload failed or its outcome is uncertain, first inspect ATR for the three expected files and their checksum/signature. An upload retry can create a new ATR revision and re-sign the archive; resume an existing complete revision when possible. Before voting, record and verify whichever revision will actually be voted on. Do not rerun signing/upload against an active or approved vote.
- If the source bundle has expired, do not assume rerunning only the failed signing job can recover it. Before a vote, rerun composition from the unchanged RC tag, compare the new archive with the recorded checksum, and verify the resulting ATR revision. During or after voting, use the voted files retained in ATR.
- If the community rejects a candidate or its content changes, cancel the vote or return the release to compose in ATR as appropriate, increment `RC`, and preserve the previous signed tags and vote history. ATR revisions and Git RC numbers need not match; record the replacement mapping explicitly.
- For an interrupted ATR publication, check the finish page's publication result and destination SVN files before retrying. Continue from a matching completed publication without recomposing or uploading the candidate.

For a failed final crates.io job, first check whether the version exists on crates.io. If it exists and its contents and metadata match the approved release, continue post-publication verification and follow-up; the upload may have succeeded before the job lost its response. If the version is absent, rerun the same final-tag workflow for a transient or publishing-infrastructure failure. A source or package change uses a new version and the full ASF vote process. A published version is immutable; if it has a confirmed problem, discuss any yank and follow-up release on the development list.

Once release follow-up is complete and the release manager no longer needs the local artifacts, remove the detached worktree with `git worktree remove "${RELEASE_DIR}/checkout"` and clean up the recorded release directory. Retain it while there are unresolved verification or recovery questions.
