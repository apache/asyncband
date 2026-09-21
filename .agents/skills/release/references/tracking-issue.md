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

# Release tracking issue

Use title `Tracking Issue to Release ${VERSION}` in `apache/asyncband`. Keep the release's current state, check results, and next action in this issue so another release manager can take over.

Fill in known values and leave unfinished items unchecked. Record the checked revision and result for each completed item. With `gh`, pass the body through standard input using `--body-file -` and preserve other participants' edits.

Set `RUNBOOK_COMMIT` to the full SHA of the upstream commit containing the runbook used when opening the issue. Keep this link pinned throughout the release.

```markdown
- **Release manager:** @<login>
- **Target version:** <VERSION>
- **Previous release:** <version and tag>
- **Source cutoff:** <commit and deferred work, if any>
- **Release PR:** pending
- **Frozen release commit:** pending
- **Current phase / next action:** initial license audit
- **Runbook:** https://github.com/apache/asyncband/blob/<RUNBOOK_COMMIT>/.agents/skills/release/SKILL.md

## Prepare and freeze

- [ ] Audit the selected source with `license-audit`; resolve release-blocking findings.
- [ ] Merge the version, lockfile, and undated changelog PR; record the frozen release commit.
- [ ] Complete the final license review, including the actual Cargo package and any audit corrections.

## Check the frozen release commit

- [ ] `cargo x lint`
- [ ] `cargo x check`
- [ ] `cargo x test --no-capture`
- [ ] `RUSTUP_TOOLCHAIN=1.86.0 cargo x test --no-capture`
- [ ] `cargo x semver --release-version <VERSION>`; document any allowed major-release API breaks.
- [ ] `cargo publish --package asyncband --locked --dry-run` from the clean checkout.
- [ ] Required GitHub CI passes for the release commit.

## Candidate

- **RC / signed tag:** pending
- **Compose run and attempt / package-check run:** pending
- **ATR candidate URL / revision:** pending
- **Source SHA-512:** pending
- **Tag signer / source signer fingerprints and provenance:** pending

- [ ] Push the signed RC tag at the frozen commit; confirm both release workflows and ATR upload.
- [ ] Download and verify the ATR revision: signatures, checksum, source contents against the RC commit, independent archive rebuild for automated signing, build, and packaging.
- [ ] Complete the license review of the actual source archive and Cargo distribution; address ATR findings.

## Vote and publish

- [ ] Start the PPMC vote in ATR; record the vote link and closing time.
- [ ] After the required duration and votes, resolve PPMC as Passed in ATR; record its result and the automatically started IPMC vote, and ensure that thread includes the PPMC tally link and any carried IPMC votes.
- [ ] After the required duration and binding votes, resolve IPMC as Passed in ATR; record its result.
- [ ] Confirm ATR publishes the voted revision to ASF distribution; record the SVN revision and download URL.
- [ ] Push the signed final tag at the approved commit and approve/verify crates.io publication.
- [ ] Verify ASF downloads and signatures, crates.io, and docs.rs.
- [ ] Send the announcement through ATR and record its archive link.
- [ ] Merge the publication-date changelog PR and confirm superseded-release archival as applicable.
- [ ] Close this issue after the preceding required items are complete.
```

For a replacement candidate, retain the previous tag, commit, ATR revision, and vote outcome in the issue history. Update the active candidate fields, invalidate checks affected by changed contents, and make the next action explicit.
