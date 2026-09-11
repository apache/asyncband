---
name: release
description: Prepare, resume, or verify Apache Asyncband releases when release-manager work is requested, including candidate artifacts, votes, publication, and recovery.
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

Help the release manager carry out the requested release work, explain the current state, and propose practical next steps. Keep coordination in the main conversation; use the shared license-audit skill for the licensing review. The release manager and project community make release decisions.

## Resume the requested work

Establish the requested scope and what has already happened from the conversation, current checkout, and relevant external records. Read the affected parts of `asyncband/Cargo.toml`, `Cargo.lock`, `CHANGELOG.md`, `.github/workflows/release.yml`, `.asf.yaml`, and `xtask/src/main.rs` when needed. Use live GitHub, ASF distribution, mailing-list archives, and registry records to resolve uncertain state. Load only the reference for the current phase; an existing candidate does not require repeating preparation or setup.

Carry forward the user's existing authorization. A status check, review, or plan stays read-only. For execution, complete authorized work and prepare any proposed external action before asking about authorization that is actually missing. Sending vote or announcement messages, publishing, merging, or changing tags needs authorization for that action; opening this skill does not provide it. Preserve the user's work when selecting a checkout or creating a release worktree.

Keep these values and supporting links in the conversation so work can resume across turns:

- `VERSION`: the final crate version, such as `0.7.2`; RCs do not change the package version.
- `RC`: the positive candidate number; `RC_TAG` is `v${VERSION}-rc.${RC}`.
- `RELEASE_COMMIT`: the merged release pull request commit bound to the candidate.
- `RELEASE_DIR`: an absolute working directory outside the repository for artifacts, verification, and SVN checkouts; reuse it while continuing the same candidate.
- Candidate tag and artifact location, checksum/signature results, relevant CI runs, PPMC/IPMC vote threads and results, and completed publication steps.

Report completed work with evidence, the next useful step, and any input still needed. Distinguish pending, failed, and unverified steps. Keep handoff notes in the conversation unless the user requests a file.

## Choose the current phase

| Current work                                       | Read                                             |
| -------------------------------------------------- | ------------------------------------------------ |
| Signing keys, ASF directories, registry setup      | [Release setup](references/setup.md)             |
| Version/changelog PR, RC, artifacts, or staging    | [Candidate preparation](references/candidate.md) |
| Voting, approved publication, follow-up, or retry  | [Publication](references/publication.md)         |
| Licensing review of the checkout or supplied files | [License audit](../license-audit/SKILL.md)       |

The phase guides are the maintained release procedure for both people and agents. `RELEASE.md` is only a discovery link. Repository paths and Git/Cargo commands refer to the repository or release-worktree root; Markdown links are relative to their containing file. Read `cargo x --help` and the relevant subcommand help before running repository checks.

## Candidate and publication continuity

The signed source archive approved by the Apache Incubator PMC and published through ASF distribution is the official Apache release. Its name is `apache-asyncband-${VERSION}-incubating-src.tar.gz`. The crates.io package is a convenience distribution from the same approved commit; keep its Cargo-generated name and layout.

Keep the RC tag, commit, artifacts, and vote tied together. A later `main` commit does not invalidate an existing candidate. Reuse an existing signed tag and staged bytes when retrying a transient failure. If candidate content changes or the community rejects it, agree on the replacement candidate and increment `RC`; preserve existing tags rather than rewriting them.

After both vote results record approval, promote the exact voted source artifacts. The signed final `v${VERSION}` tag uses the approved RC commit and starts the crates.io publication workflow, subject to the configured `release` environment review. Successful CI alone does not establish vote approval. Confirm each external action's result before reporting completion or retrying it.

Use the shared [license-audit skill](../license-audit/SKILL.md) to examine the relevant checkout or artifact contents. In Codex, the configured `license_auditor` can perform a delegated review; another agent can follow the same skill directly. Provide the candidate revision and actual artifact paths, then discuss the review's evidence and suggestions with the release manager.

Follow the current [ASF Release Policy](https://www.apache.org/legal/release-policy.html), [Release Distribution Policy](https://infra.apache.org/release-distribution), [Release Creation Process](https://infra.apache.org/release-publishing.html), and [Incubator release guidance](https://incubator.apache.org/guides/releasemanagement.html). Explain any relevant ambiguity with its source and practical options instead of treating incomplete evidence as a project defect.
