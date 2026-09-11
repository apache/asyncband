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

# Release infrastructure

This is a reference for the existing design, not a recurring release checklist. Consult it when changing configuration, troubleshooting publication, or arranging a new release manager's signing key.

## crates.io Trusted Publishing

Trusted Publishing is already configured for `asyncband`: repository `apache/asyncband`, workflow `release.yml`, and environment `release`. Routine releases reuse this configuration.

`.github/workflows/release.yml` obtains a short-lived crates.io token through GitHub OIDC. Only a final `v${VERSION}` tag can enter the publish job; RC tags run package checks. `.asf.yaml` configures version-tag deployments and required reviewers for the `release` environment. Consult those repository files and the live crate settings when diagnosing a mismatch. See the [crates.io Trusted Publishing documentation](https://crates.io/docs/trusted-publishing) for changes to this setup.

## Signing and ASF distribution

New candidates use [Apache Trusted Releases](https://releases.apache.org/) for staging, automated artifact checks, email votes, and publication to `https://dist.apache.org/repos/dist/release/incubator/asyncband/`. The existing `https://dist.apache.org/repos/dist/dev/incubator/asyncband/` area remains available for candidates already using SVN or an agreed fallback. The public verification key list is `https://downloads.apache.org/incubator/asyncband/KEYS`.

The release manager uses an ASF-associated signing key published in the existing project `KEYS` file. For a new signing key, follow the [ASF release signing guide](https://infra.apache.org/release-signing.html), verify the fingerprint through an independent channel, and add the public key while preserving existing keys. Reuse established distribution areas and signing configuration; investigate a reported access or verification failure before proposing infrastructure changes.

## ATR project configuration

The `project` block in `.asf.yaml` is the maintained ATR configuration. ASF infrastructure synchronizes it from the default branch, so edit the repository for lasting changes; subsequent syncs can overwrite corresponding UI edits. It identifies `asyncband` as both the project and its owning committee, classifies the incubating source archive, chooses email votes on the development list with a 72-hour minimum, and publishes into `{{VERSION}}`. See the official [.asf.yaml project reference](https://github.com/apache/infrastructure-asfyaml#project-metadata).

After initially merging this configuration, confirm the project and settings in ATR and record the result. Confirm that its committee is marked as a podling and that the existing signing key is available. Podling status comes from ASF committee records, not this repository. The first ATR release still requires this live check; a valid YAML file does not establish that synchronization or key import has completed.

Keep the existing SVN `KEYS` file as the source of truth and use ATR's automatic import mode unless the project chooses another ownership model. This avoids maintaining two key lists. See [KEYS management](https://releases.apache.org/docs/promoting-to-release#the-keys-file). ATR's [artifact checks](https://releases.apache.org/docs/checks) complement the local verifier and license audit. Add narrowly justified scanner exclusions only in response to inspected findings; the current policy leaves the default license checks enabled.

ATR's current [podling vote implementation](https://github.com/apache/tooling-trusted-releases/blob/55dbd1b1a69e383941505f87d6b85b840b491288/atr/storage/writers/vote.py#L706) starts the IPMC round when the release manager resolves a passing PPMC round. It does not offer automatic resolution of the first round. Preserve both rounds when changing the workflow.

## Later: automated signing and upload

ASF [Trusted Publishing](https://releases.apache.org/docs/trusted-publishing) is separate from crates.io Trusted Publishing. Browser or personal rsync uploads can use the release manager's existing signature now; GitHub Actions uploads through `apache/tooling-actions/upload-to-atr` require ATR workflow trust and an eligible committee signing key.

For automated signing, demonstrate reproducibility to ASF Security, arrange an automated project key with ASF Infrastructure, and register its public half through the committee's `KEYS` management. Then configure the repository secret and ATR compose workflow allowlist. These are one-time onboarding steps; record their completion rather than repeating them for every candidate. Neither this configuration nor the existing crates.io OIDC setup establishes that onboarding is complete.

OpenDAL's [source compose workflow](https://github.com/apache/opendal/blob/af11b6ee9ad1e4df1ff7b1e61156b1f3c4a74357/.github/workflows/release-compose.yml) separates source preparation, signing, and OIDC upload, which is a useful reference for that follow-up. Introduce compose automation after validating a release through ATR, and retain vote and finish as separate release-manager actions. Weekly scheduling, version selection, and automatic lifecycle coordination remain in [#303](https://github.com/apache/asyncband/issues/303).
