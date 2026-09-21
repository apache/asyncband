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

Consult this reference when configuring or troubleshooting automated source composition, arranging a new signing key, or diagnosing publication. Routine releases reuse the established settings.

## ATR source composition

`.github/workflows/release-compose.yml` handles upstream `vX.Y.Z-rc.N` pushes. `.asf.yaml` enables `atr_sync` and registers repository `asyncband` and compose workflow `.github/workflows/release-compose.yml`. It sets email voting to `dev@asyncband.apache.org`, a 72-hour minimum, and download path suffix `{{VERSION}}`. Confirm the live ATR project has imported those settings; repository configuration alone does not establish successful synchronization.

The `release` environment reviews the signing/upload job. ASF-provisioned secrets may be repository secrets; the workflow reads all signing settings through `secrets`, including the fingerprint:

| Secret                       | Value                                                                |
| ---------------------------- | -------------------------------------------------------------------- |
| `GPG_SECRET_KEY`             | ASCII-armored automated project private key provisioned by ASF Infra |
| `SOURCE_SIGNING_FINGERPRINT` | Full uppercase primary-key fingerprint (40 or 64 hexadecimal digits) |
| `GPG_PASSPHRASE`             | Passphrase, only when the supplied key is protected                  |

Inspect secret names with `gh secret list --repo apache/asyncband` and `gh secret list --repo apache/asyncband --env release`; these commands do not reveal values. `SOURCE_SIGNING_FINGERPRINT` is a secret, not an Actions variable. Never print or export the private key while diagnosing a failure. A missing setting or incorrect fingerprint prevents signing before any ATR upload.

The automated key setup is tracked in [INFRA-28407](https://issues.apache.org/jira/browse/INFRA-28407). Follow the [ATR Trusted Publishing setup](https://releases.apache.org/docs/trusted-publishing) for reproducibility approval, key provisioning, and recognition of the automated project identity. Confirm the public key is present in the project's published `KEYS` file and imported into ATR through the committee's configured KEYS management. The upload job uses GitHub OIDC; it does not require a personal ATR token. A successful upload still needs ATR artifact checks and candidate verification before voting.

The release manager signs Git tags with an individual key. The automated project key signs source archives. Record and verify the two fingerprints independently; neither is inferred from the other.

## crates.io Trusted Publishing

Trusted Publishing is already configured for `asyncband`: repository `apache/asyncband`, workflow `release.yml`, and environment `release`. Routine releases reuse this configuration.

`.github/workflows/release.yml` obtains a short-lived crates.io token through GitHub OIDC. Only a final `v${VERSION}` tag can enter the publish job; RC tags run package checks. `.asf.yaml` configures version-tag deployments and required reviewers for the `release` environment. Source composition and crates.io publication are separate environment approvals. Consult those repository files and the live crate settings when diagnosing a mismatch. See the [crates.io Trusted Publishing documentation](https://crates.io/docs/trusted-publishing) for changes to this setup.

## ASF distribution and public keys

Candidates are staged in [ATR](https://releases.apache.org/projects/asyncband). After PPMC and IPMC approval, ATR's finish phase publishes the voted revision to `https://dist.apache.org/repos/dist/release/incubator/asyncband/${VERSION}/`, using the configured download suffix. Inspect the destination before publication and record the resulting SVN revision. The public download location is `https://downloads.apache.org/incubator/asyncband/${VERSION}/`; the public verification key list is `https://downloads.apache.org/incubator/asyncband/KEYS`.

Follow the committee's existing [KEYS management mode](https://releases.apache.org/docs/promoting-to-release#the-keys-file) rather than introducing a second source of truth. Preserve existing keys when adding one, and independently verify its fingerprint. Legacy candidates already staged under `dist/dev` retain their recorded artifacts and vote links; an infrastructure migration is not a reason to rebuild an existing candidate.
