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

# Infrastructure reference

Routine releases assume this setup is complete. Read this reference only for an observed signing, authorization, synchronization, or publication failure.

## Source composition

`.asf.yaml` registers repository `asyncband` and `.github/workflows/release-compose.yml` with ATR through `atr_sync`. It selects email voting on `dev@asyncband.apache.org`, a 72-hour minimum, and download suffix `{{VERSION}}`. ATR must recognize Asyncband as a podling for the two-round vote. Inspect the live project policy when behavior differs from the repository settings.

The `release` environment gates signing/upload. The workflow reads `GPG_SECRET_KEY`, `SOURCE_SIGNING_FINGERPRINT`, and optional `GPG_PASSPHRASE` through GitHub `secrets`; ASF-provisioned values can be repository secrets. The fingerprint is the full uppercase primary fingerprint, not an Actions variable. Inspect names with `gh secret list --repo apache/asyncband` and `gh secret list --repo apache/asyncband --env release`; do not export private-key material for diagnosis.

The automated project key must be present in [KEYS](https://downloads.apache.org/incubator/asyncband/KEYS) and recognized by ATR under the committee's existing key-management mode. Consult [Trusted Publishing setup](https://releases.apache.org/docs/trusted-publishing) and [INFRA-28407](https://issues.apache.org/jira/browse/INFRA-28407) for provisioning issues. Check the actual primary UID and deployed ATR recognition rules if it is not identified as an automated key. Preserve old keys when adding a public key.

Git tags are signed with the release manager's individual key. Source archives are signed with the automated project key. Verify each against its own recorded fingerprint. The upload action authenticates through GitHub OIDC; a personal ATR token is not required for composition.

## Final publication

ATR publishes approved files to `https://dist.apache.org/repos/dist/release/incubator/asyncband/${VERSION}/`; they propagate to `https://downloads.apache.org/incubator/asyncband/${VERSION}/`. Inspect Finish's actual destination and result when diagnosing publication.

crates.io Trusted Publishing is configured for repository `apache/asyncband`, workflow `release.yml`, and environment `release`. Only the final `v${VERSION}` tag enters the publication job, with a separate environment approval and short-lived OIDC token. Consult the workflow and [crates.io documentation](https://crates.io/docs/trusted-publishing) for an authentication failure; do not repeat initial setup for each release.
