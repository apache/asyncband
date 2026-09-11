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

Candidates are staged under `https://dist.apache.org/repos/dist/dev/incubator/asyncband/`; approved releases are promoted to `https://dist.apache.org/repos/dist/release/incubator/asyncband/`. The public verification key list is `https://downloads.apache.org/incubator/asyncband/KEYS`.

The release manager uses an ASF-associated signing key published in the existing project `KEYS` file. For a new signing key, follow the [ASF release signing guide](https://infra.apache.org/release-signing.html), verify the fingerprint through an independent channel, and add the public key while preserving existing keys. Reuse established distribution areas and signing configuration; investigate a reported access or verification failure before proposing infrastructure changes.
