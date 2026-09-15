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

## CI source signing

`.github/workflows/source.yml` prepares a source archive and optionally signs it in one job. Pull requests and default manual runs produce unsigned artifacts. Signing runs only on an explicit manual dispatch from `apache/asyncband`'s `main` branch with `sign=true`; the secret is passed to that step. Packaging and signing share one trusted environment. The signing step imports the Infra-managed key into a temporary GPG home, checks the primary fingerprint, signs the expected source archive, and verifies the signature. The workflow uploads the resulting bundle once as a GitHub Actions artifact. It has read-only repository access and no OIDC permission; it does not stage, vote, create tags, or publish.

### Provisioning

Request a project signing key from ASF Infra and submit this workflow plus reproducibility evidence to ASF Security. Obtain workflow approval before enabling signing. Follow the [ASF automated release signing procedure](https://infra.apache.org/release-signing.html#automated-release-signing). Infra generates and installs the private key; project members must not export a personal key or request delivery of this private key.

| Configuration                | Value or owner                                                                                                                |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| Repository                   | `apache/asyncband`                                                                                                             |
| Repository secret            | `GPG_SECRET_KEY`, installed by Infra; the signing command expects the Infra key to have an empty passphrase                     |
| Repository variable          | `SOURCE_SIGNING_FINGERPRINT`, the full uppercase primary fingerprint supplied by Infra                                          |
| Public key                   | Add to the existing Asyncband `KEYS`, preserving other keys and the current KEYS management mode                                |
| Suggested primary UID        | `Apache Asyncband Automated Release Signing <private@asyncband.apache.org>`; confirm the committee association with Infra/Tooling |

Once approval, provisioning, and public-key publication are complete, the release manager opens GitHub Actions and manually runs **Source candidate** on `main` with **sign** enabled. Record the run URL, full source commit, checksum, and signing fingerprint. Download the `signed-source` artifact and retain the archive, checksum, and signature as the candidate bytes. A retry should reuse an existing signed bundle when available. This workflow has no schedule; ATR upload is a separate integration to add after key provisioning.

### Reproduce before staging and voting

The release manager must reproduce the source archive on trusted hardware outside GitHub Actions, using a fresh checkout of the exact commit printed in the workflow summary. Compare against the actual downloaded archive, not merely its checksum file. Git produces the tar.gz archive with compression level 9 and a fixed archive umask; `shasum` produces the checksum and `cmp` compares the archive bytes. Run the following from that clean checkout, after setting `DOWNLOADED_ARCHIVE` to the absolute path of the downloaded `.tar.gz`:

```shell
set -eu
git diff --quiet HEAD --
RELEASE_COMMIT="$(git rev-parse --verify 'HEAD^{commit}')"
VERSION="$(sed -n 's/^version = "\(.*\)"$/\1/p' asyncband/Cargo.toml)"
SOURCE_DIR="apache-asyncband-${VERSION}-incubating-src"
REPRO_DIR="$(mktemp -d "${TMPDIR:-/tmp}/asyncband-reproduce.XXXXXX")"
git -c tar.umask=0022 archive --format=tar.gz -9 \
  --prefix="${SOURCE_DIR}/" \
  --output="${REPRO_DIR}/${SOURCE_DIR}.tar.gz" "${RELEASE_COMMIT}"
cmp "${REPRO_DIR}/${SOURCE_DIR}.tar.gz" "${DOWNLOADED_ARCHIVE}"
shasum -a 512 "${DOWNLOADED_ARCHIVE}"
git --version
```

`cmp` fails on any byte difference, including gzip headers or compression differences. Do not accept merely equivalent extracted contents as evidence for automated signing. If the output differs, check the source commit, Git versions, compression settings, and Git configuration before accepting the candidate. Do not use local Git attribute or archive-compressor overrides in the reproduction checkout. Untracked files are not part of `git archive`; tracked edits are rejected.

After staging, download the candidate again and repeat the comparison and signature verification against the published `KEYS`. Record the candidate revision, source commit, SHA-512, tool versions, and result in the vote evidence. Reviewers independently reproduce these same bytes on trusted hardware before publication. CI generation, a valid automated signature, and a passing build do not replace this check. The existing source-content, build, licensing, PPMC/IPMC voting, and final publication requirements still apply. No compiled binaries or crates.io packages are signed by this workflow.
