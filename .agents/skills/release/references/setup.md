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

# Release setup

Check existing signing, distribution, and registry settings before making changes. Use this guide when preparing the first release or resolving a setup question.

## Signing and ASF distribution

The release manager needs an ASF-associated GPG key in the project `KEYS` file. Follow the [ASF release signing guide](https://infra.apache.org/release-signing.html), publish the public key, and verify its fingerprint through an independent channel.

Create the project distribution directories before the first release:

```shell
svn mkdir --parents https://dist.apache.org/repos/dist/dev/incubator/asyncband \
  -m "Initialize Apache Asyncband development distribution area"
svn mkdir --parents https://dist.apache.org/repos/dist/release/incubator/asyncband \
  -m "Initialize Apache Asyncband release distribution area"
```

Initialize `KEYS` with the first release manager's public key:

```shell
KEYS_FILE="$(mktemp)"
gpg --armor --export "${ASF_GPG_FINGERPRINT}" > "${KEYS_FILE}"
svn import "${KEYS_FILE}" https://dist.apache.org/repos/dist/release/incubator/asyncband/KEYS \
  -m "Add Apache Asyncband release keys"
rm "${KEYS_FILE}"
```

Add each later release manager's public key to the existing `KEYS` file through the release distribution repository before staging their first candidate. The public verification URL is <https://downloads.apache.org/incubator/asyncband/KEYS>.

## crates.io Trusted Publishing

A crate owner configures [crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing) for `asyncband` with these values:

| Setting           | Value         |
| ----------------- | ------------- |
| Repository owner  | `apache`      |
| Repository name   | `asyncband`   |
| Workflow filename | `release.yml` |
| Environment       | `release`     |

The `release` environment in `.asf.yaml` limits deployments to version tags and requires a configured reviewer. After validating the first OIDC publication, enable **Require trusted publishing for all new versions** in the crate settings.
