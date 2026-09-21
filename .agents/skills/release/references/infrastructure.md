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

# Troubleshoot release services

Start with the failed workflow step or ATR operation, then check the corresponding configuration below.

## Source composition

For a blocked or failed compose run, check the `release` environment approval and the settings required by `.github/workflows/release-compose.yml`. For ATR authentication or key-recognition failures, compare the project's Trusted Publishing settings with `.asf.yaml` and confirm that the signing key is available in the project's [KEYS](https://downloads.apache.org/incubator/asyncband/KEYS). See [ATR Trusted Publishing](https://releases.apache.org/docs/trusted-publishing).

## ATR voting and publication

Check Asyncband's podling status and voting settings in ATR, and compare its synchronized settings with `.asf.yaml`. For publication failures, inspect Finish's result and the destination `https://dist.apache.org/repos/dist/release/incubator/asyncband/${VERSION}/`. Published files propagate to `https://downloads.apache.org/incubator/asyncband/${VERSION}/`. See [ATR publication](https://releases.apache.org/docs/promoting-to-release).

## crates.io publication

Check the final tag's run of `.github/workflows/release.yml` and its `release` environment approval. For an authentication failure, confirm that the crate's Trusted Publisher matches repository `apache/asyncband`, workflow `release.yml`, and environment `release`. See [crates.io Trusted Publishing](https://crates.io/docs/trusted-publishing).
