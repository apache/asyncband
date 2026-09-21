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

# Operate the release in ATR

Use the [Asyncband project](https://releases.apache.org/projects/asyncband) with the release manager's ASF login. The normal interface is the ATR website. The repository configures email voting; voters reply on the mailing lists and the release manager reviews the tally in ATR. Do not switch to Trusted Vote mode as an incidental release step.

## Start the vote

1. Open the uploaded `${VERSION}` draft in Compose. Confirm the revision and checksum recorded in the tracking issue, inspect ATR's checks, and complete the candidate verification and artifact license review. Resolve blockers and review any concerns before acknowledging them. If ATR's commit field is empty, enter the frozen `RELEASE_COMMIT` using its commit-hash form.
2. Open the voting form. Set the first-round recipient to `dev@asyncband.apache.org`, the second-round recipient to `general@incubator.apache.org`, and the minimum duration to at least 72 hours. The second round uses that duration too.
3. Review the generated subject and body. Identify Apache Asyncband (Incubating), `${VERSION}`, and the RC; include the ATR candidate/revision, RC tag and commit, source checksum, KEYS and signer information, changelog, and verification evidence. Link ATR as the location of the voted files. Submit **Send vote email** when sending the vote is authorized, then record its archive link and closing time.

The automatic publication option is optional. Enable it only when publication after successful completion of both rounds is within the authorized scope, and confirm the download suffix is `${VERSION}`. For podlings, ATR carries this setting into round two and publishes only after that round passes; it does not publish on PPMC approval alone. Otherwise publish manually in Finish.

## Resolve both rounds

Each round needs at least 72 hours, at least three eligible `+1` votes, and more eligible `+1` than `-1` votes. Eligibility is PPMC membership for round one and IPMC membership for round two. Allow at least six days for the two sequential votes. See the [Incubator vote rules](https://incubator.apache.org/cookbook/#two-phase-vote-on-podling-releases).

1. After the PPMC period, open the vote resolution page. Compare ATR's email tally with the thread, including voters' roles and any carried IPMC votes, and review the result body. Select `Passed` only if the requirements are met, then resolve the vote.
2. ATR sends the PPMC result and automatically starts the IPMC vote on the selected second-round list. Confirm delivery and record both links. Check the actual IPMC email for the PPMC result, its `lists.apache.org` tally link, and any IPMC votes carried from round one. ATR currently regenerates this email from the project template without adding that evidence; if missing, have the release manager supplement the existing IPMC thread. Do not start another vote. The files stay in the same candidate revision.
3. After the IPMC period, review its binding tally, including eligible votes carried from round one and counting only each voter's latest vote, and review the result body before resolving it as `Passed`. ATR sends the result, also reports the passing result to the first-round thread, and moves the release to Finish. Record both vote results before final publication.

These transitions are implemented for email votes in ATR's [vote resolution](https://github.com/apache/tooling-trusted-releases/blob/0d156e9a/atr/storage/writers/vote.py#L676). A recorded state change and an email delivery can succeed or fail separately; check both before reporting completion.

## Publish and announce

In Finish, inspect **Publish to ASF Distribution Area**. If automatic publication already completed, record its result. Otherwise confirm the approved revision and destination `dist/release/incubator/asyncband/${VERSION}/`, then use the publish action. ATR commits the voted files to SVN; record the SVN revision and compare the published archive, signature, and checksum with the candidate. No local rebuild, re-signing, or `svn move` is needed.

Return to the main runbook to push the final tag and verify crates.io publication. Once both the ASF downloads and the crate are available, use **Announce** in ATR, review the recipients and message, and submit it. ATR sends the announcement and updates its release catalog. Record the announcement link in the tracking issue instead of sending the same email manually. See [ATR publication](https://releases.apache.org/docs/promoting-to-release).

Use ATR's archive action or the configured auto-archive option for superseded releases. Confirm the archive and distribution results; follow up on any cleanup warning rather than assuming a catalog entry proves completion.

## API and CLI alternative

ATR has a [CLI](https://github.com/apache/tooling-releases-client) and authenticated API. The CLI exposes `atr vote start`, `atr vote tabulate`, `atr vote resolve`, and `atr announce`; the server exposes `/api/vote/start`, `/api/vote/tabulate`, `/api/vote/resolve`, and `/api/release/announce`. See the [CLI command reference](https://github.com/apache/tooling-releases-client/blob/main/COMMANDS.md) and [server API](https://github.com/apache/tooling-trusted-releases/blob/0d156e9a/atr/api/__init__.py).

Use the website for the ordinary handoff: it exposes the current tally, editable result email, and publication state together. The CLI/API are evolving, and the current API's vote resolution sends only a short generic result body. If automation is explicitly requested, check the installed client's help and current API schema and use existing authorized credentials; do not invent endpoints or introduce vote/finish workflows as part of a routine release. Our registered GitHub workflow handles composition only.

## Recover without replacing voted files

- Failed or uncertain upload: inspect ATR before retrying. If all three files arrived and verification succeeds, record that revision even if the GitHub run reported a late failure. Otherwise retry before voting and verify the resulting revision; signing/upload can produce a new signature and revision. If the GitHub source bundle expired, rerun composition from the same RC and compare the recorded checksum.
- Source or workflow correction: use a new reviewed commit and refresh affected checks. If an RC already exists, use a new RC number. A workflow rerun uses the old tagged workflow, so changing `main` cannot repair it.
- Rejected or cancelled vote: resolve that outcome in ATR, which returns the release to Compose. Preserve the previous tag and vote history in the issue, then prepare and verify the replacement candidate. Do not modify an active vote's files.
- Vote or publication error: inspect the current round, mail-delivery status, Finish state, and destination files before retrying. Resume a completed transition; do not resend an IPMC vote or republish matching files merely to obtain a green status.
