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

Open the [Asyncband project](https://releases.apache.org/projects/asyncband) with the release manager's ASF login. Use email voting: voters reply on the mailing lists and the release manager reviews the tally in ATR.

Fill the tracking issue's ATR vote page and communication links as each step completes. Use `lists.apache.org` permalinks for the PPMC and IPMC vote threads, their result messages, and the announcement. Each result link should open the message containing the outcome and tally.

## Start the vote

1. Open the uploaded `${VERSION}` draft in Compose. Confirm the revision and checksum recorded in the tracking issue, inspect ATR's checks, and complete the candidate verification and artifact license review. Resolve blockers and review any concerns before acknowledging them. If ATR's commit field is empty, enter the frozen `RELEASE_COMMIT` using its commit-hash form.
2. Open the voting form. Set the first-round recipient to `dev@asyncband.apache.org`, the second-round recipient to `general@incubator.apache.org`, and the minimum duration to at least 72 hours. The second round uses that duration too.
3. Review the generated subject and body. The vote templates in `.asf.yaml` supply the ATR candidate page and revision, the commit, the manifest with checksums, the changelog, `KEYS`, and the source signing key; add the RC to the subject, the RC tag and its signer, and the verification evidence. Change those templates in `.asf.yaml` rather than in ATR's project settings, since synchronization overwrites the settings. Write the body as plain text for the mailing lists: no Markdown or backticks, prose wrapped near 72 characters, each label and its URL on separate lines, and no line break inside a word or checksum. Submit **Send vote email** when sending the vote is authorized, then record its archive link and closing time.
4. Cast your own first-round vote with the checks you performed, either on the ATR vote page or by replying in the thread. A release manager has no implicit `+1`, so only an explicit vote counts towards the round. See the [ASF voting process](https://www.apache.org/foundation/voting.html).

If publication is authorized, automatic publication can publish the source after IPMC approval; confirm the download suffix is `${VERSION}` when selecting it. Otherwise, publish in Finish after both votes pass.

## Resolve both rounds

Each round needs at least 72 hours, at least three eligible `+1` votes, and more eligible `+1` than `-1` votes. Eligibility is PPMC membership for round one and IPMC membership for round two. Allow at least six days for the two sequential votes. See the [Incubator vote rules](https://incubator.apache.org/cookbook/#two-phase-vote-on-podling-releases). ATR neither resolves an email vote nor sends a reminder when it ends, so resolve each round after its closing time.

1. After the PPMC period, open the vote resolution page. Compare ATR's email tally with the thread, including voters' roles and any carried IPMC votes, and review the result body. Select `Passed` only if the requirements are met, then resolve the vote.
2. ATR sends the PPMC result and automatically starts the IPMC vote on the selected second-round list. Confirm delivery and record both links. ATR renders that message from the project's stored vote template rather than from the first round's message, so candidate details written by hand do not carry over. Check that the IPMC thread includes the PPMC result, its `lists.apache.org` tally link, and any IPMC votes carried from round one; supplement that thread with any missing evidence.
3. After the IPMC period, review its binding tally, including eligible votes carried from round one and counting only each voter's latest vote, and review the result body before resolving it as `Passed`. ATR sends the result, also reports the passing result to the first-round thread, and moves the release to Finish. Record both vote results before final publication.

## Publish and announce

In Finish, inspect **Publish to ASF Distribution Area**. If automatic publication already completed, record its result. Otherwise, confirm the approved revision and destination `dist/release/incubator/asyncband/${VERSION}/`, then use the Publish action. ATR commits the voted files to SVN; record the SVN revision and compare the published archive, signature, and checksum with the candidate.

Return to the main runbook to push the final tag and verify crates.io publication. Once both the ASF downloads and the crate are available, use **Announce** in ATR, review the recipients and message, and submit it. ATR sends the announcement and updates its release catalog. Record the announcement link in the tracking issue. See [ATR publication](https://releases.apache.org/docs/promoting-to-release).

Use ATR's archive action or the configured auto-archive option for superseded releases. Confirm archival and removal from active downloads, and resolve any cleanup warnings.

## API and CLI alternative

Use the website to review the tally, edit emails, and publish. For scripted operations, consult the [CLI](https://github.com/apache/tooling-releases-client) and [server API](https://github.com/apache/tooling-trusted-releases/blob/0d156e9a/atr/api/__init__.py). Check the client's help, current API schema, and generated vote/result messages against the steps above.

## Recover without replacing voted files

- Failed or uncertain upload: inspect ATR before retrying. If all three files arrived and verification succeeds, record that revision even if the GitHub run reported a late failure. Otherwise, retry before voting and verify the resulting revision; signing/upload can produce a new signature and revision. When the cause is outside the repository, such as a signing key or an ATR setting, rerun the failed job in the same run once it is resolved: the composed source bundle is reused, a `release` environment reviewer approves it again, and GitHub accepts reruns for 30 days. If the GitHub source bundle expired, rerun composition from the same RC and compare the recorded checksum.
- Source or workflow correction: use a new reviewed commit and refresh affected checks. If an RC already exists, use a new RC number. A workflow rerun uses the old tagged workflow, so changing `main` cannot repair it.
- Rejected or cancelled vote: resolve that outcome in ATR, which returns the release to Compose. Preserve the previous tag and vote history in the issue, then prepare and verify the replacement candidate. Do not modify an active vote's files.
- Vote or publication error: inspect the current round, mail-delivery status, Finish state, and destination files before retrying. Resume a completed transition; do not resend an IPMC vote or republish matching files merely to obtain a green status.
