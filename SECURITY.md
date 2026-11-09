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

# Security Policy

Apache Asyncband (Incubating) is an in-process Rust concurrency library. The [security threat model](SECURITY-THREAT-MODEL.md) describes its security boundary and the responsibilities of applications that use it.

## Reporting a vulnerability

Report suspected vulnerabilities privately to [security@apache.org](mailto:security@apache.org), with "Asyncband" in the subject. Do not disclose an unpatched vulnerability in a public issue, pull request, GitHub Discussion, or the development mailing list. If you are unsure whether an issue has security impact, report it privately so maintainers can assess it with you.

Send a plain-text report containing:

- The affected Asyncband version or commit, enabled Cargo features, Rust version, target platform, and executor if relevant.
- A description and minimal reproducer, including the concurrent operations or cancellation sequence needed to trigger the issue.
- The observed result, expected behavior, and potential impact. Describe any attacker-controlled input or scheduling influence you have identified; a working exploit is not required to report a concern.

For ordinary bugs and feature requests, use the [public issue tracker](https://github.com/apache/asyncband/issues). Follow the [ASF security reporting guidance](https://www.apache.org/security/#reporting-a-vulnerability) for vulnerabilities in ASF infrastructure or other Apache projects.

## Handling reports

The Asyncband PPMC is responsible for coordinating investigation and fixes with the reporter and the Apache Security Team. Reports sent to the ASF security address are routed to the project privately. The PPMC should identify a maintainer to coordinate each report and keep the Apache Security Team involved throughout the investigation.

Maintainers follow the [ASF vulnerability handling process](https://www.apache.org/security/committers.html): acknowledge and assess the report privately, determine affected versions, coordinate a fix and any advisory or CVE with the Apache Security Team, and agree on disclosure with the reporter. Accepted vulnerabilities are disclosed with remediation guidance after, or together with, the release that addresses them. Reports classified as ordinary bugs should be moved to public discussion only after that assessment has been communicated to the reporter.

## Versions and fixes

Use the latest ASF-approved release. Reports concerning older releases or development code are also welcome; include whether the issue is reproducible on the latest release if you can check safely. Fixes are normally delivered in a new ASF-approved release. The PPMC evaluates backports case by case; this policy does not promise maintenance of every older release line or a fixed response or release deadline. Each advisory identifies the affected versions and available remediation.
