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

# Contributing to Apache Asyncband (Incubating)

Contributions of all kinds are welcome, including bug reports, benchmark cases, documentation improvements, code reviews, and concurrency primitive implementations.

## Communication and governance

* **Community guide and governance**: See [asyncband.apache.org/community](https://asyncband.apache.org/community/) for mailing lists, project roster, consensus and Apache voting rules, and how contributors become committers and PPMC members.
* **Design and roadmap discussions**: Discuss new primitives, API contracts, and roadmap slices on [`dev@asyncband.apache.org`](https://lists.apache.org/list.html?dev@asyncband.apache.org) ([subscribe](mailto:dev-subscribe@asyncband.apache.org)), [GitHub Issues](https://github.com/apache/asyncband/issues), or [GitHub Discussions](https://github.com/apache/asyncband/discussions).
* **Project scope**: Reusable executor-independent coordination mechanisms belong in the library; task spawning, timers, retries, and runtime scheduling policy remain with the caller.

## Workflow and conventions

Issues are not assigned or reserved; anyone can open a pull request directly for review. Follow [AGENTS.md](AGENTS.md) for Rust module and import style, waker contracts, public rustdoc structure, and changelog rules, and run the workspace checks before submitting:

```shell
cargo x lint
cargo x check
cargo x test
```

## Security

Do not report suspected security vulnerabilities in public issues or mailing lists. Report them privately to [security@apache.org](mailto:security@apache.org) with `Asyncband` in the subject line; see [SECURITY-THREAT-MODEL.md](SECURITY-THREAT-MODEL.md) for scope details.
