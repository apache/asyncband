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

# Example Style

- Start each example with concise module documentation describing the scenario and the behavior it demonstrates.
- Omit run commands, basic Cargo instructions, and comments that merely restate the code. Assume readers know how to run a Rust example.
- Keep each example focused on a small, coherent scenario. Explain relevant semantic differences beside the code; avoid catalogs of unrelated primitives.
- For migration examples, show how the same application scenario works before and after the change, including any protocol changes or limits. Do not substitute a tour of primitive APIs for a migration.
- Keep example explanations in the example source. The repository README only needs a concise entry point.
