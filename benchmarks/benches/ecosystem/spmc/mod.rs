// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! One producer moves its sender into a task; consumers compete until disconnection. Runtime,
//! channel, and task creation are excluded from timing; data operations and task completion are
//! included. Synchronous unbounded sends can finish before consumers run on a current-thread
//! executor, so the four-worker cases measure concurrent consumer contention.
//!
//! Compile with `cargo x bench --no-run`, then run the ecosystem executable with
//! `--bench --color never --sample-count 100 'spmc::'`. Record repeated measurements serially,
//! without concurrent builds or tests, together with the commit, toolchain, and machine details.

mod bounded;
mod unbounded;
