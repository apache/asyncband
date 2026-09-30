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

//! Comparisons run identical adapters and scenario parameters for each participating library.
//! `try_send_receive` and `ready_send_receive` are same-thread operation pairs, not message
//! latency. `reused_tasks`/`reused_threads` keep the channel and workers alive across batches.
//! `task_batch`/ `thread_batch` create a fresh fixture outside timing and include close/drain/join
//! in the sample. Each channel throughput counter counts published messages; broadcast also
//! delivers to every subscriber, so its delivery count is messages multiplied by subscriptions.
//!
//! Capacity 64/1024 and small producer/consumer counts are baseline workloads. Capacity-one,
//! forced parked bursts and external-thread receivers live in opt-in `diagnostics` groups.
//! Runtime worker count 0 denotes the current-thread executor; 4 denotes four worker threads.
//! Unbounded sends on one executor thread can finish before consumers run: this is a scheduling
//! baseline, not evidence of parallel contention. Payloads and batch sizes are stated in each case.
//!
//! Use `cargo x bench --bench ecosystem -- --list` or a scenario path filter. Include diagnostics
//! explicitly with `--include-ignored`. Run timings serially and record toolchain/machine/commit;
//! these synthetic workloads do not estimate application throughput or tail latency.

mod broadcast;
mod mpmc;
mod mpsc;
mod spmc;
mod waitgroup;
mod watch;

fn main() {
    divan::main();
}
