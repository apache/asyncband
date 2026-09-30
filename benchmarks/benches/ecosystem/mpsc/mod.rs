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

//! Each 16,384-message batch has 1 or 4 producers and one consumer. Channels and workers persist
//! across samples, including storage reuse; an untimed warm-up precedes measurements. Tasks put
//! both ends on the executor. Native-thread cases receive on the caller thread. Batch coordination
//! and checksum validation are timed; neither fixture times worker creation or destruction.
//!
//! Bounded sends and reservations share capacities 64/1024 and executor shapes. Unbounded burst
//! cases send then drain on one thread, with inline/boxed payloads and an optional retained
//! backlog. Diagnostic probes isolate capacity-one handoff and external receivers. Forced parked 1
//! KiB bursts also validate every sequence number, so their timing includes substantially more
//! harness work.

mod adapters;
mod bounded;
mod reservation;
mod support;
mod unbounded;
