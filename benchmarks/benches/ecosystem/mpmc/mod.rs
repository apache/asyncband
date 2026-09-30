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

//! Competing consumers drain each 16,384-message batch until producer disconnection. Threads and
//! tasks use the same topologies and validate aggregate count/checksum; no consumer has a quota.
//! Runtime/channel/worker creation is outside timing; start, transfer, close, drain and join are
//! in. Bounded capacity is 64. Topology names state producers and consumers; 8x8 is the contention
//! end. Threads use each peer's blocking API; Asyncband drives futures with its blocking bridge.

mod bounded;
mod unbounded;
