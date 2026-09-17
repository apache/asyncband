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

//! Broadcast channels grouped by producer topology.
//!
//! [`mpmc`] supports any number of concurrent producers. [`spmc`] is the single-producer
//! specialization: its sender is not [`Clone`] and publish methods require exclusive access
//! (`&mut self`). Receivers drain published slots without taking the publication lock, so a single
//! producer can fan out without serializing every subscription on that lock. Choose `spmc` when
//! the program has one publisher; choose `mpmc` when it does not.
//!
//! Both topologies are fan-out broadcast: every accepted value is delivered to every active
//! subscription. That is a different delivery family from a competing crate-root `spmc` queue,
//! which would give each value to exactly one receiver.

pub mod mpmc;
pub mod spmc;
