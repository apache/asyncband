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

use std::hash::BuildHasherDefault;
use std::hash::DefaultHasher;

use asyncband::singleflight::Group;

use crate::support::thread_slot_ticket;

pub const BATCH_SIZES: &[usize] = &[2, 8, 32];
pub const BATCH_SAMPLE_SIZE: u32 = 64;
pub const CONTENDED_SAMPLE_SIZE: u32 = 64;
pub const FAST_SAMPLE_SIZE: u32 = 256;
pub const THREAD_COUNTS: &[usize] = &[1, 2, 8, 32];

pub type BenchGroup = Group<usize, usize, BuildHasherDefault<DefaultHasher>>;

pub fn unique_thread_key() -> usize {
    const THREAD_SLOTS: usize = 64;

    let (slot, ticket) = thread_slot_ticket();
    ticket.wrapping_mul(THREAD_SLOTS).wrapping_add(slot)
}
