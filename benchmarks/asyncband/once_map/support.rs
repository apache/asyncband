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

use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

use asyncband::once::OnceMap;

use crate::support::bench_context;
use crate::support::poll_ready;
use crate::support::thread_slot_ticket;

pub const BATCH_SIZES: &[usize] = &[2, 8, 32];
pub const BATCH_SAMPLE_SIZE: u32 = 64;
pub const CONTENDED_SAMPLE_SIZE: u32 = 256;
pub const FAST_SAMPLE_SIZE: u32 = 256;
pub const HOT_ENTRY_COUNTS: &[usize] = &[64, 1024];
pub const NONEMPTY_ENTRY_COUNTS: &[usize] = &[1, 64, 1024];
pub const READY_ENTRY_COUNTS: &[usize] = &[0, 64, 1024];
pub const THREAD_COUNTS: &[usize] = &[1, 2, 8, 32];

type BenchHasher = BuildHasherDefault<DefaultHasher>;

pub type BenchMap = OnceMap<usize, usize, BenchHasher>;

pub fn ready_map(ready_entries: usize) -> BenchMap {
    let map = BenchMap::default();
    let mut context = bench_context();
    for key in 0..ready_entries {
        poll_ready(map.compute(key, || async move { key }), &mut context);
        assert_eq!(map.get(&key), Some(key));
    }
    map
}

pub fn distributed_ready_key(ready_entries: usize) -> usize {
    const THREAD_SLOTS: usize = 32;

    let (slot, ticket) = thread_slot_ticket();
    (slot + ticket * THREAD_SLOTS) % ready_entries
}

pub fn distributed_absent_key(ready_entries: usize) -> usize {
    const KEYSPACE_SIZE: usize = 1 << 16;
    const THREAD_SLOTS: usize = 32;

    let (slot, ticket) = thread_slot_ticket();
    ready_entries + 1 + (slot + ticket * THREAD_SLOTS) % KEYSPACE_SIZE
}
