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

pub const CONTENDED_ENTRY_COUNTS: &[usize] = &[64, 1024];
pub const CONTENDED_SAMPLE_SIZE: u32 = 256;
pub const CONTENDED_THREAD_SLOTS: usize = 32;
pub const THREAD_COUNTS: &[usize] = &[1, 2, 8, 32];

type BenchHasher = BuildHasherDefault<DefaultHasher>;

pub type BenchMap = OnceMap<usize, usize, BenchHasher>;

pub fn cached_map(cached_entries: usize) -> BenchMap {
    let map = BenchMap::default();
    let mut context = bench_context();
    for key in 0..cached_entries {
        poll_ready(map.compute(key, || async move { key }), &mut context);
        assert_eq!(map.get(&key), Some(key));
    }
    map
}
