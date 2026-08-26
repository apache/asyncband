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

use asyncband::once::OnceMap;

pub const CONTENDED_ENTRY_COUNTS: &[usize] = &[64, 1024];
pub const THREAD_COUNTS: &[usize] = &[1, 2, 8, 32];

// The contended get and compute benches share one map across OS threads and spread keys with
// thread_slot_ticket, so "disjoint" means threads mostly touch different keys at any moment rather
// than strict per-thread key ownership.
pub fn preloaded_map(cached_entries: usize) -> OnceMap<usize, usize> {
    (0..cached_entries).map(|key| (key, key)).collect()
}
