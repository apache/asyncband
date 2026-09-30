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
use benchmarks::support::thread_slot_ticket;

pub const BATCH_SIZES: &[usize] = &[2, 4, 16];
pub const BATCH_SAMPLE_SIZE: u32 = 64;
pub const CONTENDED_SAMPLE_SIZE: u32 = 64;
pub const FAST_SAMPLE_SIZE: u32 = 256;
pub const THREAD_COUNTS: &[usize] = &[1, 2, 4, 8];

pub type BenchGroup = Group<usize, usize, BuildHasherDefault<DefaultHasher>>;

// A worker has at most one call in flight, so its process-unique slot is sufficient. Reusing
// that key still exercises insert/work/remove because Group does not cache completed values.
pub fn unique_thread_key() -> usize {
    thread_slot_ticket().0
}
