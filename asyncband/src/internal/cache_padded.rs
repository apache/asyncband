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

// Use conservative architecture estimates, not a guarantee about every CPU's cache line.
// Keep 128 bytes for large ARM/PowerPC lines and adjacent-line prefetching on x86-64,
// 256 bytes for s390x, and at least 64 bytes elsewhere.
#[cfg_attr(target_arch = "s390x", repr(align(256)))]
#[cfg_attr(
    any(
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
        target_arch = "x86_64",
    ),
    repr(align(128))
)]
#[cfg_attr(
    not(any(
        target_arch = "s390x",
        target_arch = "aarch64",
        target_arch = "arm64ec",
        target_arch = "powerpc64",
        target_arch = "x86_64",
    )),
    repr(align(64))
)]
pub struct CachePadded<T>(T);

impl<T> CachePadded<T> {
    pub const fn new(value: T) -> Self {
        Self(value)
    }
}

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
