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

//! Reusable signals for coordinating tasks without carrying a value.
//!
//! An [`AutoResetEvent`] releases one registered wait per assigned signal. With no queued waits,
//! it retains at most one signal, coalescing further sets. A [`ManualResetEvent`] releases all
//! registered waits and remains set until explicitly reset.
//!
//! Both types retain state, unlike a condition variable's unbuffered notifications. Use a
//! semaphore when unused permits must accumulate, or a watch channel when each receiver needs to
//! observe state changes independently.

mod auto_reset;
mod manual_reset;

pub use self::auto_reset::AutoResetEvent;
pub use self::manual_reset::ManualResetEvent;
