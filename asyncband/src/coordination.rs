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

//! Higher-level coordination protocols built from synchronization primitives.
//!
//! Coordination protocols express application-level relationships rather than direct access to a
//! protected value:
//!
//! * [shutdown] coordinates shutdown initiation, participation, and observation.
//! * [singleflight] coalesces concurrent work for the same key.
//!
//! Keeping these protocols outside [crate::sync] leaves the synchronization group focused on
//! mutexes, notifications, permits, and one-time initialization.

#[cfg(feature = "shutdown")]
pub use crate::shutdown;
#[cfg(feature = "singleflight")]
pub use crate::singleflight;
