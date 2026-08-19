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

//! Admission control policies for bounded asynchronous work.
//!
//! This module provides [`FairShare`], a work-conserving admission policy for
//! workloads partitioned by key. It maintains a fixed number of permits and,
//! when contended, admits work for the key with the fewest permits currently
//! held. Ties are resolved by queue order.
//!
//! Fairness applies to the number of permits held by contending keys. It does
//! not reserve permits for idle keys or account for differences in execution
//! time or work cost.

mod fair_share;
pub use fair_share::FairShare;
pub use fair_share::FairSharePermit;
pub use fair_share::OwnedFairSharePermit;
