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

//! Lossless broadcast channels.
//!
//! Every active subscription observes every accepted value. Bounded retention applies
//! backpressure at the slowest subscription, while unbounded retention grows until subscriptions
//! advance or are dropped.
//!
//! Unlike a competing `asyncband::spmc` or `asyncband::mpmc` queue, each subscription has
//! independent receive progress. New subscriptions start at the committed tail and observe future
//! publications only. Both retention modes are lossless; there is no lag or overwrite result.
//!
//! A bounded channel enforces the requested capacity as a strict logical limit and backpressures
//! senders until the slowest subscription advances or drops. An unbounded channel grows subject to
//! process memory and reclaims a prefix only after every active subscription advances past it.
//! Pending sends and receives are cancel safe and do not publish or advance a subscription.

mod internal;

pub mod mpmc;
pub mod spmc;
