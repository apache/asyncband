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

//! Multi-producer, multi-consumer broadcast channels.
//!
//! Both channels are lossless: every value a channel accepts stays readable by every subscription
//! that was active when it was accepted, so a receive never reports lag. They differ in what a
//! producer does when the slowest subscription stops reclaiming. [`bounded`] retains at most the
//! capacity it was built with and makes producers wait for that subscription. [`unbounded`] never
//! waits to send and lets the retained backlog grow instead.

mod bounded;
mod common;
mod error;
mod unbounded;

pub use self::bounded::BoundedReceiver;
pub use self::bounded::BoundedSender;
pub use self::bounded::bounded;
pub use self::error::RecvError;
pub use self::error::TryRecvError;
pub use self::error::TrySendError;
pub use self::unbounded::UnboundedReceiver;
pub use self::unbounded::UnboundedSender;
pub use self::unbounded::unbounded;
