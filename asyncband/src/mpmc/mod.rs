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

//! A multi-producer, multi-consumer queue for sending values between asynchronous tasks.
//!
//! Receivers compete for values: each value accepted by a sender is delivered to exactly one
//! receiver while a receiver remains. Clone a receiver to distribute work across multiple
//! asynchronous tasks. Dropping the final receiver releases any buffered values and makes later
//! attempts to send return their value in an error.

mod bounded;
mod error;
mod queue;
mod unbounded;

pub use crate::mpmc::bounded::BoundedReceiver;
pub use crate::mpmc::bounded::BoundedSender;
pub use crate::mpmc::bounded::Permit;
pub use crate::mpmc::bounded::bounded;
pub use crate::mpmc::error::RecvError;
pub use crate::mpmc::error::SendError;
pub use crate::mpmc::error::TryRecvError;
pub use crate::mpmc::error::TrySendError;
pub use crate::mpmc::unbounded::UnboundedReceiver;
pub use crate::mpmc::unbounded::UnboundedSender;
pub use crate::mpmc::unbounded::unbounded;
