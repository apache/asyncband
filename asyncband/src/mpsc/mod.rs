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

//! Multi-producer, single-consumer channels for asynchronous tasks.
//!
//! [`bounded`] applies backpressure once its fixed-capacity buffer fills. [`unbounded`] never waits
//! to send while the receiver is alive, but a slow receiver can cause memory use to grow without a
//! configured limit.

mod bounded;
mod error;
mod unbounded;

pub use crate::mpsc::bounded::BoundedReceiver;
pub use crate::mpsc::bounded::BoundedSender;
pub use crate::mpsc::bounded::Permit;
pub use crate::mpsc::bounded::bounded;
pub use crate::mpsc::error::RecvError;
pub use crate::mpsc::error::SendError;
pub use crate::mpsc::error::TryRecvError;
pub use crate::mpsc::error::TrySendError;
pub use crate::mpsc::unbounded::UnboundedReceiver;
pub use crate::mpsc::unbounded::UnboundedSender;
pub use crate::mpsc::unbounded::unbounded;
