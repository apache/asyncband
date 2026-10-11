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
//! that was active when it was accepted, so receiving never reports lag. They differ in what a
//! producer does when the slowest subscription stops reclaiming. [`bounded`] retains at most the
//! capacity it was built with and makes producers wait for that subscription. [`unbounded`] never
//! waits to send and lets the retained backlog grow instead.
//!
//! # Delivery and processing
//!
//! Receiving a value advances the subscription before returning that value. The channel tracks
//! unread messages, not application work: retaining a received value or processing it
//! asynchronously does not hold backlog capacity. `recv` and `try_recv` hand out the channel's own
//! `Arc<T>`, so an `Arc` kept past the receive keeps its payload alive after the message has left
//! the backlog, and a bounded channel's capacity limits retained messages rather than memory.
//! Callers that wrapped values in an `Arc` only to avoid broadcast cloning can send `T` directly,
//! since the channel already shares one `Arc<T>` with every receiver. There is no acknowledgement
//! or processing-completion barrier. If `T::clone` panics inside `recv_cloned` or
//! `try_recv_cloned`, that subscription has still advanced past the value.
//!
//! Sending with no subscriptions discards the value and succeeds. A later subscription starts
//! with future publications; it does not replay discarded or previously retained values.
//!
//! Operations briefly acquire internal mutexes. No mutex is held across an await point or while
//! cloning or dropping payloads. The `try_*` methods do not wait for messages or capacity, but may
//! wait to acquire a mutex. Sending a value does not wait for subscribers to receive or process it.

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
