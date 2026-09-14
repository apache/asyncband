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

//! Single-producer, multi-consumer broadcast channels.
//!
//! Both channels are lossless: every value a channel accepts stays readable by every subscription
//! that was active when it was accepted, so a receive never reports lag. They differ in what the
//! producer does when the slowest subscription stops reclaiming. [`bounded`] retains at most the
//! capacity it was built with and makes the producer wait for that subscription. [`unbounded`]
//! never waits to send and lets the retained backlog grow instead.
//!
//! The sender is not [`Clone`], and every publish method takes `&mut self`. That is the static
//! single-writer contract this topology adds over [`crate::broadcast::mpmc`]: there cannot be a
//! second producer, at compile time. `&Sender` can still be shared for [`subscribe`] and the
//! inspection methods. The family is offered for that exclusive-send API. Measured against
//! `async-broadcast` and `tokio::sync::broadcast`, tight bounded wait (capacity 1) is competitive;
//! it is not a general throughput upgrade over 1-producer `broadcast::mpmc` or over Tokio's
//! non-blocking ring.
//!
//! This is fan-out broadcast, not a competing queue: every accepted value is delivered to every
//! active subscription. A competitive `asyncband::spmc` queue would give each value to exactly one
//! receiver.
//!
//! # Delivery and processing
//!
//! A receive advances its subscription before returning the value. The channel tracks unread
//! messages, not application work: retaining a received value or processing it asynchronously
//! does not hold backlog capacity. There is no acknowledgement or processing-completion barrier.
//! If cloning a received value panics, that subscription has still advanced past the value.
//!
//! Sending with no subscriptions discards the value and succeeds. A later subscription starts
//! with future publications; it does not replay discarded or previously retained values.
//!
//! Operations briefly acquire internal mutexes. No mutex is held across an await point or while
//! cloning or dropping payloads. The `try_*` methods do not wait for messages or capacity, but may
//! wait to acquire a mutex. Sending a value does not wait for subscribers to receive or process it.
//!
//! # Subscribing while a bounded send is waiting
//!
//! [`BoundedSender::send`] holds `&mut self` for the duration of the wait, so the sender cannot
//! [`BoundedSender::subscribe`] until that send completes. Create the extra subscription from a
//! live receiver with [`BoundedReceiver::resubscribe`] instead; see the example on
//! [`BoundedReceiver::resubscribe`].
//!
//! [`subscribe`]: BoundedSender::subscribe

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
