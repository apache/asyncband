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

//! Single-producer, multi-consumer queues for distributing work between asynchronous tasks.
//!
//! Enable the `spmc` Cargo feature to use this module. [`bounded`] applies backpressure at its
//! exact capacity; [`unbounded`] sends synchronously and can grow until memory is exhausted.
//! Receivers are cloneable and compete for messages: each accepted message is delivered to one
//! receiver while receivers remain. Values leave the queue in FIFO order, but consumer completion
//! order and an equal distribution of work are not guaranteed.
//!
//! The sender cannot be cloned, and every send operation requires `&mut self`, including for the
//! lifetime of a bounded send future. It can move between tasks, but shared references cannot send.
//! Dropping the sender lets receivers drain buffered messages before observing disconnection.
//! Dropping the last receiver releases buffered messages and makes sending return the unsent value.
//!
//! # Example
//!
//! ```
//! # #[tokio::main]
//! # async fn main() {
//! use asyncband::spmc;
//!
//! let (mut sender, receiver) = spmc::bounded(2);
//! let competing = receiver.clone();
//! sender.send("first").await.unwrap();
//! sender.send("second").await.unwrap();
//! drop(sender);
//!
//! assert_eq!(receiver.recv().await, Ok("first"));
//! assert_eq!(competing.recv().await, Ok("second"));
//! assert_eq!(receiver.recv().await, Err(spmc::RecvError::Disconnected));
//! # }
//! ```
//!
//! # Single-producer capability
//!
//! Neither sender supports cloning:
//!
//! ```compile_fail,E0599
//! let (sender, _receiver) = asyncband::spmc::bounded::<u8>(1);
//! let second_producer = sender.clone();
//! ```
//!
//! ```compile_fail,E0599
//! let (sender, _receiver) = asyncband::spmc::unbounded::<u8>();
//! let second_producer = sender.clone();
//! ```
//!
//! Sending through a shared reference is rejected:
//!
//! ```compile_fail,E0596
//! fn send(sender: &asyncband::spmc::BoundedSender<u8>) {
//!     let _ = sender.try_send(1);
//! }
//! ```
//!
//! ```compile_fail,E0596
//! fn send(sender: &asyncband::spmc::UnboundedSender<u8>) {
//!     let _ = sender.send(1);
//! }
//! ```
//!
//! A bounded send future retains the exclusive borrow until completion or cancellation:
//!
//! ```compile_fail,E0499
//! let (mut sender, _receiver) = asyncband::spmc::bounded(1);
//! let pending = sender.send(1);
//! let _ = sender.try_send(2);
//! drop(pending);
//! ```

mod bounded;
mod unbounded;

pub use self::bounded::BoundedReceiver;
pub use self::bounded::BoundedSender;
pub use self::bounded::bounded;
pub use self::unbounded::UnboundedReceiver;
pub use self::unbounded::UnboundedSender;
pub use self::unbounded::unbounded;
pub use crate::internal::competing_queue::RecvError;
pub use crate::internal::competing_queue::SendError;
pub use crate::internal::competing_queue::TryRecvError;
pub use crate::internal::competing_queue::TrySendError;
