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

//! Runtime-agnostic channels for asynchronous tasks.
//!
//! The module separates queue topology from delivery policy:
//!
//! * [oneshot] transfers one value once.
//! * [spsc], [mpsc], [spmc], and [mpmc] are competing-consumer queues.
//! * [broadcast] provides multicast retention policies.
//! * [watch] keeps only the latest value.
//! * [disruptor] provides bounded multicast rings with explicit sequencing.
//!
//! # Example
//!
//! ~~~
//! use std::num::NonZeroUsize;
//!
//! use asyncband::channel::mpsc;
//!
//! let (tx, mut rx) = mpsc::bounded(NonZeroUsize::new(16).unwrap());
//! pollster::block_on(async {
//!     tx.send("hello").await.unwrap();
//!     assert_eq!(rx.recv().await, Ok("hello"));
//! });
//! ~~~

#[cfg(feature = "broadcast")]
pub mod broadcast;
#[cfg(feature = "disruptor")]
pub mod disruptor;
mod error;
#[cfg(feature = "queue")]
pub mod mpmc;
#[cfg(feature = "queue")]
pub mod mpsc;
#[cfg(feature = "oneshot")]
pub mod oneshot;
#[doc(hidden)]
#[cfg(feature = "queue")]
pub mod queue;
#[cfg(feature = "queue")]
pub mod spmc;
#[cfg(feature = "queue")]
pub mod spsc;
#[cfg(any(
    feature = "broadcast",
    feature = "disruptor",
    feature = "queue",
    feature = "watch",
))]
mod wait;
#[cfg(feature = "watch")]
pub mod watch;

pub use error::FullBehavior;
pub use error::RecvError;
pub use error::SendError;
pub use error::SendOutcome;
pub use error::TryRecvError;
pub use error::TrySendError;

#[cfg(all(
    test,
    feature = "broadcast",
    feature = "disruptor",
    feature = "oneshot",
    feature = "queue",
    feature = "watch",
))]
mod tests;
