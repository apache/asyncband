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

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

//! `asyncband` is a runtime-agnostic library providing essential synchronization primitives for
//! asynchronous Rust programming. The library offers a collection of well-tested, efficient
//! synchronization tools that work with any async runtime.
//!
//! # Migrating from MEA
//!
//! Asyncband continues the project formerly published as `mea`. The old crate is deprecated and no
//! compatibility crate or re-export is provided. Replace the `mea` dependency with `asyncband` and
//! update `mea::` paths to `asyncband::`.
//!
//! # Cargo features
//!
//! The crate enables no primitives by default. Each public module has a same-named opt-in feature,
//! so applications only compile the primitives they use:
//!
//! ```toml
//! asyncband = { version = "0.7", features = ["mutex", "oneshot"] }
//! ```
//!
//! Features that build on other primitives enable those dependencies automatically. For example,
//! `condvar` enables `mutex`, while `shutdown` enables `latch` and `waitgroup`.
//!
//! # Runtime Agnostic
//!
//! All synchronization primitives in this library are runtime-agnostic, meaning they can be used
//! with any async runtime like tokio, async-std, or others. This makes the library highly versatile
//! and portable.
//!
//! # Thread Safety
//!
//! Asyncband primitives and guards implement `Send` and `Sync` only when the protected or
//! transferred value satisfies the necessary bounds. In particular, owned read guards that may move
//! destruction to another thread require the protected value to be `Send` as well as `Sync`. See
//! each type's documentation for its exact bounds.
#[cfg(any(
    feature = "admission",
    feature = "barrier",
    feature = "broadcast",
    feature = "condvar",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "rwlock",
    feature = "semaphore",
    feature = "singleflight",
    feature = "waitgroup",
))]
mod internal;

#[cfg(feature = "admission")]
#[cfg_attr(docsrs, doc(cfg(feature = "admission")))]
pub mod admission;
#[cfg(feature = "atomicbox")]
#[cfg_attr(docsrs, doc(cfg(feature = "atomicbox")))]
pub mod atomicbox;
#[cfg(feature = "barrier")]
#[cfg_attr(docsrs, doc(cfg(feature = "barrier")))]
pub mod barrier;
#[cfg(feature = "broadcast")]
#[cfg_attr(docsrs, doc(cfg(feature = "broadcast")))]
pub mod broadcast;
#[cfg(feature = "condvar")]
#[cfg_attr(docsrs, doc(cfg(feature = "condvar")))]
pub mod condvar;
#[cfg(feature = "latch")]
#[cfg_attr(docsrs, doc(cfg(feature = "latch")))]
pub mod latch;
#[cfg(feature = "mpsc")]
#[cfg_attr(docsrs, doc(cfg(feature = "mpsc")))]
pub mod mpsc;
#[cfg(feature = "mutex")]
#[cfg_attr(docsrs, doc(cfg(feature = "mutex")))]
pub mod mutex;
#[cfg(feature = "once")]
#[cfg_attr(docsrs, doc(cfg(feature = "once")))]
pub mod once;
#[cfg(feature = "oneshot")]
#[cfg_attr(docsrs, doc(cfg(feature = "oneshot")))]
pub mod oneshot;
#[cfg(feature = "rwlock")]
#[cfg_attr(docsrs, doc(cfg(feature = "rwlock")))]
pub mod rwlock;
#[cfg(feature = "semaphore")]
#[cfg_attr(docsrs, doc(cfg(feature = "semaphore")))]
pub mod semaphore;
#[cfg(feature = "shutdown")]
#[cfg_attr(docsrs, doc(cfg(feature = "shutdown")))]
pub mod shutdown;
#[cfg(feature = "singleflight")]
#[cfg_attr(docsrs, doc(cfg(feature = "singleflight")))]
pub mod singleflight;
#[cfg(feature = "waitgroup")]
#[cfg_attr(docsrs, doc(cfg(feature = "waitgroup")))]
pub mod waitgroup;

#[cfg(all(doctest, feature = "mutex", feature = "rwlock"))]
pub mod guard_variance_tests;

#[cfg(all(
    test,
    any(
        feature = "condvar",
        feature = "mpsc",
        feature = "once",
        feature = "shutdown",
        feature = "waitgroup",
    )
))]
fn test_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;

    use tokio::runtime::Runtime;
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().unwrap())
}

#[cfg(all(
    test,
    any(
        feature = "admission",
        feature = "condvar",
        feature = "once",
        feature = "singleflight",
    )
))]
pub(crate) fn poll_once<F: std::future::Future>(
    future: std::pin::Pin<&mut F>,
) -> std::task::Poll<F::Output> {
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    future.poll(&mut context)
}

#[cfg(all(
    test,
    feature = "admission",
    feature = "atomicbox",
    feature = "barrier",
    feature = "broadcast",
    feature = "condvar",
    feature = "latch",
    feature = "mpsc",
    feature = "mutex",
    feature = "once",
    feature = "oneshot",
    feature = "rwlock",
    feature = "semaphore",
    feature = "shutdown",
    feature = "singleflight",
    feature = "waitgroup",
))]
mod tests {
    use crate::admission::FairShare;
    use crate::admission::FairSharePermit;
    use crate::admission::OwnedFairSharePermit;
    use crate::barrier::Barrier;
    use crate::broadcast;
    use crate::condvar::Condvar;
    use crate::latch::Latch;
    use crate::mpsc;
    use crate::mutex::Mutex;
    use crate::mutex::MutexGuard;
    use crate::once::Once;
    use crate::once::OnceCell;
    use crate::once::OnceMap;
    use crate::oneshot;
    use crate::rwlock::OwnedRwLockReadGuard;
    use crate::rwlock::RwLock;
    use crate::rwlock::RwLockReadGuard;
    use crate::rwlock::RwLockWriteGuard;
    use crate::semaphore::Semaphore;
    use crate::shutdown::ShutdownRecv;
    use crate::shutdown::ShutdownSend;
    use crate::shutdown::ShutdownWatch;
    use crate::singleflight;
    use crate::waitgroup::Wait;
    use crate::waitgroup::WaitGroup;

    #[test]
    fn assert_send_and_sync() {
        fn do_assert_send_and_sync<T: Send + Sync>() {}
        do_assert_send_and_sync::<FairShare<String>>();
        do_assert_send_and_sync::<FairSharePermit<'_, String>>();
        do_assert_send_and_sync::<OwnedFairSharePermit<String>>();
        do_assert_send_and_sync::<Barrier>();
        do_assert_send_and_sync::<Condvar>();
        do_assert_send_and_sync::<Once>();
        do_assert_send_and_sync::<OnceCell<u32>>();
        do_assert_send_and_sync::<OnceMap<String, u32>>();
        do_assert_send_and_sync::<singleflight::Group<String, u32>>();
        do_assert_send_and_sync::<Latch>();
        do_assert_send_and_sync::<Semaphore>();
        do_assert_send_and_sync::<ShutdownSend>();
        do_assert_send_and_sync::<ShutdownRecv>();
        do_assert_send_and_sync::<ShutdownWatch>();
        do_assert_send_and_sync::<WaitGroup>();
        do_assert_send_and_sync::<Mutex<i64>>();
        do_assert_send_and_sync::<MutexGuard<'_, i64>>();
        do_assert_send_and_sync::<RwLock<i64>>();
        do_assert_send_and_sync::<OwnedRwLockReadGuard<i64>>();
        do_assert_send_and_sync::<RwLockReadGuard<'_, i64>>();
        do_assert_send_and_sync::<RwLockWriteGuard<'_, i64>>();
        do_assert_send_and_sync::<broadcast::overflow::Sender<i64>>();
        do_assert_send_and_sync::<broadcast::overflow::Receiver<i64>>();
        do_assert_send_and_sync::<broadcast::overflow::RecvError>();
        do_assert_send_and_sync::<broadcast::overflow::TryRecvError>();
        do_assert_send_and_sync::<oneshot::SendError<i64>>();
        do_assert_send_and_sync::<oneshot::Sender<i64>>();
        do_assert_send_and_sync::<mpsc::SendError<i64>>();
        do_assert_send_and_sync::<mpsc::UnboundedSender<i64>>();
        do_assert_send_and_sync::<mpsc::UnboundedReceiver<i64>>();
        do_assert_send_and_sync::<mpsc::BoundedSender<i64>>();
        do_assert_send_and_sync::<mpsc::BoundedReceiver<i64>>();
    }

    #[test]
    fn assert_send() {
        fn do_assert_send<T: Send>() {}
        do_assert_send::<RwLockReadGuard<'_, std::sync::MutexGuard<'static, ()>>>();
        do_assert_send::<oneshot::Receiver<i64>>();
        do_assert_send::<oneshot::Recv<i64>>();
    }

    #[test]
    fn assert_unpin() {
        fn do_assert_unpin<T: Unpin>() {}
        do_assert_unpin::<FairShare<String>>();
        do_assert_unpin::<FairSharePermit<'_, String>>();
        do_assert_unpin::<OwnedFairSharePermit<String>>();
        do_assert_unpin::<Barrier>();
        do_assert_unpin::<Condvar>();
        do_assert_unpin::<Latch>();
        do_assert_unpin::<Once>();
        do_assert_unpin::<OnceCell<u32>>();
        do_assert_unpin::<OnceMap<String, u32>>();
        do_assert_unpin::<singleflight::Group<String, u32>>();
        do_assert_unpin::<Semaphore>();
        do_assert_unpin::<ShutdownSend>();
        do_assert_unpin::<ShutdownRecv>();
        do_assert_unpin::<ShutdownWatch>();
        do_assert_unpin::<WaitGroup>();
        do_assert_unpin::<Wait>();
        do_assert_unpin::<Mutex<i64>>();
        do_assert_unpin::<MutexGuard<'_, i64>>();
        do_assert_unpin::<RwLock<i64>>();
        do_assert_unpin::<RwLockReadGuard<'_, i64>>();
        do_assert_unpin::<RwLockWriteGuard<'_, i64>>();
        do_assert_unpin::<broadcast::overflow::Sender<i64>>();
        do_assert_unpin::<broadcast::overflow::Receiver<i64>>();
        do_assert_unpin::<broadcast::overflow::RecvError>();
        do_assert_unpin::<broadcast::overflow::TryRecvError>();
        do_assert_unpin::<oneshot::Sender<i64>>();
        do_assert_unpin::<oneshot::SendError<i64>>();
        do_assert_unpin::<oneshot::Receiver<i64>>();
        do_assert_unpin::<oneshot::Recv<i64>>();
        do_assert_unpin::<mpsc::SendError<i64>>();
        do_assert_unpin::<mpsc::UnboundedSender<i64>>();
        do_assert_unpin::<mpsc::UnboundedReceiver<i64>>();
        do_assert_unpin::<mpsc::BoundedSender<i64>>();
        do_assert_unpin::<mpsc::BoundedReceiver<i64>>();
    }
}
