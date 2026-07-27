// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

//! `mea` is a runtime-agnostic library providing essential synchronization primitives for
//! asynchronous Rust programming. The library offers a collection of well-tested, efficient
//! synchronization tools that work with any async runtime.
//!
//! # Features
//!
//! * [`admission::FairShare`]: A work-conserving admission policy that fairly shares bounded
//!   concurrency across keys
//! * [`Barrier`]: A synchronization point where multiple tasks can wait until all participants
//!   arrive
//! * [`Condvar`]: A condition variable that allows tasks to wait for a notification
//! * [`Latch`]: A single-use barrier that allows one or more tasks to wait until a signal is given
//! * [`Mutex`]: A mutual exclusion primitive for protecting shared data
//! * [`Once`]: A primitive that ensures a one-time asynchronous operation runs at most once, even
//!   when called concurrently
//! * [`OnceCell`]: A cell that can be written to at most once and provides safe concurrent access
//! * [`OnceMap`]: A hash map that runs computation only once for each key and stores the result.
//! * [`RwLock`]: A reader-writer lock that allows multiple readers or a single writer at a time
//! * [`Semaphore`]: A synchronization primitive that controls access to a shared resource
//! * [`ShutdownSend`], [`ShutdownRecv`] & [`ShutdownWatch`]: A composite synchronization primitive
//!   for managing shutdown signals
//! * [`WaitGroup`]: A synchronization primitive that allows waiting for multiple tasks to complete
//! * [`atomicbox`]: A safe, owning version of `AtomicPtr` for heap-allocated data.
//! * [`broadcast`]: A multi-producer, multi-consumer broadcast channel.
//! * [`mpsc::bounded`]: A multi-producer, single-consumer bounded queue for sending values between
//!   asynchronous tasks.
//! * [`mpsc::unbounded`]: A multi-producer, single-consumer unbounded queue for sending values
//!   between asynchronous tasks.
//! * [`oneshot::channel`]: A one-shot channel for sending a single value between tasks.
//! * [`singleflight::Group`]: A duplicate function call suppression mechanism.
//!
//! # Runtime Agnostic
//!
//! All synchronization primitives in this library are runtime-agnostic, meaning they can be used
//! with any async runtime like tokio, async-std, or others. This makes the library highly versatile
//! and portable.
//!
//! # Thread Safety
//!
//! All primitive types in this library implement `Send` and `Sync`, making them safe to share
//! across thread boundaries. This is essential for concurrent programming where data needs to be
//! accessed from multiple threads.
//!
//! [`Barrier`]: barrier::Barrier
//! [`Condvar`]: condvar::Condvar
//! [`Latch`]: latch::Latch
//! [`Mutex`]: mutex::Mutex
//! [`Once`]: once::Once
//! [`OnceCell`]: once::OnceCell
//! [`OnceMap`]: once::OnceMap
//! [`RwLock`]: rwlock::RwLock
//! [`Semaphore`]: semaphore::Semaphore
//! [`ShutdownSend`]: shutdown::ShutdownSend
//! [`ShutdownRecv`]: shutdown::ShutdownRecv
//! [`ShutdownWatch`]: shutdown::ShutdownWatch
//! [`WaitGroup`]: waitgroup::WaitGroup

mod internal;

pub mod admission;
pub mod atomicbox;
pub mod barrier;
pub mod broadcast;
pub mod condvar;
pub mod latch;
pub mod mpsc;
pub mod mutex;
pub mod once;
pub mod oneshot;
pub mod rwlock;
pub mod semaphore;
pub mod shutdown;
pub mod singleflight;
pub mod waitgroup;

#[cfg(test)]
fn test_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;

    use tokio::runtime::Runtime;
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| Runtime::new().unwrap())
}

#[cfg(test)]
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
