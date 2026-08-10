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

//! Mutual exclusion, notification, initialization, and task synchronization primitives.

#[cfg(feature = "barrier")]
pub use crate::barrier::Barrier;
#[cfg(feature = "barrier")]
pub use crate::barrier::BarrierWaitResult;
#[cfg(feature = "condvar")]
pub use crate::condvar::Condvar;
#[cfg(feature = "latch")]
pub use crate::latch::Latch;
#[cfg(feature = "latch")]
pub use crate::latch::LatchWait;
#[cfg(feature = "latch")]
pub use crate::latch::OwnedLatchWait;
#[cfg(feature = "mutex")]
pub use crate::mutex::MappedMutexGuard;
#[cfg(feature = "mutex")]
pub use crate::mutex::Mutex;
#[cfg(feature = "mutex")]
pub use crate::mutex::MutexGuard;
#[cfg(feature = "mutex")]
pub use crate::mutex::OwnedMappedMutexGuard;
#[cfg(feature = "mutex")]
pub use crate::mutex::OwnedMutexGuard;
#[cfg(feature = "once")]
pub use crate::once::Once;
#[cfg(feature = "once-cell")]
pub use crate::once::OnceCell;
#[cfg(feature = "once-map")]
pub use crate::once::OnceMap;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::MappedRwLockReadGuard;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::MappedRwLockWriteGuard;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::OwnedMappedRwLockReadGuard;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::OwnedMappedRwLockWriteGuard;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::OwnedRwLockReadGuard;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::OwnedRwLockWriteGuard;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::RwLock;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::RwLockReadGuard;
#[cfg(feature = "rwlock")]
pub use crate::rwlock::RwLockWriteGuard;
#[cfg(feature = "semaphore")]
pub use crate::semaphore::OwnedSemaphorePermit;
#[cfg(feature = "semaphore")]
pub use crate::semaphore::Semaphore;
#[cfg(feature = "semaphore")]
pub use crate::semaphore::SemaphorePermit;
#[cfg(feature = "waitgroup")]
pub use crate::waitgroup::Wait;
#[cfg(feature = "waitgroup")]
pub use crate::waitgroup::WaitGroup;
