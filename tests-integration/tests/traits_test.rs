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

use asyncband::barrier::Barrier;
use asyncband::broadcast;
use asyncband::condvar::Condvar;
use asyncband::latch::Latch;
use asyncband::mpsc;
use asyncband::mutex::Mutex;
use asyncband::mutex::MutexGuard;
use asyncband::once::LazyLock;
use asyncband::once::Once;
use asyncband::once::OnceCell;
use asyncband::once::OnceMap;
use asyncband::oneshot;
use asyncband::rwlock::OwnedRwLockReadGuard;
use asyncband::rwlock::RwLock;
use asyncband::rwlock::RwLockReadGuard;
use asyncband::rwlock::RwLockWriteGuard;
use asyncband::semaphore::Semaphore;
use asyncband::shutdown::ShutdownRecv;
use asyncband::shutdown::ShutdownSend;
use asyncband::shutdown::ShutdownWatch;
use asyncband::singleflight;
use asyncband::waitgroup::Wait;
use asyncband::waitgroup::WaitGroup;

#[test]
fn public_types_are_send_and_sync() {
    fn assert_send_and_sync<T: Send + Sync>() {}

    assert_send_and_sync::<Barrier>();
    assert_send_and_sync::<Condvar>();
    assert_send_and_sync::<LazyLock<u32>>();
    assert_send_and_sync::<Once>();
    assert_send_and_sync::<OnceCell<u32>>();
    assert_send_and_sync::<OnceMap<String, u32>>();
    assert_send_and_sync::<singleflight::Group<String, u32>>();
    assert_send_and_sync::<Latch>();
    assert_send_and_sync::<Semaphore>();
    assert_send_and_sync::<ShutdownSend>();
    assert_send_and_sync::<ShutdownRecv>();
    assert_send_and_sync::<ShutdownWatch>();
    assert_send_and_sync::<WaitGroup>();
    assert_send_and_sync::<Mutex<i64>>();
    assert_send_and_sync::<MutexGuard<'_, i64>>();
    assert_send_and_sync::<RwLock<i64>>();
    assert_send_and_sync::<OwnedRwLockReadGuard<i64>>();
    assert_send_and_sync::<RwLockReadGuard<'_, i64>>();
    assert_send_and_sync::<RwLockWriteGuard<'_, i64>>();
    assert_send_and_sync::<broadcast::overflow::Sender<i64>>();
    assert_send_and_sync::<broadcast::overflow::Receiver<i64>>();
    assert_send_and_sync::<broadcast::overflow::RecvError>();
    assert_send_and_sync::<broadcast::overflow::TryRecvError>();
    assert_send_and_sync::<oneshot::SendError<i64>>();
    assert_send_and_sync::<oneshot::Sender<i64>>();
    assert_send_and_sync::<mpsc::SendError<i64>>();
    assert_send_and_sync::<mpsc::UnboundedSender<i64>>();
    assert_send_and_sync::<mpsc::UnboundedReceiver<i64>>();
    assert_send_and_sync::<mpsc::BoundedSender<i64>>();
    assert_send_and_sync::<mpsc::BoundedReceiver<i64>>();
}

#[test]
fn movable_public_types_are_send() {
    fn assert_send<T: Send>() {}

    assert_send::<RwLockReadGuard<'_, std::sync::MutexGuard<'static, ()>>>();
    assert_send::<oneshot::Receiver<i64>>();
    assert_send::<oneshot::Recv<i64>>();
}

#[test]
fn public_types_are_unpin() {
    fn assert_unpin<T: Unpin>() {}

    assert_unpin::<Barrier>();
    assert_unpin::<Condvar>();
    assert_unpin::<Latch>();
    assert_unpin::<LazyLock<u32>>();
    assert_unpin::<Once>();
    assert_unpin::<OnceCell<u32>>();
    assert_unpin::<OnceMap<String, u32>>();
    assert_unpin::<singleflight::Group<String, u32>>();
    assert_unpin::<Semaphore>();
    assert_unpin::<ShutdownSend>();
    assert_unpin::<ShutdownRecv>();
    assert_unpin::<ShutdownWatch>();
    assert_unpin::<WaitGroup>();
    assert_unpin::<Wait>();
    assert_unpin::<Mutex<i64>>();
    assert_unpin::<MutexGuard<'_, i64>>();
    assert_unpin::<RwLock<i64>>();
    assert_unpin::<RwLockReadGuard<'_, i64>>();
    assert_unpin::<RwLockWriteGuard<'_, i64>>();
    assert_unpin::<broadcast::overflow::Sender<i64>>();
    assert_unpin::<broadcast::overflow::Receiver<i64>>();
    assert_unpin::<broadcast::overflow::RecvError>();
    assert_unpin::<broadcast::overflow::TryRecvError>();
    assert_unpin::<oneshot::Sender<i64>>();
    assert_unpin::<oneshot::SendError<i64>>();
    assert_unpin::<oneshot::Receiver<i64>>();
    assert_unpin::<oneshot::Recv<i64>>();
    assert_unpin::<mpsc::SendError<i64>>();
    assert_unpin::<mpsc::UnboundedSender<i64>>();
    assert_unpin::<mpsc::UnboundedReceiver<i64>>();
    assert_unpin::<mpsc::BoundedSender<i64>>();
    assert_unpin::<mpsc::BoundedReceiver<i64>>();
}
