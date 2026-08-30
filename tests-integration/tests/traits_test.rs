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

use std::cell::Cell;

use asyncband::barrier::Barrier;
use asyncband::broadcast;
use asyncband::completion;
use asyncband::condvar::Condvar;
use asyncband::latch::Latch;
use asyncband::mpsc;
use asyncband::mutex::Mutex;
use asyncband::mutex::MutexGuard;
use asyncband::once::LazyCell;
use asyncband::once::Once;
use asyncband::once::OnceCell;
use asyncband::once::OnceMap;
use asyncband::oneshot;
use asyncband::pool;
use asyncband::pool::ManageObject;
use asyncband::pool::ObjectStatus;
use asyncband::rwlock::OwnedRwLockReadGuard;
use asyncband::rwlock::RwLock;
use asyncband::rwlock::RwLockReadGuard;
use asyncband::rwlock::RwLockWriteGuard;
use asyncband::semaphore::Semaphore;
use asyncband::shutdown::Shutdown;
use asyncband::shutdown::ShutdownGuard;
use asyncband::shutdown::ShutdownWatch;
use asyncband::singleflight;
use asyncband::waitgroup::Wait;
use asyncband::waitgroup::WaitGroup;
use asyncband::watch;

struct PoolManager;

impl ManageObject for PoolManager {
    type Object = i64;
    type Error = std::convert::Infallible;

    async fn create(&self) -> Result<Self::Object, Self::Error> {
        Ok(0)
    }

    async fn is_recyclable(
        &self,
        _object: &mut Self::Object,
        _status: &ObjectStatus,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[test]
fn public_types_are_send_and_sync() {
    fn assert_send_and_sync<T: Send + Sync>() {}

    assert_send_and_sync::<Barrier>();
    assert_send_and_sync::<Condvar>();
    assert_send_and_sync::<completion::Completer<Cell<u8>>>();
    assert_send_and_sync::<completion::Completer<i64>>();
    assert_send_and_sync::<completion::Completion<i64>>();
    assert_send_and_sync::<completion::CompleteError<i64>>();
    assert_send_and_sync::<completion::WaitError>();
    assert_send_and_sync::<LazyCell<u32, std::future::Ready<u32>>>();
    assert_send_and_sync::<Once>();
    assert_send_and_sync::<OnceCell<u32>>();
    assert_send_and_sync::<OnceMap<String, u32>>();
    assert_send_and_sync::<singleflight::Group<String, u32>>();
    assert_send_and_sync::<Latch>();
    assert_send_and_sync::<Semaphore>();
    assert_send_and_sync::<Shutdown>();
    assert_send_and_sync::<ShutdownGuard>();
    assert_send_and_sync::<ShutdownWatch>();
    assert_send_and_sync::<WaitGroup>();
    assert_send_and_sync::<Mutex<i64>>();
    assert_send_and_sync::<MutexGuard<'_, i64>>();
    assert_send_and_sync::<RwLock<i64>>();
    assert_send_and_sync::<OwnedRwLockReadGuard<i64>>();
    assert_send_and_sync::<RwLockReadGuard<'_, i64>>();
    assert_send_and_sync::<RwLockWriteGuard<'_, i64>>();
    assert_send_and_sync::<broadcast::mpmc::UnboundedSender<i64>>();
    assert_send_and_sync::<broadcast::mpmc::UnboundedReceiver<i64>>();
    assert_send_and_sync::<broadcast::mpmc::RecvError>();
    assert_send_and_sync::<broadcast::mpmc::TryRecvError>();
    assert_send_and_sync::<oneshot::SendError<i64>>();
    assert_send_and_sync::<oneshot::Sender<i64>>();
    assert_send_and_sync::<pool::bounded::Pool<PoolManager>>();
    assert_send_and_sync::<pool::bounded::Object<PoolManager>>();
    assert_send_and_sync::<pool::unbounded::Pool<i64>>();
    assert_send_and_sync::<pool::unbounded::Object<i64>>();
    assert_send_and_sync::<pool::unbounded::Pool<Cell<u8>>>();
    assert_send_and_sync::<mpsc::SendError<i64>>();
    assert_send_and_sync::<mpsc::UnboundedSender<i64>>();
    assert_send_and_sync::<mpsc::UnboundedReceiver<i64>>();
    assert_send_and_sync::<mpsc::BoundedSender<i64>>();
    assert_send_and_sync::<mpsc::BoundedReceiver<i64>>();
    assert_send_and_sync::<watch::Sender<i64>>();
    assert_send_and_sync::<watch::Receiver<i64>>();
    assert_send_and_sync::<watch::SendError<i64>>();
    assert_send_and_sync::<watch::RecvError>();
}

#[test]
fn movable_public_types_are_send() {
    fn assert_send<T: Send>() {}
    fn assert_send_value<T: Send>(_: T) {}

    assert_send::<RwLockReadGuard<'_, std::sync::MutexGuard<'static, ()>>>();
    assert_send::<oneshot::Receiver<i64>>();
    assert_send::<oneshot::Recv<i64>>();
    assert_send::<pool::unbounded::Object<Cell<u8>>>();

    let (_tx, mut rx) = watch::channel(0);
    assert_send_value(rx.changed());

    let (_completer, completion) = completion::channel::<i64>();
    assert_send_value(completion.wait());
}

#[test]
fn public_types_are_unpin() {
    fn assert_unpin<T: Unpin>() {}

    assert_unpin::<Barrier>();
    assert_unpin::<Condvar>();
    assert_unpin::<completion::Completer<i64>>();
    assert_unpin::<completion::Completion<i64>>();
    assert_unpin::<completion::CompleteError<i64>>();
    assert_unpin::<completion::WaitError>();
    assert_unpin::<Latch>();
    assert_unpin::<LazyCell<u32, std::future::Ready<u32>>>();
    assert_unpin::<Once>();
    assert_unpin::<OnceCell<u32>>();
    assert_unpin::<OnceMap<String, u32>>();
    assert_unpin::<singleflight::Group<String, u32>>();
    assert_unpin::<Semaphore>();
    assert_unpin::<Shutdown>();
    assert_unpin::<ShutdownGuard>();
    assert_unpin::<ShutdownWatch>();
    assert_unpin::<WaitGroup>();
    assert_unpin::<Wait>();
    assert_unpin::<Mutex<i64>>();
    assert_unpin::<MutexGuard<'_, i64>>();
    assert_unpin::<RwLock<i64>>();
    assert_unpin::<RwLockReadGuard<'_, i64>>();
    assert_unpin::<RwLockWriteGuard<'_, i64>>();
    assert_unpin::<broadcast::mpmc::UnboundedSender<i64>>();
    assert_unpin::<broadcast::mpmc::UnboundedReceiver<i64>>();
    assert_unpin::<broadcast::mpmc::RecvError>();
    assert_unpin::<broadcast::mpmc::TryRecvError>();
    assert_unpin::<oneshot::Sender<i64>>();
    assert_unpin::<oneshot::SendError<i64>>();
    assert_unpin::<oneshot::Receiver<i64>>();
    assert_unpin::<oneshot::Recv<i64>>();
    assert_unpin::<pool::bounded::Pool<PoolManager>>();
    assert_unpin::<pool::bounded::Object<PoolManager>>();
    assert_unpin::<pool::unbounded::Pool<i64>>();
    assert_unpin::<pool::unbounded::Object<i64>>();
    assert_unpin::<mpsc::SendError<i64>>();
    assert_unpin::<mpsc::UnboundedSender<i64>>();
    assert_unpin::<mpsc::UnboundedReceiver<i64>>();
    assert_unpin::<mpsc::BoundedSender<i64>>();
    assert_unpin::<mpsc::BoundedReceiver<i64>>();
    assert_unpin::<watch::Sender<i64>>();
    assert_unpin::<watch::Receiver<i64>>();
    assert_unpin::<watch::SendError<i64>>();
    assert_unpin::<watch::RecvError>();
}

#[test]
fn unbounded_manual_manager_traits_do_not_depend_on_the_object() {
    fn assert_copy<T: Copy>() {}
    fn assert_debug<T: std::fmt::Debug>() {}

    assert_copy::<pool::unbounded::NeverManageObject<String>>();
    assert_debug::<pool::unbounded::NeverManageObject<String>>();
}
