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

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use asyncband::pool::bounded::Pool;
use asyncband::pool::bounded::PoolConfig;
use tests_integration::WakeCounter;

use super::support::Manager;
use super::support::ManagerError;

fn ready<T>(future: impl Future<Output = Result<T, ManagerError>>) -> T {
    let wakes = Arc::new(WakeCounter::default());
    wakes.wake_by_ref();
    ready_after_wake(future, &wakes)
}

fn ready_after_wake<T>(
    future: impl Future<Output = Result<T, ManagerError>>,
    wakes: &Arc<WakeCounter>,
) -> T {
    let mut future = pin!(future);
    let waker = Waker::from(wakes.clone());
    // Allow cooperative yields, but never poll away a missing notification or spin forever.
    for _ in 0..32 {
        assert!(wakes.take() > 0, "missing wake");
        match future.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(Ok(value)) => return value,
            Poll::Ready(Err(ManagerError)) => panic!("operation should succeed"),
            Poll::Pending => {}
        }
    }
    panic!("operation did not finish within the test's poll budget");
}

fn cancel_waiter(return_before_cancel: bool) {
    let manager = Manager::default();
    let pool = Pool::new(PoolConfig::new(1), manager.clone());
    let held = ready(pool.get());
    let mut first_wakes = Arc::new(WakeCounter::default());
    let mut next_wakes = Arc::new(WakeCounter::default());
    let first_waker = Waker::from(first_wakes.clone());
    let next_waker = Waker::from(next_wakes.clone());
    let mut first = Box::pin(pool.get());
    let mut next = Box::pin(pool.get());
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&first_waker))
            .is_pending()
    );
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&next_waker))
            .is_pending()
    );
    assert_eq!(manager.created(), 1);

    if return_before_cancel {
        drop(held);
        // Cancel a notified waiter without requiring FIFO notification order.
        if first_wakes.count() == 0 {
            std::mem::swap(&mut first, &mut next);
            std::mem::swap(&mut first_wakes, &mut next_wakes);
        }
        assert!(first_wakes.count() > 0);
        // Cancel the notified future without polling it to retrieve its permit.
        drop(first);
    } else {
        drop(first);
        drop(held);
    }

    // A notification from returning the object is just as valid as one from cancellation.
    let object = ready_after_wake(next.as_mut(), &next_wakes);
    assert_eq!(*object, 0);
    assert_eq!(manager.created(), 1);
    assert_eq!(pool.status().current_size, 1);
    assert_eq!(pool.status().idle_count, 0);

    let mut extra = Box::pin(pool.get());
    assert!(tests_integration::poll_once(extra.as_mut()).is_pending());
    assert_eq!(manager.created(), 1);
    drop(extra);
    drop(object);
    assert_eq!(pool.status().idle_count, 1);
    assert_eq!(*ready(pool.get()), 0);
}

#[test]
fn cancelling_queued_get_preserves_follower_progress() {
    cancel_waiter(false);
}

#[test]
fn cancelling_notified_get_preserves_follower_progress() {
    cancel_waiter(true);
}

#[test]
fn cancelling_create_restores_capacity_for_waiting_get() {
    let manager = Manager::default();
    let creation = manager.pause_create();
    let pool = Pool::new(PoolConfig::new(1), manager.clone());
    let mut creating = Box::pin(pool.get());
    assert!(tests_integration::poll_once(creating.as_mut()).is_pending());
    assert_eq!(manager.created(), 1);
    assert_eq!(pool.status().current_size, 0);

    let wakes = Arc::new(WakeCounter::default());
    let waker = Waker::from(wakes.clone());
    let mut next = Box::pin(pool.get());
    assert!(
        next.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(manager.created(), 1);
    drop(creating);
    assert!(wakes.count() > 0);
    assert_eq!(pool.status().current_size, 0);
    assert_eq!(pool.status().idle_count, 0);

    assert!(creation.is_closed());
    let object = ready_after_wake(next.as_mut(), &wakes);
    assert_eq!(*object, 1);
    assert_eq!(manager.created(), 2);
    assert_eq!(pool.status().current_size, 1);
    assert_eq!(pool.status().idle_count, 0);
    let mut extra = Box::pin(pool.get());
    assert!(tests_integration::poll_once(extra.as_mut()).is_pending());
    assert_eq!(manager.created(), 2);
    drop(extra);
    drop(object);
    assert_eq!(pool.status().idle_count, 1);
    assert_eq!(*ready(pool.get()), 1);
    assert_eq!(manager.created(), 2);
}
