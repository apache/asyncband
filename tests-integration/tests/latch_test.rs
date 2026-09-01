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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;

use asyncband::latch::Latch;

struct TrackWake(AtomicUsize);

impl Wake for TrackWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn countdown_operations_saturate_at_zero() {
    let latch = Latch::new(5);

    latch.arrive(0);
    assert_eq!(latch.try_wait(), Err(5));

    latch.arrive(3);
    assert_eq!(latch.try_wait(), Err(2));

    latch.count_down();
    latch.arrive(2);
    assert_eq!(latch.try_wait(), Ok(()));

    latch.count_down();
    latch.arrive(u32::MAX);
    assert_eq!(latch.count(), 0);
}

#[test]
fn cancelled_wait_releases_its_waker() {
    let latch = Latch::new(1);
    let tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let waker = Waker::from(tracker.clone());
    let baseline = Arc::strong_count(&tracker);
    let mut context = Context::from_waker(&waker);
    let mut wait = Box::pin(latch.wait());

    assert!(wait.as_mut().poll(&mut context).is_pending());
    assert_eq!(Arc::strong_count(&tracker), baseline + 1);

    drop(wait);
    assert_eq!(Arc::strong_count(&tracker), baseline);
}

#[test]
fn final_arrival_wakes_every_waiter() {
    let latch = Latch::new(2);
    let first_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let second_tracker = Arc::new(TrackWake(AtomicUsize::new(0)));
    let first_waker = Waker::from(first_tracker.clone());
    let second_waker = Waker::from(second_tracker.clone());
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);
    let mut first = Box::pin(latch.wait());
    let mut second = Box::pin(latch.wait());

    assert!(first.as_mut().poll(&mut first_context).is_pending());
    assert!(second.as_mut().poll(&mut second_context).is_pending());

    latch.count_down();
    assert_eq!(first_tracker.0.load(Ordering::Relaxed), 0);
    assert_eq!(second_tracker.0.load(Ordering::Relaxed), 0);

    latch.count_down();
    assert_eq!(first_tracker.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_tracker.0.load(Ordering::Relaxed), 1);
    assert!(first.as_mut().poll(&mut first_context).is_ready());
    assert!(second.as_mut().poll(&mut second_context).is_ready());
}

#[tokio::test]
async fn owned_wait_can_move_to_another_task() {
    let latch = Arc::new(Latch::new(1));
    let waiter = tokio::spawn(latch.clone().wait_owned());

    latch.count_down();
    waiter.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_arrivals_complete_the_latch() {
    const TASKS: usize = 32;

    let latch = Arc::new(Latch::new(TASKS as u32));
    let start = Arc::new(tokio::sync::Barrier::new(TASKS + 1));
    let mut tasks = Vec::with_capacity(TASKS);

    for _ in 0..TASKS {
        let latch = latch.clone();
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            latch.count_down();
        }));
    }

    start.wait().await;
    latch.wait().await;

    for task in tasks {
        task.await.unwrap();
    }
    assert_eq!(latch.count(), 0);
}
