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

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Wake;
use std::task::Waker;

use super::*;

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn poll_wait(wait: Pin<&mut impl Future<Output = BarrierWaitResult>>, waker: &Waker) -> Poll<bool> {
    wait.poll(&mut Context::from_waker(waker))
        .map(|result| result.is_leader())
}

#[test]
fn a_woken_follower_completes_across_generation_wraparound() {
    let barrier = Barrier::new(2);
    barrier.state.lock().generation = u64::MAX;
    let noop = Waker::noop();
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());

    let mut old = pin!(barrier.wait());
    assert_eq!(poll_wait(old.as_mut(), noop), Poll::Pending);
    assert_eq!(poll_wait(pin!(barrier.wait()), noop), Poll::Ready(true));

    // The next generation reuses the slot the old follower registered in.
    let mut next = pin!(barrier.wait());
    assert_eq!(poll_wait(next.as_mut(), &waker), Poll::Pending);

    assert_eq!(poll_wait(old.as_mut(), noop), Poll::Ready(false));
    assert_eq!(poll_wait(pin!(barrier.wait()), noop), Poll::Ready(true));
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
}

#[test]
fn dropping_a_woken_follower_across_wraparound_keeps_the_next_generation_registered() {
    let barrier = Barrier::new(2);
    barrier.state.lock().generation = u64::MAX;
    let noop = Waker::noop();
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());

    let mut old = Box::pin(barrier.wait());
    assert_eq!(poll_wait(old.as_mut(), noop), Poll::Pending);
    assert_eq!(poll_wait(pin!(barrier.wait()), noop), Poll::Ready(true));

    let mut next = pin!(barrier.wait());
    assert_eq!(poll_wait(next.as_mut(), &waker), Poll::Pending);
    drop(old);

    assert_eq!(poll_wait(pin!(barrier.wait()), noop), Poll::Ready(true));
    assert_eq!(counter.0.load(Ordering::Relaxed), 1);
}
