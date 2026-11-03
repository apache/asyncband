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

//! Four independent shutdown observers wait for the same last worker.

use std::future::Future;
use std::future::IntoFuture;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;

use benchmarks::harness::Bencher;
use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::support::poll_pending;
use benchmarks::support::poll_pinned_ready;
use futures_util::FutureExt;
use tokio_util::task::TaskTracker;

fn drain<F: Future<Output = ()>, W>(worker: W, observers: [F; 4], tasks: &[Task; 4]) {
    let [a, b, c, d] = observers;
    let mut observers = [pin!(a), pin!(b), pin!(c), pin!(d)];
    for (wait, task) in observers.iter_mut().zip(tasks) {
        poll_pending(wait.as_mut(), &mut Context::from_waker(&task.waker));
    }
    drop(worker);
    for (wait, task) in observers.into_iter().zip(tasks) {
        assert!(task.notified.0.swap(false, Ordering::Relaxed));
        poll_pinned_ready(wait, &mut Context::from_waker(&task.waker));
    }
}

#[derive(Default)]
struct Notified(AtomicBool);

impl Wake for Notified {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct Task {
    notified: Arc<Notified>,
    waker: Waker,
}

impl Task {
    fn new() -> Self {
        let notified = Arc::<Notified>::default();
        Self {
            waker: Waker::from(notified.clone()),
            notified,
        }
    }
}

trait Drain {
    fn run(tasks: &[Task; 4]);
}

struct Asyncband;
struct WaitgroupRsShared;
struct TokioTaskTracker;

impl Drain for Asyncband {
    fn run(tasks: &[Task; 4]) {
        let group = asyncband::waitgroup::WaitGroup::new();
        let worker = group.clone();
        let wait = group.into_future();
        drain(worker, std::array::from_fn(|_| wait.clone()), tasks);
    }
}

impl Drain for WaitgroupRsShared {
    fn run(tasks: &[Task; 4]) {
        let group = waitgroup::WaitGroup::new();
        let worker = group.worker();
        let wait = group.wait().shared();
        drain(worker, std::array::from_fn(|_| wait.clone()), tasks);
    }
}

impl Drain for TokioTaskTracker {
    fn run(tasks: &[Task; 4]) {
        let tracker = TaskTracker::new();
        let worker = tracker.token();
        tracker.close();
        drain(worker, std::array::from_fn(|_| tracker.wait()), tasks);
    }
}

fn drain_four<C: Drain>(bencher: Bencher) {
    let tasks = std::array::from_fn(|_| Task::new());
    bencher.bench_local(|| C::run(&tasks));
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "waitgroup/drain-4-observers",
        2_500_000,
        implementations!([Asyncband, WaitgroupRsShared, TokioTaskTracker], |C, b| {
            drain_four::<C>(b)
        }),
    );
}
