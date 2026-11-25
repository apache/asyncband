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
use std::future::IntoFuture;
use std::hint::black_box;
use std::pin::pin;

use asyncband::barrier;
use asyncband::completion;
use asyncband::condvar::Condvar;
use asyncband::event::AutoResetEvent;
use asyncband::event::ManualResetEvent;
use asyncband::latch::Latch;
use asyncband::mutex::Mutex;
use asyncband::phaser::Phaser;
use asyncband::shutdown;
use asyncband::waitgroup::WaitGroup;
use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::poll::bench_context;
use benchmarks::poll::poll_pending;
use benchmarks::poll::poll_pinned_ready;
use benchmarks::poll::poll_ready;
use benchmarks::runtime;
use benchmarks::runtime::WORKERS;
use tokio::sync;
use tokio_util::task::TaskTracker;

struct Asyncband;
struct Tokio;
struct TokioTaskTracker;

trait BarrierFamily {
    type Barrier;

    fn new(parties: u32) -> Self::Barrier;
    fn wait(barrier: &Self::Barrier) -> impl Future<Output = bool> + '_;
}

impl BarrierFamily for Asyncband {
    type Barrier = barrier::Barrier;

    fn new(parties: u32) -> Self::Barrier {
        barrier::Barrier::new(parties)
    }

    async fn wait(barrier: &Self::Barrier) -> bool {
        barrier.wait().await.is_leader()
    }
}

impl BarrierFamily for Tokio {
    type Barrier = sync::Barrier;

    fn new(parties: u32) -> Self::Barrier {
        sync::Barrier::new(parties as usize)
    }

    async fn wait(barrier: &Self::Barrier) -> bool {
        barrier.wait().await.is_leader()
    }
}

trait WaitGroupTasks {
    fn spawn_workers(runtime: &tokio::runtime::Runtime, count: usize);
}

impl WaitGroupTasks for Asyncband {
    fn spawn_workers(runtime: &tokio::runtime::Runtime, count: usize) {
        let wg = WaitGroup::new();
        for _ in 0..count {
            let guard = wg.clone();
            runtime.spawn(async move {
                black_box(guard);
            });
        }
        runtime.block_on(wg.into_future());
    }
}

impl WaitGroupTasks for TokioTaskTracker {
    fn spawn_workers(runtime: &tokio::runtime::Runtime, count: usize) {
        let tracker = TaskTracker::new();
        for _ in 0..count {
            let token = tracker.token();
            runtime.spawn(async move {
                let _token = black_box(token);
            });
        }
        tracker.close();
        runtime.block_on(tracker.wait());
    }
}

pub fn register(runner: &mut Runner) {
    runner.local("event/manual-handoff-4", 5_500_000, |bencher| {
        let event = ManualResetEvent::new();
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let [f0, f1, f2, f3] = [event.wait(), event.wait(), event.wait(), event.wait()];
            let mut waiters = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
            for waiter in &mut waiters {
                poll_pending(waiter.as_mut(), &mut cx);
            }
            event.set();
            for waiter in &mut waiters {
                poll_pinned_ready(waiter.as_mut(), &mut cx);
            }
            event.reset();
        });
    });

    runner.local("event/manual-cancel", 24_000_000, |bencher| {
        let event = ManualResetEvent::new();
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let mut waiter = pin!(event.wait());
            poll_pending(waiter.as_mut(), &mut cx);
        });
    });

    runner.local("event/auto-handoff", 16_000_000, |bencher| {
        let event = AutoResetEvent::new();
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let mut waiter = pin!(event.wait());
            poll_pending(waiter.as_mut(), &mut cx);
            event.set();
            poll_pinned_ready(waiter.as_mut(), &mut cx);
        });
    });

    runner.compare(
        "barrier/reused-4-parties",
        4_700_000,
        implementations!([Asyncband, Tokio], |B, bencher| {
            let barrier = B::new(4);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let [f0, f1, f2] = [B::wait(&barrier), B::wait(&barrier), B::wait(&barrier)];
                let mut followers = [pin!(f0), pin!(f1), pin!(f2)];
                for follower in &mut followers {
                    poll_pending(follower.as_mut(), &mut cx);
                }
                let mut leaders = usize::from(poll_ready(B::wait(&barrier), &mut cx));
                for follower in &mut followers {
                    if poll_pinned_ready(follower.as_mut(), &mut cx) {
                        leaders += 1;
                    }
                }
                assert_eq!(leaders, 1);
            });
        }),
    );

    runner.local("latch/countdown-4-waiters", 3_300_000, |bencher| {
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let latch = Latch::new(4);
            let [f0, f1, f2, f3] = [latch.wait(), latch.wait(), latch.wait(), latch.wait()];
            let mut waiters = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
            for waiter in &mut waiters {
                poll_pending(waiter.as_mut(), &mut cx);
            }
            for _ in 0..4 {
                latch.count_down();
            }
            for waiter in &mut waiters {
                poll_pinned_ready(waiter.as_mut(), &mut cx);
            }
        });
    });

    runner.local("phaser/phase-handoff-4", 4_800_000, |bencher| {
        let phaser = Phaser::new();
        let mut batch = phaser.register(4).expect("phaser should stay open");
        let mut p0 = batch.next().unwrap();
        let mut p1 = batch.next().unwrap();
        let mut p2 = batch.next().unwrap();
        let mut p3 = batch.next().unwrap();
        drop(batch);
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let f0 = p0.wait();
            let f1 = p1.wait();
            let f2 = p2.wait();
            let mut w0 = pin!(f0);
            let mut w1 = pin!(f1);
            let mut w2 = pin!(f2);
            poll_pending(w0.as_mut(), &mut cx);
            poll_pending(w1.as_mut(), &mut cx);
            poll_pending(w2.as_mut(), &mut cx);
            let ph3 = poll_ready(p3.wait(), &mut cx).expect("phaser should stay open");
            let ph0 = poll_pinned_ready(w0.as_mut(), &mut cx).expect("phaser should stay open");
            let ph1 = poll_pinned_ready(w1.as_mut(), &mut cx).expect("phaser should stay open");
            let ph2 = poll_pinned_ready(w2.as_mut(), &mut cx).expect("phaser should stay open");
            black_box((ph0, ph1, ph2, ph3));
        });
    });

    runner.local("condvar/notify-all-4", 2_500_000, |bencher| {
        let mutex = Mutex::new(());
        let condvar = Condvar::new();
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let mut w0 = pin!(condvar.wait(poll_ready(mutex.lock(), &mut cx)));
            poll_pending(w0.as_mut(), &mut cx);
            let mut w1 = pin!(condvar.wait(poll_ready(mutex.lock(), &mut cx)));
            poll_pending(w1.as_mut(), &mut cx);
            let mut w2 = pin!(condvar.wait(poll_ready(mutex.lock(), &mut cx)));
            poll_pending(w2.as_mut(), &mut cx);
            let mut w3 = pin!(condvar.wait(poll_ready(mutex.lock(), &mut cx)));
            poll_pending(w3.as_mut(), &mut cx);
            condvar.notify_all();
            drop(poll_pinned_ready(w0.as_mut(), &mut cx));
            drop(poll_pinned_ready(w1.as_mut(), &mut cx));
            drop(poll_pinned_ready(w2.as_mut(), &mut cx));
            drop(poll_pinned_ready(w3.as_mut(), &mut cx));
        });
    });

    runner.local("completion/notify-4-observers", 2_700_000, |bencher| {
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let (completer, completion) = completion::new();
            let [f0, f1, f2, f3] = [
                completion.wait(),
                completion.wait(),
                completion.wait(),
                completion.wait(),
            ];
            let mut observers = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
            for observer in &mut observers {
                poll_pending(observer.as_mut(), &mut cx);
            }
            completer
                .complete(1usize)
                .expect("observers should remain alive");
            for observer in &mut observers {
                let value = poll_pinned_ready(observer.as_mut(), &mut cx)
                    .expect("completion should have succeeded");
                black_box(*value);
            }
        });
    });

    runner.compare(
        "waitgroup/drain-4-observers",
        4_300_000,
        &[
            ("Asyncband", &|bencher| {
                let mut cx = bench_context();
                bencher.bench_local(|| {
                    let worker = WaitGroup::new();
                    let wait = worker.clone().into_future();
                    let [f0, f1, f2, f3] = [wait.clone(), wait.clone(), wait.clone(), wait];
                    let mut observers = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
                    for observer in &mut observers {
                        poll_pending(observer.as_mut(), &mut cx);
                    }
                    drop(worker);
                    for observer in &mut observers {
                        poll_pinned_ready(observer.as_mut(), &mut cx);
                    }
                });
            }),
            ("TokioTaskTracker", &|bencher| {
                let mut cx = bench_context();
                bencher.bench_local(|| {
                    let tracker = TaskTracker::new();
                    let token = tracker.token();
                    tracker.close();
                    let [f0, f1, f2, f3] = [
                        tracker.wait(),
                        tracker.wait(),
                        tracker.wait(),
                        tracker.wait(),
                    ];
                    let mut observers = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
                    for observer in &mut observers {
                        poll_pending(observer.as_mut(), &mut cx);
                    }
                    drop(token);
                    for observer in &mut observers {
                        poll_pinned_ready(observer.as_mut(), &mut cx);
                    }
                });
            }),
        ],
    );

    runner.compare(
        "waitgroup/tasks-drain-4w",
        35_000,
        implementations!([Asyncband, TokioTaskTracker], |W, bencher| {
            let runtime = runtime::multi_thread(WORKERS);
            bencher.bench_local(|| {
                W::spawn_workers(&runtime, 16);
            });
            drop(runtime);
        }),
    );

    runner.local("shutdown/signal-and-join-4", 2_300_000, |bencher| {
        let mut cx = bench_context();
        bencher.bench_local(|| {
            let (shutdown, g0) = shutdown::new();
            let g1 = g0.clone();
            let g2 = g0.clone();
            let g3 = g0.clone();
            {
                let [f0, f1, f2, f3] = [
                    g0.shutdown_requested(),
                    g1.shutdown_requested(),
                    g2.shutdown_requested(),
                    g3.shutdown_requested(),
                ];
                let mut watchers = [pin!(f0), pin!(f1), pin!(f2), pin!(f3)];
                for watcher in &mut watchers {
                    poll_pending(watcher.as_mut(), &mut cx);
                }
                shutdown.request_shutdown();
                for watcher in &mut watchers {
                    poll_pinned_ready(watcher.as_mut(), &mut cx);
                }
            }
            drop((g0, g1, g2, g3));
            poll_ready(shutdown, &mut cx);
        });
    });
}
