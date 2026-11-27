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

use std::array;
use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::poll::bench_context;
use benchmarks::poll::poll_pending;
use benchmarks::poll::poll_pinned_ready;
use benchmarks::poll::poll_ready;
use benchmarks::runtime::WORKERS;
use benchmarks::runtime::multi_thread;

const TASKS_PUBLICATIONS_PER_ROUND: usize = 256;
const CONCURRENT_WRITES_PER_ROUND: usize = 1_024;
const CONCURRENT_READS_PER_ROUND: usize = 4_096;
const CONCURRENT_READERS: usize = 3;

trait WatchPeer {
    type Tx: Send + Sync + 'static;
    type Rx: Send + Sync + Clone + 'static;

    fn channel(initial: usize) -> (Self::Tx, Self::Rx);

    fn get(rx: &Self::Rx) -> usize;

    fn send(tx: &Self::Tx, value: usize);

    fn changed(rx: &mut Self::Rx) -> impl Future<Output = Result<(), ()>> + Send;

    fn recv(rx: &mut Self::Rx) -> impl Future<Output = usize> + Send;
}

struct Asyncband;

impl WatchPeer for Asyncband {
    type Tx = asyncband::watch::Sender<usize>;
    type Rx = asyncband::watch::Receiver<usize>;

    fn channel(initial: usize) -> (Self::Tx, Self::Rx) {
        asyncband::watch::channel(initial)
    }

    #[inline]
    fn get(rx: &Self::Rx) -> usize {
        rx.get()
    }

    #[inline]
    fn send(tx: &Self::Tx, value: usize) {
        tx.send(value).unwrap();
    }

    #[inline]
    async fn changed(rx: &mut Self::Rx) -> Result<(), ()> {
        rx.changed().await.map_err(|_| ())
    }

    #[inline]
    async fn recv(rx: &mut Self::Rx) -> usize {
        rx.recv().await.unwrap()
    }
}

struct Tokio;

impl WatchPeer for Tokio {
    type Tx = tokio::sync::watch::Sender<usize>;
    type Rx = tokio::sync::watch::Receiver<usize>;

    fn channel(initial: usize) -> (Self::Tx, Self::Rx) {
        tokio::sync::watch::channel(initial)
    }

    #[inline]
    fn get(rx: &Self::Rx) -> usize {
        *rx.borrow()
    }

    #[inline]
    fn send(tx: &Self::Tx, value: usize) {
        tx.send(value).unwrap();
    }

    #[inline]
    async fn changed(rx: &mut Self::Rx) -> Result<(), ()> {
        rx.changed().await.map_err(|_| ())
    }

    #[inline]
    async fn recv(rx: &mut Self::Rx) -> usize {
        rx.changed().await.unwrap();
        *rx.borrow_and_update()
    }
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "watch/get",
        70_000_000,
        implementations!([Asyncband, Tokio], |W, bencher| {
            let (_tx, rx) = W::channel(1usize);
            bencher.bench_local(|| W::get(&rx));
        }),
    );

    runner.compare(
        "watch/send-then-recv",
        14_000_000,
        implementations!([Asyncband, Tokio], |W, bencher| {
            let (tx, mut rx) = W::channel(0usize);
            let mut cx = bench_context();
            let mut next = 1usize;
            bencher.bench_local(|| {
                W::send(&tx, next);
                next = next.wrapping_add(1);
                poll_ready(W::recv(&mut rx), &mut cx)
            });
        }),
    );

    runner.compare(
        "watch/notify-4-waiters",
        4_500_000,
        implementations!([Asyncband, Tokio], |W, bencher| {
            let (tx, r0) = W::channel(0usize);
            let mut receivers = [r0.clone(), r0.clone(), r0.clone(), r0];
            let mut cx = bench_context();
            let mut next = 1usize;
            bencher.bench_local(|| {
                let [r0, r1, r2, r3] = &mut receivers;
                let mut f0 = pin!(W::changed(r0));
                let mut f1 = pin!(W::changed(r1));
                let mut f2 = pin!(W::changed(r2));
                let mut f3 = pin!(W::changed(r3));
                poll_pending(f0.as_mut(), &mut cx);
                poll_pending(f1.as_mut(), &mut cx);
                poll_pending(f2.as_mut(), &mut cx);
                poll_pending(f3.as_mut(), &mut cx);
                W::send(&tx, next);
                next = next.wrapping_add(1);
                poll_pinned_ready(f0.as_mut(), &mut cx).unwrap();
                poll_pinned_ready(f1.as_mut(), &mut cx).unwrap();
                poll_pinned_ready(f2.as_mut(), &mut cx).unwrap();
                poll_pinned_ready(f3.as_mut(), &mut cx).unwrap();
            });
        }),
    );

    runner.compare(
        "watch/tasks-1w4r",
        140,
        implementations!([Asyncband, Tokio], |W, bencher| {
            let runtime = multi_thread(WORKERS);
            let (tx, r0) = W::channel(0usize);
            let remaining = Arc::new(AtomicUsize::new(0));
            let done = Arc::new(tokio::sync::Notify::new());
            let mut handles = Vec::with_capacity(4);
            let receivers = [r0.clone(), r0.clone(), r0.clone(), r0];
            for mut rx in receivers {
                let remaining = remaining.clone();
                let done = done.clone();
                handles.push(runtime.spawn(async move {
                    let mut sum = 0usize;
                    while W::changed(&mut rx).await.is_ok() {
                        sum = sum.wrapping_add(W::get(&rx));
                        if remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
                            done.notify_one();
                        }
                    }
                    sum
                }));
            }
            let mut next = 1usize;
            bencher.bench_local(|| {
                runtime.block_on(async {
                    for _ in 0..TASKS_PUBLICATIONS_PER_ROUND {
                        remaining.store(4, Ordering::Release);
                        W::send(&tx, next);
                        next = next.wrapping_add(1);
                        done.notified().await;
                    }
                });
            });
            drop(tx);
            runtime.block_on(async {
                for handle in handles {
                    black_box(handle.await.unwrap());
                }
            });
        }),
    );

    runner.compare(
        "watch/concurrent-read-write",
        1_200,
        implementations!([Asyncband, Tokio], |W, bencher| {
            let runtime = multi_thread(WORKERS);
            let (tx, rx) = W::channel(0usize);
            let tx = Arc::new(tx);
            bencher.bench_local(|| {
                let writer_tx = Arc::clone(&tx);
                let writer = runtime.spawn(async move {
                    for value in 1..=CONCURRENT_WRITES_PER_ROUND {
                        W::send(&writer_tx, black_box(value));
                    }
                });
                let readers: [_; CONCURRENT_READERS] = array::from_fn(|_| {
                    let rx = rx.clone();
                    runtime.spawn(async move {
                        let mut sum = 0usize;
                        for _ in 0..CONCURRENT_READS_PER_ROUND {
                            sum = sum.wrapping_add(W::get(&rx));
                        }
                        black_box(sum)
                    })
                });
                runtime.block_on(async {
                    writer.await.expect("watch writer panicked");
                    for reader in readers {
                        reader.await.expect("watch reader panicked");
                    }
                });
            });
            drop(runtime);
        }),
    );
}
