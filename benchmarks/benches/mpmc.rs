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
use std::hint::black_box;
use std::pin::pin;

use benchmarks::harness::Runner;
use benchmarks::implementations;
use benchmarks::poll::bench_context;
use benchmarks::poll::poll_pending;
use benchmarks::poll::poll_pinned_ready;
use benchmarks::runtime::BATCH_MESSAGES;
use benchmarks::runtime::BOUNDED_CAPACITY;
use benchmarks::runtime::WORKERS;
use benchmarks::runtime::multi_thread;
use benchmarks::runtime::run_queue_batch;

trait SpmcPeer {
    type BoundedTx<T: Send + 'static>: Send + 'static;
    type BoundedRx<T: Send + 'static>: Send + Sync + Clone + 'static;
    type UnboundedTx<T: Send + 'static>: Send + 'static;
    type UnboundedRx<T: Send + 'static>: Send + Sync + Clone + 'static;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>);

    fn try_send<T: Send + 'static>(tx: &mut Self::BoundedTx<T>, value: T);

    fn try_recv_bounded<T: Send + 'static>(rx: &Self::BoundedRx<T>) -> T;

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>);

    fn send_range_bounded(
        tx: Self::BoundedTx<usize>,
        start: usize,
        end: usize,
    ) -> impl Future<Output = ()> + Send + 'static;

    fn drain_recv_bounded(
        rx: Self::BoundedRx<usize>,
    ) -> impl Future<Output = (usize, usize)> + Send + 'static;

    fn send_range_unbounded(
        tx: Self::UnboundedTx<usize>,
        start: usize,
        end: usize,
    ) -> impl Future<Output = ()> + Send + 'static;

    fn drain_recv_unbounded(
        rx: Self::UnboundedRx<usize>,
    ) -> impl Future<Output = (usize, usize)> + Send + 'static;
}

trait MpmcPeer: SpmcPeer
where
    Self::BoundedTx<usize>: Clone + Sync,
    Self::UnboundedTx<usize>: Clone + Sync,
{
    fn send_bounded(tx: &Self::BoundedTx<usize>, value: usize) -> impl Future<Output = ()> + Send;

    fn send_unbounded(tx: &Self::UnboundedTx<usize>, value: usize);

    fn recv_unbounded(rx: &Self::UnboundedRx<usize>) -> impl Future<Output = usize> + Send;
}

struct AsyncbandMpmc;

impl SpmcPeer for AsyncbandMpmc {
    type BoundedTx<T: Send + 'static> = asyncband::mpmc::BoundedSender<T>;
    type BoundedRx<T: Send + 'static> = asyncband::mpmc::BoundedReceiver<T>;
    type UnboundedTx<T: Send + 'static> = asyncband::mpmc::UnboundedSender<T>;
    type UnboundedRx<T: Send + 'static> = asyncband::mpmc::UnboundedReceiver<T>;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>) {
        asyncband::mpmc::bounded(capacity)
    }

    #[inline]
    fn try_send<T: Send + 'static>(tx: &mut Self::BoundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_bounded<T: Send + 'static>(rx: &Self::BoundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>) {
        asyncband::mpmc::unbounded()
    }

    async fn send_range_bounded(tx: Self::BoundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).await.unwrap();
        }
    }

    async fn drain_recv_bounded(rx: Self::BoundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }

    async fn send_range_unbounded(tx: Self::UnboundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).unwrap();
        }
    }

    async fn drain_recv_unbounded(rx: Self::UnboundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }
}

impl MpmcPeer for AsyncbandMpmc {
    #[inline]
    async fn send_bounded(tx: &Self::BoundedTx<usize>, value: usize) {
        tx.send(value).await.unwrap();
    }

    #[inline]
    fn send_unbounded(tx: &Self::UnboundedTx<usize>, value: usize) {
        tx.send(value).unwrap();
    }

    #[inline]
    async fn recv_unbounded(rx: &Self::UnboundedRx<usize>) -> usize {
        rx.recv().await.unwrap()
    }
}

struct AsyncbandSpmc;

impl SpmcPeer for AsyncbandSpmc {
    type BoundedTx<T: Send + 'static> = asyncband::spmc::BoundedSender<T>;
    type BoundedRx<T: Send + 'static> = asyncband::spmc::BoundedReceiver<T>;
    type UnboundedTx<T: Send + 'static> = asyncband::spmc::UnboundedSender<T>;
    type UnboundedRx<T: Send + 'static> = asyncband::spmc::UnboundedReceiver<T>;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>) {
        asyncband::spmc::bounded(capacity)
    }

    #[inline]
    fn try_send<T: Send + 'static>(tx: &mut Self::BoundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_bounded<T: Send + 'static>(rx: &Self::BoundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>) {
        asyncband::spmc::unbounded()
    }

    async fn send_range_bounded(mut tx: Self::BoundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).await.unwrap();
        }
    }

    async fn drain_recv_bounded(rx: Self::BoundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }

    async fn send_range_unbounded(mut tx: Self::UnboundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).unwrap();
        }
    }

    async fn drain_recv_unbounded(rx: Self::UnboundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }
}

struct AsyncChannel;

impl SpmcPeer for AsyncChannel {
    type BoundedTx<T: Send + 'static> = async_channel::Sender<T>;
    type BoundedRx<T: Send + 'static> = async_channel::Receiver<T>;
    type UnboundedTx<T: Send + 'static> = async_channel::Sender<T>;
    type UnboundedRx<T: Send + 'static> = async_channel::Receiver<T>;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>) {
        async_channel::bounded(capacity)
    }

    #[inline]
    fn try_send<T: Send + 'static>(tx: &mut Self::BoundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_bounded<T: Send + 'static>(rx: &Self::BoundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>) {
        async_channel::unbounded()
    }

    async fn send_range_bounded(tx: Self::BoundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).await.unwrap();
        }
    }

    async fn drain_recv_bounded(rx: Self::BoundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }

    async fn send_range_unbounded(tx: Self::UnboundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.try_send(value).unwrap();
        }
    }

    async fn drain_recv_unbounded(rx: Self::UnboundedRx<usize>) -> (usize, usize) {
        Self::drain_recv_bounded(rx).await
    }
}

impl MpmcPeer for AsyncChannel {
    #[inline]
    async fn send_bounded(tx: &Self::BoundedTx<usize>, value: usize) {
        tx.send(value).await.unwrap();
    }

    #[inline]
    fn send_unbounded(tx: &Self::UnboundedTx<usize>, value: usize) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    async fn recv_unbounded(rx: &Self::UnboundedRx<usize>) -> usize {
        rx.recv().await.unwrap()
    }
}

struct Flume;

impl SpmcPeer for Flume {
    type BoundedTx<T: Send + 'static> = flume::Sender<T>;
    type BoundedRx<T: Send + 'static> = flume::Receiver<T>;
    type UnboundedTx<T: Send + 'static> = flume::Sender<T>;
    type UnboundedRx<T: Send + 'static> = flume::Receiver<T>;

    fn bounded<T: Send + 'static>(capacity: usize) -> (Self::BoundedTx<T>, Self::BoundedRx<T>) {
        flume::bounded(capacity)
    }

    #[inline]
    fn try_send<T: Send + 'static>(tx: &mut Self::BoundedTx<T>, value: T) {
        tx.try_send(value).unwrap();
    }

    #[inline]
    fn try_recv_bounded<T: Send + 'static>(rx: &Self::BoundedRx<T>) -> T {
        rx.try_recv().unwrap()
    }

    fn unbounded<T: Send + 'static>() -> (Self::UnboundedTx<T>, Self::UnboundedRx<T>) {
        flume::unbounded()
    }

    async fn send_range_bounded(tx: Self::BoundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send_async(value).await.unwrap();
        }
    }

    async fn drain_recv_bounded(rx: Self::BoundedRx<usize>) -> (usize, usize) {
        let mut count = 0usize;
        let mut sum = 0usize;
        while let Ok(value) = rx.recv_async().await {
            count += 1;
            sum = sum.wrapping_add(value);
        }
        (count, sum)
    }

    async fn send_range_unbounded(tx: Self::UnboundedTx<usize>, start: usize, end: usize) {
        for value in start..end {
            tx.send(value).unwrap();
        }
    }

    async fn drain_recv_unbounded(rx: Self::UnboundedRx<usize>) -> (usize, usize) {
        Self::drain_recv_bounded(rx).await
    }
}

impl MpmcPeer for Flume {
    #[inline]
    async fn send_bounded(tx: &Self::BoundedTx<usize>, value: usize) {
        tx.send_async(value).await.unwrap();
    }

    #[inline]
    fn send_unbounded(tx: &Self::UnboundedTx<usize>, value: usize) {
        tx.send(value).unwrap();
    }

    #[inline]
    async fn recv_unbounded(rx: &Self::UnboundedRx<usize>) -> usize {
        rx.recv_async().await.unwrap()
    }
}

pub fn register(runner: &mut Runner) {
    runner.compare(
        "mpmc/bounded/try-roundtrip",
        25_000_000,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume], |M, bencher| {
            let (mut tx, rx) = M::bounded::<usize>(BOUNDED_CAPACITY);
            bencher.bench_local(|| {
                M::try_send(&mut tx, black_box(1usize));
                M::try_recv_bounded(&rx)
            });
        }),
    );

    runner.compare(
        "mpmc/bounded/backpressure-handoff",
        8_500_000,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume], |M, bencher| {
            let (mut tx, rx) = M::bounded::<usize>(1);
            M::try_send(&mut tx, 0usize);
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let mut send = pin!(M::send_bounded(&tx, black_box(1usize)));
                poll_pending(send.as_mut(), &mut cx);
                let drained = M::try_recv_bounded(&rx);
                poll_pinned_ready(send.as_mut(), &mut cx);
                drained
            });
        }),
    );

    runner.compare(
        "mpmc/unbounded/wake-receiver",
        10_000_000,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume], |M, bencher| {
            let (tx, rx) = M::unbounded::<usize>();
            let mut cx = bench_context();
            bencher.bench_local(|| {
                let mut recv = pin!(M::recv_unbounded(&rx));
                poll_pending(recv.as_mut(), &mut cx);
                M::send_unbounded(&tx, black_box(1usize));
                poll_pinned_ready(recv.as_mut(), &mut cx)
            });
        }),
    );

    runner.compare(
        "mpmc/bounded/tasks-4p4c-cap1",
        140,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume], |M, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, rx) = M::bounded::<usize>(1);
                let senders = vec![tx.clone(), tx.clone(), tx.clone(), tx];
                let receivers = vec![rx.clone(), rx.clone(), rx.clone(), rx];
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    senders,
                    receivers,
                    M::send_range_bounded,
                    M::drain_recv_bounded,
                );
            });
        }),
    );

    runner.compare(
        "mpmc/bounded/tasks-4p4c-cap64",
        220,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume], |M, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, rx) = M::bounded::<usize>(64);
                let senders = vec![tx.clone(), tx.clone(), tx.clone(), tx];
                let receivers = vec![rx.clone(), rx.clone(), rx.clone(), rx];
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    senders,
                    receivers,
                    M::send_range_bounded,
                    M::drain_recv_bounded,
                );
            });
        }),
    );

    runner.compare(
        "mpmc/unbounded/tasks-4p4c",
        320,
        implementations!([AsyncbandMpmc, AsyncChannel, Flume], |M, bencher| {
            let runtime = multi_thread(WORKERS);
            bencher.bench_local(|| {
                let (tx, rx) = M::unbounded::<usize>();
                let senders = vec![tx.clone(), tx.clone(), tx.clone(), tx];
                let receivers = vec![rx.clone(), rx.clone(), rx.clone(), rx];
                run_queue_batch(
                    &runtime,
                    BATCH_MESSAGES,
                    senders,
                    receivers,
                    M::send_range_unbounded,
                    M::drain_recv_unbounded,
                );
            });
        }),
    );

    runner.compare(
        "spmc/bounded/try-roundtrip",
        28_000_000,
        implementations!(
            [AsyncbandSpmc, AsyncbandMpmc, AsyncChannel, Flume],
            |M, bencher| {
                let (mut tx, rx) = M::bounded::<usize>(BOUNDED_CAPACITY);
                bencher.bench_local(|| {
                    M::try_send(&mut tx, black_box(1usize));
                    M::try_recv_bounded(&rx)
                });
            }
        ),
    );

    runner.compare(
        "spmc/bounded/tasks-1p4c-cap64",
        350,
        implementations!(
            [AsyncbandSpmc, AsyncbandMpmc, AsyncChannel, Flume],
            |M, bencher| {
                let runtime = multi_thread(WORKERS);
                bencher.bench_local(|| {
                    let (tx, rx) = M::bounded::<usize>(64);
                    let receivers = vec![rx.clone(), rx.clone(), rx.clone(), rx];
                    run_queue_batch(
                        &runtime,
                        BATCH_MESSAGES,
                        vec![tx],
                        receivers,
                        M::send_range_bounded,
                        M::drain_recv_bounded,
                    );
                });
            }
        ),
    );

    runner.compare(
        "spmc/unbounded/tasks-1p4c",
        380,
        implementations!(
            [AsyncbandSpmc, AsyncbandMpmc, AsyncChannel, Flume],
            |M, bencher| {
                let runtime = multi_thread(WORKERS);
                bencher.bench_local(|| {
                    let (tx, rx) = M::unbounded::<usize>();
                    let receivers = vec![rx.clone(), rx.clone(), rx.clone(), rx];
                    run_queue_batch(
                        &runtime,
                        BATCH_MESSAGES,
                        vec![tx],
                        receivers,
                        M::send_range_unbounded,
                        M::drain_recv_unbounded,
                    );
                });
            }
        ),
    );
}
